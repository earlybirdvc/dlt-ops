"""Tests for the plugin registry (entry points + decorator) and `plugins doctor`."""

import importlib.metadata
import json
import sys
import types
from pathlib import Path

import pytest
from click.testing import CliRunner

import dlt_ops
from dlt_ops.cli.plugins import plugins as plugins_cli
from dlt_ops.config import load_project_config
from dlt_ops.plugins import (
    AXES,
    FailedPlugin,
    PluginCollisionError,
    UnknownPluginError,
    collisions,
    failures,
    get,
    names,
    register,
    set_disambiguation,
)
from dlt_ops.plugins import registry as registry_mod


@pytest.fixture(autouse=True)
def clean_registry():
    registry_mod._reset_for_tests()
    yield
    registry_mod._reset_for_tests()


def _entry_point(group: str, name: str, value: str, dist_name: str) -> importlib.metadata.EntryPoint:
    ep = importlib.metadata.EntryPoint(name=name, value=value, group=group)
    # Mirrors EntryPoint._for(dist): real scans attach the owning Distribution.
    vars(ep).update(dist=types.SimpleNamespace(name=dist_name))
    return ep


@pytest.fixture
def fake_distributions(monkeypatch: pytest.MonkeyPatch):
    """Patch the registry's entry-point scan; returns an `add(axis, name, value, dist)` hook."""
    eps: list[importlib.metadata.EntryPoint] = []

    def fake_entry_points(*, group: str) -> tuple[importlib.metadata.EntryPoint, ...]:
        return tuple(ep for ep in eps if ep.group == group)

    monkeypatch.setattr(registry_mod, "entry_points", fake_entry_points)

    def add(axis: str, name: str, value: str, dist: str) -> None:
        eps.append(_entry_point(f"dlt_ops.{axis}", name, value, dist))

    return add


@pytest.fixture
def sentinel_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Two real importable modules; their presence in sys.modules records the import."""
    module_names = ("dltx_lazy_probe_dest", "dltx_lazy_probe_sink")
    for module in module_names:
        (tmp_path / f"{module}.py").write_text("class Plugin:\n    pass\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    yield module_names
    for module in module_names:
        sys.modules.pop(module, None)


class TestDiscovery:
    def test_installed_plugin_visible_without_config(self, fake_distributions):
        fake_distributions("destination", "jsondump", "json:dumps", "acme-dest")

        assert names("destination") == ("jsondump",)
        assert get("destination", "jsondump") is json.dumps
        assert registry_mod.source("destination", "jsondump").dist == "acme-dest"

    def test_real_builtin_alert_sink_registered(self):
        """The package's own pyproject entry point proves the loop end-to-end."""
        assert "logging" in names("alert_sink")
        sink_cls = get("alert_sink", "logging")
        assert sink_cls.__name__ == "LoggingAlertSink"
        assert registry_mod.source("alert_sink", "logging").dist == "dlt-ops"

    def test_unknown_plugin_error_lists_registered_names(self, fake_distributions):
        fake_distributions("destination", "jsondump", "json:dumps", "acme-dest")

        with pytest.raises(UnknownPluginError, match="jsondump"):
            get("destination", "nope")

    def test_unknown_axis_rejected(self):
        with pytest.raises(ValueError, match="unknown plugin axis"):
            get("flux_capacitor", "x")
        with pytest.raises(ValueError, match="unknown plugin axis"):
            names("flux_capacitor")
        with pytest.raises(ValueError, match="unknown plugin axis"):
            register("flux_capacitor", "x")(object)


class TestCollisions:
    def test_collision_names_both_distributions_and_exact_toml(self, fake_distributions):
        fake_distributions("destination", "snowflake", "json:dumps", "dist-a")
        fake_distributions("destination", "snowflake", "json:loads", "dist-b")

        with pytest.raises(PluginCollisionError) as excinfo:
            get("destination", "snowflake")
        message = str(excinfo.value)
        assert "dist-a" in message
        assert "dist-b" in message
        assert '[dlt_ops.plugins.destination]\nsnowflake = "dist-a"' in message

    def test_disambiguation_by_distribution_resolves(self, fake_distributions):
        fake_distributions("destination", "snowflake", "json:dumps", "dist-a")
        fake_distributions("destination", "snowflake", "json:loads", "dist-b")

        assert collisions() != ()
        set_disambiguation({"destination": {"snowflake": "dist-b"}})
        assert get("destination", "snowflake") is json.loads
        assert collisions() == ()

    def test_disambiguation_by_qualname_resolves(self, fake_distributions):
        fake_distributions("destination", "snowflake", "json:dumps", "dist-a")
        fake_distributions("destination", "snowflake", "json:loads", "dist-b")

        set_disambiguation({"destination": {"snowflake": "json.dumps"}})
        assert get("destination", "snowflake") is json.dumps

    def test_same_object_reexported_is_not_a_collision(self, fake_distributions):
        fake_distributions("destination", "dup", "json:dumps", "acme-dest")
        fake_distributions("destination", "dup", "json:dumps", "acme-dest")

        assert collisions() == ()
        assert get("destination", "dup") is json.dumps

    def test_disambiguation_rejects_unknown_axis(self):
        with pytest.raises(ValueError, match="unknown plugin axes"):
            set_disambiguation({"flux_capacitor": {"x": "y"}})

    def test_project_config_load_installs_disambiguation(self, fake_distributions, make_project):
        """The [dlt_ops.plugins] table takes effect through load_project_config —
        every CLI verb and runtime path loads the config, so the TOML knob is live."""
        fake_distributions("destination", "snowflake", "json:dumps", "dist-a")
        fake_distributions("destination", "snowflake", "json:loads", "dist-b")
        root = make_project(
            config="""
            [dlt_ops]
            [dlt_ops.plugins.destination]
            snowflake = "dist-b"
            """
        )
        assert collisions() != ()
        load_project_config(root)
        assert get("destination", "snowflake") is json.loads
        assert collisions() == ()


class TestSoftFail:
    def test_import_failure_recorded_and_runtime_continues(self, fake_distributions):
        fake_distributions("destination", "broken", "dltx_missing_module_abc:Thing", "acme-broken")
        fake_distributions("destination", "healthy", "json:dumps", "acme-dest")

        with pytest.raises(ModuleNotFoundError):
            get("destination", "broken")

        # Runtime continues: the failure is a record, not a crash.
        assert get("destination", "healthy") is json.dumps
        assert failures() == (
            FailedPlugin(
                axis="destination",
                name="broken",
                dist="acme-broken",
                error="ModuleNotFoundError: No module named 'dltx_missing_module_abc'",
            ),
        )

        # Repeat lookups re-raise without duplicating the failure record.
        with pytest.raises(ModuleNotFoundError):
            get("destination", "broken")
        assert len(failures()) == 1


class TestRegisterDecorator:
    def test_decorator_feeds_same_registry_and_returns_object(self):
        @register("secret_backend", "memory")
        class MemoryBackend:
            pass

        assert get("secret_backend", "memory") is MemoryBackend
        assert "memory" in names("secret_backend")

    def test_top_level_export_is_registry_register(self):
        assert dlt_ops.register is register

    def test_double_registration_of_same_object_is_idempotent(self):
        class Backend:
            pass

        register("secret_backend", "memory")(Backend)
        register("secret_backend", "memory")(Backend)
        assert collisions() == ()
        assert get("secret_backend", "memory") is Backend


class TestLaziness:
    def test_loading_one_axis_never_imports_other_axes(self, fake_distributions, sentinel_modules):
        dest_module, sink_module = sentinel_modules
        fake_distributions("destination", "probe_dest", f"{dest_module}:Plugin", "acme-dest")
        fake_distributions("alert_sink", "probe_sink", f"{sink_module}:Plugin", "acme-sink")

        # Scans are metadata-only: enumerating an axis imports nothing.
        assert names("alert_sink") == ("probe_sink",)
        assert sink_module not in sys.modules

        get("destination", "probe_dest")
        assert dest_module in sys.modules
        assert sink_module not in sys.modules


class TestDoctor:
    def test_doctor_lists_all_six_axes_when_empty(self, fake_distributions):
        result = CliRunner().invoke(plugins_cli, ["doctor"])
        assert result.exit_code == 0
        for axis in AXES:
            assert f"{axis}: (none)" in result.output
        assert len(AXES) == 6

    def test_doctor_shows_plugin_source_distribution(self, fake_distributions):
        fake_distributions("alert_sink", "test_sink", "json:dumps", "acme-sink")

        result = CliRunner().invoke(plugins_cli, ["doctor"])
        assert result.exit_code == 0
        assert "test_sink" in result.output
        assert "acme-sink" in result.output
        assert "json:dumps" in result.output

    def test_doctor_soft_fail_prints_error_and_exits_1(self, fake_distributions):
        fake_distributions("destination", "broken", "dltx_missing_module_abc:Thing", "acme-broken")

        result = CliRunner().invoke(plugins_cli, ["doctor"])
        assert result.exit_code == 1
        assert "FAILED" in result.output
        assert "dltx_missing_module_abc" in result.output
        # Doctor still reports every axis despite the failure.
        for axis in AXES:
            assert axis in result.output

    def test_doctor_collision_prints_exact_toml_and_exits_1(self, fake_distributions):
        fake_distributions("destination", "snowflake", "json:dumps", "dist-a")
        fake_distributions("destination", "snowflake", "json:loads", "dist-b")

        result = CliRunner().invoke(plugins_cli, ["doctor"])
        assert result.exit_code == 1
        assert "COLLISION" in result.output
        assert "dist-a" in result.output
        assert "dist-b" in result.output
        assert '[dlt_ops.plugins.destination]\nsnowflake = "dist-a"' in result.output

    def test_doctor_real_environment_is_healthy(self):
        result = CliRunner().invoke(plugins_cli, ["doctor"])
        assert result.exit_code == 0
        assert "alert_sink:" in result.output
        assert "logging" in result.output
        assert "dlt-ops" in result.output


def test_plugins_group_registered_on_cli():
    cli_mod = pytest.importorskip("dlt_ops.cli.cli")
    assert "plugins" in cli_mod.cli.commands

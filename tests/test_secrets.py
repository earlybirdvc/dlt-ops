"""Secret-backend axis: Protocol, default backend, resolution, setup_secrets, preflight, validate.

Backend selection is implicit per plugin (no ``secret_backend`` config key in
v0.1): a backend claims a source through the optional ``secret_requests``
hook; no claim = the ``secrets_toml`` default no-op. Registry-facing tests run
against monkeypatched entry points or runtime registrations; preflight tests
inject a duck-typed stub registry.
"""

from __future__ import annotations

import importlib.metadata
import json
import logging
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import dlt
import pytest
from click.testing import CliRunner

import dlt_ops
from dlt_ops import SourceInfo, ValidationContext
from dlt_ops.cli.plugins import plugins as plugins_cli
from dlt_ops.config import ProjectConfig
from dlt_ops.destinations.duckdb import DuckDBAdapter
from dlt_ops.discovery.validators import CORE_RULES
from dlt_ops.discovery.validators.config import validate_secret_backends
from dlt_ops.plugins import register
from dlt_ops.plugins import registry as registry_mod
from dlt_ops.plugins.registry import FailedPlugin, PluginCollisionError
from dlt_ops.preflight import (
    PluginLoadFailedError,
    PluginNotRegisteredError,
    check_secret_backends,
    run_preflight,
)
from dlt_ops.secrets import (
    SecretBackend,
    SecretNotFoundError,
    SecretRequest,
    SecretsTomlBackend,
    setup_secrets,
)
from dlt_ops.secrets.default import DEFAULT_BACKEND_NAME
from dlt_ops.secrets.setup import BackendEngagement, resolve_backend


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


class _SecretsValue:
    """Sentinel whose repr mimics a dlt.secrets.value default parameter."""

    def __repr__(self) -> str:
        return "dlt.secrets.value"


def _secrets_source(api_key: Any = _SecretsValue()) -> None:
    return None


def make_source(name: str, *, source_fn: Any = None) -> SourceInfo:
    return SourceInfo(
        name=name,
        pipeline_name=name.split("_")[0],
        path=Path(f"/proj/{name}"),
        function_name=f"{name}_source",
        resources=("rows",),
        module_stem=name,
        source_fn=source_fn or (lambda: None),
    )


def ext_config(section: str, **ext: Any) -> dict[str, Any]:
    return {"sources": {section: {"dlt_ops": {"schedule": "@daily", **ext}}}}


class MemoryBackend:
    """Fixture backend: claims sources carrying ``memory_var``, serves from a dict."""

    name = "memory"
    store = {"events-key-ref": "s3cr3t"}

    def get(self, key: str) -> str:
        try:
            return self.store[key]
        except KeyError as exc:
            raise SecretNotFoundError(f"no secret {key!r} in memory store") from exc

    def secret_requests(self, ext: dict[str, Any]) -> tuple[SecretRequest, ...]:
        if ext.get("memory_var"):
            return (SecretRequest(ref=ext["memory_var"], key=ext.get("memory_var_key", "api_secret_key")),)
        return ()


class StubRegistry:
    """Duck-typed stand-in for dlt_ops.plugins.registry."""

    def __init__(
        self,
        *,
        names_by_axis: dict[str, tuple[str, ...]] | None = None,
        failures: tuple[FailedPlugin, ...] = (),
        plugins: dict[tuple[str, str], Any] | None = None,
        load_errors: dict[tuple[str, str], Exception] | None = None,
    ) -> None:
        self._names = names_by_axis or {}
        self._failures = failures
        self._plugins = plugins or {}
        self._load_errors = load_errors or {}

    def names(self, axis: str) -> tuple[str, ...]:
        return tuple(self._names.get(axis, ()))

    def failures(self) -> tuple[FailedPlugin, ...]:
        return self._failures

    def get(self, axis: str, name: str) -> Any:
        key = (axis, name)
        if key in self._load_errors:
            raise self._load_errors[key]
        return self._plugins[key]


class TestProtocol:
    def test_default_backend_conforms(self):
        assert isinstance(SecretsTomlBackend(), SecretBackend)

    def test_fixture_backend_conforms(self):
        assert isinstance(MemoryBackend(), SecretBackend)

    def test_object_without_get_does_not_conform(self):
        class Named:
            name = "nope"

        assert not isinstance(Named(), SecretBackend)

    def test_secret_not_found_is_a_lookup_error(self):
        assert issubclass(SecretNotFoundError, LookupError)

    def test_top_level_export(self):
        assert dlt_ops.SecretBackend is SecretBackend
        assert "SecretBackend" in dlt_ops.__all__


class TestDefaultBackend:
    def test_registered_via_entry_points(self):
        assert DEFAULT_BACKEND_NAME in registry_mod.names("secret_backend")
        backend_cls = registry_mod.get("secret_backend", DEFAULT_BACKEND_NAME)
        assert backend_cls is SecretsTomlBackend
        assert registry_mod.source("secret_backend", DEFAULT_BACKEND_NAME).dist == "dlt-ops"

    def test_visible_in_plugins_doctor(self):
        result = CliRunner().invoke(plugins_cli, ["doctor"])
        assert result.exit_code == 0
        assert "secret_backend:" in result.output
        assert DEFAULT_BACKEND_NAME in result.output

    def test_get_passes_through_to_dlt_secrets(self):
        dlt.secrets["sources.dflt_probe_api.api_key"] = "native-value"
        assert SecretsTomlBackend().get("sources.dflt_probe_api.api_key") == "native-value"

    def test_get_missing_raises_typed_error(self):
        with pytest.raises(SecretNotFoundError, match="sources.nope_missing_api.api_key"):
            SecretsTomlBackend().get("sources.nope_missing_api.api_key")

    def test_default_never_claims_a_source(self):
        """Even with backend-ish keys present, only real entry points claim; default = fallback."""
        engagement = resolve_backend("events_api", ext_config("events_api", some_var="ref"))
        assert engagement == BackendEngagement(name=DEFAULT_BACKEND_NAME, requests=())


class TestResolution:
    def test_no_claim_falls_back_to_default(self):
        register("secret_backend", "memory")(MemoryBackend)
        engagement = resolve_backend("events_api", ext_config("events_api"))
        assert engagement == BackendEngagement(name=DEFAULT_BACKEND_NAME, requests=())

    def test_claiming_backend_engages_with_fetch_plan(self):
        register("secret_backend", "memory")(MemoryBackend)
        engagement = resolve_backend("events_api", ext_config("events_api", memory_var="events-key-ref"))
        assert engagement.name == "memory"
        assert engagement.requests == (SecretRequest(ref="events-key-ref", key="api_secret_key"),)

    def test_missing_config_section_resolves_to_default(self):
        engagement = resolve_backend("events_api", {})
        assert engagement == BackendEngagement(name=DEFAULT_BACKEND_NAME, requests=())

    def test_two_claimants_hard_fail(self):
        class OtherBackend(MemoryBackend):
            name = "other"

        register("secret_backend", "memory")(MemoryBackend)
        register("secret_backend", "other")(OtherBackend)
        with pytest.raises(PluginCollisionError, match="'memory'.*'other'"):
            resolve_backend("events_api", ext_config("events_api", memory_var="events-key-ref"))


class TestSetupSecrets:
    def test_entry_point_backend_serves_end_to_end(self, tmp_path, monkeypatch, fake_distributions):
        """A fixture third-party backend registers via entry points and lands in dlt.secrets."""
        module = "dltx_fixture_vault"
        (tmp_path / f"{module}.py").write_text(
            "from dlt_ops.secrets import SecretNotFoundError, SecretRequest\n"
            "\n"
            "class FixtureVault:\n"
            '    name = "fixture_vault"\n'
            '    store = {"orders-key-ref": "t0ps3cret"}\n'
            "\n"
            "    def get(self, key):\n"
            "        try:\n"
            "            return self.store[key]\n"
            "        except KeyError as exc:\n"
            "            raise SecretNotFoundError(key) from exc\n"
            "\n"
            "    def secret_requests(self, ext):\n"
            '        if ext.get("fixture_var"):\n'
            "            return (\n"
            "                SecretRequest(\n"
            '                    ref=ext["fixture_var"],\n'
            '                    key=ext.get("fixture_var_key", "api_secret_key"),\n'
            "                ),\n"
            "            )\n"
            "        return ()\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        fake_distributions("secret_backend", "fixture_vault", f"{module}:FixtureVault", "acme-secrets")
        try:
            setup_secrets(
                sources={"orders_api": make_source("orders_api")},
                config=ext_config("orders_api", fixture_var="orders-key-ref"),
            )
        finally:
            sys.modules.pop(module, None)
        assert dlt.secrets["sources.orders_api.api_secret_key"] == "t0ps3cret"

    def test_custom_secrets_key(self):
        register("secret_backend", "memory")(MemoryBackend)
        setup_secrets(
            sources={"billing_api": make_source("billing_api")},
            config=ext_config("billing_api", memory_var="events-key-ref", memory_var_key="db_password"),
        )
        assert dlt.secrets["sources.billing_api.db_password"] == "s3cr3t"

    def test_unclaimed_source_writes_nothing(self):
        register("secret_backend", "memory")(MemoryBackend)
        setup_secrets(sources={"quiet_api": make_source("quiet_api")}, config=ext_config("quiet_api"))
        with pytest.raises(KeyError):
            dlt.secrets["sources.quiet_api.api_secret_key"]

    def test_missing_secret_raises_by_default(self):
        register("secret_backend", "memory")(MemoryBackend)
        with pytest.raises(SecretNotFoundError, match="absent-ref"):
            setup_secrets(
                sources={"ghost_api": make_source("ghost_api")},
                config=ext_config("ghost_api", memory_var="absent-ref"),
            )

    def test_missing_secret_warns_and_continues_when_not_failing(self, caplog):
        register("secret_backend", "memory")(MemoryBackend)
        sources = {"ghost_api": make_source("ghost_api"), "sound_api": make_source("sound_api")}
        config = {
            "sources": {
                "ghost_api": {"dlt_ops": {"schedule": "@daily", "memory_var": "absent-ref"}},
                "sound_api": {"dlt_ops": {"schedule": "@daily", "memory_var": "events-key-ref"}},
            }
        }
        with caplog.at_level(logging.WARNING, logger="dlt_ops.secrets.setup"):
            setup_secrets(sources=sources, config=config, fail_on_missing=False)
        assert "absent-ref" in caplog.text
        assert dlt.secrets["sources.sound_api.api_secret_key"] == "s3cr3t"
        with pytest.raises(KeyError):
            dlt.secrets["sources.ghost_api.api_secret_key"]

    def test_requires_sources_or_project_root(self):
        with pytest.raises(ValueError, match="sources or project_root"):
            setup_secrets()

    def test_requires_config_or_project_root(self):
        with pytest.raises(ValueError, match="config or project_root"):
            setup_secrets(sources={"events_api": make_source("events_api")})

    def test_discovers_sources_and_config_from_project_root(self, make_project):
        register("secret_backend", "memory")(MemoryBackend)
        root = make_project(
            config="""
                [dlt_ops]
                default_destination = "duckdb"

                [sources.disk_api.dlt_ops]
                schedule = "@daily"
                memory_var = "events-key-ref"
            """,
            files={
                "disk/source/disk_api.py": """
                    import dlt

                    @dlt.resource(name="rows")
                    def rows():
                        yield {"id": 1}

                    @dlt.source(name="disk_api")
                    def disk_api_source():
                        return rows
                    """,
            },
        )
        setup_secrets(project_root=root)
        assert dlt.secrets["sources.disk_api.api_secret_key"] == "s3cr3t"


class TestPreflight:
    def test_engaged_but_unregistered_backend_fails_with_validate_skipped(self):
        """Tier-2 catches the missing chain even when Tier-1 validate never ran."""
        registry = StubRegistry(
            names_by_axis={"destination": ("duckdb",)},
            plugins={("destination", "duckdb"): DuckDBAdapter},
        )
        with pytest.raises(PluginNotRegisteredError, match="secrets_toml.*secret_backend"):
            run_preflight(
                destination="duckdb",
                project_config=ProjectConfig(),
                sources={"events_api": make_source("events_api", source_fn=_secrets_source)},
                raw_config=ext_config("events_api"),
                registry=registry,
            )

    def test_run_preflight_without_sources_skips_the_check(self):
        registry = StubRegistry(
            names_by_axis={"destination": ("duckdb",)},
            plugins={("destination", "duckdb"): DuckDBAdapter},
        )
        run_preflight(destination="duckdb", project_config=ProjectConfig(), registry=registry)

    def test_source_without_secrets_passes_on_empty_registry(self):
        check_secret_backends({"events_api": make_source("events_api")}, {}, registry=StubRegistry())

    def test_uninspected_source_is_conservatively_checked(self):
        phase1_only = SourceInfo(
            name="events_api",
            pipeline_name="events",
            path=Path("/proj/events"),
            function_name="events_api_source",
            resources=("rows",),
            module_stem="events_api",
        )
        with pytest.raises(PluginNotRegisteredError, match="secret_backend"):
            check_secret_backends({"events_api": phase1_only}, {}, registry=StubRegistry())

    def test_healthy_claiming_backend_passes(self):
        registry = StubRegistry(
            names_by_axis={"secret_backend": ("memory",)},
            plugins={("secret_backend", "memory"): MemoryBackend},
        )
        check_secret_backends(
            {"events_api": make_source("events_api")},
            ext_config("events_api", memory_var="events-key-ref"),
            registry=registry,
        )

    def test_registered_default_satisfies_secret_using_source(self):
        registry = StubRegistry(
            names_by_axis={"secret_backend": (DEFAULT_BACKEND_NAME,)},
            plugins={("secret_backend", DEFAULT_BACKEND_NAME): SecretsTomlBackend},
        )
        check_secret_backends(
            {"events_api": make_source("events_api", source_fn=_secrets_source)},
            ext_config("events_api"),
            registry=registry,
        )

    def test_backend_load_error_wrapped_as_typed_error(self):
        registry = StubRegistry(
            names_by_axis={"secret_backend": ("vault",)},
            load_errors={("secret_backend", "vault"): ImportError("no module named hvac")},
        )
        with pytest.raises(PluginLoadFailedError, match="resolution failed.*no module named hvac"):
            check_secret_backends({"events_api": make_source("events_api")}, {}, registry=registry)

    def test_collision_wrapped_as_typed_error(self):
        class OtherBackend(MemoryBackend):
            name = "other"

        registry = StubRegistry(
            names_by_axis={"secret_backend": ("memory", "other")},
            plugins={
                ("secret_backend", "memory"): MemoryBackend,
                ("secret_backend", "other"): OtherBackend,
            },
        )
        with pytest.raises(PluginLoadFailedError, match="resolution failed"):
            check_secret_backends(
                {"events_api": make_source("events_api")},
                ext_config("events_api", memory_var="events-key-ref"),
                registry=registry,
            )

    def test_soft_failed_default_fails(self):
        registry = StubRegistry(
            names_by_axis={"secret_backend": (DEFAULT_BACKEND_NAME,)},
            plugins={("secret_backend", DEFAULT_BACKEND_NAME): SecretsTomlBackend},
            failures=(FailedPlugin(axis="secret_backend", name=DEFAULT_BACKEND_NAME, dist=None, error="boom"),),
        )
        with pytest.raises(PluginLoadFailedError, match="boom"):
            check_secret_backends(
                {"events_api": make_source("events_api", source_fn=_secrets_source)},
                ext_config("events_api"),
                registry=registry,
            )


class TestValidateRule:
    def _ctx(self, sources: dict[str, SourceInfo], config: dict[str, Any]) -> ValidationContext:
        return ValidationContext(sources=sources, config=config, project_root=Path("/proj"))

    def test_rule_registered_in_core_rules(self):
        spec = next(spec for spec in CORE_RULES if spec.rule_id == "secret_backend_registered")
        assert spec.plugin == "core"
        assert spec.default_on is True

    def test_healthy_real_environment_passes(self):
        """The real entry-point registration of secrets_toml closes the chain."""
        ctx = self._ctx(
            {"events_api": make_source("events_api", source_fn=_secrets_source)},
            ext_config("events_api"),
        )
        assert validate_secret_backends(ctx) == []

    def test_unregistered_default_errors_for_secret_using_source(self, fake_distributions):
        ctx = self._ctx(
            {"events_api": make_source("events_api", source_fn=_secrets_source)},
            ext_config("events_api"),
        )
        errors = validate_secret_backends(ctx)
        assert len(errors) == 1
        assert errors[0].field == "secret_backend"
        assert DEFAULT_BACKEND_NAME in errors[0].message
        assert "plugins doctor" in errors[0].message

    def test_source_without_secrets_passes_on_empty_registry(self, fake_distributions):
        ctx = self._ctx({"events_api": make_source("events_api")}, ext_config("events_api"))
        assert validate_secret_backends(ctx) == []

    def test_broken_backend_surfaces_as_error_not_crash(self, fake_distributions):
        fake_distributions("secret_backend", "vault", "dltx_missing_module_xyz:Backend", "acme-vault")
        ctx = self._ctx({"events_api": make_source("events_api")}, ext_config("events_api"))
        errors = validate_secret_backends(ctx)
        assert len(errors) == 1
        assert "resolution failed" in errors[0].message
        assert "dltx_missing_module_xyz" in errors[0].message


class TestSoftFailSurfaces:
    def test_load_failure_recorded_and_doctor_reports(self, fake_distributions):
        """A broken backend soft-fails: names() works, failures() records, doctor flags."""
        fake_distributions("secret_backend", "vault", "dltx_missing_module_xyz:Backend", "acme-vault")

        assert registry_mod.names("secret_backend") == ("vault",)
        with pytest.raises(ModuleNotFoundError):
            registry_mod.get("secret_backend", "vault")
        assert registry_mod.failures() == (
            FailedPlugin(
                axis="secret_backend",
                name="vault",
                dist="acme-vault",
                error="ModuleNotFoundError: No module named 'dltx_missing_module_xyz'",
            ),
        )

        result = CliRunner().invoke(plugins_cli, ["doctor"])
        assert result.exit_code == 1
        assert "FAILED" in result.output
        assert "dltx_missing_module_xyz" in result.output


class TestImportHygiene:
    def test_secrets_core_pulls_no_orchestrator_modules(self):
        """Bare install: the secrets chain (and preflight on top) never imports Airflow."""
        code = (
            "import json, sys\n"
            "import dlt_ops.secrets\n"
            "import dlt_ops.secrets.setup\n"
            "import dlt_ops.preflight\n"
            "print(json.dumps(sorted(sys.modules)))\n"
        )
        proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
        loaded = json.loads(proc.stdout)
        offenders = [module for module in loaded if module == "airflow" or module.startswith("airflow.")]
        assert offenders == []

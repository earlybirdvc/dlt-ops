"""Tests for the `dlt-ops` CLI: top-level --root resolution, the
genericized help surface, required checkpoints options, resolved
destination/dataset wiring on run/clean, the run verb's capability-tier
display, core-mode clean refusals, and the reconcile subcommand.

Uses click.testing.CliRunner against tmp-path project trees (conftest's
make_project). The reconciler and cleanup internals are stubbed via
sys.modules — the CLI imports them lazily inside the command bodies, so the
stubs only need to exist at call time.
"""

from __future__ import annotations

import itertools
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import click
import pytest
from click.testing import CliRunner

from dlt_ops.cli import pipeline as pipeline_cli_mod
from dlt_ops.cli.cli import cli
from dlt_ops.destinations import ADAPTER_GATED_FEATURES, UnregisteredDestinationError

PROJECT_CONFIG = """\
    [dlt_ops]
    default_destination = "duckdb"
    default_dataset = "analytics"

    [sources.github_events.dlt_ops]
    schedule = "@daily"
"""

# `filesystem` resolves in core dlt but has no DestinationAdapter registered,
# so it runs in core mode — the adapter-less lane for capability-tier tests.
CORE_MODE_PROJECT_CONFIG = """\
    [dlt_ops]
    default_destination = "filesystem"
    default_dataset = "analytics"

    [sources.github_events.dlt_ops]
    schedule = "@daily"
"""

GITHUB_EVENTS_SOURCE = """\
    import dlt

    @dlt.resource(name="events")
    def events():
        yield {"id": 1}

    @dlt.source(name="github_events")
    def github_events_source():
        return events
"""


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def project(make_project) -> Path:
    """Project tree with one discoverable source and resolvable defaults."""
    return make_project(config=PROJECT_CONFIG, files={"github/source/github_events.py": GITHUB_EVENTS_SOURCE})


def _result(source_name: str, *, findings: tuple = (), error: str | None = None) -> SimpleNamespace:
    """Duck-typed stand-in for ReconcileResult (real class is unimportable here)."""
    return SimpleNamespace(source_name=source_name, findings=findings, duration_ms=42, error=error)


def _finding(resource: str = "event_payloads") -> SimpleNamespace:
    return SimpleNamespace(
        kind="additive",
        resource_name=resource,
        columns=("surprise_column",),
        first_seen_at=datetime(2026, 7, 7, 12, 0, 0, tzinfo=UTC),
    )


@pytest.fixture
def reconciler_stub(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Install a fake dlt_ops.reconciler module and record calls.

    The reconcile command imports the reconciler lazily inside its body, so a
    sys.modules entry is enough — the real package is never imported.
    """
    mod = ModuleType("dlt_ops.reconciler")
    calls: dict[str, list[dict[str, Any]]] = {"reconcile_source": [], "reconcile_all": [], "detect_removal": []}

    def reconcile_source(source_name: str, *, dry_run: bool = False, project_root: Path | None = None):
        calls["reconcile_source"].append({"source_name": source_name, "dry_run": dry_run, "project_root": project_root})
        return _result(source_name)

    def reconcile_all(*, dry_run: bool = False, project_root: Path | None = None):
        calls["reconcile_all"].append({"dry_run": dry_run, "project_root": project_root})
        return [_result("github_events"), _result("my_api")]

    def detect_removal(source_name: str, *, dry_run: bool = False, project_root: Path | None = None):
        calls["detect_removal"].append({"source_name": source_name, "dry_run": dry_run, "project_root": project_root})
        return _result(source_name)

    mod.reconcile_source = reconcile_source  # type: ignore[attr-defined]
    mod.reconcile_all = reconcile_all  # type: ignore[attr-defined]
    mod.detect_removal = detect_removal  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "dlt_ops.reconciler", mod)
    return SimpleNamespace(module=mod, calls=calls)


@pytest.fixture
def cleanup_stub(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Install a fake dlt_ops.discovery.cleanup module and record calls."""
    mod = ModuleType("dlt_ops.discovery.cleanup")
    calls: dict[str, list[dict[str, Any]]] = {"get_cleanup_plan": [], "clean_pipeline": []}

    def get_cleanup_plan(*, source, resources, local, remote, dataset_name, destination=None):
        calls["get_cleanup_plan"].append(
            {
                "source": source.name,
                "resources": resources,
                "local": local,
                "remote": remote,
                "dataset_name": dataset_name,
                "destination": destination,
            }
        )
        return {
            "pipeline_name": source.pipeline_name,
            "is_full": True,
            "local_exists": False,
            "working_dir": "~/.dlt/pipelines/github_pipeline",
            "data_tables": ["events"],
            "target_resources": ["events"],
            "system_tables": ["_dlt_loads"],
        }

    def clean_pipeline(*, source, resources, local, remote, dataset_name, destination=None):
        calls["clean_pipeline"].append(
            {
                "source": source.name,
                "resources": resources,
                "local": local,
                "remote": remote,
                "dataset_name": dataset_name,
                "destination": destination,
            }
        )
        return {"local": [], "remote": []}

    mod.get_cleanup_plan = get_cleanup_plan  # type: ignore[attr-defined]
    mod.clean_pipeline = clean_pipeline  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "dlt_ops.discovery.cleanup", mod)
    return SimpleNamespace(module=mod, calls=calls)


def _iter_command_paths(cmd: click.Command, path: tuple[str, ...] = ()):
    """Yield the invocation path of every command in the click tree."""
    yield path
    if isinstance(cmd, click.Group):
        for name, sub in cmd.commands.items():
            yield from _iter_command_paths(sub, (*path, name))


def _iter_json_command_paths(cmd: click.Command, path: tuple[str, ...] = ()):
    """Yield the invocation path of every command exposing a ``--json`` flag."""
    if isinstance(cmd, click.Group):
        for name, sub in cmd.commands.items():
            yield from _iter_json_command_paths(sub, (*path, name))
    elif any("--json" in param.opts for param in cmd.params):
        yield path


class TestHelpSurface:
    def test_every_verb_renders_help(self, runner):
        """--help resolves for every command in the tree, groups included."""
        paths = list(_iter_command_paths(cli))
        assert len(paths) >= 11  # cli, pipeline{6}, checkpoints{2}, plugins{1} groups+verbs
        for path in paths:
            result = runner.invoke(cli, [*path, "--help"])
            label = " ".join(path) or "dlt-ops"
            assert result.exit_code == 0, f"`{label} --help` failed: {result.output}"

    def test_pipeline_help_lists_reconcile(self, runner):
        result = runner.invoke(cli, ["pipeline", "--help"])
        assert result.exit_code == 0, result.output
        assert "reconcile" in result.output

    def test_top_level_help_offers_root(self, runner):
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0, result.output
        assert "--root" in result.output
        assert "ingestion" not in result.output

    def test_pipeline_group_has_no_ingestion_dir_option(self, runner):
        result = runner.invoke(cli, ["pipeline", "--help"])
        assert result.exit_code == 0, result.output
        assert "ingestion" not in result.output


class TestJsonOutputIsMachineParseable:
    """``--json`` puts a document on stdout and nothing else.

    The progress indicator writes its label to stdout, so a verb that still
    renders it under ``--json`` emits a line ahead of the document and no
    consumer can parse the result.
    """

    JSON_INVOCATIONS = [
        ("pipeline", "list"),
        ("pipeline", "resources", "-s", "github_events"),
        ("pipeline", "validate"),
        ("pipeline", "validate", "--strict"),
        ("pipeline", "status"),
    ]

    @pytest.mark.parametrize("argv", JSON_INVOCATIONS, ids=" ".join)
    def test_stdout_is_a_json_document(self, runner, project, argv):
        result = runner.invoke(cli, ["--root", str(project), *argv, "--json"])
        json.loads(result.output)  # raises if anything precedes or follows the document

    def test_every_json_verb_is_covered(self):
        """A verb that gains ``--json`` joins JSON_INVOCATIONS or this fails."""
        covered = {
            tuple(itertools.takewhile(lambda token: not token.startswith("-"), argv)) for argv in self.JSON_INVOCATIONS
        }
        assert covered == set(_iter_json_command_paths(cli))


class TestRootResolution:
    def test_root_option_works_from_any_cwd(self, runner, make_project, tmp_path, monkeypatch):
        project = make_project()
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "list"])
        assert result.exit_code == 0, result.output
        assert "No sources found" in result.output

    def test_missing_root_fails_with_init_hint(self, runner, tmp_path, monkeypatch):
        """Root-resolution misses surface config.py's message (init hint), not
        internal-deployment wording."""
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(cli, ["pipeline", "list"])
        assert result.exit_code == 1
        assert "dlt-ops init" in result.output
        assert "config.toml" in result.output
        assert "monorepo" not in result.output.lower()

    def test_explicit_root_that_is_not_a_project_fails_with_init_hint(self, runner, tmp_path):
        empty = tmp_path / "not_a_project"
        empty.mkdir()
        result = runner.invoke(cli, ["--root", str(empty), "pipeline", "list"])
        assert result.exit_code == 1
        assert "dlt-ops init" in result.output


class TestCheckpointsRequirePipeline:
    @pytest.mark.parametrize("verb", ["cleanup", "list"])
    def test_bare_invocation_exits_2_with_missing_option(self, runner, verb):
        result = runner.invoke(cli, ["checkpoints", verb])
        assert result.exit_code == 2
        assert "Missing option" in result.output
        assert "--pipeline" in result.output


class TestRunResolvedDisplay:
    @pytest.fixture
    def run_calls(self, monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
        recorded: list[dict[str, Any]] = []
        monkeypatch.setattr(pipeline_cli_mod, "run_pipeline", lambda **kwargs: recorded.append(kwargs))
        return recorded

    def test_run_shows_resolved_destination_and_dataset(self, runner, project, run_calls):
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "run", "-s", "github_events", "-y"])
        assert result.exit_code == 0, result.output
        assert "duckdb" in result.output
        assert "analytics" in result.output
        assert "Capabilities: full" in result.output
        assert len(run_calls) == 1
        assert run_calls[0]["dataset_name"] == "analytics"

    def test_run_shows_core_capabilities_on_adapterless_destination(self, runner, make_project, run_calls):
        """Core mode is display-only at the run verb: the capabilities line
        names every gated feature (from the canonical list) and the run
        still proceeds."""
        project = make_project(
            config=CORE_MODE_PROJECT_CONFIG, files={"github/source/github_events.py": GITHUB_EVENTS_SOURCE}
        )
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "run", "-s", "github_events", "-y"])
        assert result.exit_code == 0, result.output
        assert "Capabilities: core (no adapter:" in result.output
        assert ", ".join(ADAPTER_GATED_FEATURES) in result.output
        assert len(run_calls) == 1

    def test_run_explicit_dataset_overrides_config_chain(self, runner, project, run_calls):
        result = runner.invoke(
            cli, ["--root", str(project), "pipeline", "run", "-s", "github_events", "-y", "-d", "scratch_ds"]
        )
        assert result.exit_code == 0, result.output
        assert "scratch_ds" in result.output
        assert run_calls[0]["dataset_name"] == "scratch_ds"

    def test_run_unresolved_destination_surfaces_config_error(self, runner, make_project, run_calls):
        project = make_project(files={"github/source/github_events.py": GITHUB_EVENTS_SOURCE})
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "run", "-s", "github_events", "-y"])
        assert result.exit_code == 1
        assert "default_destination" in result.output
        assert run_calls == []


class TestCleanDatasetResolution:
    def test_clean_uses_config_default_dataset(self, runner, project, cleanup_stub):
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "clean", "-s", "github_events", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert cleanup_stub.calls["get_cleanup_plan"][0]["dataset_name"] == "analytics"
        assert cleanup_stub.calls["get_cleanup_plan"][0]["destination"] == "duckdb"
        assert cleanup_stub.calls["clean_pipeline"] == []  # dry-run never executes

    def test_clean_explicit_dataset_wins(self, runner, project, cleanup_stub):
        result = runner.invoke(
            cli,
            ["--root", str(project), "pipeline", "clean", "-s", "github_events", "--dataset", "scratch", "--dry-run"],
        )
        assert result.exit_code == 0, result.output
        assert cleanup_stub.calls["get_cleanup_plan"][0]["dataset_name"] == "scratch"

    def test_clean_without_dataset_or_default_errors(self, runner, make_project, cleanup_stub):
        project = make_project(files={"github/source/github_events.py": GITHUB_EVENTS_SOURCE})
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "clean", "-s", "github_events", "--dry-run"])
        assert result.exit_code == 1
        assert "default_dataset" in result.output
        assert "--dataset" in result.output
        assert cleanup_stub.calls["get_cleanup_plan"] == []

    def test_clean_local_only_needs_no_dataset(self, runner, make_project, cleanup_stub):
        project = make_project(files={"github/source/github_events.py": GITHUB_EVENTS_SOURCE})
        result = runner.invoke(
            cli, ["--root", str(project), "pipeline", "clean", "-s", "github_events", "--local-only", "--dry-run"]
        )
        assert result.exit_code == 0, result.output
        assert cleanup_stub.calls["get_cleanup_plan"][0]["dataset_name"] is None
        assert cleanup_stub.calls["get_cleanup_plan"][0]["destination"] is None


class TestCleanCapabilityTiers:
    """Remote cleanup is adapter-gated; the --local-only path never resolves
    the destination, so core mode cannot break it."""

    @staticmethod
    def _raise_core_mode(**kwargs):
        raise UnregisteredDestinationError("adapter-gated operation in core mode")

    def _core_mode_project(self, make_project) -> Path:
        return make_project(
            config=CORE_MODE_PROJECT_CONFIG, files={"github/source/github_events.py": GITHUB_EVENTS_SOURCE}
        )

    def test_clean_remote_refused_at_plan_with_capability_message(self, runner, make_project, cleanup_stub):
        project = self._core_mode_project(make_project)
        cleanup_stub.module.get_cleanup_plan = self._raise_core_mode
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "clean", "-s", "github_events"])
        assert result.exit_code == 1
        assert "'filesystem'" in result.output
        assert "DestinationAdapter" in result.output
        assert "--local-only" in result.output
        assert cleanup_stub.calls["clean_pipeline"] == []

    def test_clean_remote_refused_at_execute_with_capability_message(self, runner, make_project, cleanup_stub):
        project = self._core_mode_project(make_project)
        cleanup_stub.module.clean_pipeline = self._raise_core_mode
        result = runner.invoke(
            cli, ["--root", str(project), "pipeline", "clean", "-s", "github_events", "--auto-approve"]
        )
        assert result.exit_code == 1
        assert "'filesystem'" in result.output
        assert "DestinationAdapter" in result.output
        assert "--local-only" in result.output

    def test_clean_local_only_works_on_adapterless_destination(self, runner, make_project, cleanup_stub):
        """Regression: --local-only never touches the destination, so it keeps
        working when the configured destination has no adapter."""
        project = self._core_mode_project(make_project)
        result = runner.invoke(
            cli, ["--root", str(project), "pipeline", "clean", "-s", "github_events", "--local-only", "--auto-approve"]
        )
        assert result.exit_code == 0, result.output
        assert "Cleanup complete" in result.output
        assert cleanup_stub.calls["get_cleanup_plan"][0]["destination"] is None
        assert cleanup_stub.calls["clean_pipeline"][0]["destination"] is None


class TestReconcileCommand:
    def test_source_flag_calls_reconcile_source(self, runner, make_project, reconciler_stub):
        project = make_project()
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "reconcile", "--source", "github_events"])
        assert result.exit_code == 0, result.output
        assert reconciler_stub.calls["reconcile_source"] == [
            {"source_name": "github_events", "dry_run": False, "project_root": project}
        ]
        assert reconciler_stub.calls["reconcile_all"] == []

    def test_source_flag_short_alias(self, runner, make_project, reconciler_stub):
        project = make_project()
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "reconcile", "-s", "github_events"])
        assert result.exit_code == 0, result.output
        assert reconciler_stub.calls["reconcile_source"] == [
            {"source_name": "github_events", "dry_run": False, "project_root": project}
        ]

    def test_source_dry_run_passes_dry_run_true(self, runner, make_project, reconciler_stub):
        project = make_project()
        result = runner.invoke(
            cli, ["--root", str(project), "pipeline", "reconcile", "--source", "github_events", "--dry-run"]
        )
        assert result.exit_code == 0, result.output
        assert reconciler_stub.calls["reconcile_source"] == [
            {"source_name": "github_events", "dry_run": True, "project_root": project}
        ]

    def test_all_flag_calls_reconcile_all(self, runner, make_project, reconciler_stub):
        project = make_project()
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "reconcile", "--all"])
        assert result.exit_code == 0, result.output
        assert reconciler_stub.calls["reconcile_all"] == [{"dry_run": False, "project_root": project}]
        assert reconciler_stub.calls["reconcile_source"] == []

    def test_reconcile_all_reports_each_source(self, runner, make_project, reconciler_stub):
        project = make_project()
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "reconcile", "--all"])
        assert result.exit_code == 0, result.output
        assert "github_events" in result.output
        assert "my_api" in result.output

    def test_no_flag_errors_with_usage(self, runner, reconciler_stub):
        result = runner.invoke(cli, ["pipeline", "reconcile"])
        assert result.exit_code == 2
        assert reconciler_stub.calls["reconcile_source"] == []
        assert reconciler_stub.calls["reconcile_all"] == []
        assert "--source" in result.output or "--all" in result.output

    def test_both_flags_errors_with_usage(self, runner, reconciler_stub):
        result = runner.invoke(cli, ["pipeline", "reconcile", "--source", "github_events", "--all"])
        assert result.exit_code == 2
        assert reconciler_stub.calls["reconcile_source"] == []
        assert reconciler_stub.calls["reconcile_all"] == []
        assert "--source" in result.output or "--all" in result.output

    def test_reconciler_error_yields_exit_code_1(self, runner, make_project, reconciler_stub):
        project = make_project()

        def fail_source(source_name: str, *, dry_run: bool = False, project_root: Path | None = None):
            return _result(source_name, error="auth failed")

        reconciler_stub.module.reconcile_source = fail_source
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "reconcile", "-s", "github_events"])
        assert result.exit_code == 1, result.output
        assert "auth failed" in result.output

    def test_findings_do_not_fail_exit_code(self, runner, make_project, reconciler_stub):
        project = make_project()

        def drift_source(source_name: str, *, dry_run: bool = False, project_root: Path | None = None):
            return _result(source_name, findings=(_finding(),))

        reconciler_stub.module.reconcile_source = drift_source
        result = runner.invoke(
            cli, ["--root", str(project), "pipeline", "reconcile", "-s", "github_events", "--dry-run"]
        )
        assert result.exit_code == 0, result.output
        assert "event_payloads" in result.output
        assert "additive" in result.output
        assert "surprise_column" in result.output

    def test_default_never_calls_detect_removal(self, runner, make_project, reconciler_stub):
        project = make_project()
        result = runner.invoke(cli, ["--root", str(project), "pipeline", "reconcile", "--all"])
        assert result.exit_code == 0, result.output
        assert reconciler_stub.calls["detect_removal"] == []

    def test_include_removal_runs_removal_per_source(self, runner, make_project, reconciler_stub):
        project = make_project()
        result = runner.invoke(
            cli, ["--root", str(project), "pipeline", "reconcile", "--all", "--include-removal", "--dry-run"]
        )
        assert result.exit_code == 0, result.output
        assert reconciler_stub.calls["detect_removal"] == [
            {"source_name": "github_events", "dry_run": True, "project_root": project},
            {"source_name": "my_api", "dry_run": True, "project_root": project},
        ]
        assert result.output.count("(removal)") == 2

    def test_include_removal_single_source(self, runner, make_project, reconciler_stub):
        project = make_project()
        result = runner.invoke(
            cli, ["--root", str(project), "pipeline", "reconcile", "-s", "github_events", "--include-removal"]
        )
        assert result.exit_code == 0, result.output
        assert reconciler_stub.calls["detect_removal"] == [
            {"source_name": "github_events", "dry_run": False, "project_root": project}
        ]

    def test_removal_skip_warning_surfaces(self, runner, make_project, reconciler_stub):
        project = make_project()

        def warn_removal(source_name: str, *, dry_run: bool = False, project_root: Path | None = None):
            result = _result(source_name)
            result.warnings = ("removal detection skipped: load_timestamp_column is not set",)
            return result

        reconciler_stub.module.detect_removal = warn_removal
        result = runner.invoke(
            cli, ["--root", str(project), "pipeline", "reconcile", "-s", "github_events", "--include-removal"]
        )
        assert result.exit_code == 0, result.output
        assert "removal detection skipped" in result.output

    def test_removal_error_yields_exit_code_1(self, runner, make_project, reconciler_stub):
        project = make_project()

        def fail_removal(source_name: str, *, dry_run: bool = False, project_root: Path | None = None):
            return _result(source_name, error="coverage query failed")

        reconciler_stub.module.detect_removal = fail_removal
        result = runner.invoke(
            cli, ["--root", str(project), "pipeline", "reconcile", "-s", "github_events", "--include-removal"]
        )
        assert result.exit_code == 1, result.output
        assert "coverage query failed" in result.output

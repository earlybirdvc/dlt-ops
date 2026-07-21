"""`dlt-ops pipeline status`: multi-destination merge, ordering/limit,
--resource filter, --json stability, and graceful no-runs paths.

Uses click.testing.CliRunner against tmp-path project trees; runs execute
against real DuckDB files (credential-free lane). Two sources map to two
DuckDB files (per-pipeline database resolution), so each destination carries
its own `_dlt_ops_runs` and status must merge them.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from dlt_ops.cli.cli import cli
from dlt_ops.discovery.runner import run_pipeline
from dlt_ops.discovery.scanner import discover_sources
from dlt_ops.runs.writer import RUNS_COLUMNS, pipeline_name_for_source

_WORKER_ENV_VARS = ("NORMALIZE__WORKERS", "LOAD__WORKERS", "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS")

PROJECT_CONFIG = """\
    [dlt_ops]
    default_destination = "duckdb"
    default_dataset = "analytics"

    [sources.web_events.dlt_ops]
    schedule = "@daily"

    [sources.orders_api.dlt_ops]
    schedule = "@daily"
    dataset = "orders_raw"
"""

WEB_EVENTS_SOURCE = """\
    import dlt

    @dlt.resource(name="page_views")
    def page_views():
        yield [{"id": 1, "path": "/"}, {"id": 2, "path": "/pricing"}]

    @dlt.source(name="web_events")
    def web_events_source():
        return page_views
"""

ORDERS_SOURCE = """\
    import dlt

    @dlt.resource(name="orders")
    def orders():
        yield [{"id": 1, "total": 10}]

    @dlt.source(name="orders_api")
    def orders_api_source():
        return orders
"""

AUDIT_SOURCE = """\
    import dlt

    @dlt.resource(name="audit_entries")
    def audit_entries():
        yield [{"id": 1, "action": "login"}]

    @dlt.source(name="audit_log")
    def audit_log_source():
        return audit_entries
"""


@pytest.fixture(autouse=True)
def _isolate_run_env(tmp_path, monkeypatch):
    """Keep dlt state + DuckDB files in tmp_path; restore worker env vars."""
    saved = {var: os.environ.get(var) for var in _WORKER_ENV_VARS}
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
    monkeypatch.chdir(tmp_path)
    yield
    for var, value in saved.items():
        if value is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = value


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def project(make_project) -> Path:
    return make_project(
        config=PROJECT_CONFIG,
        files={
            "web/source/web_events.py": WEB_EVENTS_SOURCE,
            "orders/source/orders_api.py": ORDERS_SOURCE,
        },
    )


def _run_source(root: Path, name: str, **kwargs) -> None:
    sources = discover_sources(root)
    run_pipeline(sources[name], project_root=root, **kwargs)


def _status(runner: CliRunner, root: Path, *args: str):
    return runner.invoke(cli, ["--root", str(root), "pipeline", "status", *args])


def _status_json(runner: CliRunner, root: Path, *args: str) -> list[dict]:
    result = _status(runner, root, "--json", *args)
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


def _runs_for(data: list[dict], source: str) -> list[dict]:
    """The `runs` list of one source's entry in the per-source JSON output."""
    return next(entry for entry in data if entry["source"] == source)["runs"]


class TestStatusMerge:
    def test_merges_runs_across_two_duckdb_destinations(self, runner, project, tmp_path):
        _run_source(project, "web_events")
        _run_source(project, "orders_api")
        # Two sources -> two physical DuckDB files, each with its own ledger.
        for name in ("web_events", "orders_api"):
            assert (tmp_path / f"{pipeline_name_for_source(name)}.duckdb").exists()

        result = _status(runner, project)
        assert result.exit_code == 0, result.output
        assert "web_events" in result.output
        assert "orders_api" in result.output
        assert result.output.count("completed") == 2

    def test_json_output_is_stable(self, runner, project):
        _run_source(project, "web_events")
        _run_source(project, "orders_api")

        data = _status_json(runner, project)
        assert [entry["source"] for entry in data] == ["orders_api", "web_events"]
        for entry in data:
            assert list(entry) == ["source", "ledger", "error", "runs"]
            assert entry["ledger"] == "ok"
            assert entry["error"] is None
            (run,) = entry["runs"]
            assert list(run) == list(RUNS_COLUMNS)
            assert run["status"] == "completed"
            assert run["destination"] == "duckdb"
            # ISO-8601 timestamps round-trip.
            assert dt.datetime.fromisoformat(run["started_at"])
            assert dt.datetime.fromisoformat(run["completed_at"])
        assert {entry["runs"][0]["dataset"] for entry in data} == {"analytics", "orders_raw"}


class TestStatusOrderingAndFilters:
    def test_last_n_ordering_and_limit(self, runner, project):
        _run_source(project, "web_events")
        _run_source(project, "web_events")

        web_rows = _runs_for(_status_json(runner, project), "web_events")
        assert len(web_rows) == 2
        # Newest first within a source.
        assert web_rows[0]["started_at"] > web_rows[1]["started_at"]

        limited_web = _runs_for(_status_json(runner, project, "--limit", "1"), "web_events")
        assert len(limited_web) == 1
        assert limited_web[0]["run_id"] == web_rows[0]["run_id"]

    def test_resource_filter(self, runner, project):
        _run_source(project, "web_events", resources=("page_views",))
        _run_source(project, "orders_api")  # source-level run: resource_name NULL

        data = _status_json(runner, project, "--resource", "page_views")
        web_runs = _runs_for(data, "web_events")
        assert [run["resource_name"] for run in web_runs] == ["page_views"]
        # The filtered-out source keeps its entry — table exists, zero matches.
        assert _runs_for(data, "orders_api") == []

        no_match = _status_json(runner, project, "--resource", "nope")
        assert all(entry["runs"] == [] for entry in no_match)


class TestStatusGracefulPaths:
    def test_fresh_project_reports_no_runs(self, runner, project):
        result = _status(runner, project)
        assert result.exit_code == 0, result.output
        assert result.output.count("no runs recorded") == 2
        data = _status_json(runner, project)
        assert [entry["source"] for entry in data] == ["orders_api", "web_events"]
        for entry in data:
            assert entry["ledger"] == "missing"
            assert entry["error"] is None
            assert entry["runs"] == []

    def test_unreadable_ledger_reported_distinctly_not_fatal(self, runner, make_project):
        """An unresolvable / unreachable destination must not read as an empty
        history — the outage is reported per source, exit stays 0."""
        root = make_project(
            config="""\
            [dlt_ops]
            default_destination = "unregistered_wh"
            default_dataset = "analytics"

            [sources.web_events.dlt_ops]
            schedule = "@daily"
            """,
            files={"web/source/web_events.py": WEB_EVENTS_SOURCE},
        )
        result = _status(runner, root)
        assert result.exit_code == 0, result.output
        assert "ledger unreadable" in result.output
        assert "no runs recorded" not in result.output

        (entry,) = _status_json(runner, root)
        assert entry["source"] == "web_events"
        assert entry["ledger"] == "unreadable"
        assert entry["error"]
        assert entry["runs"] == []

    def test_unresolved_destination_reported_as_unreadable(self, runner, make_project):
        """No default_destination and no per-source override: the config error
        lands in the source's ledger entry instead of failing the verb."""
        root = make_project(
            config="""\
            [dlt_ops]
            default_dataset = "analytics"

            [sources.web_events.dlt_ops]
            schedule = "@daily"
            """,
            files={"web/source/web_events.py": WEB_EVENTS_SOURCE},
        )
        result = _status(runner, root)
        assert result.exit_code == 0, result.output
        assert "ledger unreadable" in result.output

        (entry,) = _status_json(runner, root)
        assert entry["ledger"] == "unreadable"
        assert "destination" in entry["error"]

    def test_three_way_absence_states_stay_distinct(self, runner, make_project):
        """missing (never ran), unsupported (core mode — no adapter, so no
        ledger can exist), and unreadable (broken read path) each report
        distinctly in text and JSON: an outage never masquerades as an empty
        history, and a capability gap never masquerades as an outage."""
        root = make_project(
            config="""\
            [dlt_ops]
            default_dataset = "analytics"

            [sources.web_events.dlt_ops]
            schedule = "@daily"
            destination = "duckdb"

            [sources.orders_api.dlt_ops]
            schedule = "@daily"
            destination = "filesystem"

            [sources.audit_log.dlt_ops]
            schedule = "@daily"
            """,
            files={
                "web/source/web_events.py": WEB_EVENTS_SOURCE,
                "orders/source/orders_api.py": ORDERS_SOURCE,
                "audit/source/audit_log.py": AUDIT_SOURCE,
            },
        )
        result = _status(runner, root)
        assert result.exit_code == 0, result.output
        assert "no runs recorded" in result.output
        assert "! ledger unsupported: destination 'filesystem' has no DestinationAdapter (core mode)" in result.output
        assert "! ledger unreadable" in result.output

        data = _status_json(runner, root)
        entries = {entry["source"]: entry for entry in data}
        assert {name: entry["ledger"] for name, entry in entries.items()} == {
            "audit_log": "unreadable",
            "orders_api": "unsupported",
            "web_events": "missing",
        }
        assert entries["web_events"]["error"] is None
        assert entries["orders_api"]["error"] == "destination 'filesystem' has no DestinationAdapter (core mode)"
        assert "destination" in entries["audit_log"]["error"]
        for entry in data:
            assert list(entry) == ["source", "ledger", "error", "runs"]
            assert entry["runs"] == []

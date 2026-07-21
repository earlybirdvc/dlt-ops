"""Run ledger (`_dlt_ops_runs`): writer start/terminal rows, core-mode
skip, runner wiring, fault injection, and the stale_sources Tier-1 rule.

Everything runs against real DuckDB files in tmp_path (credential-free lane).
Ledger assertions read the destination directly via duckdb — the test harness
legitimately bypasses the adapter boundary to verify destination state.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import dlt
import duckdb
import pytest

from dlt_ops.config import UnresolvedDestinationError
from dlt_ops.discovery.models import Schedule, SourceConfig, ValidationContext
from dlt_ops.discovery.runner import run_pipeline
from dlt_ops.discovery.validators import CORE_RULES
from dlt_ops.discovery.validators.staleness import validate_stale_sources
from dlt_ops.preflight import MissingIncrementalCursorError
from dlt_ops.runs.writer import RUNS_COLUMNS, RUNS_TABLE, RunsWriter, pipeline_name_for_source
from tests.test_runner import PROJECT_CONFIG, make_source_info, simple_rows_source

_WORKER_ENV_VARS = ("NORMALIZE__WORKERS", "LOAD__WORKERS", "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS")


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


def _db_file(source_name: str) -> Path:
    return Path.cwd() / f"{pipeline_name_for_source(source_name)}.duckdb"


def _query(source_name: str, sql: str, params: list[Any] | None = None) -> list[Any]:
    with duckdb.connect(str(_db_file(source_name))) as conn:
        return conn.execute(sql, params or []).fetchall()


def _runs_rows(source_name: str, dataset: str = "analytics") -> list[dict[str, Any]]:
    """All ledger rows for a source's destination file, oldest first; [] when absent."""
    if not _db_file(source_name).exists():
        return []
    try:
        rows = _query(
            source_name,
            f"SELECT {', '.join(RUNS_COLUMNS)} FROM {dataset}.{RUNS_TABLE} ORDER BY started_at",
        )
    except duckdb.CatalogException:
        return []
    return [dict(zip(RUNS_COLUMNS, row, strict=True)) for row in rows]


def _age_latest_run(source_name: str, days: int, dataset: str = "analytics") -> None:
    _query(
        source_name,
        f"UPDATE {dataset}.{RUNS_TABLE} SET started_at = ?",
        [dt.datetime.now(dt.UTC) - dt.timedelta(days=days)],
    )


def failing_source():
    @dlt.resource(name="boom")
    def boom():
        raise RuntimeError("upstream exploded")
        yield  # noqa: B901 — makes boom a generator; never reached

    return dlt.source(lambda: boom, name="failing_rows")()


class TestWriterUnit:
    def test_start_row_then_terminal_update(self):
        """write_start inserts status="running"; write_end flips it terminal in place."""
        writer = RunsWriter(destination="duckdb", dataset="analytics", source_section="unit_src")

        writer.write_start()
        (row,) = _runs_rows("unit_src")
        assert row["status"] == "running"
        assert row["started_at"] is not None
        assert row["completed_at"] is None
        assert row["run_id"] == writer.run_id
        assert row["trigger_source"] == "cli"
        assert row["pipeline_name"] == "unit_src_pipeline"

        writer.write_end(status="completed", dlt_run_id="load123", records_extracted=7, records_loaded=5)
        (row,) = _runs_rows("unit_src")
        assert row["status"] == "completed"
        assert row["completed_at"] is not None
        assert row["dlt_run_id"] == "load123"
        assert row["records_extracted"] == 7
        assert row["records_loaded"] == 5

    def test_invalid_trigger_source_raises(self):
        with pytest.raises(ValueError, match="trigger_source"):
            RunsWriter(destination="duckdb", dataset="analytics", source_section="s", trigger_source="cron")

    def test_invalid_terminal_status_logs_not_raises(self, caplog):
        writer = RunsWriter(destination="duckdb", dataset="analytics", source_section="unit_src2")
        writer.write_end(status="running")
        assert "Invalid terminal run status" in caplog.text

    def test_terminal_write_without_start_row_is_loud_not_silent(self, caplog):
        """A terminal UPDATE that matches no start row (start never landed) is a
        silent gap unless caught — it must log loudly at ERROR."""
        # Seed the ledger table (and an unrelated row) so the orphan UPDATE hits an existing table.
        RunsWriter(destination="duckdb", dataset="analytics", source_section="gap_src").write_start()
        orphan = RunsWriter(destination="duckdb", dataset="analytics", source_section="gap_src")

        with caplog.at_level(logging.ERROR):
            orphan.write_end(status="completed")

        loud = [r for r in caplog.records if "matched no start row" in r.getMessage()]
        assert loud and all(r.levelno == logging.ERROR for r in loud)
        # Best-effort outcome preserved: the orphan run left no ledger entry.
        assert all(row["run_id"] != orphan.run_id for row in _runs_rows("gap_src"))

    def test_terminal_write_with_start_row_stays_quiet(self, caplog):
        """The loud gap warning is a true positive only: a normal start+end never trips it."""
        writer = RunsWriter(destination="duckdb", dataset="analytics", source_section="quiet_src")
        writer.write_start()
        with caplog.at_level(logging.ERROR):
            writer.write_end(status="completed")
        assert "matched no start row" not in caplog.text
        (row,) = _runs_rows("quiet_src")
        assert row["status"] == "completed"


class TestWriterCoreMode:
    """No adapter = the ledger has nowhere to live: skip at INFO, never ERROR."""

    def test_writes_skip_at_info_without_touching_the_destination(self, caplog, monkeypatch):
        boundary = MagicMock()
        monkeypatch.setattr("dlt_ops.runs.writer.open_destination_boundary", boundary)
        writer = RunsWriter(destination="filesystem", dataset="analytics", source_section="fs_src")

        with caplog.at_level(logging.INFO):
            writer.write_start()
            writer.write_end(status="completed")

        skips = [r for r in caplog.records if "runs ledger skipped" in r.getMessage()]
        assert len(skips) == 2
        assert all(r.levelno == logging.INFO for r in skips)
        assert "destination 'filesystem' has no DestinationAdapter (core mode)" in skips[0].getMessage()
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        # The skip fires before adapter resolution or client acquisition.
        boundary.assert_not_called()


class TestRunnerLedger:
    def test_completed_run_writes_start_and_terminal_row(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("web_events", simple_rows_source)
        run_pipeline(info, project_root=root)

        (row,) = _runs_rows("web_events")
        assert row["status"] == "completed"
        assert row["source_section"] == "web_events"
        assert row["pipeline_name"] == "web_events_pipeline"
        assert row["destination"] == "duckdb"
        assert row["dataset"] == "analytics"
        assert row["trigger_source"] == "cli"
        assert row["resource_name"] is None
        assert row["backfill_id"] is None
        assert row["error_summary"] is None
        assert len(row["run_id"]) == 32  # generated uuid4 hex
        assert row["started_at"] is not None and row["completed_at"] is not None
        assert row["started_at"] <= row["completed_at"]
        assert row["records_extracted"] == 3
        assert row["records_loaded"] == 3
        # dlt_run_id is the join key into _dlt_loads.
        load_ids = {r[0] for r in _query("web_events", "SELECT load_id FROM analytics._dlt_loads")}
        assert row["dlt_run_id"] in load_ids

    def test_failed_run_writes_failed_terminal_row(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("failing_rows", failing_source)
        with pytest.raises(Exception, match="extract"):
            run_pipeline(info, project_root=root)

        (row,) = _runs_rows("failing_rows")
        assert row["status"] == "failed"
        assert row["completed_at"] is not None
        assert row["dlt_run_id"] is None
        assert row["records_extracted"] is None and row["records_loaded"] is None
        # One-line summary; the full trace stays in logs.
        assert row["error_summary"]
        assert "\n" not in row["error_summary"]

    def test_writer_failure_never_fails_the_run(self, make_project, monkeypatch, caplog):
        """Fault injection: every adapter write raises, the data run still succeeds."""
        from dlt_ops.destinations.duckdb import DuckDBAdapter

        def _boom(self, client, canonical_sql, *params):
            raise RuntimeError("ledger down")

        monkeypatch.setattr(DuckDBAdapter, "execute_sql", _boom)

        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("web_events", simple_rows_source)
        pipeline = run_pipeline(info, project_root=root)

        with pipeline.sql_client() as client:
            with client.execute_query("SELECT COUNT(*) FROM analytics.events") as cursor:
                assert cursor.fetchall()[0][0] == 3
        assert "non-fatal" in caplog.text
        assert _runs_rows("web_events") == []

    def test_backfill_params_pass_through(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("web_events", simple_rows_source)
        run_pipeline(info, project_root=root, run_id="chunk0001", backfill_id="bf123", trigger_source="backfill")

        (row,) = _runs_rows("web_events")
        assert row["run_id"] == "chunk0001"
        assert row["backfill_id"] == "bf123"
        assert row["trigger_source"] == "backfill"

    def test_single_resource_run_stamps_resource_name(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("web_events", simple_rows_source)
        run_pipeline(info, resources=("events",), project_root=root)

        (row,) = _runs_rows("web_events")
        assert row["resource_name"] == "events"

    def test_run_after_local_schema_wipe_still_records_ledger_row(self, make_project, tmp_path, caplog):
        """Selective `clean` deletes the source's local schema file (dlt re-derives
        it on the next run). The ledger start-write precedes extract — before that
        rebuild — so it must not depend on the local schema. Both runs stay visible."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("web_events", simple_rows_source)
        run_pipeline(info, project_root=root)
        assert [row["status"] for row in _runs_rows("web_events")] == ["completed"]

        # Reproduce selective clean's local half: delete the schema file, keep the working dir.
        schema_files = list((tmp_path / "dlt-data" / "pipelines").glob("*/schemas/*.schema.json"))
        assert schema_files, "precondition: a local schema file exists after the first run"
        for path in schema_files:
            path.unlink()

        with caplog.at_level(logging.ERROR):
            run_pipeline(info, project_root=root)
        assert "Failed to write run-start row" not in caplog.text
        # The second run is recorded too — invisible before the fix.
        assert [row["status"] for row in _runs_rows("web_events")] == ["completed", "completed"]


class TestSetupFailuresAreRecorded:
    """A run that dies during setup is the case the ledger exists for.

    The pre-extract `running` row is this ledger's one capability dlt's own
    `_dlt_loads` lacks — `_dlt_loads` is written at complete_load, so it holds
    nothing before or on failure. A run killed by an unresolvable secret never
    reaches extract at all, so it used to exit 1 leaving no row anywhere and
    nothing for `pipeline status` to show.
    """

    def test_source_instantiation_failure_is_recorded(self, make_project):
        """The reported case: dlt raises inside source_fn() resolving secrets,
        before a single resource exists."""

        def unresolvable_secret_source():
            raise RuntimeError("Missing 1 field(s) in configuration: `api_key`")

        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("secret_src", unresolvable_secret_source)

        with pytest.raises(RuntimeError, match="api_key"):
            run_pipeline(info, project_root=root)

        (row,) = _runs_rows("secret_src")
        assert row["status"] == "failed"
        assert row["completed_at"] is not None
        assert "api_key" in row["error_summary"]
        assert row["records_extracted"] is None

    def test_preflight_failure_is_recorded(self, make_project):
        """Preflight runs after instantiation, so it is inside the window too."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("preflight_src", simple_rows_source)
        bounds = (dt.datetime(2024, 2, 1, tzinfo=dt.UTC), dt.datetime(2024, 3, 1, tzinfo=dt.UTC))

        with pytest.raises(MissingIncrementalCursorError):
            run_pipeline(info, project_root=root, bounds=bounds)

        (row,) = _runs_rows("preflight_src")
        assert row["status"] == "failed"
        assert "incremental cursor" in row["error_summary"]

    def test_unknown_resource_exit_does_not_strand_a_running_row(self, make_project):
        """_validate_resources leaves via SystemExit, which `except Exception`
        would miss — stranding a row that reads as a run still in flight."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("exit_src", simple_rows_source)

        with pytest.raises(SystemExit):
            run_pipeline(info, resources=("nope",), project_root=root)

        (row,) = _runs_rows("exit_src")
        assert row["status"] == "failed", "a SystemExit left the run reading as still running"

    def test_no_row_is_written_when_the_destination_never_resolves(self, make_project):
        """The one failure class that genuinely cannot be recorded: the ledger
        lives in the run's destination, so with no destination there is nowhere
        to write. It must still fail loudly — the CLI turns this typed error
        into a red one-line exit."""
        root = make_project(config='[dlt_ops]\ndefault_dataset = "analytics"\n')
        info = make_source_info("no_dest_src", simple_rows_source)

        with pytest.raises(UnresolvedDestinationError, match="default_destination"):
            run_pipeline(info, project_root=root)

        assert _runs_rows("no_dest_src") == []


def _staleness_ctx(root: Path, name: str = "web_events") -> ValidationContext:
    info = make_source_info(name, simple_rows_source, config=SourceConfig(schedule=Schedule.DAILY))
    return ValidationContext(sources={name: info}, config={}, project_root=root)


class TestStaleness:
    def test_registered_as_default_on_core_rule(self):
        spec = next(s for s in CORE_RULES if s.rule_id == "stale_sources")
        assert spec.plugin == "core"
        assert spec.default_on is True

    def test_history_gone_stale_warns(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        run_pipeline(make_source_info("web_events", simple_rows_source), project_root=root)
        _age_latest_run("web_events", days=30)

        findings = validate_stale_sources(_staleness_ctx(root))
        assert len(findings) == 1
        assert findings[0].source_name == "web_events"
        assert findings[0].is_warning is True
        assert "stale" in findings[0].message

    def test_recent_history_is_quiet(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        run_pipeline(make_source_info("web_events", simple_rows_source), project_root=root)
        assert validate_stale_sources(_staleness_ctx(root)) == []

    def test_zero_history_is_skipped_never_fails(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        assert validate_stale_sources(_staleness_ctx(root)) == []

    def test_staleness_days_config_key(self, make_project):
        default_root = make_project(config=PROJECT_CONFIG, name="default_days")
        tuned_root = make_project(config=PROJECT_CONFIG + "staleness_days = 3\n", name="three_days")
        run_pipeline(make_source_info("web_events", simple_rows_source), project_root=default_root)
        _age_latest_run("web_events", days=5)

        # 5 days old: within the default 7-day window, beyond a 3-day one.
        assert validate_stale_sources(_staleness_ctx(default_root)) == []
        findings = validate_stale_sources(_staleness_ctx(tuned_root))
        assert len(findings) == 1
        assert "staleness_days = 3" in findings[0].message

    def test_unregistered_destination_degrades_silently(self, make_project):
        root = make_project(
            config='[dlt_ops]\ndefault_destination = "unregistered_wh"\ndefault_dataset = "analytics"\n'
        )
        assert validate_stale_sources(_staleness_ctx(root)) == []

    def test_unresolved_destination_degrades_silently(self, make_project):
        root = make_project(config="[dlt_ops]\n")
        assert validate_stale_sources(_staleness_ctx(root)) == []

    def test_stale_source_never_blocks_run(self, make_project):
        """Tier-2 preflight excludes stale_sources: a stale ledger never gates `run`."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("web_events", simple_rows_source)
        run_pipeline(info, project_root=root)
        _age_latest_run("web_events", days=30)
        assert validate_stale_sources(_staleness_ctx(root)), "precondition: source is stale"

        run_pipeline(info, project_root=root)  # must not raise
        rows = _runs_rows("web_events")
        assert [row["status"] for row in rows] == ["completed", "completed"]

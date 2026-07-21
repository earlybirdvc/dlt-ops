"""End-to-end acceptance suite: ``examples/basic_project`` driven through the real CLI.

This is the Definition-of-Done executable: a complete
Path-A project exercised through the public surface only — discovery, config
resolution, validators, runner, checkpoints, runs ledger, reconciler, cleanup —
with zero mocks of package code, zero cloud credentials, and zero network.

Convention rule map — where the example demonstrates each KEEP rule:

- Rule 1  (pipeline dir):        ``github_events/`` — plain name, no ``.``/``_``.
- Rule 2  (``source/`` subdir):  ``github_events/source/`` holds both source modules.
- Rule 3  (module stem = section): ``github_events_api.py`` <-> ``[sources.github_events_api]``
                                  (and the ``_full`` sibling likewise).
- Rule 4  (``_source`` suffix):  ``github_events_api_source`` / ``github_events_full_source``.
- Rule 5  (explicit name):       ``@dlt.source(name="github_events_api")`` on both sources.
- Rule 6  (config section):      ``[sources.github_events_api]`` / ``[sources.github_events_full]``
                                  in ``.dlt/config.toml``.
- Rule 7  (schedule):            ``schedule = "@hourly"`` / ``"@daily"`` under each
                                  ``[sources.<X>.dlt_ops]``.
- Rule 9  (no resource overlap): ``events``+``actors`` (api) vs ``event_types`` (full) —
                                  disjoint within the shared pipeline dir.
- Rule 10 (relaxed contract):    ``events``/``event_types`` declare the canonical literal;
                                  ``actors`` deliberately omits it and the run still applies
                                  the canonical contract at runtime (step 4 loads it green).
- Rule 12 (runtime-owned):       no source ever writes ``allow_external_schedulers``;
                                  the runner injects the TimeIntervalContext.
- Rule 14 (Pydantic columns=):   ``columns=Event`` / ``Actor`` / ``EventType`` on every
                                  resource; ``Event.actor_login`` is a nullable column.
- Rule 15 (import safety):       fixture JSONL is read at call time via ``__file__``-relative
                                  paths; ``pipeline validate`` (sandboxed import) passes.

Suite mechanics:

- ``TestExampleProjectEndToEnd`` and ``TestCheckpointResume`` are STAGED suites:
  methods run in definition order against one shared tmp project per class
  (class-scoped env fixture). Running a single step with ``-k`` is unsupported.
- Step 2 goes through a real subprocess (``uv run dlt-ops ...``) to prove
  the console-script entry point; every other step uses CliRunner for speed.
- The socket guard below fails any test that attempts an INET/INET6 connect in
  this process (DuckDB is in-process and needs no sockets). Subprocess steps
  are covered by construction: fixture data is bundled and dlt telemetry is
  disabled via env.
- Destination state is asserted through the DestinationAdapter's own query
  path (``adapter_for_pipeline`` + ``open_client``) and the runs-ledger reader
  ``fetch_runs`` — dog-fooding the package's read surface.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import socket
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import dlt
import duckdb
import pytest
from click.testing import CliRunner

from dlt_ops.cli.cli import cli
from dlt_ops.config import PROJECT_MARKER, RESOURCE_DIR, SOURCE_DIR
from dlt_ops.destinations import adapter_for_pipeline, open_client
from dlt_ops.runs.reader import fetch_runs

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_PROJECT = REPO_ROOT / "examples" / "basic_project"

# Names fixed by the example project (examples/basic_project/.dlt/config.toml
# and github_events/): keep in sync with the checked-in fixtures.
DATASET = "github_events_raw"
API_SOURCE = "github_events_api"
FULL_SOURCE = "github_events_full"
API_PIPELINE = f"{API_SOURCE}_pipeline"
CHECKPOINT_TABLE = "_dlt_custom_checkpoints"
# Fault-injection env var read by the example's FixtureClient
# (github_events/resource/events.py).
FAIL_AFTER_PAGE_ENV = "GITHUB_EVENTS_FAIL_AFTER_PAGE"

# Fixture shape (github_events/data/*.jsonl): 24 event rows total, 4 of them
# before the incremental initial_value (2026-01-01) — so a run loads 20.
# Page size 3 -> 7 pages; checkpoint frequency 2 -> checkpoints at pages 2/4/6.
EVENT_ROWS_IN_WINDOW = 20
EVENT_ROWS_WITH_NULL_ACTOR = 4
ACTOR_ROWS = 5
EVENT_TYPE_ROWS = 4
API_RECORDS_LOADED = EVENT_ROWS_IN_WINDOW + ACTOR_ROWS
CHECKPOINT_PAGES = [2, 4, 6]

# Resume math: failing after page 3 leaves one active checkpoint from page 2
# (rows 4..6 of the window, max occurred_at = 05:00). Resume subtracts the
# 1-second safety offset, so the client re-fetches from the 05:00 boundary row
# (inclusive) -> 15 of the 20 window rows land on the second run.
FAULT_AFTER_PAGE = "3"
FAULT_CHECKPOINT_VALUE = "2026-01-01T05:00:00+00:00"
RESUME_MIN_OCCURRED_AT = datetime(2026, 1, 1, 5, 0, tzinfo=UTC)
WINDOW_MAX_OCCURRED_AT = datetime(2026, 1, 1, 19, 0, tzinfo=UTC)
RESUMED_EVENT_ROWS = 15
RESUME_RECORDS_LOADED = RESUMED_EVENT_ROWS + ACTOR_ROWS

_WORKER_ENV_VARS = ("NORMALIZE__WORKERS", "LOAD__WORKERS", "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS")


@pytest.fixture(autouse=True)
def _no_network(monkeypatch):
    """Fail the test on any INET socket connect — the suite must run fully offline.

    AF_UNIX (local IPC) stays allowed; DuckDB is in-process and never opens a
    socket. dlt's anonymous telemetry is disabled so the guard stays quiet by
    design, not by timeout.
    """
    monkeypatch.setenv("RUNTIME__DLTHUB_TELEMETRY", "false")
    attempts: list[tuple] = []
    real_connect = socket.socket.connect

    def guarded_connect(self, address):
        if self.family in (socket.AF_INET, socket.AF_INET6):
            attempts.append((self.family, address))
            raise RuntimeError(f"network access attempted in the offline E2E suite: {address!r}")
        return real_connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    yield
    assert attempts == [], f"network connect attempted during the offline E2E suite: {attempts}"


def _make_env(tmp_path_factory: pytest.TempPathFactory, label: str):
    """Class-scoped project env: tmp root + isolated DLT_DATA_DIR (manual, no monkeypatch)."""
    base = tmp_path_factory.mktemp(label)
    saved = {var: os.environ.get(var) for var in (*_WORKER_ENV_VARS, "DLT_DATA_DIR")}
    data_dir = base / "dlt-data"
    os.environ["DLT_DATA_DIR"] = str(data_dir)
    env = SimpleNamespace(project=base / "project", data_dir=data_dir)
    try:
        yield env
    finally:
        for var, value in saved.items():
            if value is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = value


@pytest.fixture(scope="class")
def ordered_env(tmp_path_factory):
    yield from _make_env(tmp_path_factory, "e2e-ordered")


@pytest.fixture(scope="class")
def resume_env(tmp_path_factory):
    yield from _make_env(tmp_path_factory, "e2e-resume")


def _invoke(project: Path, *args: str, env: dict[str, str] | None = None):
    """Run the CLI from inside the project dir (root walking + cwd-local DuckDB files)."""
    with contextlib.chdir(project):
        return CliRunner().invoke(cli, list(args), env=env)


def _init_and_copy_example(project: Path) -> None:
    """`init` a fresh root, then copy the example pipeline + config in (step-1 flow)."""
    result = CliRunner().invoke(cli, ["init", str(project)])
    assert result.exit_code == 0, result.output
    shutil.copytree(EXAMPLE_PROJECT / "github_events", project / "github_events")
    shutil.copy(EXAMPLE_PROJECT / PROJECT_MARKER, project / PROJECT_MARKER)


@contextlib.contextmanager
def _adapter_boundary(project: Path, source_name: str):
    """DestinationAdapter + live client for a source's DuckDB — the package's own read path.

    Mirrors the runs-reader/cleanup acquisition pattern: a throwaway pipeline
    (temp working dir) is only the client-acquisition vehicle; cwd must be the
    project so the pipeline-named DuckDB file resolves to the real one.
    """
    with contextlib.chdir(project), tempfile.TemporaryDirectory() as tmp:
        pipeline = dlt.pipeline(
            pipeline_name=f"{source_name}_pipeline",
            destination="duckdb",
            dataset_name=DATASET,
            pipelines_dir=tmp,
        )
        adapter = adapter_for_pipeline(pipeline)
        with open_client(pipeline) as client:
            yield adapter, client


def _query(project: Path, source_name: str, canonical_sql: str, *params):
    with _adapter_boundary(project, source_name) as (adapter, client):
        return adapter.execute_query(client, canonical_sql, *params).fetchall()


def _table_exists(project: Path, source_name: str, table: str) -> bool:
    with _adapter_boundary(project, source_name) as (adapter, client):
        return adapter.table_exists(client, DATASET, table)


def _table_ref(project: Path, source_name: str, table: str) -> str:
    with _adapter_boundary(project, source_name) as (adapter, _client):
        return adapter.render_table_ref(DATASET, table)


def _api_runs(project: Path):
    """Ledger rows for the api source via the package's own reader (dog-food)."""
    with contextlib.chdir(project):
        return fetch_runs(API_PIPELINE, "duckdb", DATASET, source_section=API_SOURCE)


def _checkpoint_rows(project: Path):
    return _query(
        project,
        API_SOURCE,
        f"SELECT resource_name, page_number, status, checkpoint_value "
        f"FROM {_table_ref(project, API_SOURCE, CHECKPOINT_TABLE)} ORDER BY page_number",
    )


class TestExampleValidatesUntouched:
    def test_repo_example_passes_validate_in_place(self, tmp_path, monkeypatch):
        """Acceptance criterion: examples/basic_project passes `pipeline validate` untouched.

        cwd is a tmp dir so read-only verbs that open a destination boundary
        (staleness) create their throwaway DuckDB files there, never inside
        the checked-in example tree.
        """
        monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
        monkeypatch.chdir(tmp_path)
        result = CliRunner().invoke(cli, ["--root", str(EXAMPLE_PROJECT), "pipeline", "validate"])
        assert result.exit_code == 0, result.output
        assert "validated successfully" in result.output


class TestExampleProjectEndToEnd:
    """Steps 1-8 of the quickstart acceptance sequence; ordered, shared tmp project."""

    def test_step1_init_root_then_copy_example_in(self, ordered_env):
        project = ordered_env.project
        _init_and_copy_example(project)

        # init output and the example agree on layout: same marker path, and
        # the example pipeline dir carries the same mandatory subdirs the
        # scaffold produced for its starter pipeline.
        assert (project / PROJECT_MARKER).is_file()
        scaffold_dirs = {p.name for p in (project / "my_pipeline").iterdir() if p.is_dir()}
        example_dirs = {p.name for p in (project / "github_events").iterdir() if p.is_dir()}
        assert {SOURCE_DIR, RESOURCE_DIR} == scaffold_dirs
        assert {SOURCE_DIR, RESOURCE_DIR} <= example_dirs

    def test_step2_validate_exits_zero_via_console_script(self, ordered_env):
        """The one subprocess step: proves the `dlt-ops` console script end to end."""
        proc = subprocess.run(
            ["uv", "run", "--no-sync", "--project", str(REPO_ROOT), "dlt-ops", "pipeline", "validate"],
            cwd=ordered_env.project,
            capture_output=True,
            text=True,
            env={**os.environ, "RUNTIME__DLTHUB_TELEMETRY": "false"},
        )
        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert "validated successfully" in proc.stdout

    def test_step3_list_and_resources_show_expected_rows(self, ordered_env):
        result = _invoke(ordered_env.project, "pipeline", "list")
        assert result.exit_code == 0, result.output
        assert API_SOURCE in result.output
        assert FULL_SOURCE in result.output
        assert "@hourly" in result.output
        assert "@daily" in result.output

        result = _invoke(ordered_env.project, "pipeline", "resources", "-s", API_SOURCE)
        assert result.exit_code == 0, result.output
        assert f"[sources.{API_SOURCE}]" in result.output
        assert "events" in result.output
        assert "actors" in result.output

    def test_step4_run_loads_tables_checkpoints_and_ledger(self, ordered_env):
        project = ordered_env.project
        for source in (FULL_SOURCE, API_SOURCE):
            result = _invoke(project, "pipeline", "run", "-s", source, "-y")
            assert result.exit_code == 0, result.output
        assert (project / f"{API_PIPELINE}.duckdb").is_file()

        # Data tables, asserted through the adapter's own query path. The
        # incremental window boundary shows here: 24 fixture rows, 20 loaded.
        events_ref = _table_ref(project, API_SOURCE, "events")
        rows = _query(
            project,
            API_SOURCE,
            f"SELECT COUNT(*), MIN(occurred_at), MAX(occurred_at), COUNT(DISTINCT loaded_at), "
            f"COUNT(*) - COUNT(actor_login) FROM {events_ref}",
        )
        count, min_ts, max_ts, distinct_stamps, null_actors = rows[0]
        assert count == EVENT_ROWS_IN_WINDOW
        assert min_ts == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
        assert max_ts == WINDOW_MAX_OCCURRED_AT
        assert distinct_stamps == 1  # one load_timestamp_column stamp per run
        assert null_actors == EVENT_ROWS_WITH_NULL_ACTOR  # nullable column made it through

        actors_ref = _table_ref(project, API_SOURCE, "actors")
        assert _query(project, API_SOURCE, f"SELECT COUNT(*) FROM {actors_ref}")[0][0] == ACTOR_ROWS
        types_ref = _table_ref(project, FULL_SOURCE, "event_types")
        assert _query(project, FULL_SOURCE, f"SELECT COUNT(*) FROM {types_ref}")[0][0] == EVENT_TYPE_ROWS

        # Checkpoints: frequency=2 over 7 pages -> pages 2/4/6, all terminal.
        checkpoints = _checkpoint_rows(project)
        assert [(r[0], r[1], r[2]) for r in checkpoints] == [("events", p, "completed") for p in CHECKPOINT_PAGES]
        assert checkpoints[0][3] == FAULT_CHECKPOINT_VALUE  # page-2 cursor value

        # Runs ledger: the start row was written and updated to a terminal row.
        runs = _api_runs(project)
        assert runs is not None and len(runs) == 1
        run = runs[0]
        assert run.status == "completed"
        assert run.trigger_source == "cli"
        assert run.records_loaded == API_RECORDS_LOADED
        assert run.started_at is not None
        assert run.completed_at is not None

    def test_step5_status_shows_the_run(self, ordered_env):
        result = _invoke(ordered_env.project, "pipeline", "status")
        assert result.exit_code == 0, result.output
        assert API_SOURCE in result.output
        assert FULL_SOURCE in result.output
        assert "completed" in result.output
        assert str(API_RECORDS_LOADED) in result.output

    def test_step6_reconcile_clean_then_additive_drift_after_alter(self, ordered_env):
        project = ordered_env.project
        result = _invoke(project, "pipeline", "reconcile", "-s", API_SOURCE, "--dry-run")
        assert result.exit_code == 0, result.output
        assert "No drift" in result.output

        # Mutate the live destination behind the model's back (the test is the
        # hostile engineer here, so a raw connection is the point).
        con = duckdb.connect(str(project / f"{API_PIPELINE}.duckdb"))
        try:
            con.execute(f'ALTER TABLE "{DATASET}"."events" ADD COLUMN unexpected_note VARCHAR')
        finally:
            con.close()

        result = _invoke(project, "pipeline", "reconcile", "-s", API_SOURCE, "--dry-run")
        assert result.exit_code == 0, result.output
        assert "additive drift" in result.output
        assert "unexpected_note" in result.output
        assert "events" in result.output

    def test_step7_selective_clean_drops_one_resource_keeps_the_rest(self, ordered_env):
        project = ordered_env.project
        result = _invoke(project, "pipeline", "clean", "-s", API_SOURCE, "-r", "events", "--auto-approve")
        assert result.exit_code == 0, result.output

        assert _table_exists(project, API_SOURCE, "events") is False
        assert _table_exists(project, API_SOURCE, "actors") is True
        assert _table_exists(project, FULL_SOURCE, "event_types") is True
        checkpoint_ref = _table_ref(project, API_SOURCE, CHECKPOINT_TABLE)
        remaining = _query(
            project, API_SOURCE, f"SELECT COUNT(*) FROM {checkpoint_ref} WHERE resource_name = ?", "events"
        )
        assert remaining[0][0] == 0

        # State surgery, not deletion: the working dir survives and the events
        # entry is gone from the resource state.
        state_path = ordered_env.data_dir / "pipelines" / API_PIPELINE / "state.json"
        assert state_path.is_file()
        state = json.loads(state_path.read_text(encoding="utf-8"))
        assert "events" not in state["sources"][API_SOURCE].get("resources", {})

    def test_step8_full_clean_removes_tables_system_rows_and_local_dir(self, ordered_env):
        project = ordered_env.project
        result = _invoke(project, "pipeline", "clean", "-s", API_SOURCE, "--auto-approve")
        assert result.exit_code == 0, result.output

        assert _table_exists(project, API_SOURCE, "events") is False
        assert _table_exists(project, API_SOURCE, "actors") is False
        for table, column, value in (
            ("_dlt_pipeline_state", "pipeline_name", API_PIPELINE),
            ("_dlt_loads", "schema_name", API_SOURCE),
            (CHECKPOINT_TABLE, "pipeline_name", API_PIPELINE),
        ):
            ref = _table_ref(project, API_SOURCE, table)
            rows = _query(project, API_SOURCE, f"SELECT COUNT(*) FROM {ref} WHERE {column} = ?", value)
            assert rows[0][0] == 0, f"{table} still has rows for this pipeline"
        assert not (ordered_env.data_dir / "pipelines" / API_PIPELINE).exists()
        # The sibling source's data is untouched by the api source's cleanup.
        assert _table_exists(project, FULL_SOURCE, "event_types") is True


class TestCheckpointResume:
    """Fault injection between pages -> re-run resumes from the checkpoint minus offset."""

    @pytest.fixture(scope="class", autouse=True)
    def _project(self, resume_env):
        _init_and_copy_example(resume_env.project)
        return resume_env.project

    def test_faulted_run_fails_but_persists_an_active_checkpoint(self, resume_env):
        project = resume_env.project
        result = _invoke(
            project, "pipeline", "run", "-s", API_SOURCE, "-y", env={FAIL_AFTER_PAGE_ENV: FAULT_AFTER_PAGE}
        )
        assert result.exit_code != 0
        assert "injected API failure" in str(result.exception)

        # Extract died -> nothing was loaded; the checkpoint write is the only
        # destination-side progress record, and it is still active.
        assert _table_exists(project, API_SOURCE, "events") is False
        checkpoints = _checkpoint_rows(project)
        assert [tuple(r) for r in checkpoints] == [("events", 2, "active", FAULT_CHECKPOINT_VALUE)]

        runs = _api_runs(project)
        assert runs is not None and [r.status for r in runs] == ["failed"]
        assert runs[0].error_summary

    def test_rerun_resumes_from_last_checkpoint_minus_offset(self, resume_env):
        project = resume_env.project
        result = _invoke(project, "pipeline", "run", "-s", API_SOURCE, "-y")
        assert result.exit_code == 0, result.output

        events_ref = _table_ref(project, API_SOURCE, "events")
        rows = _query(project, API_SOURCE, f"SELECT COUNT(*), MIN(occurred_at), MAX(occurred_at) FROM {events_ref}")
        count, min_ts, max_ts = rows[0]
        # 15 of the 20 window rows: the run restarted from the checkpointed
        # cursor minus the 1s safety offset, so the 05:00 boundary row was
        # re-fetched (inclusive) and everything before it was skipped.
        assert count == RESUMED_EVENT_ROWS
        assert min_ts == RESUME_MIN_OCCURRED_AT
        assert max_ts == WINDOW_MAX_OCCURRED_AT

        # The failed run's checkpoint and the new run's checkpoints are all terminal.
        checkpoints = _checkpoint_rows(project)
        assert len(checkpoints) >= 2
        assert all(r[2] == "completed" for r in checkpoints)
        assert FAULT_CHECKPOINT_VALUE in {r[3] for r in checkpoints}

        runs = _api_runs(project)
        assert runs is not None
        assert sorted(r.status for r in runs) == ["completed", "failed"]
        completed = next(r for r in runs if r.status == "completed")
        assert completed.records_loaded == RESUME_RECORDS_LOADED

"""Chunked backfill: parsing, chunk math, `[from, to)` boundary
semantics, kill-and-resume, CAS claiming under concurrency, per-chunk runs
ledger, checkpoints interplay, and the validate-side entry enforcement.

Everything runs against real DuckDB files in tmp_path (credential-free lane).
State/ledger assertions read the destination directly via duckdb — the test
harness legitimately bypasses the adapter boundary to verify destination
state.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import os
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import dlt
import duckdb
import pytest
from click.testing import CliRunner

from dlt_ops import with_checkpoints
from dlt_ops.checkpoints.manager import DEFAULT_CHECKPOINT_TABLE
from dlt_ops.cli import backfill as backfill_mod
from dlt_ops.cli.backfill import (
    BackfillChunkError,
    BackfillUsageError,
    compute_chunks,
    execute_backfill,
    parse_chunk_interval,
    parse_utc_timestamp,
)
from dlt_ops.cli.cli import cli
from dlt_ops.discovery import SourceInfo
from dlt_ops.preflight import DestinationCapabilityError
from dlt_ops.runs.backfill_state import (
    BACKFILL_COLUMNS,
    BACKFILLS_TABLE,
    BackfillStateError,
    backfill_id_for,
    chunk_run_id,
    default_claim_token,
    open_backfill_state,
)
from dlt_ops.runs.writer import RUNS_COLUMNS, RUNS_TABLE, pipeline_name_for_source
from tests.test_runner import PROJECT_CONFIG, make_source_info

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


def day(n: int) -> dt.datetime:
    return dt.datetime(2024, 1, n, tzinfo=dt.UTC)


ROWS = [{"id": n, "ts": day(n)} for n in range(1, 7)]


def make_incremental_source(name: str, rows: list[dict[str, Any]]) -> Any:
    """Factory building a fresh incremental source per call (one per chunk run)."""

    def factory():
        @dlt.resource(name="events", write_disposition="append")
        def events(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2020, 1, 1, tzinfo=dt.UTC))):
            yield rows

        return dlt.source(lambda: events, name=name)()

    return factory


def _db_file(source_name: str) -> Path:
    return Path.cwd() / f"{pipeline_name_for_source(source_name)}.duckdb"


def _query(source_name: str, sql: str, params: list[Any] | None = None) -> list[Any]:
    with duckdb.connect(str(_db_file(source_name))) as conn:
        return conn.execute(sql, params or []).fetchall()


def _chunk_rows(source_name: str, dataset: str = "analytics") -> list[dict[str, Any]]:
    rows = _query(
        source_name,
        f"SELECT {', '.join(BACKFILL_COLUMNS)} FROM {dataset}.{BACKFILLS_TABLE} ORDER BY chunk_id",
    )
    return [dict(zip(BACKFILL_COLUMNS, row, strict=True)) for row in rows]


def _runs_rows(source_name: str, dataset: str = "analytics") -> list[dict[str, Any]]:
    rows = _query(
        source_name,
        f"SELECT {', '.join(RUNS_COLUMNS)} FROM {dataset}.{RUNS_TABLE} ORDER BY started_at",
    )
    return [dict(zip(RUNS_COLUMNS, row, strict=True)) for row in rows]


def _event_ids(source_name: str, dataset: str = "analytics") -> list[int]:
    return [row[0] for row in _query(source_name, f"SELECT id FROM {dataset}.events ORDER BY id")]


def _backfill(info: SourceInfo, root: Path, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "project_root": root,
        "window_from": day(1),
        "window_to": day(6),
        "chunk": dt.timedelta(days=1),
        "chunk_label": "1d",
        "echo": lambda _msg: None,
    }
    kwargs.update(overrides)
    return execute_backfill(info, **kwargs)


class TestChunkParsing:
    @pytest.mark.parametrize(
        ("raw", "expected", "label"),
        [
            ("7d", dt.timedelta(days=7), "7d"),
            ("24h", dt.timedelta(hours=24), "24h"),
            ("30m", dt.timedelta(minutes=30), "30m"),
            ("1d", dt.timedelta(days=1), "1d"),
        ],
    )
    def test_simple_forms_accepted(self, raw, expected, label):
        assert parse_chunk_interval(raw) == (expected, label)

    @pytest.mark.parametrize("raw", ["1w", "7", "d", "7 d", "-3d", "1.5h", "", "P1D", "7dd", "h7"])
    def test_everything_else_rejected_loudly(self, raw):
        with pytest.raises(BackfillUsageError, match="<N>d / <N>h / <N>m"):
            parse_chunk_interval(raw)

    def test_zero_chunk_rejected(self):
        with pytest.raises(BackfillUsageError, match="must be > 0"):
            parse_chunk_interval("0d")


class TestBoundsParsing:
    def test_z_suffix_normalized_to_utc(self):
        parsed = parse_utc_timestamp("2024-01-01T00:00:00Z", "--from")
        assert parsed == day(1)
        assert parsed.tzinfo == dt.UTC

    def test_offset_normalized_to_utc(self):
        assert parse_utc_timestamp("2024-01-01T05:00:00+05:00", "--from") == day(1)

    def test_naive_timestamp_rejected(self):
        with pytest.raises(BackfillUsageError, match="timezone-naive"):
            parse_utc_timestamp("2024-01-01T00:00:00", "--from")

    def test_date_only_is_naive_and_rejected(self):
        with pytest.raises(BackfillUsageError, match="timezone-naive"):
            parse_utc_timestamp("2024-01-01", "--to")

    def test_garbage_rejected(self):
        with pytest.raises(BackfillUsageError, match="not a parsable ISO-8601"):
            parse_utc_timestamp("not-a-date", "--from")


class TestChunkMath:
    def test_even_split(self):
        chunks = compute_chunks(day(1), day(6), dt.timedelta(days=1))
        assert chunks == [(day(n), day(n + 1)) for n in range(1, 6)]

    def test_last_chunk_clamped_to_window_end(self):
        chunks = compute_chunks(day(1), day(6), dt.timedelta(days=2))
        assert chunks == [(day(1), day(3)), (day(3), day(5)), (day(5), day(6))]

    def test_window_smaller_than_chunk_is_one_chunk(self):
        assert compute_chunks(day(1), day(2), dt.timedelta(days=7)) == [(day(1), day(2))]

    def test_chunk_run_id_matches_locked_recipe(self):
        expected = hashlib.sha256(f"src|{day(1).isoformat()}|{day(2).isoformat()}".encode()).hexdigest()[:16]
        assert chunk_run_id("src", day(1), day(2)) == expected

    def test_backfill_id_hashes_the_full_triple(self):
        base = backfill_id_for("src", day(1), day(6), "1d")
        assert base == backfill_id_for("src", day(1), day(6), "1d")  # deterministic
        assert base != backfill_id_for("src", day(1), day(6), "2d")
        assert base != backfill_id_for("src", day(1), day(5), "1d")
        assert base != backfill_id_for("other", day(1), day(6), "1d")


class TestBoundarySemantics:
    def test_from_inclusive_to_exclusive(self, make_project):
        """CR1-3 [from, to): the record at exactly `from` IS loaded, at exactly `to` is NOT."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("bf_rows", make_incremental_source("bf_rows", ROWS))

        summary = _backfill(info, root)

        assert (summary.total, summary.completed, summary.skipped, summary.lost) == (5, 5, 0, 0)
        # Rows 1-5 in [day1, day6); row 6 sits exactly at `to` and must be excluded.
        assert _event_ids("bf_rows") == [1, 2, 3, 4, 5]

    def test_state_rows_carry_plan_and_claim_metadata(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("bf_state", make_incremental_source("bf_state", ROWS))

        summary = _backfill(info, root)

        rows = _chunk_rows("bf_state")
        assert [row["chunk_id"] for row in rows] == [f"{n:06d}" for n in range(5)]
        assert {row["status"] for row in rows} == {"completed"}
        assert {row["backfill_id"] for row in rows} == {summary.backfill_id}
        assert {row["claimed_by"] for row in rows} == {default_claim_token()}
        for n, row in enumerate(rows, start=1):
            assert row["chunk_from"].astimezone(dt.UTC) == day(n)
            assert row["chunk_to"].astimezone(dt.UTC) == day(n + 1)
            assert row["backfill_from"].astimezone(dt.UTC) == day(1)
            assert row["backfill_to"].astimezone(dt.UTC) == day(6)
            assert row["chunk_size"] == "1d"
            assert row["run_id"] == chunk_run_id("bf_state", day(n), day(n + 1))
            assert row["records_loaded"] == 1
            assert row["claimed_at"] is not None
            assert row["started_at"] is not None and row["completed_at"] is not None

    def test_each_chunk_writes_its_own_runs_ledger_row(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("bf_ledger", make_incremental_source("bf_ledger", ROWS))

        summary = _backfill(info, root)

        runs = [row for row in _runs_rows("bf_ledger") if row["trigger_source"] == "backfill"]
        assert len(runs) == 5
        assert {row["status"] for row in runs} == {"completed"}
        assert {row["backfill_id"] for row in runs} == {summary.backfill_id}
        assert all(row["records_loaded"] == 1 for row in runs)
        # Join path: _dlt_backfills.run_id <-> _dlt_ops_runs.run_id.
        state_run_ids = {row["run_id"] for row in _chunk_rows("bf_ledger")}
        assert {row["run_id"] for row in runs} == state_run_ids


class TestResume:
    def test_kill_and_resume(self, make_project):
        """Fail chunk 3/5, re-run the same triple: 1-2 skipped, 3 retried, 4-5 run;
        final table state identical to an uninterrupted run."""
        from dlt_ops.discovery.runner import run_pipeline

        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("resume_rows", make_incremental_source("resume_rows", ROWS))

        poison = {day(3)}

        def run_fn(source, **kwargs):
            if kwargs["bounds"][0] in poison:
                raise RuntimeError("chunk killed")
            return run_pipeline(source, **kwargs)

        with pytest.raises(BackfillChunkError, match="chunk 3/5"):
            _backfill(info, root, run_fn=run_fn)

        statuses = {row["chunk_id"]: row["status"] for row in _chunk_rows("resume_rows")}
        assert statuses == {
            "000000": "completed",
            "000001": "completed",
            "000002": "failed",
            "000003": "pending",
            "000004": "pending",
        }
        assert _event_ids("resume_rows") == [1, 2]

        poison.clear()
        summary = _backfill(info, root, run_fn=run_fn)
        assert (summary.completed, summary.skipped, summary.lost) == (3, 2, 0)
        assert {row["status"] for row in _chunk_rows("resume_rows")} == {"completed"}
        assert _event_ids("resume_rows") == [1, 2, 3, 4, 5]

        # Identical to a never-interrupted backfill of the same window.
        straight = make_source_info("straight_rows", make_incremental_source("straight_rows", ROWS))
        _backfill(straight, root)
        assert _event_ids("resume_rows") == _event_ids("straight_rows")

        # Exactly one ledger row per executed chunk, all completed.
        runs = [row for row in _runs_rows("resume_rows") if row["trigger_source"] == "backfill"]
        assert len(runs) == 5
        assert {row["status"] for row in runs} == {"completed"}

    def test_reseeding_is_idempotent(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("reseed_rows", make_incremental_source("reseed_rows", ROWS))
        _backfill(info, root)
        summary = _backfill(info, root)  # same triple again: nothing to do
        assert (summary.completed, summary.skipped) == (0, 5)
        assert len(_chunk_rows("reseed_rows")) == 5  # no duplicate chunk rows

    def test_seed_verifies_the_stored_triple(self):
        """The stored inputs are checked on resume, not just trusted via the hash."""
        chunks = [(day(1), day(2))]
        with open_backfill_state("tamper_rows", "duckdb", "analytics") as state:
            state.ensure_table()
            state.seed_chunks(
                backfill_id="bfid", chunks=chunks, backfill_from=day(1), backfill_to=day(2), chunk_size="1d"
            )
        _query("tamper_rows", f"UPDATE analytics.{BACKFILLS_TABLE} SET chunk_size = '2d'")
        with open_backfill_state("tamper_rows", "duckdb", "analytics") as state:
            with pytest.raises(BackfillStateError, match="different inputs"):
                state.seed_chunks(
                    backfill_id="bfid", chunks=chunks, backfill_from=day(1), backfill_to=day(2), chunk_size="1d"
                )


class TestConcurrency:
    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Windows locks the DuckDB file against the second worker's connection; CAS claiming itself is destination-agnostic.",
    )
    def test_two_workers_execute_every_chunk_exactly_once(self, make_project):
        """Two concurrent invocations of the same backfill coordinate purely via
        CAS claiming: every chunk runs exactly once, the loser moves on silently."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("conc_rows", make_incremental_source("conc_rows", ROWS))

        # Pre-seed so the workers race on claiming, not on DDL.
        chunks = compute_chunks(day(1), day(6), dt.timedelta(days=1))
        backfill_id = backfill_id_for("conc_rows", day(1), day(6), "1d")
        with open_backfill_state("conc_rows", "duckdb", "analytics") as state:
            state.ensure_table()
            state.seed_chunks(
                backfill_id=backfill_id, chunks=chunks, backfill_from=day(1), backfill_to=day(6), chunk_size="1d"
            )

        executed: list[tuple[dt.datetime, int]] = []
        lock = threading.Lock()
        barrier = threading.Barrier(2)
        results: dict[int, Any] = {}
        errors: dict[int, BaseException] = {}

        def make_run_fn(worker: int):
            def run_fn(source, **kwargs):
                with lock:
                    executed.append((kwargs["bounds"][0], worker))
                return SimpleNamespace(last_trace=None)

            return run_fn

        def work(worker: int) -> None:
            barrier.wait()
            try:
                results[worker] = _backfill(info, root, claimed_by=f"worker-{worker}", run_fn=make_run_fn(worker))
            except BaseException as exc:  # noqa: BLE001 — recorded and asserted below
                errors[worker] = exc

        threads = [threading.Thread(target=work, args=(worker,)) for worker in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert not errors, f"CAS loser must be silent, got: {errors}"
        chunk_starts = [start for start, _worker in executed]
        assert sorted(chunk_starts) == [day(n) for n in range(1, 6)], "every chunk exactly once"
        for worker in (1, 2):
            summary = results[worker]
            assert summary.completed + summary.skipped + summary.lost == summary.total == 5
        assert results[1].completed + results[2].completed == 5
        assert {row["status"] for row in _chunk_rows("conc_rows")} == {"completed"}


def make_checkpointed_source(name: str, rows: list[dict[str, Any]]) -> Any:
    def factory():
        @dlt.resource(name="events", write_disposition="append")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def events(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2020, 1, 1, tzinfo=dt.UTC))):
            for row in rows:
                yield [row]

        return dlt.source(lambda: events, name=name)()

    return factory


class TestCheckpointsInterplay:
    def test_per_chunk_checkpoint_isolation(self, make_project):
        """Under TimeIntervalContext injection the incremental's start value
        becomes chunk_from, so @with_checkpoints derives a distinct checkpoint
        namespace per chunk — no cross-chunk bleed. The decorator's hash (of
        the bare start value) and the backfill run_id (of source|from|to) are
        different by construction; the join path is _dlt_backfills.run_id <->
        _dlt_ops_runs.run_id, never the checkpoint run_id."""
        root = make_project(config=PROJECT_CONFIG)
        name = "ckpt_rows"
        info = make_source_info(name, make_checkpointed_source(name, ROWS[:3]))

        summary = _backfill(info, root, window_to=day(3), chunk_label="1d")

        assert summary.completed == 2
        # No bleed, functionally: had chunk 2 resumed from chunk 1's checkpoint
        # namespace (max seen ts = day 3), its incremental start would have been
        # pushed past day 2 and id=2 would be missing.
        assert _event_ids(name) == [1, 2]

        ckpt_run_ids = {
            row[0] for row in _query(name, f"SELECT DISTINCT run_id FROM analytics.{DEFAULT_CHECKPOINT_TABLE}")
        }
        # One namespace per chunk, keyed by the chunk's start value.
        expected = {
            hashlib.sha256(day(1).isoformat().encode()).hexdigest()[:16],
            hashlib.sha256(day(2).isoformat().encode()).hexdigest()[:16],
        }
        assert ckpt_run_ids == expected

        # Decorator hash != backfill run_id by construction (different recipes).
        backfill_run_ids = {row["run_id"] for row in _chunk_rows(name)}
        assert backfill_run_ids == {chunk_run_id(name, day(1), day(2)), chunk_run_id(name, day(2), day(3))}
        assert ckpt_run_ids.isdisjoint(backfill_run_ids)

        # The sanctioned join path holds: state rows <-> ledger rows via run_id.
        ledger_run_ids = {row["run_id"] for row in _runs_rows(name) if row["trigger_source"] == "backfill"}
        assert ledger_run_ids == backfill_run_ids


INCREMENTAL_SOURCE_FILE = """\
    import datetime as dt

    import dlt

    @dlt.resource(name="events")
    def events(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2020, 1, 1, tzinfo=dt.UTC))):
        yield [
            {"id": 1, "ts": dt.datetime(2024, 1, 1, tzinfo=dt.UTC)},
            {"id": 2, "ts": dt.datetime(2024, 1, 2, tzinfo=dt.UTC)},
        ]

    @dlt.source(name="web_events")
    def web_events_source():
        return events
"""

CURSORLESS_SOURCE_FILE = """\
    import dlt

    @dlt.resource(name="events")
    def events():
        yield [{"id": 1}]

    @dlt.source(name="web_events")
    def web_events_source():
        return events
"""

DISK_WRITE_SOURCE_FILE = """\
    from pathlib import Path

    import dlt

    Path(__file__).with_name("canary.txt").write_text("side effect")

    @dlt.resource(name="events")
    def events():
        yield [{"id": 1}]

    @dlt.source(name="web_events")
    def web_events_source():
        return events
"""

# `filesystem` resolves in core dlt but has no DestinationAdapter registered,
# so it runs in core mode — and backfill's chunk state is adapter-gated.
CORE_MODE_PROJECT_CONFIG = """\
    [dlt_ops]
    default_destination = "filesystem"
    default_dataset = "analytics"
"""


def _invoke_backfill(root: Path, *args: str) -> Any:
    argv = ["--root", str(root), "pipeline", "backfill", *args]
    return CliRunner().invoke(cli, argv)


_VALID_WINDOW = ("--from", "2024-01-01T00:00:00Z", "--to", "2024-01-03T00:00:00Z", "--chunk", "1d")


class TestValidateEnforcement:
    """The locked entry rejections, each with a clear error + non-zero exit."""

    def test_unparsable_bounds_rejected(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        result = _invoke_backfill(
            root, "web_events", "--from", "not-a-date", "--to", "2024-01-02T00:00:00Z", "--chunk", "1d"
        )
        assert result.exit_code == 1
        assert "not a parsable ISO-8601" in result.output

    def test_naive_bounds_rejected(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        result = _invoke_backfill(
            root, "web_events", "--from", "2024-01-01T00:00:00", "--to", "2024-01-02T00:00:00Z", "--chunk", "1d"
        )
        assert result.exit_code == 1
        assert "timezone-naive" in result.output

    def test_zero_chunk_rejected(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        result = _invoke_backfill(
            root, "web_events", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-02T00:00:00Z", "--chunk", "0d"
        )
        assert result.exit_code == 1
        assert "must be > 0" in result.output

    def test_non_simple_chunk_form_rejected(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        result = _invoke_backfill(
            root, "web_events", "--from", "2024-01-01T00:00:00Z", "--to", "2024-01-02T00:00:00Z", "--chunk", "1w"
        )
        assert result.exit_code == 1
        assert "<N>d / <N>h / <N>m" in result.output

    def test_inverted_window_rejected(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        result = _invoke_backfill(
            root, "web_events", "--from", "2024-02-01T00:00:00Z", "--to", "2024-01-01T00:00:00Z", "--chunk", "1d"
        )
        assert result.exit_code == 1
        assert "empty window" in result.output

    def test_source_without_incremental_cursor_rejected(self, make_project):
        root = make_project(config=PROJECT_CONFIG, files={"web/source/web_events.py": CURSORLESS_SOURCE_FILE})
        result = _invoke_backfill(root, "web_events", *_VALID_WINDOW)
        assert result.exit_code == 1
        assert "incremental cursor" in result.output

    def test_import_unsafe_source_rejected(self, make_project):
        root = make_project(config=PROJECT_CONFIG, files={"web/source/web_events.py": DISK_WRITE_SOURCE_FILE})
        result = _invoke_backfill(root, "web_events", *_VALID_WINDOW)
        assert result.exit_code == 1
        assert "import safety" in result.output

    def test_import_failed_source_rejected_functionally(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        phase1_only = SourceInfo(
            name="broken",
            pipeline_name="broken",
            path=Path("."),
            function_name="broken_source",
            resources=(),
            module_stem="broken",
            import_error="boom at import",
        )
        with pytest.raises(BackfillUsageError, match="failed to import"):
            _backfill(phase1_only, root)

    def test_unknown_source_rejected(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        result = _invoke_backfill(root, "nope", *_VALID_WINDOW)
        assert result.exit_code == 1
        assert "Unknown source" in result.output

    def test_adapterless_destination_refused_before_chunk_state(self, make_project, monkeypatch):
        """The capability refusal fires in preflight — before chunk math, state
        seeding, or any chunk run: chunk state in _dlt_backfills IS the gated
        feature, so nothing may touch the state table."""
        root = make_project(config=CORE_MODE_PROJECT_CONFIG)
        info = make_source_info("bf_core_mode", make_incremental_source("bf_core_mode", ROWS))
        monkeypatch.setattr(
            backfill_mod,
            "open_backfill_state",
            lambda *args, **kwargs: pytest.fail("chunk state must not be touched on a refused backfill"),
        )
        with pytest.raises(DestinationCapabilityError, match=r"backfill \(chunk state in _dlt_backfills\)"):
            _backfill(info, root, run_fn=lambda *args, **kwargs: pytest.fail("no chunk may run"))

    def test_adapterless_destination_refused_via_cli(self, make_project):
        """The refusal surfaces through the CLI's PreflightError rendering:
        one red Error line naming the destination and the gated feature."""
        root = make_project(
            config=CORE_MODE_PROJECT_CONFIG, files={"web/source/web_events.py": INCREMENTAL_SOURCE_FILE}
        )
        result = _invoke_backfill(root, "web_events", *_VALID_WINDOW)
        assert result.exit_code == 1
        assert "'filesystem'" in result.output
        assert "backfill (chunk state in _dlt_backfills)" in result.output


class TestCliEndToEnd:
    def test_backfill_verb_runs_under_the_pipeline_group(self, make_project):
        root = make_project(config=PROJECT_CONFIG, files={"web/source/web_events.py": INCREMENTAL_SOURCE_FILE})
        result = _invoke_backfill(root, "web_events", *_VALID_WINDOW)
        assert result.exit_code == 0, result.output
        assert "chunk 1/2" in result.output and "chunk 2/2" in result.output
        assert "2 completed" in result.output
        assert _event_ids("web_events") == [1, 2]
        runs = [row for row in _runs_rows("web_events") if row["trigger_source"] == "backfill"]
        assert len(runs) == 2

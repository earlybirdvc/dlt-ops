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
from collections import Counter
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
from dlt_ops.destinations.duckdb import DuckDBAdapter
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


_CLAIM_UPDATE_SQL = "SET status = 'claimed'"
"""Fragment identifying the CAS claim UPDATE inside canonical SQL."""


def _patch_claim_update(monkeypatch: pytest.MonkeyPatch, effect: Any) -> None:
    """Replace the CAS claim UPDATE's execution with `effect`; leave all other SQL real.

    `effect` raises to simulate a transient destination error, or returns None
    to simulate a destination that reports success and changes nothing.
    """
    real = DuckDBAdapter.execute_sql

    def patched(self: Any, client: Any, canonical_sql: str, *params: Any) -> Any:
        if _CLAIM_UPDATE_SQL in canonical_sql:
            return effect()
        return real(self, client, canonical_sql, *params)

    monkeypatch.setattr(DuckDBAdapter, "execute_sql", patched)


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


class TestClaimOutcomes:
    """A CAS claim has three outcomes, and only one of them is a lost race.

    The regression these guard: every claim failure used to be reported as
    "another worker won", which on the single-worker CLI meant a transient DB
    error silently skipped that chunk's time window and still exited 0.
    """

    def _seed_one_chunk(self, source_name: str) -> None:
        with open_backfill_state(source_name, "duckdb", "analytics") as state:
            state.ensure_table()
            state.seed_chunks(
                backfill_id="bfid",
                chunks=[(day(1), day(2))],
                backfill_from=day(1),
                backfill_to=day(2),
                chunk_size="1d",
            )

    def test_foreign_token_on_the_row_is_the_only_lost_race(self):
        self._seed_one_chunk("claim_race")
        with open_backfill_state("claim_race", "duckdb", "analytics") as state:
            assert state.claim("bfid", "000000", claimed_by="worker-a") is True
            # worker-a holds it now; worker-b loses the race, non-fatally.
            assert state.claim("bfid", "000000", claimed_by="worker-b") is False

    def test_missing_chunk_row_raises_instead_of_looking_like_a_lost_race(self):
        with open_backfill_state("claim_missing", "duckdb", "analytics") as state:
            state.ensure_table()
            with pytest.raises(BackfillStateError, match="missing from"):
                state.claim("bfid", "000000", claimed_by="worker-a")

    def test_erroring_claim_update_aborts_and_leaves_the_chunk_visibly_unclaimed(self, make_project, monkeypatch):
        """A rate limit / dropped connection on the CAS is a destination failure:
        nobody holds the chunk, so the invocation must stop rather than move on."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("claim_err", make_incremental_source("claim_err", ROWS))

        def boom() -> Any:
            raise RuntimeError("connection reset by peer")

        _patch_claim_update(monkeypatch, boom)

        with pytest.raises(BackfillStateError, match="never applied"):
            _backfill(info, root, run_fn=lambda *args, **kwargs: pytest.fail("an unclaimed chunk must not run"))

        assert {row["status"] for row in _chunk_rows("claim_err")} == {"pending"}

    def test_silently_ineffective_claim_update_aborts_too(self, make_project, monkeypatch):
        """Same bug with no exception to notice: a destination that reports
        success and changes nothing. The row state is what settles it."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("claim_noop", make_incremental_source("claim_noop", ROWS))
        _patch_claim_update(monkeypatch, lambda: None)

        with pytest.raises(BackfillStateError, match="no error"):
            _backfill(info, root, run_fn=lambda *args, **kwargs: pytest.fail("an unclaimed chunk must not run"))

        assert {row["status"] for row in _chunk_rows("claim_noop")} == {"pending"}


class TestHeldChunkAccounting:
    """`lost` gates the exit code, so it must mean "still not covered" — a gate
    that cries wolf on a healthy concurrent run gets ignored like the old one."""

    def _seed_two_chunks(self, source_name: str, backfill_id: str) -> None:
        with open_backfill_state(source_name, "duckdb", "analytics") as state:
            state.ensure_table()
            state.seed_chunks(
                backfill_id=backfill_id,
                chunks=compute_chunks(day(1), day(3), dt.timedelta(days=1)),
                backfill_from=day(1),
                backfill_to=day(3),
                chunk_size="1d",
            )
            state.claim(backfill_id, "000001", claimed_by="other-host:99")
            state.mark_running(backfill_id, "000001", claimed_by="other-host:99")

    def test_chunk_the_other_worker_finishes_counts_as_covered(self, make_project):
        name = "held_done"
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info(name, make_incremental_source(name, ROWS))
        backfill_id = backfill_id_for(name, day(1), day(3), "1d")
        self._seed_two_chunks(name, backfill_id)

        def run_fn(source, **kwargs):
            # The other worker finishes its chunk while this one runs chunk 1.
            with open_backfill_state(name, "duckdb", "analytics") as other:
                other.mark_completed(backfill_id, "000001", claimed_by="other-host:99", records_loaded=1)
            return SimpleNamespace(last_trace=None)

        summary = _backfill(info, root, window_to=day(3), run_fn=run_fn)

        assert (summary.completed, summary.skipped, summary.lost) == (1, 1, 0)
        assert summary.window_covered

    def test_chunk_still_running_elsewhere_stays_lost(self, make_project):
        name = "held_stuck"
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info(name, make_incremental_source(name, ROWS))
        self._seed_two_chunks(name, backfill_id_for(name, day(1), day(3), "1d"))

        summary = _backfill(info, root, window_to=day(3), run_fn=lambda *a, **k: SimpleNamespace(last_trace=None))

        assert (summary.completed, summary.skipped, summary.lost) == (1, 0, 1)
        assert not summary.window_covered


class TestInterruption:
    def test_keyboard_interrupt_leaves_the_chunk_reclaimable(self, make_project):
        """Ctrl-C skips `except Exception`. A row abandoned in `running` sits
        outside the CAS target set and could never be reclaimed, so the chunk
        must be demoted to `failed` before the interrupt propagates."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("interrupt_rows", make_incremental_source("interrupt_rows", ROWS))

        def run_fn(source, **kwargs):
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            _backfill(info, root, run_fn=run_fn)

        statuses = {row["chunk_id"]: row["status"] for row in _chunk_rows("interrupt_rows")}
        assert statuses["000000"] == "failed", "an interrupted chunk must land in a reclaimable status"

        # And a re-run actually reclaims it rather than reporting it lost.
        summary = _backfill(info, root)
        assert (summary.completed, summary.lost) == (5, 0)
        assert _event_ids("interrupt_rows") == [1, 2, 3, 4, 5]


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

        assert not errors, f"CAS loser must be non-fatal, got: {errors}"
        chunk_starts = [start for start, _worker in executed]
        assert sorted(chunk_starts) == [day(n) for n in range(1, 6)], "every chunk exactly once"

        # The tallies are not decorative: each worker's `completed` count must
        # equal what that worker actually ran. This holds whichever way the race
        # falls (including worker 1 draining every chunk), so it is not flaky —
        # but it fails the moment a claim outcome is miscounted.
        executed_by_worker = Counter(worker for _start, worker in executed)
        for worker in (1, 2):
            summary = results[worker]
            assert summary.completed + summary.skipped + summary.lost == summary.total == 5
            assert executed_by_worker[worker] == summary.completed
        assert results[1].completed + results[2].completed == 5

        rows = _chunk_rows("conc_rows")
        assert {row["status"] for row in rows} == {"completed"}
        # Exactly-once with ownership: the token on the row is the worker that
        # ran the chunk — no chunk is executed by a worker that never held it.
        owner_by_chunk_start = {start: f"worker-{worker}" for start, worker in executed}
        for row in rows:
            assert row["claimed_by"] == owner_by_chunk_start[row["chunk_from"].astimezone(dt.UTC)]


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

    def _seed_cli_window(self, backfill_id: str) -> None:
        """Seed the plan `_VALID_WINDOW` resolves to, so a chunk can be pre-held."""
        with open_backfill_state("web_events", "duckdb", "analytics") as state:
            state.ensure_table()
            state.seed_chunks(
                backfill_id=backfill_id,
                chunks=compute_chunks(day(1), day(3), dt.timedelta(days=1)),
                backfill_from=day(1),
                backfill_to=day(3),
                chunk_size="1d",
            )

    def test_claim_failure_exits_non_zero_and_never_reports_success(self, make_project, monkeypatch):
        """The headline regression: a transient error on the CAS UPDATE used to
        be logged as "another worker won" and the command still exited 0 with a
        green summary, having silently skipped the window."""
        root = make_project(config=PROJECT_CONFIG, files={"web/source/web_events.py": INCREMENTAL_SOURCE_FILE})

        def boom() -> Any:
            raise RuntimeError("429 rate limit exceeded")

        _patch_claim_update(monkeypatch, boom)

        result = _invoke_backfill(root, "web_events", *_VALID_WINDOW)

        assert result.exit_code == 1
        assert "never applied" in result.output
        assert "2 completed" not in result.output
        assert {row["status"] for row in _chunk_rows("web_events")} == {"pending"}

    def test_chunk_held_elsewhere_exits_non_zero_and_is_not_summarized_green(self, make_project):
        """A single-worker invocation that skipped a window must not exit clean,
        even though a chunk another worker holds is not itself a failure."""
        root = make_project(config=PROJECT_CONFIG, files={"web/source/web_events.py": INCREMENTAL_SOURCE_FILE})
        self._seed_cli_window(backfill_id_for("web_events", day(1), day(3), "1d"))
        _query(
            "web_events",
            f"UPDATE analytics.{BACKFILLS_TABLE} SET status = 'running', claimed_by = 'other-host:99' "
            "WHERE chunk_id = '000001'",
        )

        result = _invoke_backfill(root, "web_events", *_VALID_WINDOW)

        assert result.exit_code == 1
        assert "1 completed" in result.output and "1 claimed elsewhere" in result.output
        assert "did not cover the whole window" in result.output
        # The claimable chunk still ran; only the held one was left alone.
        assert _event_ids("web_events") == [1]

    def test_fully_skipped_rerun_still_exits_zero(self, make_project):
        """`skipped` is coverage, not absence: a resume that finds everything
        already completed has covered the window and stays green."""
        root = make_project(config=PROJECT_CONFIG, files={"web/source/web_events.py": INCREMENTAL_SOURCE_FILE})
        assert _invoke_backfill(root, "web_events", *_VALID_WINDOW).exit_code == 0

        result = _invoke_backfill(root, "web_events", *_VALID_WINDOW)

        assert result.exit_code == 0, result.output
        assert "0 completed, 2 skipped" in result.output

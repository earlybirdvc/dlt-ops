"""Backfill state — ``_dlt_backfills`` chunk rows + optimistic claiming (CR1-3).

The state table lives in the SOURCE'S OWN resolved destination + dataset —
the same locality rule as ``_dlt_ops_runs`` — so resumability never
depends on a coordination store the destination itself doesn't provide.
All SQL is canonical (DuckDB dialect), parameterized, and routed through the
DestinationAdapter boundary.

Chunk claiming is the locked optimistic compare-and-swap on ``status``:

.. code-block:: sql

    UPDATE _dlt_backfills
    SET status = 'claimed', claimed_by = ?, claimed_at = NOW()
    WHERE backfill_id = ? AND chunk_id = ? AND status IN ('pending','failed')

The adapter boundary exposes no rows-affected count, so the CAS outcome is
read back from the row itself — the only trustworthy arbiter. After the
UPDATE the worker re-selects the chunk and the row state decides between the
three real outcomes: its own token holds the row (won), another worker's
token holds it (lost — non-fatal, move on), or the row is still inside the
CAS target set (``pending`` / ``failed``), meaning the UPDATE never applied.
The last one is a destination failure, not a race, and it raises: reporting an
unclaimed chunk as "someone else has it" would skip that time window while
exiting clean. No in-process locks; concurrent invocations on the same source
coordinate exclusively via this table.

Unlike the best-effort runs ledger, state writes here are load-bearing
(exactly-once chunk execution) and raise on failure.
"""

from __future__ import annotations

import hashlib
import logging
import os
import socket
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

import attrs

from dlt_ops.destinations import DestinationAdapter, adapter_for_pipeline, open_client, throwaway_pipeline
from dlt_ops.runs.writer import pipeline_name_for_source

logger = logging.getLogger(__name__)

BACKFILLS_TABLE = "_dlt_backfills"
"""Backfill state table name — the single copy other modules import."""

BACKFILL_COLUMNS = (
    "pipeline_name",
    "source_section",
    "resource_name",
    "backfill_id",
    "chunk_id",
    "chunk_from",
    "chunk_to",
    "backfill_from",
    "backfill_to",
    "chunk_size",
    "status",
    "claimed_by",
    "claimed_at",
    "started_at",
    "completed_at",
    "records_loaded",
    "run_id",
)
"""Locked columns in DDL order; readers key rows by these.

``backfill_from`` / ``backfill_to`` / ``chunk_size`` persist the CLI inputs
so resume can verify the ``--from --to --chunk`` triple actually matches the
stored plan instead of trusting the hash alone.
"""


class ChunkStatus(StrEnum):
    """Locked chunk-status vocabulary of ``_dlt_backfills.status``.

    Lifecycle: ``PENDING`` → ``CLAIMED`` → ``RUNNING`` → ``COMPLETED`` /
    ``FAILED``; a resume re-claims ``PENDING`` and ``FAILED`` rows. StrEnum
    members hash and compare as their plain string values, so destination
    round-trips and caller-supplied strings interoperate.
    """

    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


CHUNK_STATUSES = tuple(status.value for status in ChunkStatus)
"""Valid chunk status values in lifecycle order; the closed set is :class:`ChunkStatus`."""


class BackfillStateError(Exception):
    """A load-bearing ``_dlt_backfills`` invariant was violated."""


def backfill_id_for(source_section: str, backfill_from: datetime, backfill_to: datetime, chunk_size: str) -> str:
    """Deterministic backfill id: hash of ``source_section + from_iso + to_iso + chunk_size``.

    The same ``--from --to --chunk`` triple always resolves to the same
    backfill, which is what makes re-running it a resume.
    """
    key = f"{source_section}|{backfill_from.isoformat()}|{backfill_to.isoformat()}|{chunk_size}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def chunk_run_id(source_section: str, chunk_from: datetime, chunk_to: datetime) -> str:
    """Locked CR1-3 recipe: ``sha256(f"{source_section}|{chunk_from_iso}|{chunk_to_iso}")[:16]``.

    Deterministic so a resumed chunk reuses its checkpoint state; also the
    join key between ``_dlt_backfills`` and ``_dlt_ops_runs``.
    """
    key = f"{source_section}|{chunk_from.isoformat()}|{chunk_to.isoformat()}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def chunk_id_for(index: int) -> str:
    """Deterministic per-chunk id: the zero-based window position, zero-padded to sort."""
    return f"{index:06d}"


def default_claim_token() -> str:
    """``host:pid`` worker hint for ``claimed_by`` (FEATURES §Backfill state)."""
    return f"{socket.gethostname()}:{os.getpid()}"


@attrs.frozen
class ChunkRecord:
    """One ``_dlt_backfills`` row (locked columns)."""

    pipeline_name: str
    source_section: str
    resource_name: str | None
    backfill_id: str
    chunk_id: str
    chunk_from: datetime
    chunk_to: datetime
    backfill_from: datetime
    backfill_to: datetime
    chunk_size: str
    status: str
    claimed_by: str | None
    claimed_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    records_loaded: int | None
    run_id: str


def backfills_table_ddl(adapter: DestinationAdapter, dataset: str, table: str = BACKFILLS_TABLE) -> str:
    """One canonical (DuckDB-dialect) backfill-state DDL for every destination.

    Mirrors the runs-table DDL policy: locked column names and order, no
    PARTITION BY / CLUSTER BY (state volume is tiny and those clauses don't
    transpile).
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {adapter.render_table_ref(dataset, table)} ("
        "pipeline_name VARCHAR NOT NULL, "
        "source_section VARCHAR NOT NULL, "
        "resource_name VARCHAR, "
        "backfill_id VARCHAR NOT NULL, "
        "chunk_id VARCHAR NOT NULL, "
        "chunk_from TIMESTAMPTZ NOT NULL, "
        "chunk_to TIMESTAMPTZ NOT NULL, "
        "backfill_from TIMESTAMPTZ NOT NULL, "
        "backfill_to TIMESTAMPTZ NOT NULL, "
        "chunk_size VARCHAR NOT NULL, "
        "status VARCHAR NOT NULL, "
        "claimed_by VARCHAR, "
        "claimed_at TIMESTAMPTZ, "
        "started_at TIMESTAMPTZ, "
        "completed_at TIMESTAMPTZ, "
        "records_loaded BIGINT, "
        "run_id VARCHAR NOT NULL)"
    )


def _as_utc(value: datetime) -> datetime:
    """Normalize a destination-returned timestamp to UTC for comparisons."""
    return value.astimezone(UTC)


class BackfillState:
    """``_dlt_backfills`` accessor bound to one (pipeline, destination, dataset).

    The pipeline is only the client-acquisition vehicle (mirrors RunsWriter);
    construction is pure, every method acquires a client per call so no
    connection is held open across long-running chunk executions.
    """

    def __init__(
        self,
        pipeline: Any,
        *,
        destination: str,
        dataset: str,
        source_section: str,
        resource_name: str | None = None,
    ) -> None:
        self._pipeline = pipeline
        self.destination = destination
        self.dataset = dataset
        self.source_section = source_section
        self.resource_name = resource_name

    def _boundary(self) -> tuple[DestinationAdapter, str]:
        adapter = adapter_for_pipeline(self._pipeline)
        return adapter, adapter.render_table_ref(self.dataset, BACKFILLS_TABLE)

    def ensure_table(self) -> None:
        """Lazily create the state table (and its schema where supported)."""
        adapter, _ = self._boundary()
        with open_client(self._pipeline) as client:
            adapter.ensure_schema(client, self.dataset)
            adapter.execute_sql(client, backfills_table_ddl(adapter, self.dataset))

    def fetch_chunks(self, backfill_id: str) -> list[ChunkRecord]:
        """All chunk rows of one backfill, ordered by chunk position."""
        adapter, table_ref = self._boundary()
        query = f"SELECT {', '.join(BACKFILL_COLUMNS)} FROM {table_ref} WHERE backfill_id = ? ORDER BY chunk_id"
        with open_client(self._pipeline) as client:
            cursor = adapter.execute_query(client, query, backfill_id)
        return [ChunkRecord(**dict(zip(BACKFILL_COLUMNS, row, strict=True))) for row in cursor.fetchall()]

    def seed_chunks(
        self,
        *,
        backfill_id: str,
        chunks: Sequence[tuple[datetime, datetime]],
        backfill_from: datetime,
        backfill_to: datetime,
        chunk_size: str,
    ) -> None:
        """Idempotently insert the chunk plan: missing chunks only, as ``pending``.

        Existing rows first verify the stored ``--from --to --chunk`` triple
        against the caller's inputs — the hash already encodes the triple, but
        the stored columns make the resume contract checkable, not assumed.

        Raises:
            BackfillStateError: stored inputs don't match the supplied triple.
        """
        for row in self.fetch_chunks(backfill_id):
            stored = (_as_utc(row.backfill_from), _as_utc(row.backfill_to), row.chunk_size)
            if stored != (backfill_from, backfill_to, chunk_size):
                raise BackfillStateError(
                    f"backfill {backfill_id} exists with different inputs: stored "
                    f"(from={stored[0].isoformat()}, to={stored[1].isoformat()}, chunk={stored[2]}), "
                    f"got (from={backfill_from.isoformat()}, to={backfill_to.isoformat()}, chunk={chunk_size})"
                )
        adapter, table_ref = self._boundary()
        insert_sql = (
            f"INSERT INTO {table_ref} "
            "(pipeline_name, source_section, resource_name, backfill_id, chunk_id, "
            "chunk_from, chunk_to, backfill_from, backfill_to, chunk_size, status, run_id) "
            f"SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{ChunkStatus.PENDING}', ? "
            f"WHERE NOT EXISTS (SELECT 1 FROM {table_ref} WHERE backfill_id = ? AND chunk_id = ?)"
        )
        with open_client(self._pipeline) as client:
            for index, (chunk_from, chunk_to) in enumerate(chunks):
                chunk_id = chunk_id_for(index)
                adapter.execute_sql(
                    client,
                    insert_sql,
                    self._pipeline.pipeline_name,
                    self.source_section,
                    self.resource_name,
                    backfill_id,
                    chunk_id,
                    chunk_from,
                    chunk_to,
                    backfill_from,
                    backfill_to,
                    chunk_size,
                    chunk_run_id(self.source_section, chunk_from, chunk_to),
                    backfill_id,
                    chunk_id,
                )

    def claim(self, backfill_id: str, chunk_id: str, *, claimed_by: str) -> bool:
        """Optimistic CAS claim (locked CR1-3 SQL); True iff this worker won.

        The claim targets ``status IN ('pending','failed')`` so a resume
        retries failed chunks and skips completed ones by construction. The
        adapter boundary exposes no rows-affected count, so the verify SELECT
        classifies the row into the three outcomes the CAS actually has:

        - this worker's token holds a ``claimed`` row — won (True);
        - another worker's token holds the row — lost the race (False), the
          documented non-fatal outcome;
        - the row is still ``pending`` / ``failed``, i.e. still inside the CAS
          target set — the UPDATE never applied, so *nobody* claimed this
          chunk. That is a destination failure, not a race, and it raises.

        A raising UPDATE is classified by the same rule rather than assumed to
        be a race. Destinations with optimistic transaction semantics surface a
        genuine concurrency conflict as an exception, and no portable exception
        taxonomy separates that from a rate limit, a dropped connection, or a
        permission error — so the exception is tolerated only when the row
        independently shows another worker holding the chunk. Anything else
        re-raises: swallowing it would silently skip that time window.

        Raises:
            BackfillStateError: the CAS did not apply, or the chunk row is gone.
        """
        adapter, table_ref = self._boundary()
        cas_sql = (
            f"UPDATE {table_ref} "
            f"SET status = '{ChunkStatus.CLAIMED}', claimed_by = ?, claimed_at = {adapter.timestamp_now_sql} "
            f"WHERE backfill_id = ? AND chunk_id = ? AND status IN ('{ChunkStatus.PENDING}', '{ChunkStatus.FAILED}')"
        )
        cas_error: Exception | None = None
        try:
            with open_client(self._pipeline) as client:
                adapter.execute_sql(client, cas_sql, claimed_by, backfill_id, chunk_id)
        except Exception as exc:
            cas_error = exc

        verify_sql = f"SELECT claimed_by, status FROM {table_ref} WHERE backfill_id = ? AND chunk_id = ?"
        with open_client(self._pipeline) as client:
            cursor = adapter.execute_query(client, verify_sql, backfill_id, chunk_id)
        row = cursor.fetchone()
        if row is None:
            raise BackfillStateError(
                f"chunk {chunk_id} of backfill {backfill_id} is missing from {BACKFILLS_TABLE} while claiming it; "
                f"the chunk plan was seeded before this claim, so its row cannot legitimately be absent"
            ) from cas_error

        holder = None if row[0] is None else str(row[0])
        status = str(row[1])
        if holder == claimed_by and status == ChunkStatus.CLAIMED:
            if cas_error is not None:
                logger.warning(
                    f"Chunk {chunk_id} claim UPDATE raised ({cas_error}) but the row is claimed by "
                    f"{claimed_by!r} — the write landed; proceeding"
                )
            return True
        if status in (ChunkStatus.PENDING, ChunkStatus.FAILED):
            reason = f": {cas_error}" if cas_error is not None else " (the destination reported no error)"
            raise BackfillStateError(
                f"chunk {chunk_id} of backfill {backfill_id} is still {status!r} after its claim UPDATE — "
                f"the compare-and-swap never applied, so no worker holds this chunk and its time window "
                f"would go unbackfilled. This is a destination failure, not a lost claim{reason}"
            ) from cas_error
        logger.info(f"Chunk {chunk_id} is held by {holder!r} (status {status!r}); this worker moves on")
        return False

    def _transition(self, backfill_id: str, chunk_id: str, claimed_by: str, set_clause: str, *params: Any) -> None:
        """Status transition scoped to the claiming worker's own chunk row."""
        adapter, table_ref = self._boundary()
        update_sql = f"UPDATE {table_ref} SET {set_clause} WHERE backfill_id = ? AND chunk_id = ? AND claimed_by = ?"
        with open_client(self._pipeline) as client:
            adapter.execute_sql(client, update_sql, *params, backfill_id, chunk_id, claimed_by)

    def mark_running(self, backfill_id: str, chunk_id: str, *, claimed_by: str) -> None:
        self._transition(
            backfill_id, chunk_id, claimed_by, f"status = '{ChunkStatus.RUNNING}', started_at = ?", datetime.now(UTC)
        )

    def mark_completed(self, backfill_id: str, chunk_id: str, *, claimed_by: str, records_loaded: int | None) -> None:
        self._transition(
            backfill_id,
            chunk_id,
            claimed_by,
            f"status = '{ChunkStatus.COMPLETED}', completed_at = ?, records_loaded = ?",
            datetime.now(UTC),
            records_loaded,
        )

    def mark_failed(self, backfill_id: str, chunk_id: str, *, claimed_by: str) -> None:
        self._transition(
            backfill_id, chunk_id, claimed_by, f"status = '{ChunkStatus.FAILED}', completed_at = ?", datetime.now(UTC)
        )


@contextmanager
def open_backfill_state(
    source_section: str,
    destination: str,
    dataset: str,
    *,
    resource_name: str | None = None,
) -> Iterator[BackfillState]:
    """BackfillState over the source's own resolved destination + dataset.

    The shared throwaway pipeline is only the client-acquisition vehicle,
    named via ``pipeline_name_for_source`` so file-based destinations (DuckDB)
    resolve the same physical database the data run uses.
    """
    with throwaway_pipeline(pipeline_name_for_source(source_section), destination, dataset) as pipeline:
        yield BackfillState(
            pipeline,
            destination=destination,
            dataset=dataset,
            source_section=source_section,
            resource_name=resource_name,
        )

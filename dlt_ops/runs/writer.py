"""Run-ledger writer — ``_dlt_ops_runs`` start + terminal rows.

Every run/backfill inserts a ``status="running"`` row before extract and
updates it to a terminal status (``completed`` / ``failed`` / ``skipped``)
when it ends. The writer receives the resolved (destination, dataset) pair
from the caller — it never resolves them itself — and the ledger table lives
in that same destination + dataset (per-destination location, CR1-4).

Best-effort semantics mirror trace persistence in ``discovery/runner.py``:
a ledger write failure logs loudly but never fails the data run — the data
write succeeding is the priority; the ledger is observability.

The ledger is adapter-gated: on a destination with no registered
``DestinationAdapter`` (core mode) both writes skip with one INFO line each —
nothing is broken, the ledger has nowhere to live. ERROR is reserved for real
failures (adapter present, write failed).
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from dlt_ops.destinations import DestinationAdapter, has_adapter, open_destination_boundary

logger = logging.getLogger(__name__)

RUNS_TABLE = "_dlt_ops_runs"
"""Run-ledger table name — the single copy other modules (backfill, status) import."""

RUNS_COLUMNS = (
    "pipeline_name",
    "source_section",
    "resource_name",
    "destination",
    "dataset",
    "run_id",
    "dlt_run_id",
    "backfill_id",
    "trigger_source",
    "started_at",
    "completed_at",
    "status",
    "records_extracted",
    "records_loaded",
    "error_summary",
)
"""Locked CR1-4 columns in DDL order; readers key rows by these."""


class RunStatus(StrEnum):
    """Closed vocabulary of the ledger's ``status`` column.

    ``RUNNING`` marks the start row; the rest are terminal. StrEnum members
    hash and compare as their plain string values, so destination round-trips
    and caller-supplied strings interoperate.
    """

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class TriggerSource(StrEnum):
    """Locked trigger_source vocabulary. Y_SCHEDULER is reserved; no emitter exists in v0.1."""

    CLI = "cli"
    AIRFLOW = "airflow"
    Y_SCHEDULER = "y-scheduler"
    BACKFILL = "backfill"


TRIGGER_SOURCES = tuple(source.value for source in TriggerSource)
"""Valid trigger_source values in declaration order; the closed set is :class:`TriggerSource`."""

_TERMINAL_STATUSES = frozenset(RunStatus) - {RunStatus.RUNNING}


def pipeline_name_for_source(source_name: str) -> str:
    """The dlt pipeline name the run path uses for a source.

    Lives here because the ledger's physical location keys on it: file-based
    destinations (DuckDB) resolve their database path from the pipeline name,
    so every ledger reader must derive the same name the runner used.
    """
    return f"{source_name}_pipeline"


def new_run_id() -> str:
    """Extension run id for plain runs; backfill passes its own deterministic per-chunk id."""
    return uuid.uuid4().hex


def summarize_error(exc: BaseException, max_length: int = 500) -> str:
    """One-line error summary for the ledger; the full trace stays in logs."""
    return " ".join(f"{type(exc).__name__}: {exc}".split())[:max_length]


def dlt_run_id_from_load_info(load_info: Any) -> str | None:
    """dlt's load id — the join key into ``_dlt_loads``.

    First package id when a run produced several; None when unavailable.
    """
    try:
        loads_ids = list(load_info.loads_ids)
    except Exception:
        return None
    return str(loads_ids[0]) if loads_ids else None


def record_counts_from_trace(trace: Any) -> tuple[int | None, int | None]:
    """``(records_extracted, records_loaded)`` from ``pipeline.last_trace``.

    dlt-internal tables/resources (``_dlt*``) are excluded from both counts.
    Either count is None when the trace does not carry it.
    """
    return _extracted_count(trace), _loaded_count(trace)


def _extracted_count(trace: Any) -> int | None:
    try:
        total = 0
        for metrics_list in trace.last_extract_info.metrics.values():
            for metrics in metrics_list:
                for resource_name, writer_metrics in metrics["resource_metrics"].items():
                    if not str(resource_name).startswith("_dlt"):
                        total += int(writer_metrics.items_count)
        return total
    except Exception:
        return None


def _loaded_count(trace: Any) -> int | None:
    try:
        return sum(
            int(count)
            for table, count in trace.last_normalize_info.row_counts.items()
            if not str(table).startswith("_dlt")
        )
    except Exception:
        return None


def runs_table_ddl(adapter: DestinationAdapter, dataset: str, table: str = RUNS_TABLE) -> str:
    """One canonical (DuckDB-dialect) runs-table DDL for every destination.

    Locked CR1-4 shape, column names and order verbatim. No PARTITION BY /
    CLUSTER BY: retention and partitioning are explicitly deferred, and those
    clauses don't transpile.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {adapter.render_table_ref(dataset, table)} ("
        "pipeline_name VARCHAR NOT NULL, "
        "source_section VARCHAR NOT NULL, "
        "resource_name VARCHAR, "
        "destination VARCHAR NOT NULL, "
        "dataset VARCHAR NOT NULL, "
        "run_id VARCHAR NOT NULL, "
        "dlt_run_id VARCHAR, "
        "backfill_id VARCHAR, "
        "trigger_source VARCHAR NOT NULL, "
        "started_at TIMESTAMPTZ NOT NULL, "
        "completed_at TIMESTAMPTZ, "
        "status VARCHAR NOT NULL, "
        "records_extracted BIGINT, "
        "records_loaded BIGINT, "
        "error_summary VARCHAR)"
    )


class RunsWriter:
    """Best-effort ledger writer bound to one run.

    The ledger's physical location keys on the pipeline name, which derives
    from ``source_section`` (via :func:`pipeline_name_for_source`) — the same
    derivation the reader uses, so writer and reader always resolve the same
    location. The client is acquired schema-independently through
    :func:`open_destination_boundary`, the same path the ledger reader uses:
    ``write_start`` runs before extract, when a selective ``clean`` may have
    wiped the source's local schema file, and the ledger sidecar
    (``_dlt_ops_runs``) must never depend on the source's dlt schema to write a
    row about it. Construction is pure (no I/O); only ``write_start`` /
    ``write_end`` touch the destination, and both swallow every failure after
    logging it loudly. On a core-mode destination (no registered adapter) both
    skip with one INFO line instead — ERROR means a real write failure.

    Args:
        destination: Resolved destination adapter name (ledger column value).
        dataset: Resolved dataset the run writes to (ledger location + column).
        source_section: Config-section name of the source being run.
        resource_name: Set when the run is scoped to exactly one resource;
            None = source-level run.
        run_id: Extension run id; generated (uuid4 hex) when None. Backfill
            passes its deterministic per-chunk id.
        backfill_id: Reference into ``_dlt_backfills``; None for plain runs.
        trigger_source: One of :data:`TRIGGER_SOURCES` ("cli" default).
    """

    def __init__(
        self,
        *,
        destination: str,
        dataset: str,
        source_section: str,
        resource_name: str | None = None,
        run_id: str | None = None,
        backfill_id: str | None = None,
        trigger_source: str = TriggerSource.CLI,
    ) -> None:
        if trigger_source not in TRIGGER_SOURCES:
            raise ValueError(f"invalid trigger_source {trigger_source!r}; valid: {', '.join(TRIGGER_SOURCES)}")
        self.destination = destination
        self.dataset = dataset
        self.source_section = source_section
        self.resource_name = resource_name
        self.run_id = run_id or new_run_id()
        self.backfill_id = backfill_id
        self.trigger_source = trigger_source

    def _core_mode_skip(self) -> bool:
        """True (after one INFO line) when the destination has no adapter — the ledger has nowhere to live."""
        if has_adapter(self.destination):
            return False
        logger.info(f"runs ledger skipped: destination {self.destination!r} has no DestinationAdapter (core mode)")
        return True

    def _start_row_present(self, adapter: DestinationAdapter, client: Any, table_ref: str) -> bool:
        """Whether this run's ledger row exists — the terminal UPDATE's target.

        Absent means the start write never landed, so the terminal UPDATE
        matched zero rows; the caller turns that otherwise-silent gap loud.
        """
        cursor = adapter.execute_query(
            client,
            f"SELECT 1 FROM {table_ref} WHERE run_id = ? AND source_section = ?",
            self.run_id,
            self.source_section,
        )
        return cursor.fetchone() is not None

    def write_start(self) -> None:
        """Insert the ``status="running"`` row; lazily creates the ledger table.

        Best effort: never raises. Skipped at INFO in core mode (no adapter).
        """
        if self._core_mode_skip():
            return
        started_at = datetime.now(UTC)
        pipeline_name = pipeline_name_for_source(self.source_section)
        try:
            with open_destination_boundary(pipeline_name, self.destination, self.dataset) as (adapter, client):
                insert_sql = (
                    f"INSERT INTO {adapter.render_table_ref(self.dataset, RUNS_TABLE)} "
                    "(pipeline_name, source_section, resource_name, destination, dataset, "
                    "run_id, backfill_id, trigger_source, started_at, status) "
                    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '{RunStatus.RUNNING}')"
                )
                adapter.ensure_schema(client, self.dataset)
                adapter.execute_sql(client, runs_table_ddl(adapter, self.dataset))
                adapter.execute_sql(
                    client,
                    insert_sql,
                    pipeline_name,
                    self.source_section,
                    self.resource_name,
                    self.destination,
                    self.dataset,
                    self.run_id,
                    self.backfill_id,
                    self.trigger_source,
                    started_at,
                )
        except Exception as exc:
            logger.error(f"Failed to write run-start row to {RUNS_TABLE} (non-fatal, run continues): {exc}")

    def write_end(
        self,
        status: str,
        *,
        dlt_run_id: str | None = None,
        records_extracted: int | None = None,
        records_loaded: int | None = None,
        error_summary: str | None = None,
    ) -> None:
        """Update this run's ``running`` start row to a terminal status.

        Best effort: never raises. Skipped at INFO in core mode (no adapter).
        The terminal write is an UPDATE of the start row; if that row is absent
        (the start write never landed) the UPDATE matches nothing and this run
        has no ledger record. That silent gap is detected and logged loudly at
        ERROR — the run stays unrecorded (the documented best-effort outcome),
        but its absence never masquerades as a successful terminal write.
        """
        if status not in _TERMINAL_STATUSES:
            logger.error(
                f"Invalid terminal run status {status!r} (valid: {', '.join(sorted(_TERMINAL_STATUSES))}); "
                f"skipping {RUNS_TABLE} update (non-fatal)"
            )
            return
        if self._core_mode_skip():
            return
        completed_at = datetime.now(UTC)
        pipeline_name = pipeline_name_for_source(self.source_section)
        try:
            with open_destination_boundary(pipeline_name, self.destination, self.dataset) as (adapter, client):
                table_ref = adapter.render_table_ref(self.dataset, RUNS_TABLE)
                update_sql = (
                    f"UPDATE {table_ref} "
                    "SET status = ?, completed_at = ?, dlt_run_id = ?, "
                    "records_extracted = ?, records_loaded = ?, error_summary = ? "
                    f"WHERE run_id = ? AND source_section = ? AND status = '{RunStatus.RUNNING}'"
                )
                adapter.execute_sql(
                    client,
                    update_sql,
                    status,
                    completed_at,
                    dlt_run_id,
                    records_extracted,
                    records_loaded,
                    error_summary,
                    self.run_id,
                    self.source_section,
                )
                if not self._start_row_present(adapter, client, table_ref):
                    logger.error(
                        f"Terminal {RUNS_TABLE} write for run {self.run_id} matched no start row "
                        f"(run-start was never recorded); this run has no ledger entry (non-fatal)"
                    )
        except Exception as exc:
            logger.error(f"Failed to write run-end row to {RUNS_TABLE} (non-fatal, run continues): {exc}")

"""Quarantine writer — ``_dlt_rejected`` rows for quarantined assertion failures.

One JSON-payload table per destination dataset (assertions spec §4): rejected
rows come from arbitrary resources with disjoint schemas, so the row payload
is a single JSON column, immune to schema evolution.

Colocated conventions with ``runs/writer.py`` (same dataset the data lands in,
lazy ``CREATE TABLE IF NOT EXISTS``, adapter-routed parameterized SQL) with
the DELIBERATE opposite failure policy: **a write failure fails the run**.
The runs ledger is observability; ``_dlt_rejected`` holds actual data rows
removed from the load — dropping rows AND failing to record them is silent
data loss, so :meth:`QuarantineWriter.write` raising aborts the run before
``load()`` (nothing has been written yet; the pending package is dropped like
any other fail).

Retention / partitioning: deferred with the runs ledger's deferral. Cleanup
is manual (``DELETE FROM _dlt_rejected WHERE ...``) until a retention design
lands for both ledgers together.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import attrs

from dlt_ops.destinations import DestinationAdapter, adapter_for_pipeline, open_client

REJECTED_TABLE = "_dlt_rejected"
"""Quarantine table name — the single copy other modules import."""

REJECTED_COLUMNS = (
    "pipeline_name",
    "source_section",
    "resource_name",
    "run_id",
    "assertion_type",
    "assertion_params",
    "violation",
    "rejected_at",
    "row_json",
)
"""Locked spec §4 columns in DDL order; readers key rows by these."""

_INSERT_CHUNK_ROWS = 200
"""Rows per INSERT statement — modest chunks; quarantine volume is expected small."""


class QuarantineWriteError(RuntimeError):
    """Writing quarantined rows to ``_dlt_rejected`` failed; the run must abort."""


@attrs.frozen
class QuarantinedRow:
    """One rejected row, JSON-serialized and ready for insertion."""

    resource_name: str
    assertion_type: str
    """Plugin name (e.g. ``"unique_columns"``) or ``"custom"``."""
    assertion_params: str
    """JSON snapshot of the normalized params (incl. the predicate qualname)."""
    violation: str
    """The verdict message (e.g. ``"duplicate key id=42"``)."""
    row_json: str
    """The full rejected row, JSON-serialized."""


def rejected_table_ddl(adapter: DestinationAdapter, dataset: str, table: str = REJECTED_TABLE) -> str:
    """One canonical (DuckDB-dialect) ``_dlt_rejected`` DDL for every destination.

    Locked spec §4 shape, column names and order verbatim. No PARTITION BY /
    CLUSTER BY: retention and partitioning are explicitly deferred, and those
    clauses don't transpile.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {adapter.render_table_ref(dataset, table)} ("
        "pipeline_name VARCHAR NOT NULL, "
        "source_section VARCHAR NOT NULL, "
        "resource_name VARCHAR NOT NULL, "
        "run_id VARCHAR NOT NULL, "
        "assertion_type VARCHAR NOT NULL, "
        "assertion_params VARCHAR NOT NULL, "
        "violation VARCHAR NOT NULL, "
        "rejected_at TIMESTAMPTZ NOT NULL, "
        "row_json VARCHAR NOT NULL)"
    )


class QuarantineWriter:
    """Fail-hard ``_dlt_rejected`` writer bound to one run.

    The live pipeline is only the client-acquisition vehicle — it is already
    bound to the same destination + dataset the caller resolved. Construction
    is pure (no I/O); only :meth:`write` touches the destination, and it
    raises :class:`QuarantineWriteError` on any failure instead of swallowing
    it (the opposite of the runs writer, by design).

    Args:
        pipeline: Live dlt pipeline for the run (client acquisition).
        dataset: Resolved dataset the run writes to (quarantine location).
        source_section: Config-section name of the source being run.
        run_id: Extension run id; joins ``_dlt_ops_runs``.
    """

    def __init__(self, pipeline: Any, *, dataset: str, source_section: str, run_id: str) -> None:
        self._pipeline = pipeline
        self.dataset = dataset
        self.source_section = source_section
        self.run_id = run_id

    def write(self, rows: Sequence[QuarantinedRow]) -> None:
        """Insert quarantined rows; lazily creates the table. One ``rejected_at`` per flush.

        Raises:
            QuarantineWriteError: any part of the write failed — the caller
                must abort the run (rows were removed from the load stream and
                could not be recorded).
        """
        if not rows:
            return
        rejected_at = datetime.now(UTC)
        try:
            adapter = adapter_for_pipeline(self._pipeline)
            table_ref = adapter.render_table_ref(self.dataset, REJECTED_TABLE)
            with open_client(self._pipeline) as client:
                adapter.ensure_schema(client, self.dataset)
                adapter.execute_sql(client, rejected_table_ddl(adapter, self.dataset))
                for start in range(0, len(rows), _INSERT_CHUNK_ROWS):
                    chunk = rows[start : start + _INSERT_CHUNK_ROWS]
                    placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(chunk))
                    insert_sql = f"INSERT INTO {table_ref} ({', '.join(REJECTED_COLUMNS)}) VALUES {placeholders}"
                    params: list[Any] = []
                    for row in chunk:
                        params.extend(
                            (
                                self._pipeline.pipeline_name,
                                self.source_section,
                                row.resource_name,
                                self.run_id,
                                row.assertion_type,
                                row.assertion_params,
                                row.violation,
                                rejected_at,
                                row.row_json,
                            )
                        )
                    adapter.execute_sql(client, insert_sql, *params)
        except Exception as exc:
            raise QuarantineWriteError(
                f"failed to write {len(rows)} quarantined row(s) to {REJECTED_TABLE} in dataset "
                f"{self.dataset!r}: {exc}. Quarantined rows were removed from the load stream; "
                f"proceeding would be silent data loss, so the run aborts."
            ) from exc

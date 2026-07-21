"""Run-ledger read side — per-destination fetches for ``pipeline status`` and staleness.

The ledger lives WHERE THE DATA LANDS (destination + dataset), and on
file-based destinations (DuckDB) the physical database additionally keys on
the dlt pipeline name. Reads therefore open one boundary per source pipeline
rather than one per (destination, dataset) pair; sources sharing a physical
destination stay disjoint through the ``source_section`` filter, so merged
output never duplicates rows.
"""

from __future__ import annotations

import operator
from datetime import datetime
from typing import Any

import attrs

from dlt_ops.destinations import open_destination_boundary
from dlt_ops.runs.writer import RUNS_COLUMNS, RUNS_TABLE


@attrs.frozen
class RunRecord:
    """One ``_dlt_ops_runs`` row (locked CR1-4 columns)."""

    pipeline_name: str
    source_section: str
    resource_name: str | None
    destination: str
    dataset: str
    run_id: str
    dlt_run_id: str | None
    backfill_id: str | None
    trigger_source: str
    started_at: datetime
    completed_at: datetime | None
    status: str
    records_extracted: int | None
    records_loaded: int | None
    error_summary: str | None

    def as_dict(self) -> dict[str, Any]:
        """JSON-ready dict: locked column order, ISO-8601 timestamps."""
        data = attrs.asdict(self)
        for key in ("started_at", "completed_at"):
            value = data[key]
            if hasattr(value, "isoformat"):
                data[key] = value.isoformat()
        return data


def fetch_runs(
    pipeline_name: str,
    destination: str,
    dataset: str,
    *,
    source_section: str,
    resource_name: str | None = None,
    limit: int = 10,
) -> list[RunRecord] | None:
    """Last-N ledger rows for one source, newest first.

    None = the ledger table (or its dataset) does not exist yet — the source
    never ran against this destination. Empty list = the table exists but has
    no matching rows.
    """
    bounded = operator.index(limit)
    if bounded <= 0:
        raise ValueError(f"limit must be positive, got {limit}")
    with open_destination_boundary(pipeline_name, destination, dataset) as (adapter, client):
        if not adapter.table_exists(client, dataset, RUNS_TABLE):
            return None
        conditions = ["source_section = ?"]
        params: list[Any] = [source_section]
        if resource_name is not None:
            conditions.append("resource_name = ?")
            params.append(resource_name)
        query = (
            f"SELECT {', '.join(RUNS_COLUMNS)} "
            f"FROM {adapter.render_table_ref(dataset, RUNS_TABLE)} "
            f"WHERE {' AND '.join(conditions)} "
            f"ORDER BY started_at DESC LIMIT {bounded}"
        )
        cursor = adapter.execute_query(client, query, *params)
        return [RunRecord(**dict(zip(RUNS_COLUMNS, row, strict=True))) for row in cursor.fetchall()]


def latest_run_started_at(
    pipeline_name: str,
    destination: str,
    dataset: str,
    *,
    source_section: str,
) -> datetime | None:
    """``started_at`` of the most recent run for a source; None = zero history."""
    runs = fetch_runs(pipeline_name, destination, dataset, source_section=source_section, limit=1)
    return runs[0].started_at if runs else None

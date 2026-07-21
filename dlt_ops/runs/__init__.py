"""Extension-owned run ledger — ``_dlt_ops_runs`` (CR1-4 locked).

Written at every run/backfill start and end; read by ``pipeline status`` and
the ``stale_sources`` Tier-1 rule. The ledger lives WHERE THE DATA LANDS
(per destination + dataset) so status writes can never fail orthogonally to
the data write succeeding.
"""

from dlt_ops.runs.reader import RunRecord, fetch_runs, latest_run_started_at
from dlt_ops.runs.writer import (
    RUNS_COLUMNS,
    RUNS_TABLE,
    TRIGGER_SOURCES,
    RunsWriter,
    RunStatus,
    TriggerSource,
    dlt_run_id_from_load_info,
    new_run_id,
    pipeline_name_for_source,
    record_counts_from_trace,
    runs_table_ddl,
    summarize_error,
)

__all__ = [
    "RUNS_COLUMNS",
    "RUNS_TABLE",
    "TRIGGER_SOURCES",
    "RunRecord",
    "RunStatus",
    "RunsWriter",
    "TriggerSource",
    "dlt_run_id_from_load_info",
    "fetch_runs",
    "latest_run_started_at",
    "new_run_id",
    "pipeline_name_for_source",
    "record_counts_from_trace",
    "runs_table_ddl",
    "summarize_error",
]

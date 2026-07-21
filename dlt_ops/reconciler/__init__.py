"""Schema-drift reconciler for dlt ingestion.

Runs against every discovered source regardless of contract mode:

- Additive detection (`reconcile_source`, `reconcile_all`) compares each
  resource's live destination schema against its `columns=<PydanticModel>`
  declaration and emits one alert event per drifted RESOURCE.
- Removal detection (`detect_removal`) runs a windowed non-null-coverage
  diff against each resource's destination table (on the configured
  ``load_timestamp_column``), emitting when a known column's coverage
  collapses.

Each source reconciles against its own destination + dataset resolved from
the project config chain; all SQL is canonical (DuckDB dialect) and executes
through the DestinationAdapter boundary. Emission goes through the
``AlertSink`` protocol — the contract of the ``alert_sink`` plugin axis —
with sinks selected per project via ``[dlt_ops] alert_sinks``
(default: structured logging), so no alerting SDK loads on this path.

Test-callers inject destination side effects via the ``SchemaFetcher`` /
``QueryRunner`` Protocols and capture events via an ``AlertSink`` fake (see
``reconciler.protocols``) — no destination SDK import required.
"""

from dlt_ops.reconciler.additive import reconcile_all, reconcile_source
from dlt_ops.reconciler.models import DriftFinding, ReconcileResult
from dlt_ops.reconciler.protocols import AlertSink, QueryRunner, SchemaFetcher
from dlt_ops.reconciler.removal import detect_removal

__all__ = [
    "AlertSink",
    "DriftFinding",
    "QueryRunner",
    "ReconcileResult",
    "SchemaFetcher",
    "detect_removal",
    "reconcile_all",
    "reconcile_source",
]

"""Injectable side-effect protocols for the reconciler.

The public entry points (`reconcile_source`, `reconcile_all`, `detect_removal`)
never accept a destination client — they accept these Protocols so callers can
inject fakes without touching any destination SDK. Prod callers pass nothing
and the reconciler builds DestinationAdapter-backed defaults per source from
its resolved destination + dataset (see ``_adapters``).

Shapes are minimal — only what the reconciler actually calls:

- ``SchemaFetcher.fetch(refs)`` batch-fetches live column metadata; the
  default adapter wraps ``DestinationAdapter.fetch_columns`` per table.
- ``QueryRunner.query(sql, params)`` runs one canonical (DuckDB-dialect,
  ``?``-placeholder) SELECT for the additive sample-values query and the
  removal coverage query.
- ``AlertSink`` is where findings and reconciler-internal errors go — the
  contract of the ``alert_sink`` plugin axis, so plugin sinks slot in
  without reconciler changes.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Protocol

import attrs

if TYPE_CHECKING:
    from dlt_ops.destinations import ColumnInfo
    from dlt_ops.reconciler.models import DriftFinding


@attrs.frozen
class TableRef:
    """Destination-neutral table coordinate: dataset (schema) + table name.

    Connection identity (project / database / file path) is owned by the
    destination adapter and its dlt client — never part of the reference.
    """

    dataset: str
    table: str


class SchemaFetcher(Protocol):
    """Batch-fetch live column metadata for a list of tables.

    Return value is keyed by every requested ``TableRef``; a ``None`` value
    means the table was not found at the destination. Column shape is
    ``dlt_ops.destinations.ColumnInfo``.
    """

    def fetch(self, refs: list[TableRef]) -> dict[TableRef, "tuple[ColumnInfo, ...] | None"]: ...


class QueryRunner(Protocol):
    """Run one canonical (DuckDB-dialect) SELECT, return the result rows.

    ``params`` bind positionally to ``?`` placeholders. Rows are read
    positionally (``row[0]``, ``row[1]``, ...) in SELECT-list order — the one
    access pattern portable across destination cursor row types.
    """

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[Any]: ...


class AlertSink(Protocol):
    """Destination for reconciler events: drift findings + internal errors.

    The contract of the ``alert_sink`` plugin axis (entry-point group
    ``dlt_ops.alert_sink``): ``emit_drift(finding)`` /
    ``emit_error(exc, *, source_name, resource_name, context)`` /
    ``flush(timeout)``. Sinks are selected per project via
    ``[dlt_ops] alert_sinks = ["logging", ...]`` (default: the core
    logging sink) and constructed with their
    ``[dlt_ops.alert_sink.<name>]`` options as keyword arguments; all
    configured sinks receive every event. ``--dry-run`` suppresses all
    emission.

    ``flush(timeout)`` is called by every public reconciler entry point on
    the way out — sinks with a background transport queue must drain it
    there (deterministic, bounded by ``timeout``) because a short-lived CLI
    or orchestrator task may exit immediately after.
    """

    def emit_drift(self, finding: "DriftFinding") -> None: ...

    def emit_error(
        self,
        exc: BaseException,
        *,
        source_name: str,
        resource_name: str | None = None,
        context: str,
    ) -> None: ...

    def flush(self, timeout: float = 2.0) -> None: ...


__all__ = ["AlertSink", "QueryRunner", "SchemaFetcher", "TableRef"]

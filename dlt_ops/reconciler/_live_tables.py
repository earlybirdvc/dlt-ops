"""Live destination tables shared by both detectors: one schema fetch, one time-column rule.

Both detectors read each resource's live columns before they query the table.
A resource whose table is absent is skipped, so a declared resource that never
landed produces no query and no error. Each present table gets its own time
column, which drives removal's coverage windows, additive sampling, and the
``reproduce_sql`` on every finding.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dlt_ops.reconciler.protocols import TableRef

if TYPE_CHECKING:
    from collections.abc import Iterable

    from dlt_ops.destinations import ColumnInfo
    from dlt_ops.discovery.models import SourceInfo
    from dlt_ops.reconciler.common import _Normalizer
    from dlt_ops.reconciler.protocols import AlertSink, SchemaFetcher


logger = logging.getLogger(__name__)


def fetch_live_tables(
    source: SourceInfo,
    *,
    fetcher: SchemaFetcher,
    dataset: str,
    sink: AlertSink,
) -> dict[str, tuple[ColumnInfo, ...]]:
    """Live columns of every resource table of ``source`` that exists, keyed by resource name.

    One ``fetcher.fetch`` call covers all resources. A table the destination
    does not report is left out of the result. A fetch failure is reported to
    ``sink`` under the ``fetch_schemas`` context and then re-raised, so the
    caller turns it into ``ReconcileResult.error`` without reporting it again.
    """
    refs = [TableRef(dataset=dataset, table=resource_name) for resource_name in source.resources]
    try:
        schemas = fetcher.fetch(refs)
    except Exception as exc:
        sink.emit_error(exc, source_name=source.name, context="fetch_schemas")
        logger.warning("schema fetch failed for source=%s: %s", source.name, exc)
        raise

    tables: dict[str, tuple[ColumnInfo, ...]] = {}
    for ref in refs:
        columns = schemas.get(ref)
        if columns is None:
            # The resource has never landed, or the dataset is stale; there is
            # nothing to diff or window in either case.
            logger.debug("resource %s.%s not present in %s — skipping", source.name, ref.table, dataset)
            continue
        tables[ref.table] = columns
    return tables


def resolve_time_column(
    columns: Iterable[ColumnInfo],
    configured: str | None,
    naming: _Normalizer,
) -> str | None:
    """The column that orders and windows one table's rows, or None when it has none.

    The configured ``load_timestamp_column``, normalized with the source's
    naming convention, wins when the table has it. Otherwise the column the
    destination reports as the partition column is used. The returned name is
    the destination-side name, ready for canonical SQL.
    """
    columns = tuple(columns)
    if configured:
        normalized = naming.normalize_identifier(configured)
        if any(col.name == normalized for col in columns):
            return normalized
    return next((col.name for col in columns if col.is_partition_column), None)

"""Live destination tables shared by both detectors: one schema fetch, one time-column rule.

Both detectors read each resource's live columns before they query the table.
A resource's table name is the resource name normalized with the source's
naming convention, and its dataset is the configured dataset normalized with
the same convention. Every query and ``reproduce_sql`` uses both names.
A resource whose table is absent is skipped, so a declared resource that never
landed produces no query and no error. Each present table gets its own time
column, which drives removal's coverage windows, additive sampling, and the
``reproduce_sql`` on every finding.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import attrs

from dlt_ops.reconciler.protocols import TableRef

if TYPE_CHECKING:
    from collections.abc import Iterable

    from dlt_ops.destinations import ColumnInfo
    from dlt_ops.discovery.models import SourceInfo
    from dlt_ops.reconciler.common import _Normalizer
    from dlt_ops.reconciler.protocols import AlertSink, SchemaFetcher


logger = logging.getLogger(__name__)


@attrs.frozen
class LiveTable:
    """One resource's table as the destination reports it."""

    dataset: str
    """The dataset (schema) the table lives in, as the destination names it; ready for canonical SQL."""
    name: str
    """The table name in the destination, ready for canonical SQL."""
    columns: tuple[ColumnInfo, ...]


def table_name_for(resource_name: str, naming: _Normalizer) -> str:
    """The name dlt gives a resource's table when the resource sets no ``table_name`` hint.

    dlt normalizes the name as a table path by default: each ``__``-separated
    part is normalized on its own, so ``OrderItems`` lands as ``order_items``
    and ``orders__archive`` keeps its separator.
    """
    return naming.normalize_tables_path(resource_name)


def dataset_name_for(dataset: str, naming: _Normalizer) -> str:
    """The name dlt gives the configured dataset in the destination.

    dlt normalizes the dataset name as one table identifier by default
    (``enable_dataset_name_normalization``), so ``RAW_ORDERS`` lands as
    ``raw_orders``. A destination configured with that flag turned off keeps
    the configured name, and this lookup then misses a dataset whose name
    needs normalization.
    """
    return naming.normalize_table_identifier(dataset) if dataset else dataset


def fetch_live_tables(
    source: SourceInfo,
    *,
    fetcher: SchemaFetcher,
    dataset: str,
    naming: _Normalizer,
    sink: AlertSink,
) -> dict[str, LiveTable]:
    """The live table of every resource of ``source`` that exists, keyed by resource name.

    ``dataset`` is the configured dataset; tables are looked up in
    ``dataset_name_for(dataset)``, and each ``LiveTable`` carries that name.
    One ``fetcher.fetch`` call covers all resources. A table the destination
    does not report is left out of the result. A fetch failure is reported to
    ``sink`` under the ``fetch_schemas`` context and then re-raised, so the
    caller turns it into ``ReconcileResult.error`` without reporting it again.
    """
    physical_dataset = dataset_name_for(dataset, naming)
    refs = {
        resource_name: TableRef(dataset=physical_dataset, table=table_name_for(resource_name, naming))
        for resource_name in source.resources
    }
    try:
        schemas = fetcher.fetch(list(refs.values()))
    except Exception as exc:
        sink.emit_error(exc, source_name=source.name, context="fetch_schemas")
        logger.warning("schema fetch failed for source=%s: %s", source.name, exc)
        raise

    tables: dict[str, LiveTable] = {}
    for resource_name, ref in refs.items():
        columns = schemas.get(ref)
        if columns is None:
            # The resource has never landed, or the dataset is stale; there is
            # nothing to diff or window in either case.
            logger.debug(
                "resource %s.%s (table %s) not present in %s — skipping",
                source.name,
                resource_name,
                ref.table,
                ref.dataset,
            )
            continue
        tables[resource_name] = LiveTable(dataset=ref.dataset, name=ref.table, columns=columns)
    return tables


def resolve_time_column(
    columns: Iterable[ColumnInfo],
    configured: str | None,
    naming: _Normalizer,
) -> str | None:
    """The column that orders and windows one table's rows, or None when it has none.

    The configured ``load_timestamp_column`` wins when the table has it. It is
    normalized as a column path with the source's naming convention, as dlt
    names columns by default, so ``loaded__at`` keeps its separator. Otherwise
    the column the destination reports as the partition column is used. The returned name is
    the destination-side name, ready for canonical SQL.
    """
    columns = tuple(columns)
    if configured:
        normalized = naming.normalize_path(configured)
        if any(col.name == normalized for col in columns):
            return normalized
    return next((col.name for col in columns if col.is_partition_column), None)

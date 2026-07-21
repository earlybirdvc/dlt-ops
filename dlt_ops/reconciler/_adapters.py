"""Default ``SchemaFetcher`` + ``QueryRunner`` backed by the DestinationAdapter boundary.

Constructed per source from its resolved destination + dataset (the OSS
config chain: ``[dlt_ops].default_*`` overridden by
``[sources.<X>.dlt_ops]``). Acquisition goes through the shared
``open_destination_boundary`` on the pipeline name the runner used
(``pipeline_name_for_source``), so file-based destinations resolve the same
physical database the data run wrote.

Private module — the reconciler picks these up only when a caller passes
``fetcher=None`` / ``runner=None``. Tests inject their own protocol-shaped
fakes and never touch this file.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from dlt_ops.destinations import DestinationAdapter, open_destination_boundary
from dlt_ops.reconciler.protocols import TableRef
from dlt_ops.runs import pipeline_name_for_source

if TYPE_CHECKING:
    from dlt_ops.destinations import ColumnInfo


class AdapterSchemaFetcher:
    """SchemaFetcher over ``DestinationAdapter.fetch_columns``, one call per table."""

    def __init__(self, adapter: DestinationAdapter, client: Any) -> None:
        self._adapter = adapter
        self._client = client

    def fetch(self, refs: list[TableRef]) -> dict[TableRef, "tuple[ColumnInfo, ...] | None"]:
        schemas: dict[TableRef, tuple[ColumnInfo, ...] | None] = {}
        for ref in refs:
            columns = self._adapter.fetch_columns(self._client, ref.dataset, ref.table)
            schemas[ref] = tuple(columns) if columns is not None else None
        return schemas


class AdapterQueryRunner:
    """QueryRunner over ``DestinationAdapter.execute_query`` (canonical SQL in, rows out)."""

    def __init__(self, adapter: DestinationAdapter, client: Any) -> None:
        self._adapter = adapter
        self._client = client

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[Any]:
        return list(self._adapter.execute_query(self._client, sql, *params).fetchall())


@contextmanager
def destination_defaults(
    source_name: str, destination: str, dataset: str
) -> Iterator[tuple[AdapterSchemaFetcher, AdapterQueryRunner]]:
    """Open the source's destination boundary; yield default fetcher + runner.

    The client closes when the ``with`` block exits, so detection must run
    inside it.
    """
    with open_destination_boundary(pipeline_name_for_source(source_name), destination, dataset) as (adapter, client):
        yield AdapterSchemaFetcher(adapter, client), AdapterQueryRunner(adapter, client)


__all__ = ["AdapterQueryRunner", "AdapterSchemaFetcher", "destination_defaults"]

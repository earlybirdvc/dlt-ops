"""BigQuery destination adapter.

The one first-party destination whose driver disagrees with its dialect, so it
is also the one that still declares anything: the dialect, the DDL flags and
both timestamp fragments are derived, while the placeholder style, NULL
binding, dataset creation and the per-dataset ``INFORMATION_SCHEMA`` scope are
declared because no capability dlt publishes describes them.

Deliberately imports no Google SDK: execution flows through the dlt
``sql_client`` the caller passes in, and dlt already maps SDK errors to its
destination exceptions (missing dataset/table -> ``DatabaseUndefinedRelation``),
so ``import dlt_ops`` and adapter loading stay credential- and SDK-free.
"""

from __future__ import annotations

from typing import Any, ClassVar

from dlt_ops.destinations._base import SqlAdapterBase


class BigQueryAdapter(SqlAdapterBase):
    name = "bigquery"
    # dlt's BigQuery sql_client feeds positional args to the BigQuery DB-API,
    # which is pyformat (%s) — verified against dlt docs + source. The GoogleSQL
    # dialect itself writes '?', so this cannot be derived from the dialect.
    placeholder_style = "%s"
    # Dataset creation (location, IAM) is owned by dlt/infra; ensure_schema is a
    # documented no-op here.
    supports_create_schema_if_not_exists = False
    # The DB-API raises on a bound None ("parameter ... of unexpected type");
    # inline NULLs as literals instead. Every ledger write binds NULLs
    # (resource_name/backfill_id on a plain source run), so without this the
    # runs ledger silently fails here.
    inline_null_params: ClassVar[bool] = True

    def _columns_query(self, dataset: str, table: str) -> tuple[str, tuple[Any, ...]]:
        # INFORMATION_SCHEMA is scoped per dataset, so the dataset is an
        # identifier in the table ref (validated), not a bindable param.
        return (
            f"SELECT column_name, data_type FROM {self.render_identifier(dataset)}.INFORMATION_SCHEMA.COLUMNS"
            " WHERE table_name = ? ORDER BY ordinal_position",
            (table,),
        )

"""BigQuery destination adapter.

Deliberately imports no Google SDK: execution flows through the dlt
``sql_client`` the caller passes in, and dlt already maps SDK errors to its
destination exceptions (missing dataset/table -> ``DatabaseUndefinedRelation``),
so ``import dlt_ops`` and adapter loading stay credential- and SDK-free.
"""

from __future__ import annotations

import operator
import re
from typing import Any, ClassVar, Literal

from dlt_ops.destinations._base import SqlAdapterBase


class BigQueryAdapter(SqlAdapterBase):
    name: ClassVar[str] = "bigquery"
    # dlt's BigQuery sql_client feeds positional args to the BigQuery DB-API,
    # which is pyformat (%s) — verified against dlt docs + source.
    placeholder_style: ClassVar[Literal["%s", "?", "$1"]] = "%s"
    supports_if_exists: ClassVar[bool] = True
    supports_alter_add_column_if_not_exists: ClassVar[bool] = True
    # Dataset creation on BigQuery (location, IAM) is owned by dlt/infra;
    # ensure_schema is a documented no-op here.
    supports_create_schema_if_not_exists: ClassVar[bool] = False
    timestamp_now_sql: ClassVar[str] = "CURRENT_TIMESTAMP()"
    # BigQuery's DB-API raises on a bound None ("parameter ... of unexpected
    # type"); inline NULLs as literals instead. Every ledger write binds NULLs
    # (resource_name/backfill_id on a plain source run), so without this the
    # runs ledger silently fails on BigQuery.
    inline_null_params: ClassVar[bool] = True

    _identifier_re: ClassVar[re.Pattern[str]] = re.compile(r"[A-Za-z0-9_]+")

    def timestamp_sub_days_sql(self, days: int) -> str:
        # Canonical fragment, NOT native TIMESTAMP_SUB: the canonical (DuckDB)
        # read mangles TIMESTAMP_SUB into a double INTERVAL. This form
        # transpiles to `CURRENT_TIMESTAMP() - INTERVAL 'n' DAY`, which is
        # valid GoogleSQL interval arithmetic (snapshot-locked in tests).
        return f"CURRENT_TIMESTAMP() - INTERVAL '{operator.index(days)}' DAY"

    def _columns_query(self, dataset: str, table: str) -> tuple[str, tuple[Any, ...]]:
        # BigQuery scopes INFORMATION_SCHEMA per dataset, so the dataset is an
        # identifier in the table ref (validated), not a bindable param.
        return (
            f"SELECT column_name, data_type FROM {self.render_identifier(dataset)}.INFORMATION_SCHEMA.COLUMNS"
            " WHERE table_name = ? ORDER BY ordinal_position",
            (table,),
        )

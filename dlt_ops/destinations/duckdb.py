"""DuckDB destination adapter — canonical dialect, near-passthrough."""

from __future__ import annotations

import operator
import re
from typing import Any, ClassVar, Literal

from dlt_ops.destinations._base import SqlAdapterBase


class DuckDBAdapter(SqlAdapterBase):
    name: ClassVar[str] = "duckdb"
    placeholder_style: ClassVar[Literal["%s", "?", "$1"]] = "?"
    supports_if_exists: ClassVar[bool] = True
    supports_alter_add_column_if_not_exists: ClassVar[bool] = True
    supports_create_schema_if_not_exists: ClassVar[bool] = True
    timestamp_now_sql: ClassVar[str] = "CURRENT_TIMESTAMP"

    # Conservative v0.1 grammar shared with BigQuery (generalized from
    # cleanup.py's _BQ_IDENTIFIER_RE): dlt datasets/tables are snake_case, and
    # anything outside it would not be portable across destinations anyway.
    _identifier_re: ClassVar[re.Pattern[str]] = re.compile(r"[A-Za-z0-9_]+")

    def timestamp_sub_days_sql(self, days: int) -> str:
        return f"CURRENT_TIMESTAMP - INTERVAL '{operator.index(days)} days'"

    def _columns_query(self, dataset: str, table: str) -> tuple[str, tuple[Any, ...]]:
        # dataset/table are data here (bound params), not identifiers.
        return (
            "SELECT column_name, data_type FROM information_schema.columns"
            " WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            (dataset, table),
        )

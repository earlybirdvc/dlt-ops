"""PostgreSQL destination adapter.

Deliberately imports no psycopg: execution flows through the dlt
``sql_client`` the caller passes in, and dlt already maps driver errors to its
destination exceptions (missing schema/table -> ``DatabaseUndefinedRelation``),
so ``import dlt_ops`` and adapter loading stay driver-free — the
``[postgres]`` extra provides psycopg2 only where a live client is built.
"""

from __future__ import annotations

import operator
import re
from typing import Any, ClassVar, Literal

from dlt_ops.destinations._base import SqlAdapterBase


class PostgresAdapter(SqlAdapterBase):
    name: ClassVar[str] = "postgres"
    # psycopg2 is pyformat: positional args bind to %s; '?' raises SyntaxError
    # (probed against a live Postgres). sqlglot's postgres writer happens to
    # rewrite '?' -> %s itself, but conversion stays adapter-owned in
    # _transpile: placeholders are swapped as AST nodes before rendering, so
    # the output style never depends on sqlglot's per-dialect behavior.
    placeholder_style: ClassVar[Literal["%s", "?", "$1"]] = "%s"
    supports_if_exists: ClassVar[bool] = True
    supports_alter_add_column_if_not_exists: ClassVar[bool] = True
    # dlt dataset maps to a Postgres schema; CREATE SCHEMA IF NOT EXISTS is
    # plain DDL (executed on pg16).
    supports_create_schema_if_not_exists: ClassVar[bool] = True
    timestamp_now_sql: ClassVar[str] = "CURRENT_TIMESTAMP"

    _identifier_re: ClassVar[re.Pattern[str]] = re.compile(r"[A-Za-z0-9_]+")

    def timestamp_sub_days_sql(self, days: int) -> str:
        # Canonical fragment; transpiles to `CURRENT_TIMESTAMP - INTERVAL '7 DAYS'`,
        # valid Postgres interval arithmetic (snapshot-locked in tests).
        return f"CURRENT_TIMESTAMP - INTERVAL '{operator.index(days)} days'"

    def _columns_query(self, dataset: str, table: str) -> tuple[str, tuple[Any, ...]]:
        # Postgres shares DuckDB's globally-scoped information_schema shape:
        # dataset/table are data here (bound params), not identifiers.
        return (
            "SELECT column_name, data_type FROM information_schema.columns"
            " WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            (dataset, table),
        )

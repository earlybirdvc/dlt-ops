"""Shared transpile-bind-execute plumbing for first-party destination adapters.

Internal — the public surface is the ``DestinationAdapter`` Protocol; third-party
adapters are free to implement it without this base.
"""

from __future__ import annotations

import re
from typing import Any, ClassVar, Literal

import sqlglot
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from sqlglot import exp

from dlt_ops.destinations.protocol import ColumnInfo, Cursor

CANONICAL_DIALECT = "duckdb"


class _MaterializedCursor:
    """Rows fetched eagerly from a dlt cursor.

    dlt's ``execute_query`` cursors only live inside a context manager; the
    adapter drains them before the context closes. Result sets at this
    boundary (checkpoint lookups, column listings) are small by design.
    """

    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows
        self._position = 0

    def fetchone(self) -> Any:
        if self._position >= len(self._rows):
            return None
        row = self._rows[self._position]
        self._position += 1
        return row

    def fetchall(self) -> list[Any]:
        remaining = self._rows[self._position :]
        self._position = len(self._rows)
        return remaining


class SqlAdapterBase:
    """Implements the DestinationAdapter boundary on top of a dlt ``sql_client``."""

    name: ClassVar[str]
    placeholder_style: ClassVar[Literal["%s", "?", "$1"]]
    supports_if_exists: ClassVar[bool]
    supports_alter_add_column_if_not_exists: ClassVar[bool]
    supports_create_schema_if_not_exists: ClassVar[bool]
    timestamp_now_sql: ClassVar[str]
    _identifier_re: ClassVar[re.Pattern[str]]
    # BigQuery's DB-API cannot type a bound NULL parameter; adapters that set
    # this inline None params as NULL literals instead of binding them. Every
    # other destination binds NULL natively, so the default keeps them untouched.
    inline_null_params: ClassVar[bool] = False

    def timestamp_sub_days_sql(self, days: int) -> str:
        raise NotImplementedError

    def _columns_query(self, dataset: str, table: str) -> tuple[str, tuple[Any, ...]]:
        """Canonical ``information_schema.columns`` SELECT + params for this destination."""
        raise NotImplementedError

    def render_identifier(self, ident: str) -> str:
        if not isinstance(ident, str) or not self._identifier_re.fullmatch(ident):
            raise ValueError(f"invalid {self.name} identifier {ident!r}: must match {self._identifier_re.pattern}")
        # Canonical (DuckDB) quoting; the transpile step in execute_sql converts
        # it to the destination-native quoting (e.g. BigQuery backticks).
        return f'"{ident}"'

    def render_table_ref(self, dataset: str, table: str) -> str:
        return f"{self.render_identifier(dataset)}.{self.render_identifier(table)}"

    def _parse_canonical(self, canonical_sql: str, param_count: int) -> tuple[Any, list[Any]]:
        """Parse one canonical (DuckDB-dialect) statement; validate its placeholders.

        Returns (statement, positional placeholders). Shared by ``_transpile``
        and ``_prepare_params`` so the single-statement / named-placeholder /
        count checks live in one place.
        """
        statements = sqlglot.parse(canonical_sql, read=CANONICAL_DIALECT)
        if len(statements) != 1 or statements[0] is None:
            raise ValueError(f"expected exactly one canonical SQL statement, got {len(statements)}")
        statement = statements[0]
        placeholders = list(statement.find_all(exp.Placeholder))
        if any(placeholder.this for placeholder in placeholders):
            raise ValueError("canonical SQL must use positional '?' placeholders, not named ones")
        if len(placeholders) != param_count:
            raise ValueError(f"placeholder/param mismatch: {len(placeholders)} '?' placeholders, {param_count} params")
        return statement, placeholders

    def _transpile(self, canonical_sql: str, param_count: int) -> str:
        """Canonical (DuckDB-dialect) SQL -> one native statement with native placeholders.

        Values never enter the SQL text: placeholders are swapped as AST nodes,
        so quoting bugs cannot reintroduce injection, and the placeholder count
        is asserted against the params the caller actually bound.
        """
        statement, placeholders = self._parse_canonical(canonical_sql, param_count)
        for position, placeholder in enumerate(placeholders, start=1):
            native = self._native_placeholder(position)
            if native != "?":
                # Var renders as raw unquoted text, giving e.g. %s / $1 in the output.
                placeholder.replace(exp.Var(this=native))
        return statement.sql(dialect=self.name)

    def _native_placeholder(self, position: int) -> str:
        if self.placeholder_style == "$1":
            return f"${position}"
        return self.placeholder_style

    def _prepare_params(self, canonical_sql: str, params: tuple[Any, ...]) -> tuple[str, tuple[Any, ...]]:
        """Native SQL plus the params to actually bind.

        Default: transpile and bind every param — destinations bind NULL
        natively. When ``inline_null_params`` is set (BigQuery, whose DB-API
        cannot type a bound ``None``), each ``None`` is inlined as a ``NULL``
        literal and dropped from the bound tuple, keeping the surviving
        placeholders (and any ``$n`` numbering) aligned.
        """
        if not self.inline_null_params or not any(param is None for param in params):
            return self._transpile(canonical_sql, len(params)), params

        statement, placeholders = self._parse_canonical(canonical_sql, len(params))
        bound: list[Any] = []
        for placeholder, value in zip(placeholders, params):
            if value is None:
                placeholder.replace(exp.Null())
                continue
            native = self._native_placeholder(len(bound) + 1)
            if native != "?":
                placeholder.replace(exp.Var(this=native))
            bound.append(value)
        return statement.sql(dialect=self.name), tuple(bound)

    def execute_sql(self, client: Any, canonical_sql: str, *params: Any) -> None:
        native_sql, bound = self._prepare_params(canonical_sql, params)
        client.execute_sql(native_sql, *bound)

    def execute_query(self, client: Any, canonical_sql: str, *params: Any) -> Cursor:
        native_sql, bound = self._prepare_params(canonical_sql, params)
        with client.execute_query(native_sql, *bound) as cursor:
            return _MaterializedCursor(list(cursor.fetchall()))

    def table_exists(self, client: Any, dataset: str, table: str) -> bool:
        return self.fetch_columns(client, dataset, table) is not None

    def drop_table_if_exists(self, client: Any, dataset: str, table: str) -> None:
        table_ref = self.render_table_ref(dataset, table)
        if self.supports_if_exists:
            self.execute_sql(client, f"DROP TABLE IF EXISTS {table_ref}")
        elif self.table_exists(client, dataset, table):
            self.execute_sql(client, f"DROP TABLE {table_ref}")

    def ensure_schema(self, client: Any, dataset: str) -> None:
        if not self.supports_create_schema_if_not_exists:
            return
        self.execute_sql(client, f"CREATE SCHEMA IF NOT EXISTS {self.render_identifier(dataset)}")

    def fetch_columns(self, client: Any, dataset: str, table: str) -> list[ColumnInfo] | None:
        canonical_sql, params = self._columns_query(dataset, table)
        try:
            cursor = self.execute_query(client, canonical_sql, *params)
        except DatabaseUndefinedRelation:
            # Destinations that scope information_schema per dataset (BigQuery)
            # error on an absent dataset instead of returning zero rows.
            return None
        rows = cursor.fetchall()
        if not rows:
            return None
        return [ColumnInfo(name=str(row[0]), data_type=str(row[1])) for row in rows]

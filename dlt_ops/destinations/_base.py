"""Shared transpile-bind-execute plumbing for first-party destination adapters.

Internal — the public surface is the ``DestinationAdapter`` Protocol; third-party
adapters are free to implement it without this base.

The base is capability-derived: for every fact dlt publishes about a destination
it reads dlt's own ``DestinationCapabilitiesContext`` (see ``_capabilities.py``)
instead of asking each adapter to restate it. A subclass declares an attribute
only to override the derived answer — which it must where the fact belongs to
the *driver* rather than the dialect, because dlt publishes nothing about
driver behaviour.

Derivation never invents a dialect. A destination dlt cannot resolve, or one
publishing no ``sqlglot_dialect``, falls back to the adapter's own ``name`` as
the transpile target — the pre-derivation behaviour, which is correct for a
third-party adapter whose engine name *is* its dialect and honest for everyone
else, since a wrong dialect name fails loudly at ``sqlglot`` rather than
silently transpiling into the wrong SQL.
"""

from __future__ import annotations

import operator
import re
from typing import Any, ClassVar

import sqlglot
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from sqlglot import exp

from dlt_ops.destinations._capabilities import derive_capabilities
from dlt_ops.destinations.protocol import (
    CANONICAL_IDENTIFIER_RE,
    ColumnInfo,
    Cursor,
    render_canonical_identifier,
    render_canonical_table_ref,
)

CANONICAL_DIALECT = "duckdb"

CANONICAL_TIMESTAMP_NOW_SQL = "CURRENT_TIMESTAMP"
"""Canonical "now" fragment, shared by every adapter.

sqlglot's writers already carry each dialect's spelling of it (a parenthesized
call, a vendor-specific function), so a per-adapter override would only restate
what transpilation performs — snapshot-locked in the adapter tests.
"""


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

    name: str
    """dlt engine name; also the adapter's registry key."""

    dialect: str = ""
    """sqlglot dialect canonical SQL is transpiled into; derived when unset.

    Separate from ``name`` because the two genuinely differ: engines can share
    a dialect, and an engine's name is not always one sqlglot knows.
    """

    placeholder_style: str = "?"
    """Positional placeholder token this adapter emits; derived when unset.

    Derivation asks the dialect. A subclass must declare it when its driver's
    paramstyle disagrees with its dialect's convention — that is a property of
    the client library, which dlt's capabilities do not describe.
    """

    supports_if_exists: bool = True
    """``IF (NOT) EXISTS`` is valid on table DDL; derived when unset."""

    supports_create_schema_if_not_exists: bool = True
    """The adapter may create the schema/dataset; not derived.

    Whether schema creation is the adapter's to make is a deployment fact —
    placement, access control, who owns the namespace — and dlt publishes no
    capability for it.
    """

    timestamp_now_sql: str = CANONICAL_TIMESTAMP_NOW_SQL

    # The shared default grammar; a subclass whose destination accepts less may
    # tighten it, and render_identifier then validates against the tighter one.
    _identifier_re: ClassVar[re.Pattern[str]] = CANONICAL_IDENTIFIER_RE
    # Some DB-APIs cannot type a bound NULL parameter; adapters that set this
    # inline None params as NULL literals instead of binding them. A driver
    # fact, so it is declared rather than derived.
    inline_null_params: ClassVar[bool] = False

    def __init__(self) -> None:
        """Fill in every derivable fact this class did not declare itself."""
        derived = derive_capabilities(self.name)
        if not self._declares("dialect"):
            self.dialect = derived.dialect if derived is not None else self.name
        if not self._declares("placeholder_style"):
            self.placeholder_style = derived.placeholder_style if derived is not None else "?"
        if not self._declares("supports_if_exists") and derived is not None:
            self.supports_if_exists = derived.supports_if_exists

    @classmethod
    def _declares(cls, attribute: str) -> bool:
        """Whether a subclass sets ``attribute`` itself rather than inheriting the derived default.

        A declaration is an override of the derivation, so it must survive
        ``__init__`` — which means asking the class, not the instance, since
        ``__init__`` is what would otherwise shadow it.
        """
        for klass in cls.__mro__:
            if klass is SqlAdapterBase:
                return False
            if attribute in klass.__dict__:
                return True
        return False

    def timestamp_sub_days_sql(self, days: int) -> str:
        """Canonical "now minus N days" fragment.

        Interval arithmetic is the idiom sqlglot most often mistranslates, so
        this is written in the canonical dialect and snapshot-locked per
        adapter rather than assumed portable.
        """
        return f"{self.timestamp_now_sql} - INTERVAL '{operator.index(days)} days'"

    def _columns_query(self, dataset: str, table: str) -> tuple[str, tuple[Any, ...]]:
        """Canonical ``information_schema.columns`` SELECT + params for this destination.

        The default assumes the SQL-standard shape: one globally-scoped
        ``information_schema`` in which dataset and table are *data*, bound as
        params rather than spliced in as identifiers. Destinations that scope
        the view per dataset override this.
        """
        return (
            "SELECT column_name, data_type FROM information_schema.columns"
            " WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            (dataset, table),
        )

    def render_identifier(self, ident: str) -> str:
        # Canonical quoting; the transpile step in execute_sql converts it to
        # the destination-native quoting. Validation and quoting are the shared
        # boundary rule — this passes the adapter's own grammar into it rather
        # than restating either half.
        return render_canonical_identifier(ident, grammar=self._identifier_re, subject=f"{self.name} identifier")

    def render_table_ref(self, dataset: str, table: str) -> str:
        return render_canonical_table_ref(
            dataset, table, grammar=self._identifier_re, subject=f"{self.name} identifier"
        )

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
        return statement.sql(dialect=self.dialect)

    def _native_placeholder(self, position: int) -> str:
        if self.placeholder_style == "$1":
            return f"${position}"
        return self.placeholder_style

    def _prepare_params(self, canonical_sql: str, params: tuple[Any, ...]) -> tuple[str, tuple[Any, ...]]:
        """Native SQL plus the params to actually bind.

        Default: transpile and bind every param — most destinations bind NULL
        natively. When ``inline_null_params`` is set (a driver that cannot type
        a bound ``None``), each ``None`` is inlined as a ``NULL`` literal and
        dropped from the bound tuple, keeping the surviving placeholders (and
        any ``$n`` numbering) aligned.
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
        return statement.sql(dialect=self.dialect), tuple(bound)

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
            # Destinations that scope information_schema per dataset error on an
            # absent dataset instead of returning zero rows.
            return None
        rows = cursor.fetchall()
        if not rows:
            return None
        return [ColumnInfo(name=str(row[0]), data_type=str(row[1])) for row in rows]

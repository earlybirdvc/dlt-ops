"""DestinationAdapter Protocol — the single boundary between package code and destination SQL.

Boundary contract:

- Callers build CANONICAL SQL in the DuckDB dialect (DuckDB stays the universal
  dev-loop destination) and hand it to ``execute_sql`` / ``execute_query``
  together with positional ``*params`` bound to ``?`` placeholders.
- The adapter owns transpile (``sqlglot.transpile(read="duckdb", write=self.name)``),
  placeholder conversion to its native style, and execution via the live dlt
  ``sql_client`` the caller passes in. Callers never transpile, never pick
  placeholder styles, never touch the raw dlt client.
- Adapters never construct credentials or clients; callers own pipeline
  attachment and hand a live client in.

Fragments exposed as attributes (``timestamp_now_sql``, ``timestamp_sub_days_sql``)
are written in the CANONICAL dialect too: they exist because sqlglot transpiles
syntax, not every function idiom, so each adapter owns a fragment it guarantees
survives its own transpile step (snapshot-locked in tests/test_destinations.py).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, Protocol, runtime_checkable

import attrs

__all__ = ["ColumnInfo", "Cursor", "DestinationAdapter"]


@attrs.frozen
class ColumnInfo:
    """One column of a destination table, as reported by ``fetch_columns``.

    ``data_type`` is the destination-native type string from
    ``information_schema.columns`` (e.g. ``VARCHAR`` on DuckDB, ``STRING`` on
    BigQuery) — callers comparing types must compare within one destination.
    """

    name: str
    data_type: str


@runtime_checkable
class Cursor(Protocol):
    """Minimal structural cursor returned by ``execute_query``.

    Matches the fetch surface of dlt's ``DBApiCursor`` so callers stay
    adapter-generic. Rows are destination-native row objects (tuples on
    DuckDB, ``Row`` on BigQuery) — index access (``row[0]``) is the portable
    way to read them.
    """

    def fetchone(self) -> Any: ...

    def fetchall(self) -> list[Any]: ...


@runtime_checkable
class DestinationAdapter(Protocol):
    """Everything checkpoint/cleanup/reconciler code may know about a destination.

    Implementations register as ``dlt_ops.destination`` entry points (or
    via ``dlt_ops.register("destination", name)``) and are resolved by
    name through the plugin registry.
    """

    name: str
    """Registry / sqlglot dialect name: ``"bigquery"``, ``"duckdb"``, ``"postgres"``, ..."""

    placeholder_style: Literal["%s", "?", "$1"]
    """Native positional placeholder style of the destination's dlt sql_client.

    Informational for diagnostics; conversion from canonical ``?`` happens
    inside ``execute_sql`` / ``execute_query``, never in caller code.
    """

    supports_if_exists: bool
    """``CREATE TABLE IF NOT EXISTS`` / ``DROP TABLE IF EXISTS`` are valid DDL.

    Consumers: checkpoint table DDL and ``drop_table_if_exists``,
    which falls back to probe-then-drop when False.
    """

    supports_alter_add_column_if_not_exists: bool
    """``ALTER TABLE ... ADD COLUMN IF NOT EXISTS`` is valid DDL.

    Consumer: the checkpoint ``run_id`` column migration — when
    False the caller probes ``fetch_columns`` before altering.
    """

    supports_create_schema_if_not_exists: bool
    """The adapter may create the schema/dataset via ``ensure_schema``.

    Consumer: CheckpointManager setup — DuckDB needs
    ``CREATE SCHEMA IF NOT EXISTS`` before the checkpoint DDL; on BigQuery
    dataset creation (location, IAM) is owned by dlt/infra, so the flag is
    False and ``ensure_schema`` is a no-op.
    """

    timestamp_now_sql: str
    """Canonical-dialect fragment for "now" that survives this adapter's transpile.

    Consumers: checkpoint ``_mark_completed``, cleanup retention.
    """

    timestamp_sub_days_sql: Callable[[int], str]
    """``days -> canonical-dialect fragment`` for "now minus N days".

    Same consumers as ``timestamp_now_sql``; kept per-adapter because interval
    arithmetic is the idiom sqlglot most often mistranslates.
    """

    def render_identifier(self, ident: str) -> str:
        """Validate ``ident`` against this destination's identifier grammar and quote it.

        Returns the CANONICAL-dialect (DuckDB-quoted) form for embedding in
        canonical SQL — native quoting is applied by the transpile step inside
        ``execute_sql``. Raises ``ValueError`` for names outside the grammar
        (the generalization of cleanup.py's BigQuery identifier check).
        """
        ...

    def render_table_ref(self, dataset: str, table: str) -> str:
        """``dataset.table`` reference in canonical form; both parts validated."""
        ...

    def execute_sql(self, client: Any, canonical_sql: str, *params: Any) -> None:
        """Transpile canonical SQL, bind ``*params``, execute via ``client``.

        Params bind natively by default. An adapter whose driver cannot bind a
        given value (BigQuery rejects a bound ``None``) may inline it as a
        literal instead — a ``None`` becomes a ``NULL`` literal in the SQL, never
        injected text (the value still goes through the sqlglot AST, not string
        interpolation).
        """
        ...

    def execute_query(self, client: Any, canonical_sql: str, *params: Any) -> Cursor:
        """Like ``execute_sql`` but returns a ``Cursor`` over the result rows."""
        ...

    def table_exists(self, client: Any, dataset: str, table: str) -> bool: ...

    def drop_table_if_exists(self, client: Any, dataset: str, table: str) -> None: ...

    def ensure_schema(self, client: Any, dataset: str) -> None:
        """Create the schema/dataset when the destination supports and needs it.

        No-op when ``supports_create_schema_if_not_exists`` is False, so
        callers may invoke it unconditionally.
        """
        ...

    def fetch_columns(self, client: Any, dataset: str, table: str) -> list[ColumnInfo] | None:
        """Columns of ``dataset.table`` via one canonical ``information_schema.columns`` SELECT.

        Returns ``None`` when the table (or its dataset) is absent. Consumer:
        the reconciler's drift detection.
        """
        ...

"""DestinationAdapter Protocol — the single boundary between package code and destination SQL.

Boundary contract:

- Callers build CANONICAL SQL and hand it to ``execute_sql`` /
  ``execute_query`` together with positional ``*params`` bound to ``?``
  placeholders. The canonical dialect is the package's interchange grammar —
  DuckDB syntax, double-quoted identifiers, ``?`` placeholders — and is the
  only SQL any caller ever writes.
- The adapter owns transpile from the canonical dialect to its own, placeholder
  conversion to its native style, and execution via the live dlt ``sql_client``
  the caller passes in. Callers never transpile, never pick placeholder styles,
  never touch the raw dlt client.
- Adapters never construct credentials or clients; callers own pipeline
  attachment and hand a live client in.

Fragments exposed as attributes (``timestamp_now_sql``, ``timestamp_sub_days_sql``)
are written in the CANONICAL dialect too: they exist because transpilation covers
syntax, not every function idiom, so an adapter may own a fragment it guarantees
survives its own transpile step (snapshot-locked in tests/test_destinations.py).

:data:`CANONICAL_IDENTIFIER_RE` and :func:`render_canonical_identifier` are the
package's one identifier grammar and quoting rule: the shared default every
adapter validates against, and the only implementation any other caller building
canonical SQL may use.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any, Protocol, runtime_checkable

import attrs

__all__ = [
    "CANONICAL_IDENTIFIER_RE",
    "ColumnInfo",
    "Cursor",
    "DestinationAdapter",
    "render_canonical_identifier",
    "render_canonical_table_ref",
]

CANONICAL_IDENTIFIER_RE = re.compile(r"[A-Za-z0-9_]+")
"""Default identifier grammar: the conservative set every destination accepts.

dlt-normalized datasets/tables/columns are snake_case, and anything outside
this grammar would not be portable across destinations anyway. An adapter may
tighten it for its own destination; nothing may loosen it, because the grammar
check — not the quoting — is what keeps a hostile name out of SQL text.
"""


def render_canonical_identifier(
    ident: str,
    *,
    grammar: re.Pattern[str] = CANONICAL_IDENTIFIER_RE,
    subject: str = "identifier",
) -> str:
    """Validate ``ident`` against ``grammar`` and return it canonically quoted.

    The single implementation of the boundary's identifier rule: reject first,
    then double-quote. Quoting defends against a name colliding with a reserved
    SQL keyword; the grammar check rejects anything that could break out of the
    quotes. ``subject`` names the value in the error message so a caller can
    say which identifier it rejected.

    Raises:
        ValueError: ``ident`` is not a string or falls outside ``grammar``.
    """
    if not isinstance(ident, str) or not grammar.fullmatch(ident):
        raise ValueError(f"invalid {subject} {ident!r}: must match {grammar.pattern}")
    return f'"{ident}"'


def render_canonical_table_ref(
    dataset: str,
    table: str,
    *,
    grammar: re.Pattern[str] = CANONICAL_IDENTIFIER_RE,
    subject: str = "identifier",
) -> str:
    """``dataset.table`` reference in canonical form; both parts validated."""
    return (
        f"{render_canonical_identifier(dataset, grammar=grammar, subject=subject)}."
        f"{render_canonical_identifier(table, grammar=grammar, subject=subject)}"
    )


@attrs.frozen
class ColumnInfo:
    """One column of a destination table, as reported by ``fetch_columns``.

    ``data_type`` is the destination-native type string exactly as
    ``information_schema.columns`` reports it, never normalized to a common
    vocabulary — two destinations spell the same logical type differently, so
    callers comparing types must compare within one destination.
    """

    name: str
    data_type: str


@runtime_checkable
class Cursor(Protocol):
    """Minimal structural cursor returned by ``execute_query``.

    Matches the fetch surface of dlt's ``DBApiCursor`` so callers stay
    adapter-generic. Rows are destination-native row objects whose type is
    whatever the underlying driver returns — positional access (``row[0]``) is
    the one way to read them that holds across every destination.
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
    """Registry key: the destination's dlt engine name — ``"bigquery"``, ``"duckdb"``, ...

    Not the SQL dialect. Which dialect an adapter transpiles into is its own
    business and stays out of this port; engines can share a dialect, and an
    engine name is not always one any transpiler knows.
    """

    placeholder_style: str
    """Native positional placeholder token of the destination's dlt sql_client.

    Deliberately an open string: the set of placeholder styles a DB-API driver
    can use is not the package's to close, so an adapter may declare any token
    its client binds against. Informational for diagnostics; conversion from
    canonical ``?`` happens inside ``execute_sql`` / ``execute_query``, never
    in caller code.
    """

    supports_if_exists: bool
    """``CREATE TABLE IF NOT EXISTS`` / ``DROP TABLE IF EXISTS`` are valid DDL.

    Consumers: checkpoint table DDL and ``drop_table_if_exists``,
    which falls back to probe-then-drop when False.
    """

    supports_create_schema_if_not_exists: bool
    """The adapter may create the schema/dataset via ``ensure_schema``.

    Consumer: CheckpointManager setup, which needs the schema to exist before
    the checkpoint DDL runs. False when schema creation is not the adapter's
    to make — it can carry placement or access-control decisions owned by dlt
    or the surrounding infrastructure — and ``ensure_schema`` is then a no-op.
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

        Returns the canonically-quoted form for embedding in canonical SQL —
        native quoting is applied by the transpile step inside ``execute_sql``.
        Defaults to :func:`render_canonical_identifier`; an adapter whose
        destination accepts less may tighten the grammar, never loosen it.
        Raises ``ValueError`` for names outside the grammar.
        """
        ...

    def render_table_ref(self, dataset: str, table: str) -> str:
        """``dataset.table`` reference in canonical form; both parts validated."""
        ...

    def execute_sql(self, client: Any, canonical_sql: str, *params: Any) -> None:
        """Transpile canonical SQL, bind ``*params``, execute via ``client``.

        Params bind natively by default. An adapter whose driver cannot type a
        given bound value — a rejected bound ``None`` is the usual case — may
        inline it as a literal instead: the ``None`` becomes a ``NULL`` literal
        in the SQL, never injected text (the value still goes through the
        sqlglot AST, not string interpolation).
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

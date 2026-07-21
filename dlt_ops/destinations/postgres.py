"""PostgreSQL destination adapter.

Every capability is derived. Notably the placeholder style: psycopg2 is
pyformat (positional args bind to ``%s``; ``?`` raises SyntaxError, probed
against a live Postgres), and the Postgres dialect's own convention is ``%s``
too — driver and dialect agree, so nothing needs declaring. Conversion still
happens as an AST swap inside ``_transpile``, never as a dependency on
sqlglot's per-dialect rewrite.

Deliberately imports no psycopg: execution flows through the dlt
``sql_client`` the caller passes in, and dlt already maps driver errors to its
destination exceptions (missing schema/table -> ``DatabaseUndefinedRelation``),
so ``import dlt_ops`` and adapter loading stay driver-free — the
``[postgres]`` extra provides psycopg2 only where a live client is built.
"""

from __future__ import annotations

from dlt_ops.destinations._base import SqlAdapterBase


class PostgresAdapter(SqlAdapterBase):
    name = "postgres"

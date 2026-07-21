"""DuckDB destination adapter — canonical dialect, near-passthrough.

Every capability is derived: DuckDB's dlt factory publishes the dialect the
canonical SQL is already written in, and its driver's paramstyle matches that
dialect's. The class exists to claim the registry name.
"""

from __future__ import annotations

from dlt_ops.destinations._base import SqlAdapterBase


class DuckDBAdapter(SqlAdapterBase):
    name = "duckdb"

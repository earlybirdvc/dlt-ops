"""The removal detector's canonical coverage query: pure SQL text and bound parameters, no I/O."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dlt_ops.reconciler.common import canonical_ident, canonical_table_ref

if TYPE_CHECKING:
    from datetime import datetime


def build_coverage_query(
    dataset: str,
    resource_name: str,
    columns: tuple[str, ...],
    *,
    time_column: str,
    recent_start: datetime,
    baseline_start: datetime,
) -> tuple[str, tuple[Any, ...]]:
    """One canonical query: all columns' coverage for recent + baseline windows.

    An inner projection tags every row with ``in_recent`` / ``in_baseline``
    window flags (bounds parameter-bound, so the SQL text is value-free);
    the outer SELECT computes NULL-safe coverage ratios per column in
    transpilable form (``CAST(SUM(CASE ...) AS DOUBLE) / NULLIF(SUM(...), 0)``
    — a zero-row window divides by NULL and yields NULL, which the caller
    reads as unknown coverage rather than as zero).

    The inner ``WHERE <time_column> >= ?`` (baseline start) is a hard lower
    bound on the table's time column. When the destination partitions the
    table on that column, it prunes to the trailing baseline window, so a
    pass reads at most ``baseline_window_days``' worth of partitions per
    resource.

    Result columns come in pairs, positionally: ``(recent, baseline)`` per
    input column, in input order.
    """
    ts = canonical_ident(time_column)
    inner_projection = ", ".join(canonical_ident(col) for col in columns)
    inner = (
        f"SELECT {inner_projection}, "
        f"CASE WHEN {ts} >= ? THEN 1 ELSE 0 END AS in_recent, "
        f"CASE WHEN {ts} >= ? AND {ts} <= ? THEN 1 ELSE 0 END AS in_baseline "
        f"FROM {canonical_table_ref(dataset, resource_name)} "
        f"WHERE {ts} >= ?"
    )
    per_col = []
    for col in columns:
        safe = canonical_ident(col)
        alias = ident_alias(col)
        per_col.append(
            f"CAST(SUM(CASE WHEN {safe} IS NOT NULL AND in_recent = 1 THEN 1 ELSE 0 END) AS DOUBLE)"
            f" / NULLIF(SUM(in_recent), 0) AS recent_{alias}"
        )
        per_col.append(
            f"CAST(SUM(CASE WHEN {safe} IS NOT NULL AND in_baseline = 1 THEN 1 ELSE 0 END) AS DOUBLE)"
            f" / NULLIF(SUM(in_baseline), 0) AS baseline_{alias}"
        )
    select_list = ",\n  ".join(per_col)
    sql = f"SELECT\n  {select_list}\nFROM ({inner}) AS windowed"
    params = (recent_start, baseline_start, recent_start, baseline_start)
    return sql, params


def ident_alias(col: str) -> str:
    """Sanitise a column name for use as a SQL alias.

    ``a-b`` / ``a.b`` / ``a b`` all become ``a_b``. Purely alias-level — the
    original column reference in the coverage expression stays intact via
    canonical quoting. Callers feed in destination-normalized names via
    ``destination_column_names``, which is already snake_case-clean; this
    pass is defensive against a future naming-convention override that might
    leak a non-identifier character.
    """
    return "".join(c if c.isalnum() or c == "_" else "_" for c in col)

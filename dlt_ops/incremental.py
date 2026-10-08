"""Run-window helpers for resources that dlt cannot filter by a cursor."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from dlt.extract.incremental import Incremental

from dlt_ops._datetimes import parse_datetime

__all__ = ["WINDOW_CURSOR_PATH", "resolve_incremental_window"]

WINDOW_CURSOR_PATH = "$['_window_cursor']"
"""Cursor path for a ``dlt.sources.incremental`` whose rows never contain the field.

Do not change this string. dlt keys incremental state by the cursor path, so a
new string starts a new state. A bare name instead of a JSONPath would add a
column to the table schema.
"""


def _cursor_bound(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (str, date)):
        return parse_datetime(value, field)
    raise TypeError(f"Cursor {field} must be a datetime, a date or an ISO-8601 string, got {type(value).__name__}")


def resolve_incremental_window(
    interval: Incremental[Any],
    incremental_start_value: str | None = None,
    incremental_end_value: str | None = None,
    lookback_hours: int = 0,
) -> tuple[datetime, datetime | None]:
    """Return the ``(since, until)`` window a resource should request.

    The first available source wins. A bounded cursor comes first: one with an
    ``end_value``, which an external interval sets, or one created with an
    explicit ``end_value``. Next come the config bounds
    ``incremental_start_value`` and ``incremental_end_value``. Last is the
    cursor's ``start_value``, which is its ``initial_value`` or the value
    stored by the previous run. Cursor bounds given as a date or an ISO-8601
    string are parsed to a datetime.

    Args:
        interval: The resource's ``dlt.sources.incremental``. Only its
            ``start_value`` and ``end_value`` are read.
        incremental_start_value: ISO-8601 start from config, or None.
        incremental_end_value: ISO-8601 end from config, or None. Used only
            when ``incremental_start_value`` is set.
        lookback_hours: Hours to move ``since`` earlier, to catch rows that
            arrive late. Zero or a negative value changes nothing.

    Returns:
        ``(since, until)``. ``until`` is None when the window has no upper
        bound.

    Raises:
        ValueError: No source supplies a window, or a value is not an
            ISO-8601 datetime.
        TypeError: A cursor bound is not a datetime, a date or a string, for
            example a number.
    """
    if interval.end_value is not None:
        since = _cursor_bound(interval.start_value, "start_value")
        until = _cursor_bound(interval.end_value, "end_value")
    elif incremental_start_value:
        since = parse_datetime(incremental_start_value, "incremental_start_value")
        until = parse_datetime(incremental_end_value, "incremental_end_value") if incremental_end_value else None
    elif interval.start_value is not None:
        since = _cursor_bound(interval.start_value, "start_value")
        until = None
    else:
        raise ValueError(
            "No incremental window available: the cursor has no end_value or initial_value, "
            "and incremental_start_value is not configured"
        )
    if lookback_hours > 0:
        since = since - timedelta(hours=lookback_hours)
    return since, until

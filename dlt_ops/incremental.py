"""Run-window helpers for resources that dlt cannot filter by a cursor.

Use these helpers when dlt cannot filter rows by a cursor. This happens when the
API filters on the server side (for example, an ``after=`` parameter) or when
the rows carry no cursor field. The resource still needs the run window from an
orchestrator interval, ``DLT_INTERVAL_START``/``DLT_INTERVAL_END``, or dlt-ops
run bounds. The helpers read that window from a ``dlt.sources.incremental``::

    @dlt.resource
    def events(
        cursor=dlt.sources.incremental(
            WINDOW_CURSOR_PATH,
            initial_value="2024-01-01T00:00:00Z",
            on_cursor_value_missing="include",
        ),
    ):
        since, until = resolve_incremental_window(cursor, None, None, lookback_hours=2)
        yield from fetch_events(after=since, before=until)

:data:`WINDOW_CURSOR_PATH` is a cursor path that no row contains. With
``on_cursor_value_missing="include"``, dlt keeps every row.

:func:`resolve_incremental_window` returns the window as ``(since, until)``. It
can also move ``since`` earlier by ``lookback_hours`` to catch rows that arrive
late. The resource must apply this overlap itself: dlt's ``lag=`` does not move
``start_value`` when an external interval is set.

Under the dlt-ops runner, do not set ``allow_external_schedulers`` on the
incremental. The runner turns it on when a run has an interval. If you set it to
``True`` yourself, a local run with no interval fails with
``ExternalSchedulerNotAvailable``.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from dlt.extract.incremental import Incremental

from dlt_ops._datetimes import parse_datetime

__all__ = ["WINDOW_CURSOR_PATH", "resolve_incremental_window"]

WINDOW_CURSOR_PATH = "$['_window_cursor']"
"""Cursor path for an incremental whose rows carry no cursor value.

It is spelled as a JSONPath on purpose. dlt adds a bare cursor name to the
table schema as a column, and a ``columns: freeze`` schema contract then fails
the run. dlt adds no column for a JSONPath cursor. Do not change the string:
dlt keys the incremental state by the cursor path, so a new string starts a new
state.
"""


def resolve_incremental_window(
    interval: Incremental[Any],
    incremental_start_value: str | None,
    incremental_end_value: str | None,
    lookback_hours: int = 0,
) -> tuple[datetime, datetime | None]:
    """Return the ``(since, until)`` window a resource should request.

    The first available source wins. An external interval (the cursor has an
    ``end_value``) comes first. Next come the config bounds
    ``incremental_start_value`` and ``incremental_end_value``. Last is the
    cursor's ``start_value``, which is its ``initial_value`` or the value
    stored by the previous run.

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
        ValueError: No source supplies a window, or a config value is not an
            ISO-8601 datetime.
    """
    if interval.end_value:
        since, until = interval.start_value, interval.end_value
    elif incremental_start_value:
        since = parse_datetime(incremental_start_value, "incremental_start_value")
        until = parse_datetime(incremental_end_value, "incremental_end_value") if incremental_end_value else None
    elif interval.start_value is not None:
        since = interval.start_value
        if not isinstance(since, datetime):
            since = parse_datetime(since, "start_value")
        until = None
    else:
        raise ValueError(
            "No incremental window available: no external interval is set, "
            "incremental_start_value is not configured, and the cursor has no initial_value"
        )
    if lookback_hours > 0:
        since = since - timedelta(hours=lookback_hours)
    return since, until

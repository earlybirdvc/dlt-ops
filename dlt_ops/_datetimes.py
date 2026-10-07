"""Datetime parsing shared by the run-window helpers."""

from datetime import datetime
from typing import Any

import pendulum


def parse_datetime(value: Any, field: str) -> datetime:
    """Parse an ISO-8601 datetime; ``field`` names the input in the error message.

    Raises:
        ValueError: ``value`` is not an ISO-8601 datetime (a bare duration or
            time of day is rejected too).
    """
    try:
        parsed = pendulum.parse(str(value))
        if not isinstance(parsed, pendulum.DateTime):
            raise ValueError(f"expected a datetime, got {type(parsed).__name__}")
    except Exception as exc:
        raise ValueError(f"Invalid {field} format: {value!r}") from exc
    return parsed

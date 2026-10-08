"""Declared schedules — the project's own named schedules next to the built-in ones.

A project adds schedules in the ``[dlt_ops.schedules]`` table of
``.dlt/config.toml``, one ``"@name" = "<cron>"`` entry each, and a source picks
one by name in ``[sources.<X>.dlt_ops] schedule``. Built-in names are the
:class:`~dlt_ops.discovery.models.Schedule` values and cannot be redefined.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from croniter import croniter

from dlt_ops.discovery.models import Schedule

SCHEDULES_KEY = "schedules"
"""The ``[dlt_ops]`` sub-table holding declared schedules — the single copy of this key name."""

_NAME_RE = re.compile(r"@[A-Za-z0-9_.-]+")
"""The name without ``@`` becomes part of an orchestrator job id, such as an Airflow DAG id.

An Airflow DAG id allows only letters, digits, ``_``, ``.`` and ``-``.
"""

_CRON_FIELD_COUNT = 5
"""Standard five-field cron. Presets such as ``@daily`` and croniter's seconds and year forms are rejected."""

_RANDOM_FIELD_RE = re.compile(r"r(\(\d+-\d+\))?(/\d+)?", re.IGNORECASE)
"""croniter's random field (``R``, ``R(1-5)``, ``R/15``). It picks a new value on every parse."""


def parse_declared_schedules(raw: object) -> dict[str, str]:
    """Validate the raw ``[dlt_ops.schedules]`` value and return ``{name: cron}``.

    ``None`` (the table is absent) gives an empty mapping.

    Raises:
        ValueError: the value is not a table, or an entry has an unusable name
            or cron expression. The message names the entry.
    """
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError(f'must be a table of name = "cron" entries, got {type(raw).__name__}')
    builtin = {s.value for s in Schedule}
    declared: dict[str, str] = {}
    for name, cron in raw.items():
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise ValueError(
                f"{name!r}: a schedule name must be '@' followed by letters, digits, '_', '.' or '-', "
                "because it becomes part of an orchestrator job id"
            )
        if name in builtin:
            raise ValueError(f"{name!r} is a built-in schedule and cannot be redefined")
        if not isinstance(cron, str):
            raise ValueError(f"{name!r}: the cron expression must be a string, got {type(cron).__name__}")
        fields = cron.split()
        if len(fields) != _CRON_FIELD_COUNT:
            raise ValueError(
                f"{name!r} = {cron!r}: the cron expression must have exactly {_CRON_FIELD_COUNT} fields, "
                f"got {len(fields)}"
            )
        if any(_RANDOM_FIELD_RE.fullmatch(field) for field in fields):
            raise ValueError(
                f"{name!r} = {cron!r}: the random 'R' field is not allowed, "
                "because it picks a new time each time the schedule is read"
            )
        if not croniter.is_valid(cron, strict=True):
            raise ValueError(f"{name!r} = {cron!r} is not a valid cron expression")
        declared[name] = cron
    return declared


def resolve_schedule(value: object, declared: Mapping[str, str]) -> str:
    """Return ``value`` as a plain string when it names a built-in or declared schedule.

    Raises:
        ValueError: the name is unknown. The message lists every valid name.
    """
    builtin = [s.value for s in Schedule]
    if isinstance(value, str) and (value in builtin or value in declared):
        return str.__str__(value)
    valid = builtin + sorted(declared)
    raise ValueError(f"Invalid schedule '{value}'. Valid: {valid}")

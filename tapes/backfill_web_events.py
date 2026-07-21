"""Backfillable source: every selected resource declares an incremental cursor."""

import os
from datetime import UTC, datetime

import dlt
import pydantic


class PageView(pydantic.BaseModel):
    id: int
    occurred_at: datetime


_ROWS = [{"id": n, "occurred_at": datetime(2026, 1, n, 12, 0, tzinfo=UTC)} for n in range(1, 7)]

# Fault-injection hook for the resume demo: set to a chunk's start timestamp
# (ISO-8601) and the "API" dies when that chunk runs.
FAIL_FROM_ENV = "WEB_EVENTS_FAIL_FROM"


@dlt.resource(name="page_views", columns=PageView, primary_key="id", write_disposition="append")
def page_views(occurred_at=dlt.sources.incremental("occurred_at", initial_value=datetime(2020, 1, 1, tzinfo=UTC))):
    if os.environ.get(FAIL_FROM_ENV) == occurred_at.start_value.isoformat():
        raise RuntimeError(f"injected API failure for window starting {occurred_at.start_value}")
    yield _ROWS


@dlt.source(name="web_events")
def web_events_source():
    return page_views

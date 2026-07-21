"""Shared plumbing for the github_events pipeline: models, fixture client, `events`.

The Pydantic models are the single source of truth for each resource's schema
(Rule 14): dlt derives typed destination columns from ``columns=<Model>``
instead of inferring them at load time. ``Event.actor_login`` is deliberately
nullable so the example carries a nullable column end to end.

``FixtureClient`` stands in for a paginated HTTP API client and reads the
bundled JSONL under ``../data`` at call time. Imports stay side-effect-light,
so the module passes Rule 15 (no network I/O or disk writes at module load;
disk reads at call time are ordinary reads).
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import dlt
import pydantic

from dlt_ops import with_checkpoints

_DATA_DIR = Path(__file__).resolve().parents[1] / "data"

# Fault-injection hook for the checkpoint-resume demo: when set to N, the
# client raises after serving page N, simulating an API dying mid-pagination.
FAIL_AFTER_PAGE_ENV = "GITHUB_EVENTS_FAIL_AFTER_PAGE"

# Business-timestamp lower bound of the incremental window. Fixture rows that
# occurred before it are never requested, so even the first run demonstrates
# the incremental window boundary.
EVENTS_INITIAL_TIMESTAMP = datetime(2026, 1, 1, tzinfo=UTC)


# extra="forbid" on every model is what turns "the model is the schema" into an
# enforced contract: dlt reads it as schema_contract columns="freeze", so a
# field the API adds and the model does not declare fails the extract step.
# Leave it off and Pydantic's default takes over — dlt derives
# columns="discard_value" and strips the unknown field from every row without
# a word. The pydantic_model_forbids_extra rule fails validate on models that
# omit it.
class Event(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    id: int
    event_type: str
    actor_login: str | None  # nullable: system events carry no actor
    occurred_at: datetime


class EventType(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    name: str
    description: str | None


class Actor(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    login: str
    display_name: str | None
    followers: int


class FixtureClient:
    """Paginated stand-in for an HTTP API client, reading bundled JSONL.

    A real client would page through HTTP responses and send ``since`` as a
    query parameter; this one slices the fixture rows so the example runs
    fully offline. Timestamps are parsed the way a real SDK would deserialize
    a response payload.
    """

    page_size = 3

    def __init__(self, fixture_name: str) -> None:
        self._path = _DATA_DIR / fixture_name

    def _rows(self) -> list[dict]:
        rows = []
        for line in self._path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            if "occurred_at" in row:
                row["occurred_at"] = datetime.fromisoformat(row["occurred_at"])
            rows.append(row)
        return rows

    def pages(self, since: datetime | None = None):
        rows = self._rows()
        if since is not None:
            rows = [row for row in rows if row["occurred_at"] >= since]
        fail_after = os.environ.get(FAIL_AFTER_PAGE_ENV)
        for number, start in enumerate(range(0, len(rows), self.page_size), start=1):
            yield rows[start : start + self.page_size]
            if fail_after is not None and number >= int(fail_after):
                raise RuntimeError(f"injected API failure after page {number} ({FAIL_AFTER_PAGE_ENV})")


# @with_checkpoints sits UNDER @dlt.resource (the other order raises at
# decoration time). Every second page persists the max cursor seen so far to
# _dlt_custom_checkpoints; a failed run resumes from that value minus a
# one-second safety overlap instead of from the incremental window start.
@dlt.resource(
    name="events",
    columns=Event,
    primary_key="id",
    write_disposition="append",
    schema_contract={"tables": "evolve", "columns": "freeze", "data_type": "freeze"},
)
@with_checkpoints(cursor_field="occurred_at", frequency=2)
def events(occurred_at=dlt.sources.incremental("occurred_at", initial_value=EVENTS_INITIAL_TIMESTAMP)):
    for page in FixtureClient("events.jsonl").pages(since=occurred_at.start_value):
        yield page

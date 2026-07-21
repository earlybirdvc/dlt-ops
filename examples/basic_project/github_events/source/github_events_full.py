"""Example full-refresh source: reloads the whole event-type catalog each run.

`write_disposition="replace"` makes every run a full refresh: the destination
table is rebuilt from the fixture instead of appended to. Naming follows the
same conventions as the sibling source (rules 3-5): github_events_full.py <->
[sources.github_events_full] <-> @dlt.source(name="github_events_full") <->
github_events_full_source.
"""

import dlt

from ..resource.events import EventType, FixtureClient


@dlt.resource(
    name="event_types",
    columns=EventType,
    primary_key="name",
    write_disposition="replace",
    schema_contract={"tables": "evolve", "columns": "freeze", "data_type": "freeze"},
)
def event_types():
    for page in FixtureClient("event_types.jsonl").pages():
        yield page


@dlt.source(name="github_events_full")
def github_events_full_source():
    return event_types

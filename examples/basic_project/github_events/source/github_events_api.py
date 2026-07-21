"""Example incremental source: a paginated events API with mid-run checkpoints.

Naming links this module demonstrates (rules 3-5):
- module stem equals the config section: github_events_api.py <-> [sources.github_events_api]
- the source function carries the `_source` suffix: github_events_api_source
- the decorator names the section explicitly: @dlt.source(name="github_events_api")

The `events` resource (incremental cursor + checkpoints) is shared plumbing
and lives with its Pydantic model in ../resource/events.py; `actors` is
declared here to show the in-module idiom.
"""

import dlt

from ..resource.events import Actor, FixtureClient, events


# schema_contract is deliberately omitted on this resource: dlt derives it from
# Actor's extra="forbid" and reaches the canonical contract ({"tables":
# "evolve", "columns": "freeze", "data_type": "freeze"}) on its own. The sibling
# resources spell the same literal out to show the declared form — both routes
# land on one project policy, which is why `validate` accepts either.
@dlt.resource(name="actors", columns=Actor, primary_key="login", write_disposition="replace")
def actors():
    for page in FixtureClient("actors.jsonl").pages():
        yield page


@dlt.source(name="github_events_api")
def github_events_api_source():
    return events, actors

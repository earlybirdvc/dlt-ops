"""String templates for the `init` scaffold (no template engine).

Layout facts (``source/`` / ``resource/`` directory names) come from
``dlt_ops.config`` so the scaffold can never drift from discovery.
The example source family here is the package's ONE canonical runnable
example — anything that needs it (example projects, docs, end-to-end suites)
should render these functions rather than copying the strings.
"""

from dlt_ops.config import RESOURCE_DIR, SOURCE_DIR

DEFAULT_PIPELINE_NAME = "my_pipeline"

# The example source: a fixture-backed, network-free source that validates
# and runs against local DuckDB out of the box.
EXAMPLE_SOURCE_SECTION = "demo_events"
EXAMPLE_RESOURCE_MODULE = "events"
EXAMPLE_RESOURCE_NAME = "events"
EXAMPLE_DATASET = "demo_data"
# Row count in the resource template's fixture data; the init end-to-end test
# asserts exactly this many rows land in DuckDB, so the two cannot drift.
EXAMPLE_ROW_COUNT = 6


def render_config_toml(*, example_section: str | None = None, example_dataset: str = EXAMPLE_DATASET) -> str:
    """Body of the scaffolded .dlt/config.toml.

    default_destination is written REAL (DuckDB needs no credentials) so the
    scaffold never fails its own first validate/run; everything else is a
    fully-commented worked example. When `example_section` is given, a live
    [sources.<section>] block for the example source is appended.
    """
    body = """\
# dlt-ops project configuration.
#
# The [dlt_ops] table below is the project marker: `dlt-ops`
# commands walk up from the current directory until they find a
# .dlt/config.toml containing it.

[dlt_ops]
default_destination = "duckdb"
# default_dataset = "raw_data"          # project-wide dataset default
"""
    if example_section is None:
        body += """
# Per-rule on/off knob for `pipeline validate` (missing entry = on):
# [dlt_ops.rules]
# import_safety = true
"""
    else:
        body += """
# Per-rule on/off knob for `pipeline validate` (missing entry = on):
# [dlt_ops.rules]
# import_safety = true
"""
    body += f"""
# One [sources.<section>] table per source. The section name must equal the
# source module stem (<pipeline>/{SOURCE_DIR}/<section>.py) and the explicit
# @dlt.source(name="<section>") value:
#
# [sources.my_api]
# base_url = "https://api.example.com"  # dlt-native source config
#
# [sources.my_api.dlt_ops]
# schedule = "@daily"                   # required: @hourly|@daily|@weekly|@monthly|@manual
# destination = "duckdb"                # optional; overrides default_destination
# dataset = "raw_my_api"                # optional; overrides default_dataset
"""
    if example_section is not None:
        body += f"""
[sources.{example_section}]
# dlt-native source config would go here (the example client is in-memory).

[sources.{example_section}.dlt_ops]
schedule = "@daily"
dataset = "{example_dataset}"
"""
    return body


SECRETS_TOML = """\
# dlt secrets — destination credentials and source API keys live here, laid
# out per dlt's own conventions:
# https://dlthub.com/docs/general-usage/credentials/setup
#
# Keep this file out of version control. The local DuckDB destination needs
# no credentials, so an empty file is a valid starting point.
"""


def render_example_source_module(section: str = EXAMPLE_SOURCE_SECTION) -> str:
    """Body of <pipeline>/source/<section>.py for the example source."""
    return f'''\
"""Example source: wires the fixture-backed `{EXAMPLE_RESOURCE_NAME}` resource into a source.

Naming conventions this file demonstrates:
- module stem equals the config section: {section}.py <-> [sources.{section}]
- the source function carries the `_source` suffix: {section}_source
- the decorator names the section explicitly: @dlt.source(name="{section}")
"""

import dlt

from ..{RESOURCE_DIR}.{EXAMPLE_RESOURCE_MODULE} import {EXAMPLE_RESOURCE_NAME}


@dlt.source(name="{section}")
def {section}_source():
    return {EXAMPLE_RESOURCE_NAME}
'''


def render_example_resource_module() -> str:
    """Body of <pipeline>/resource/events.py for the example source."""
    return '''\
"""Example resource: a paginated in-memory client serving fixture rows.

A real source would page through an HTTP API here; the fixture client keeps
the example runnable offline (`pipeline validate` also forbids network I/O
at import time). The Pydantic model is the schema: dlt derives typed columns
from `columns=Event` instead of inferring them at load time.
"""

from datetime import UTC, datetime

import dlt
import pydantic


class Event(pydantic.BaseModel):
    id: int
    kind: str
    occurred_at: datetime


_ROWS = [
    {"id": 1, "kind": "signup", "occurred_at": datetime(2026, 1, 1, 9, 0, tzinfo=UTC)},
    {"id": 2, "kind": "login", "occurred_at": datetime(2026, 1, 1, 10, 30, tzinfo=UTC)},
    {"id": 3, "kind": "purchase", "occurred_at": datetime(2026, 1, 2, 11, 15, tzinfo=UTC)},
    {"id": 4, "kind": "login", "occurred_at": datetime(2026, 1, 3, 8, 45, tzinfo=UTC)},
    {"id": 5, "kind": "logout", "occurred_at": datetime(2026, 1, 3, 9, 5, tzinfo=UTC)},
    {"id": 6, "kind": "purchase", "occurred_at": datetime(2026, 1, 4, 16, 20, tzinfo=UTC)},
]


class FixtureClient:
    """Stand-in for a paginated API client; replace with real calls."""

    page_size = 2

    def pages(self):
        for start in range(0, len(_ROWS), self.page_size):
            yield _ROWS[start : start + self.page_size]


@dlt.resource(name="events", columns=Event, primary_key="id", write_disposition="replace")
def events():
    for page in FixtureClient().pages():
        yield page
'''

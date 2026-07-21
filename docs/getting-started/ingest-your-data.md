---
description: The front door for ingesting data with dlt-ops — dlt provides the source (REST, SQL, filesystem, or a custom @dlt.source) and dlt-ops adds the project layout plus the operational verbs; choose a source helper, choose a destination tier, then wire and run one source end to end.
---

# Ingest your data

You came here to move data from a REST API, a database, or a bucket into your warehouse or lake, and you want to know where `dlt-ops` fits. Here is the honest frame: `dlt-ops` ships no connectors and moves zero rows itself — dlt provides the source, and `dlt-ops` wraps it in a project layout plus the operational verbs (`validate`, `run`, `status`, `backfill`) that make an ingestion production-shaped. This page is the router: pick the dlt helper for your source, pick a destination and learn its capability tier, then wire one source end to end.

**At a glance**

- **What `dlt-ops` gives you** — the project layout, discovery, and the operational verbs: `validate`, `run`, `status`, `backfill`.
- **What you bring** — the source itself: a dlt helper (`rest_api`, `sql_database`, `filesystem`) or a `@dlt.resource` you write. `dlt-ops` ships zero connectors and moves zero rows; dlt owns the write.
- **Your first decision** — [which dlt source helper matches yours](#choose-your-source).
- **Your second decision** — [which destination, and its capability tier](#choose-your-destination).

## Choose your source

**`dlt-ops` discovers and runs whatever `@dlt.source` you place in the layout — the connector is dlt's, not ours.** Pick the dlt helper that matches your source, write it into `<pipeline>/source/`, and the naming chain wires the module to its config with no registration code.

| Your source | The dlt helper you bring | Where it lands |
|---|---|---|
| A REST API | dlt's declarative [`rest_api`](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api) source — a `RESTAPIConfig` dict, the [AI-friendly](../guides/build-sources-with-ai.md) path | `<pipeline>/source/orders_api.py` |
| A database, read as a source | dlt's [`sql_database`](https://dlthub.com/docs/dlt-ecosystem/verified-sources/sql_database) source | `<pipeline>/source/warehouse.py` |
| Files or objects in a bucket (S3, GCS, Azure, local) | dlt's [`filesystem`](https://dlthub.com/docs/dlt-ecosystem/verified-sources/filesystem) source | `<pipeline>/source/exports.py` |
| Anything custom | a plain [`@dlt.resource`](https://dlthub.com/docs/general-usage/resource) / `@dlt.source` you write by hand | `<pipeline>/source/<name>.py` |

Every helper above is a dlt feature — its pagination, its auth, and its incremental loading are dlt's; `dlt-ops` adds the layout, discovery, validation, and the operational verbs around it, never connector code. [Add a source](../guides/add-a-source.md) walks one through by hand; [build sources with AI](../guides/build-sources-with-ai.md) has an assistant write the `rest_api` config and lets `validate` vet it. The file name is load-bearing — [project layout](project-layout.md) covers the nine conventions that make discovery work without importing anything.

## Choose your destination

**Where the rows land decides which operational features you get — that is the capability tier, and it resolves per destination, not per install.** A destination reaches full tier when a `DestinationAdapter` is registered for its engine name; everything else dlt can resolve runs at core tier.

| Tier | Destinations | What runs there |
|---|---|---|
| **Full** | `duckdb`, `postgres`, `bigquery` | the core loop, plus the six adapter-gated features: the runs ledger and `status`, checkpoints, backfill, remote `clean`, reconcile, and assertion quarantine |
| **Core** | `snowflake`, `databricks`, `s3`, `gcs`, `azure`, `filesystem` — any destination dlt can resolve | the core loop only: discovery, `validate`, `run` with `fail`/`warn` assertions, schema contracts, scheduling metadata, and trace persistence; the six adapter-gated features refuse loudly |

**Read the second row before you commit to Snowflake, Databricks, or an object store.** `run` and its `fail`/`warn` assertions work on every destination, but core tier has no SQL engine for the [six adapter-gated features](../reference/destinations.md) to live in: observability (the runs ledger, `status`) skips with an INFO line, and a feature your config demands — a checkpoint, a backfill, an assertion `quarantine` — is refused at preflight rather than silently downgraded. Object stores are core tier permanently; Snowflake and Databricks stay core tier until someone registers a `DestinationAdapter` for them.

The tier is per destination, not per install — one project can load into full-tier DuckDB and a core-tier bucket side by side — and every `run` prints its resolved tier (`Capabilities: full` or `Capabilities: core`) before it touches anything. [Destinations and tiers](../concepts/destinations-and-tiers.md) explains how the tier resolves; the [destinations reference](../reference/destinations.md) is the full feature × tier matrix.

## Build one source in the layout

**The shortest real path starts with the layout: scaffold a project, drop one source module into it, and wire the source's destination and schedule in config.** This example uses a tiny in-memory source so it runs offline on DuckDB; swap the fixture for a helper from the [source table](#choose-your-source) and the shape is identical.

Scaffold an empty project — it writes the `.dlt/config.toml` marker (default destination `duckdb`) and one empty pipeline directory:

```bash
dlt-ops init catalog
cd catalog
```

Write the source into `<pipeline>/source/`. The module stem, the `@dlt.source(name=...)`, and the function name all carry the same string — that is the naming chain discovery keys on:

```python title="my_pipeline/source/products.py"
"""Products source: a stand-in for a paginated REST endpoint, in-memory so it runs offline.

Naming chain: module stem `products` = config section [sources.products]
= @dlt.source(name="products") = function name products_source.
"""

import dlt
import pydantic


class Product(pydantic.BaseModel):
    id: int
    name: str
    price_cents: int


# In production this is dlt's `rest_api` source or your own client. Keep module
# level free of network I/O — `validate` fails imports that touch the network.
_CATALOG = [
    {"id": 1, "name": "notebook", "price_cents": 450},
    {"id": 2, "name": "pen", "price_cents": 120},
    {"id": 3, "name": "eraser", "price_cents": 80},
]


@dlt.resource(name="products", columns=Product, primary_key="id", write_disposition="replace")
def products():
    yield _CATALOG


@dlt.source(name="products")
def products_source():
    return products
```

Declare the source's config section and its `dlt_ops` block in `.dlt/config.toml`. `schedule` is required; `destination` and `dataset` are the per-source knobs the [destination table](#choose-your-destination) points at — omit `destination` and the source falls back to the project-wide `default_destination`:

```toml title=".dlt/config.toml"
[sources.products]
# dlt-native source config (API base URL, auth key names) would go here.

[sources.products.dlt_ops]
schedule = "@daily"          # required — one of @hourly|@2hourly|@daily|@weekly|@monthly|@manual
destination = "duckdb"       # optional; overrides default_destination
dataset = "catalog_raw"      # optional; overrides default_dataset
```

## Validate, run, read the ledger

**Three verbs take the source from static-checked to loaded to accounted-for.** `validate` runs every static check before anything executes; `run` loads the rows; `status` reads the outcome back out of the destination.

Validate first — it imports each source in a sandbox, checks the naming chain and the required `schedule`, and confirms every `columns=` model and referenced plugin resolves:

```bash
dlt-ops pipeline validate
```

```text
Validating sources

✓ All sources validated successfully
```

Run the source. The resolved configuration prints first — source, destination, dataset, and the destination's capability tier, each traced to where it came from — then dlt loads the rows:

```bash
dlt-ops pipeline run -s products -y
```

```text
Pipeline Configuration
----------------------------------------
  Source: products
  Function: products_source
  Resources: all (1 total)
  Destination: duckdb
  Dataset: catalog_raw (from .dlt/config.toml)
  Capabilities: full
...
1 load package(s) were loaded to destination duckdb and into dataset catalog_raw
Load package 1784326528.993419 is LOADED and contains no failed jobs
```

`Capabilities: full` is DuckDB's tier line: the run recorded its outcome to the `_dlt_ops_runs` ledger in the destination itself. `-y` is the non-interactive flag a scheduler uses; the exit code is `0` on success and `1` on any failure. Read the ledger back:

```bash
dlt-ops pipeline status
```

```text
Source: products
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  completed  2026-07-17 22:15:28  2026-07-17 22:15:29  3         cli        -               b241c4466660
```

Three typed rows are in `catalog_raw.products`, and the ledger — written destination-side, not on the machine that triggered the run — keeps the outcome. That is the whole loop; everything else is depth on one of these steps.

## Where next

- [Build sources with AI](../guides/build-sources-with-ai.md) — let an assistant write the source; `dlt-ops`'s `validate` loop is what makes it trustworthy
- [Add a source](../guides/add-a-source.md) — the same path by hand, in depth, with the naming chain and its refusals
- [Adopt an existing project](../guides/adopt-existing-project.md) — you already have a dlt project to bring under the layout
- [Deployment and scheduling](../guides/deployment.md) — put the source on a cron, a CI runner, or an orchestrator

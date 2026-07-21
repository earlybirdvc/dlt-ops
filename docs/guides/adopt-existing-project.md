---
description: Task guide — adopt dlt-ops in an existing dlt project one source at a time without breaking what runs now; mark the project root, move a source into the layout, translate the pipeline script into config, add Pydantic column models, and handle the pipeline-name-driven incremental-state reset.
---

# Adopt dlt-ops in an existing dlt project

You have a dlt project that works today — a script that calls `dlt.pipeline(...)` and `pipeline.run(...)`, secrets in `.dlt/`, maybe a cron entry or a Makefile — and you want the dlt-ops layout, validation, and operational verbs without breaking what runs now. This guide adopts one source at a time, leaving the rest of the project running exactly as before. Read [project layout](../getting-started/project-layout.md) for the conventions you are opting into and [add a source](add-a-source.md) for the greenfield version of the same steps.

**Prerequisites**

- A working dlt project — a `@dlt.source` module and a script that calls `dlt.pipeline(...).run(...)`, with secrets in `.dlt/`.
- `dlt-ops` with the DuckDB extra — see [installation](../getting-started/installation.md).

**Steps at a glance**

1. [Mark the project root](#1-mark-the-project-root)
2. [Move one source into the layout](#2-move-one-source-into-the-layout)
3. [Translate the pipeline script into config](#3-translate-the-pipeline-script-into-config)
4. [Add the column models and validate](#4-add-the-column-models-and-validate)
5. [Run and verify](#5-run-and-verify)

## What adoption changes — and what it doesn't

**Your source functions do not change** — a `@dlt.source` and its `@dlt.resource` functions are plain dlt code; dlt-ops discovers and runs them, it does not rewrite them. What adoption adds is around the source:

- a mandatory directory layout so discovery can find it without importing it;
- a `[sources.<name>.dlt_ops]` config block for schedule and dataset;
- one Pydantic `columns=` model per resource;
- the operational verbs (`validate`, `status`, `reconcile`, `backfill`, `clean`) that read that structure.

The one behavioral change to know about up front is the pipeline name — dlt-ops runs each source under a fixed `<source>_pipeline` name, which has consequences for existing state covered in [Existing data and incremental state](#existing-data-and-incremental-state) below.

## The project you start from

**A minimal but representative starting point:** one source module and a script that runs it.

```text
acme/
├── .dlt/
│   ├── config.toml
│   └── secrets.toml
├── github_api.py        # @dlt.source(name="github_api") + two @dlt.resource functions
├── run_pipeline.py      # dlt.pipeline(pipeline_name="github", ...).run(github_api_source())
└── Makefile
```

`github_api.py` is an ordinary dlt source — two resources, `repositories` (full refresh) and `issues` (incremental on `updated_at`) — and `run_pipeline.py` builds a pipeline and runs it:

```python
pipeline = dlt.pipeline(
    pipeline_name="github",
    destination="duckdb",
    dataset_name="github_data",
)
load_info = pipeline.run(github_api_source())
```

It runs today with plain dlt (`python run_pipeline.py` loads 3 repositories and 4 issues into `github_data`), and it will keep running throughout this guide.

## 1. Mark the project root

**dlt-ops finds a project by walking up from the current directory for a `.dlt/config.toml` that contains a `[dlt_ops]` table** — that table is the project marker. Your project already has `.dlt/config.toml`, so pointing dlt-ops at it before you add the marker refuses:

```bash
dlt-ops pipeline list
```

```text
Error: No dlt-ops project found at /path/to/acme or any parent directory: looked for .dlt/config.toml with a [dlt_ops] table. Run `dlt-ops init` to scaffold one.
```

Do not run `dlt-ops init` on an existing project — it is for greenfield scaffolding and refuses rather than touch a config file you already own:

```bash
dlt-ops init .
```

```text
Error: .dlt/config.toml already exists — refusing to overwrite it. There is no --force; remove the file yourself to re-initialize.
```

Adoption is additive hand-editing instead. Add a `[dlt_ops]` table to your existing `.dlt/config.toml`; everything already in the file (dlt's `[runtime]`, destination config, source config) stays untouched:

```toml
[dlt_ops]
default_destination = "duckdb"
```

## 2. Move one source into the layout

**Discovery scans `<pipeline>/source/` and `<pipeline>/resource/` directories directly under the root** — a module sitting loose at the root is invisible to it. Create one pipeline directory and move the source module into its `source/` folder:

```bash
mkdir -p ingest/source ingest/resource
mv github_api.py ingest/source/github_api.py
```

The module keeps the naming chain the layout enforces — module stem `github_api`, function `github_api_source`, and `@dlt.source(name="github_api")` all agree, which is how discovery maps the file to `[sources.github_api]` without importing it. If your existing source already follows that chain (many do), the move is all the restructuring it needs; if not, [project layout](../getting-started/project-layout.md) lists what has to line up.

## 3. Translate the pipeline script into config

**The parameters your script passed to `dlt.pipeline(...)` become config under the source's section.** The mapping:

| Plain-dlt script | dlt-ops home |
|---|---|
| `pipeline_name=` | Derived as `<source>_pipeline` — not configurable |
| `destination=` | `[dlt_ops].default_destination`, or `[sources.<name>.dlt_ops].destination` to override per source |
| `dataset_name=` | `[sources.<name>.dlt_ops].dataset`, or `[dlt_ops].default_dataset` project-wide |
| cron / Makefile cadence | `[sources.<name>.dlt_ops].schedule` — a coarse tag, compiled by the orchestrator adapter |
| `.dlt/secrets.toml` | Unchanged — dlt-ops reads secrets through dlt's own resolver |

Add the source's section and its `dlt_ops` block to `.dlt/config.toml`:

```toml
[sources.github_api]
# dlt-native source config (API base URL, pagination knobs) still lives here.

[sources.github_api.dlt_ops]
schedule = "@daily"
dataset = "github_data"
```

Your `.dlt/secrets.toml` needs no changes at all. The source still resolves `api_token = dlt.secrets.value` from `[sources.github_api]` in that file, because dlt-ops runs the source through dlt's normal config resolution — the same section keys work unchanged.

## 4. Add the column models and validate

**The one convention an existing source almost never satisfies is [Rule 9](../getting-started/project-layout.md):** every `@dlt.resource` declares its schema as a Pydantic model via `columns=`. `validate` catches the gap:

```bash
dlt-ops pipeline validate
```

```text
✗ 2 error(s):
  [github_api] resource.repositories: @dlt.resource for 'repositories' is missing columns= hint. Add a Pydantic model: @dlt.resource(columns=MyModel, ...)
  [github_api] resource.issues: @dlt.resource for 'issues' is missing columns= hint. Add a Pydantic model: @dlt.resource(columns=MyModel, ...)
```

Declare a model per resource and pass it as `columns=`. Match the field types to what your data already is — the model is the schema dlt loads with (see [Troubleshooting](#troubleshooting-model-types-must-match-the-existing-tables) for what happens when they disagree with an existing table):

```python
class Repository(pydantic.BaseModel):
    id: int
    name: str
    stars: int
    language: str


@dlt.resource(name="repositories", columns=Repository, write_disposition="replace", primary_key="id")
def repositories():
    ...
```

Validate again — the layout, config, naming chain, and column models now all check out:

```bash
dlt-ops pipeline validate
```

```text
Validating sources

✓ All sources validated successfully
```

## 5. Run and verify

**Run the source through dlt-ops; the resolved configuration prints first** — confirm the destination, dataset, and tier match what you configured:

```bash
dlt-ops pipeline run -s github_api -y
```

```text
Pipeline Configuration
----------------------------------------
  Source: github_api
  Function: github_api_source
  Resources: all (2 total)
  Destination: duckdb
  Dataset: github_data (from .dlt/config.toml)
  Capabilities: full

...
repositories: 3  | Time: 0.03s | Rate: 114.96/s
issues: 4  | Time: 0.01s | Rate: 292.57/s
1 load package(s) were loaded to destination duckdb and into dataset github_data
The duckdb destination used duckdb:///.../github_api_pipeline.duckdb location to store data
```

`Capabilities: full` means DuckDB has a registered `DestinationAdapter`, so the run wrote its outcome to the `_dlt_ops_runs` ledger. Read it back:

```bash
dlt-ops pipeline status
```

```text
Source: github_api
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  completed  2026-07-17 14:23:30  2026-07-17 14:23:31  7         cli        -               2f9421e2606d
```

That is a working migrated source: discovered by layout, validated statically, run through the shared runner, and recorded in the ledger.

## Existing data and incremental state

**This is the part most likely to surprise you.** dlt-ops runs every source under the pipeline name `<source>_pipeline` — here `github_api_pipeline` — and that name is fixed, derived from the source section, not read from config (`dlt_ops/runs/writer.py::pipeline_name_for_source`). If your old script used a different `pipeline_name` (`github`, above), dlt treats the migrated run as a **new pipeline**, and pipeline-scoped state does not carry over:

- **The DuckDB database file changes.** dlt names a DuckDB file after the pipeline, so the migrated run writes a fresh `github_api_pipeline.duckdb` beside the old `github.duckdb` — two physically separate databases. The old file is left untouched; its data is not migrated into the new one.
- **Incremental cursors reset.** dlt stores each pipeline's incremental state under its pipeline name, so the new name starts with an empty cursor. The first dlt-ops run re-extracts the full window — all 4 issues again — and only then does the cursor settle. The second run is incremental as normal:

```bash
dlt-ops pipeline run -s github_api -y   # again
```

```text
Normalized data for the following tables:
- repositories: 3 row(s)
```

(`issues` is absent from the second run — the cursor, now stored under `github_api_pipeline`, filtered every row.)

The cursor reset happens even when the destination and dataset stay identical — it is keyed on the pipeline name, not the tables. Four runs against one DuckDB file and one dataset, varying only the pipeline name, make it visible:

```text
pipeline_name='legacy_name'   issues_extracted_this_run=4
pipeline_name='legacy_name'   issues_extracted_this_run=0
pipeline_name='ops_name'      issues_extracted_this_run=4
pipeline_name='ops_name'      issues_extracted_this_run=0
final issues rows in wh_data.issues: 4
state pipelines present: ['legacy_name', 'ops_name']
```

Runs one and two settle `legacy_name`'s cursor (4 rows extracted, then 0). Run three switches to a new pipeline name against the very same tables and re-extracts the whole window (4 again); run four settles the new name. Because both resources use `merge`, the table stays at 4 rows throughout — no duplication — but the re-extract still ran.

So on a warehouse, where the physical database is fixed by credentials and the dataset by config rather than by the pipeline name, keeping the same `dataset` means the migrated source writes to the **same tables** and the data is not duplicated. What still resets is the cursor: one full re-extract of the window.

What that re-extract costs depends on write disposition. For `replace` and `merge` resources (primary key set) it is idempotent — the window is rebuilt in place and the dataset ends identical, as the `4` above shows. For an `append` resource it re-appends the window, so size the first run accordingly.

Two honest options, both verified:

1. **Accept the one-time full refresh (recommended default).** For `replace`/`merge` sources the first dlt-ops run rebuilds the dataset with no lasting difference, and every run after is incremental. This is the simplest path and correct for most projects.
2. **Align the name before cutover.** If a re-extract is expensive, rename the legacy pipeline to `<source>_pipeline` and run it once on your existing schedule *before* migrating. The one-time re-extract then happens ahead of the cutover, and dlt-ops resumes the settled cursor. For DuckDB this also aligns the database filename, so the migrated pipeline reuses the same file and data instead of starting a new one.

## The rest of your project keeps running

**Adopt one source; leave the others alone.** Discovery only sees modules inside a `<pipeline>/source/` directory, so a second source still sitting at the project root — say a `billing.py` run by its own `run_billing.py` — is invisible to dlt-ops and entirely unaffected:

```bash
dlt-ops pipeline list
```

```text
Found 1 source(s)

Name                           Pipeline        Schedule   Resources
----------------------------------------------------------------------
github_api                     ingest          @daily     2
```

Nothing about dlt-ops stops the un-migrated source from running exactly as before — its old script still works, into its own pipeline and dataset:

```bash
python run_billing.py
```

```text
1 load package(s) were loaded to destination duckdb and into dataset billing_data
```

Migrate the rest on your own schedule, one source per pipeline directory, whenever each is ready.

## Warehouse-side footprint

**On a full-tier destination, dlt-ops keeps its operational state in sidecar tables in your dataset**, alongside dlt's own `_dlt_*` tables:

- `_dlt_ops_runs` — the runs ledger; one row per run, updated in place to its terminal status (see [runs ledger](../concepts/runs-ledger.md)).
- `_dlt_backfills` — one row per backfill chunk, written only when you run `backfill`.
- `_dlt_custom_checkpoints` — pagination checkpoints, written only by resources using `@with_checkpoints`.
- `_dlt_rejected` — quarantined rows, written only when an assertion's policy is `quarantine`.

These need ordinary DDL and DML rights in the one dataset the source already writes to: `CREATE TABLE` (the sidecars are created lazily with `CREATE TABLE IF NOT EXISTS`), `INSERT` and `UPDATE` for the ledger row, and `SELECT` to read it back. `reconcile` reads column metadata and runs windowed `SELECT`s; the destructive verbs — remote `clean` and checkpoint cleanup — additionally need `DELETE` and `DROP TABLE`.

All of it lands in the same dataset the source already loads into, so there is no separate status store to provision or grant access to. The storage footprint is a handful of narrow bookkeeping rows per run (the ledger caps its error text at 500 characters); `_dlt_rejected` is the only sidecar that holds row data, and only for resources you configure to quarantine.

## Leaving dlt-ops later

**Nothing holds your data or your code hostage.** Because your source functions are plain dlt code, they run under a plain `dlt.pipeline(...)` with no dlt-ops import at all — the same functions dlt-ops was discovering:

```python
import sys

import dlt

sys.path.insert(0, "ingest/source")
from github_api import github_api_source

pipeline = dlt.pipeline(pipeline_name="github_ejected", destination="duckdb", dataset_name="ejected_data")
print(pipeline.run(github_api_source()))
```

```text
1 load package(s) were loaded to destination duckdb and into dataset ejected_data
The duckdb destination used duckdb:///.../github_ejected.duckdb location to store data
```

To leave, point your own scripts at the source modules where they now live, drop the sidecar tables (`_dlt_ops_runs` and the others above) if you want them gone, and delete the `[dlt_ops]` config. The data in your dataset stays put — it was always written by dlt, not by dlt-ops.

## Two patterns: sub-hourly schedules and multi-project repos

**Sub-hourly cadence.** The `schedule` value is a coarse grouping tag, and its finest built-in is `@hourly` — it is metadata the orchestrator adapter compiles into a native schedule, not a scheduler inside dlt-ops. Cadences finer than hourly are the orchestrator's to own: it fires the schedule group as often as you configure it. See [scheduling and orchestration](../concepts/scheduling-and-orchestration.md) for the tag set and how adapters materialize it.

**Multiple projects in one repo.** Each dlt-ops project is a directory with its own `[dlt_ops]`-marked `.dlt/config.toml`. To operate one without `cd`-ing into it, pass `-r`/`--root`. Two roots in one tree resolve independently:

```bash
dlt-ops -r analytics pipeline list
```

```text
Found 1 source(s)
alpha                          ingest          @daily     1
```

```bash
dlt-ops -r marketing pipeline list
```

```text
Found 1 source(s)
beta                           ingest          @hourly    1
```

Without `-r`, dlt-ops walks up from the current directory and stops at the first `[dlt_ops]` marker — so a monorepo with several projects wants `-r` (or a per-project working directory) in every command and CI step.

## Troubleshooting: model types must match the existing tables

**Adding a `columns=` model against tables dlt already populated is a schema change if the model's types differ from what dlt inferred.** dlt inferred `updated_at` (an ISO-8601 string) as `timestamp with time zone`; a model that declares it `str` (text) is a data-type change, and the canonical freeze contract rejects it against the existing table with a `DataValidationError` at extract. Declare model field types that match the columns already in the destination — inspect the live schema if you are unsure. This only bites when the migrated pipeline writes to tables that already exist (a warehouse where you kept the dataset, or an aligned pipeline name); a fresh `<source>_pipeline.duckdb` has no prior schema to disagree with.

## Where next

- [Project layout](../getting-started/project-layout.md) — every convention you adopted here, each with its reason
- [Deployment](deployment.md) — the ladder from dev loop to cron to orchestrator for the source you migrated
- [Runs ledger](../concepts/runs-ledger.md) — the `_dlt_ops_runs` table `status` reads, and its three absence states

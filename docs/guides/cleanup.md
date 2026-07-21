---
description: Task guide — remove one resource (its table, incremental state, and checkpoints) or a whole source from a live destination with dlt-ops pipeline clean, verifying at each step that sibling sources and shared dlt system tables survive; covers the dry-run, the remote/local split, and the core-tier --local-only refusal.
---

# Clean up a resource or a source

This guide removes one resource from a live destination — its table, its incremental state, its checkpoints — then removes a whole source, and verifies at every step that nothing else was touched.

**Prerequisites**

- `dlt-ops` with the DuckDB extra — see [installation](../getting-started/installation.md).
- A checkout of the [repository](https://github.com/earlybirdvc/dlt-ops) for the `examples/basic_project` used throughout.
- DuckDB is full tier, so remote cleanup (the destination-side half) is available; step 6 shows the core-tier refusal.

**Steps at a glance**

1. [Start state: two sources sharing one DuckDB file](#1-start-state-two-sources-sharing-one-duckdb-file)
2. [Dry-run the selective clean](#2-dry-run-the-selective-clean)
3. [Clean the resource and verify](#3-clean-the-resource-and-verify)
4. [Clean the whole source](#4-clean-the-whole-source)
5. [Verify: rows deleted, tables kept](#5-verify-rows-deleted-tables-kept)
6. [Core tier: `--local-only`](#6-core-tier-local-only)

## Why clean exists and what it touches

**Selective cleanup is the gap `pipeline clean` exists to fill:** removing one resource's data *and* its incremental state from a live destination has no supported path in dlt (dlt's own `pipeline.drop()` is whole-pipeline removal), and hand-deleting just the table leaves the incremental cursor behind, so the next run silently resumes from where the deleted data ended instead of re-extracting it.

`clean` operates on both halves of a pipeline's footprint: **remote** (data tables in the destination, resource entries inside dlt's `_dlt_pipeline_state` blob, rows in the `_dlt_custom_checkpoints` table) and **local** (the pipeline working directory under `~/.dlt/pipelines/`, or `DLT_DATA_DIR`). By default it cleans both; `--local-only` / `--remote-only` narrow it. All remote surgery goes through the [`DestinationAdapter` boundary](../concepts/destinations-and-tiers.md), which makes remote cleanup a full-tier verb — step 6 shows what happens without an adapter.

## 1. Start state: two sources sharing one DuckDB file

**The start state is the repository's `examples/basic_project`: two sources that will share one DuckDB file.** Copy it out — `github_events_api` (an `events` resource with an incremental cursor and checkpoints, plus a cursor-less `actors`) and `github_events_full` (an `event_types` catalog), both loading into the `github_events_raw` dataset:

```bash
cp -R examples/basic_project /tmp/dlt-demo
cd /tmp/dlt-demo
```

By default dlt gives each pipeline its own DuckDB file, which would hide the most important thing this guide demonstrates — what happens to **shared** system tables when one of several pipelines in a dataset is cleaned. Point both pipelines at one file by appending to `.dlt/secrets.toml` (dlt refuses `credentials` keys in `config.toml`, even when the "credential" is just a file path; the path is cwd-relative, so run commands from the project root):

```toml
[destination.duckdb]
credentials = "github_events.duckdb"
```

Run both sources:

```bash
dlt-ops pipeline run -s github_events_api -y
dlt-ops pipeline run -s github_events_full -y
```

```text
2026-07-17 10:24:15|[INFO]|root|[events] Checkpoint saved: page 2, 6 records, value: 2026-01-01T05:00:00+00:00
...
1 load package(s) were loaded to destination duckdb and into dataset github_events_raw
```

Now look at what actually exists in the destination — the data tables, plus dlt's shared system tables and the dlt-ops ones:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("github_events.duckdb", read_only=True)
print(con.sql("SELECT table_name FROM information_schema.tables WHERE table_schema='github_events_raw' ORDER BY table_name"))
print(con.sql("SELECT pipeline_name, resource_name, status FROM github_events_raw._dlt_custom_checkpoints"))
con.close()
PY
```

```text
┌─────────────────────────┐
│       table_name        │
├─────────────────────────┤
│ _dlt_custom_checkpoints │
│ _dlt_loads              │
│ _dlt_ops_runs           │
│ _dlt_pipeline_state     │
│ _dlt_trace              │
│ _dlt_version            │
│ actors                  │
│ event_types             │
│ events                  │
└─────────────────────────┘

┌────────────────────────────┬───────────────┬───────────┐
│       pipeline_name        │ resource_name │  status   │
├────────────────────────────┼───────────────┼───────────┤
│ github_events_api_pipeline │ events        │ completed │
│ github_events_api_pipeline │ events        │ completed │
│ github_events_api_pipeline │ events        │ completed │
└────────────────────────────┴───────────────┴───────────┘
```

The three `_dlt_*` state tables (`_dlt_pipeline_state`, `_dlt_loads`, `_dlt_version`) are dlt's, and they are **shared by every pipeline writing to this dataset** — both sources' rows live in the same tables. That fact drives cleanup's cardinal safety rule: data tables are dropped, but system tables only ever have *rows* deleted, never the table itself.

## 2. Dry-run the selective clean

**Remove the `events` resource from `github_events_api` — but dry-run first.** `--dry-run` prints the plan and exits without touching anything:

```bash
dlt-ops pipeline clean -s github_events_api -r events --dry-run
```

```text
Cleanup Plan:

  Source:    github_events_api
  Pipeline:  github_events_api_pipeline
  Resources: events

  Local:  update state.json + schema (keep working dir)
  Remote: github_events_raw
          - 1 data table(s): events
          - 1 resource state(s)
          - state: surgical update (remove resource entries)
          - checkpoints for 1 resource(s)

Dry-run mode: no changes will be made
```

The plan names the three remote targets: the `events` data table (dropped), the resource's entry inside the pipeline state blob (surgically removed — the state is decoded, the one resource entry deleted, and the result re-encoded and appended as a new state version), and its checkpoint rows. Locally, a selective clean edits `state.json` and deletes the schema file but keeps the working directory, because the source's other resources still live there. Table identification uses a three-tier fallback — local schema file, then the destination's `_dlt_version` table, then source introspection — so `clean` works even on a machine that never ran the pipeline.

## 3. Clean the resource and verify

**The real command asks for confirmation; `--auto-approve` skips the prompt** (the flag for scripts):

```bash
dlt-ops pipeline clean -s github_events_api -r events --auto-approve
```

```text
Cleaning...

10:25:16|dlt_ops.discovery.cleanup|Dropped table: github_events_raw.events
10:25:16|dlt_ops.discovery.cleanup|Removed resource state: github_events_api.events
10:25:16|dlt_ops.discovery.cleanup|Updated pipeline state in destination
10:25:16|dlt_ops.discovery.cleanup|Deleted checkpoints for resource: events
10:25:16|dlt_ops.discovery.cleanup|Updated local state.json
Local:
  - state.json (resource entries removed)
  - schema (deleted github_events_api.schema.json)
Remote:
  - table: events
  - state: updated (removed 1 resource(s))
  - checkpoint: events

Cleanup complete
```

Verify the blast radius — `events` gone, everything else intact, checkpoint rows deleted:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("github_events.duckdb", read_only=True)
print(con.sql("SELECT table_name FROM information_schema.tables WHERE table_schema='github_events_raw' AND table_name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY table_name"))
print(con.sql("SELECT count(*) AS checkpoint_rows FROM github_events_raw._dlt_custom_checkpoints WHERE resource_name = 'events'"))
con.close()
PY
```

```text
┌─────────────┐
│ table_name  │
├─────────────┤
│ actors      │
│ event_types │
└─────────────┘

┌─────────────────┐
│ checkpoint_rows │
├─────────────────┤
│               0 │
└─────────────────┘
```

The incremental state went with it. Re-run the source and `events` starts over from its initial cursor value and re-extracts everything — which is the point of cleaning a resource, and exactly what dropping only the table would *not* have done:

```bash
dlt-ops pipeline run -s github_events_api -y
```

```text
2026-07-17 10:25:50|[INFO]|root|[events] Run isolation: value=2026-01-01T00:00:00+00:00, run_id=2db0bb0b60653f75
...
events: 20  | Time: 0.09s | Rate: 230.05/s
1 load package(s) were loaded to destination duckdb and into dataset github_events_raw
```

## 4. Clean the whole source

**Omit `-r` and the unit becomes the source:** all its data tables, all its state, its local working directory — and row-level deletes from the shared system tables:

```bash
dlt-ops pipeline clean -s github_events_api --dry-run
```

```text
Cleanup Plan:

  Source:    github_events_api
  Pipeline:  github_events_api_pipeline
  Resources: all (2 total)

  Local:  ~/.dlt/pipelines/github_events_api_pipeline
  Remote: github_events_raw
          - 2 data table(s): events, actors
          - 2 resource state(s)
          - system tables: DELETE rows from _dlt_pipeline_state, _dlt_loads, _dlt_version
          - checkpoints for 2 resource(s)

Dry-run mode: no changes will be made
```

```bash
dlt-ops pipeline clean -s github_events_api --auto-approve
```

```text
Cleaning...

10:26:17|dlt_ops.discovery.cleanup|Dropped table: github_events_raw.events
10:26:17|dlt_ops.discovery.cleanup|Dropped table: github_events_raw.actors
10:26:17|dlt_ops.discovery.cleanup|Deleted _dlt_pipeline_state rows where pipeline_name = github_events_api_pipeline
10:26:17|dlt_ops.discovery.cleanup|Deleted _dlt_version rows where schema_name = github_events_api
10:26:17|dlt_ops.discovery.cleanup|Deleted _dlt_loads rows where schema_name = github_events_api
10:26:17|dlt_ops.discovery.cleanup|Deleted _dlt_custom_checkpoints rows where pipeline_name = github_events_api_pipeline
10:26:17|dlt_ops.discovery.cleanup|Removed local pipeline directory: ~/.dlt/pipelines/github_events_api_pipeline
Local:
  - ~/.dlt/pipelines/github_events_api_pipeline
Remote:
  - table: events
  - table: actors
  - state: _dlt_pipeline_state (rows deleted)
  - state: _dlt_version (rows deleted)
  - state: _dlt_loads (rows deleted)
  - state: _dlt_custom_checkpoints (rows deleted)

Cleanup complete
```

## 5. Verify: rows deleted, tables kept

**This is the shared-system-tables fact, checked against the live file.** The state tables still exist; only `github_events_api`'s rows are gone; the sibling source's row in each — and its `event_types` data — survived untouched:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("github_events.duckdb", read_only=True)
print(con.sql("SELECT table_name FROM information_schema.tables WHERE table_schema='github_events_raw' ORDER BY table_name"))
print(con.sql("SELECT schema_name, count(*) AS rows FROM github_events_raw._dlt_loads GROUP BY 1 ORDER BY 1"))
print(con.sql("SELECT count(*) AS event_type_rows FROM github_events_raw.event_types"))
con.close()
PY
```

```text
┌─────────────────────────┐
│       table_name        │
├─────────────────────────┤
│ _dlt_custom_checkpoints │
│ _dlt_loads              │
│ _dlt_ops_runs           │
│ _dlt_pipeline_state     │
│ _dlt_trace              │
│ _dlt_version            │
│ event_types             │
└─────────────────────────┘

┌────────────────────┬───────┐
│    schema_name     │ rows  │
├────────────────────┼───────┤
│ _dlt_traces        │     3 │
│ github_events_full │     1 │
└────────────────────┴───────┘

┌─────────────────┐
│ event_type_rows │
├─────────────────┤
│               4 │
└─────────────────┘
```

Had cleanup dropped `_dlt_loads` instead of deleting rows from it, `github_events_full` would have lost its load history and its persisted state — that is why "DELETE rows, never DROP" is the rule for anything shared. Two tables are deliberately not in `clean`'s scope at all: `_dlt_ops_runs` (the [runs ledger](../concepts/runs-ledger.md) — the operational record of what ran survives the removal of what it ran on) and `_dlt_trace`; the `_dlt_traces` rows above are that machinery's own internal pipeline state, equally untouched.

## 6. Core tier: `--local-only`

**Remote cleanup speaks SQL to the destination, so it needs a registered `DestinationAdapter`.** On a [core-tier destination](../concepts/destinations-and-tiers.md) — here a scaffolded demo project (`dlt-ops init demo --example`) pointed at a local `filesystem` bucket and run once — the remote half refuses:

```bash
dlt-ops pipeline clean -s demo_events --auto-approve
```

```text
Error: destination 'filesystem' has no DestinationAdapter — remote cleanup needs one (core mode). Clean local state with --local-only, or register an adapter; see docs/reference/destinations.md.
```

The command exits 1 and nothing is deleted, on either side. The local half needs no adapter — `--local-only` works at any tier:

```bash
dlt-ops pipeline clean -s demo_events --local-only --auto-approve
```

```text
Cleanup Plan:

  Source:    demo_events
  Pipeline:  demo_events_pipeline
  Resources: all (1 total)

  Local:  ~/.dlt/pipelines/demo_events_pipeline

Cleaning...

10:32:56|dlt_ops.discovery.cleanup|Removed local pipeline directory: ~/.dlt/pipelines/demo_events_pipeline
Local:
  - ~/.dlt/pipelines/demo_events_pipeline

Cleanup complete
```

The bucket's files are untouched — on core tier, removing destination-side data is your storage tooling's job; `clean` refuses to half-do it.

!!! warning "The dlt-version guard"
    Remote cleanup rewrites dlt-internal state tables whose layout is reverse-engineered and re-verified per dlt minor. On a dlt minor outside the verified set ([compatibility](../reference/compatibility.md); currently 1.27.x–1.29.x) it raises `CleanupUnsupportedError` and refuses to guess against an unverified layout — the one feature in the package that gates on the verified matrix. `clean --local-only` and every other verb keep working; dlt's own `pipeline.drop()` remains the whole-pipeline escape hatch on any version.

## The first run after a selective clean

**Selective cleanup deletes the pipeline's local schema file — dlt re-derives it from the source on the next run, and surgically editing it instead would break dlt's schema-hash verification.** That re-run rebuilds the schema as it extracts and loads normally (step 3 above re-extracted all 20 `events` rows), and it records its [runs-ledger](../concepts/runs-ledger.md) row like any other run: the ledger writer reaches the destination's `_dlt_ops_runs` table through the [`DestinationAdapter`](../concepts/destinations-and-tiers.md) boundary, independently of the local schema, so a wiped local schema never costs a run its `status` row. dlt logs one `restored from destination with inconsistent state` notice while it re-derives the schema — expected, and absent from the following run once the file is back.

## Where next

- [Destinations and tiers](../concepts/destinations-and-tiers.md) — why remote `clean` is a full-tier verb, and what core tier keeps
- [Runs ledger](../concepts/runs-ledger.md) — the operational record `clean` deliberately leaves alone
- [Compatibility](../reference/compatibility.md) — the verified dlt matrix behind the version guard

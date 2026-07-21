---
description: The dlt-ops first-run loop, fully offline on DuckDB — scaffold an example project, validate it, run into DuckDB, read the runs ledger, then tour pre-load assertions, checkpoint resume, drift detection, selective cleanup, and backfill.
---

# Quickstart

This walks the full first-run loop end to end, entirely offline — no credentials, no network. You scaffold a project, validate it, run a pipeline into local DuckDB, read the runs ledger back, then tour the operational features: pre-load assertions, checkpoint resume, drift detection, selective cleanup, and backfill.

**Prerequisites**

- `dlt-ops` with the DuckDB extra ([installation](installation.md)) — enough for the first half, scaffold through pre-load assertions.
- A repository checkout for the last four sections (checkpoint resume onward): they drive the richer example project that ships in the repository — `git clone` the repo, or follow those sections against your own project instead.
- Everything runs offline: no credentials, no network.

**Steps at a glance**

1. [Scaffold](#scaffold) — `init --example` a runnable, fixture-backed project
2. [Validate](#validate) — every static check runs before anything executes (Tier 1)
3. [Run](#run) — load six typed rows into local DuckDB
4. [Status](#status) — read the runs ledger back out of the destination
5. [Pre-load assertions](#pre-load-assertions) — gate bad data out between extract and load
6. [Checkpoint resume](#checkpoint-resume) — resume mid-pagination after a failure
7. [Drift detection](#drift-detection) — diff the live schema against your models
8. [Selective cleanup](#selective-cleanup) — remove one resource or a whole source, surgically
9. [Backfill](#backfill) — split a window into resumable chunks

![Terminal recording of the first-run loop: dlt-ops init --example scaffolds the demo project, pipeline validate passes, pipeline run -s demo_events -y loads six rows into local DuckDB, and pipeline status reads the completed run back from the ledger](../assets/terminal/quickstart.gif)

*Steps 1–4 as one real recorded run — regenerate it with `tapes/render.sh` from a repository checkout.*

## Scaffold

**Install the DuckDB extra, then scaffold a complete example project with `init --example`.**

```bash
pip install "dlt-ops[duckdb]"

dlt-ops init demo --example
cd demo
```

```text
✓ Initialized dlt-ops project at /tmp/demo

  .dlt/config.toml
  .dlt/secrets.toml
  my_pipeline/source/
  my_pipeline/resource/
  my_pipeline/source/demo_events.py
  my_pipeline/resource/events.py

Next steps:
  1. Try the example: dlt-ops pipeline run -s demo_events -y
  2. Validate the project: dlt-ops pipeline validate
```

`init --example` scaffolds a complete project with a runnable, fixture-backed source:

```text
demo/
├── .dlt/
│   ├── config.toml        # the project marker + all dlt-ops config
│   └── secrets.toml       # dlt-native secrets (empty; DuckDB needs none)
└── my_pipeline/           # one directory per pipeline
    ├── source/
    │   └── demo_events.py # the source module (stem = config section)
    └── resource/
        └── events.py      # shared @dlt.resource definitions + Pydantic model
```

Three things to notice:

- **The marker.** A directory is a `dlt-ops` project iff `.dlt/config.toml` exists and contains a `[dlt_ops]` table. Every command walks up from the current directory to find it (or takes `--root`).
- **The naming chain.** `source/demo_events.py` defines `demo_events_source` decorated `@dlt.source(name="demo_events")`, configured under `[sources.demo_events]`. Module stem, function suffix, decorator name, and config section all line up — that chain is how discovery works without any registration code. `validate` enforces most of it: the decorator must name its section (`explicit_source_name`), the module stem must equal that section (`module_name_matches_section`), and the section must exist in config (`config_section_required`). The `_source` function-name suffix is the one link no rule checks — it is the fallback discovery uses when a decorator names no section, and a convention worth keeping for readers. [Project layout](project-layout.md) covers all nine conventions.
- **The model is the schema.** `resource/events.py` declares a Pydantic `Event` model and passes it as `columns=Event`. dlt derives typed destination columns from it instead of inferring types at load time.

## Validate

**`validate` is Tier 1 of the enforcement model: everything statically checkable is checked before anything runs.**

```bash
dlt-ops pipeline validate
```

```text
Validating sources

✓ All sources validated successfully
```

Concretely, `validate`:

- imports each source module in a sandbox and fails on network I/O or disk writes at import time (the orchestrator-parse foot-gun);
- checks the naming chain above, the `[sources.<X>]` section, and the required `schedule`;
- checks every `@dlt.resource` declares a Pydantic `columns=` model, and that any declared `schema_contract` is the canonical freeze contract (or a justified evolve opt-in);
- checks every plugin referenced from config (destinations, secret backends, alert sinks) is actually registered and loadable;
- flags resource-name overlaps as errors, and orphan config sections and stale sources (had run history, then stopped) as warnings. Warnings print in every run — the project above simply has none — but only errors fail the command. Reach for `validate --strict` when you want warnings to fail it too.

See exactly which rules resolved and their on/off state:

```bash
dlt-ops pipeline validate --show-resolved-rules
```

```text
Resolved rules (23):
  bigquery_partitioning                on   bigquery
  bigquery_partition_hints             on   bigquery
  import_safety                        on   core
  config_section_required              on   core
  schedule_required                    on   core
  ...
  assertion_config_valid               on   core
  assertion_columns_exist              on   core
  assertion_predicate_resolvable       on   core
```

The `core` provider owns 21 rules; the two `bigquery` rules are plugin-owned and auto-active because the BigQuery plugin loads (its rules are AST and column-hint checks with no BigQuery SDK involved, so they resolve even without the `[bigquery]` extra installed). All but one core rule are on by default — `incremental_cursor_required` ships off, and `--show-resolved-rules` is where you discover it. Every rule can be switched per project in `[dlt_ops.rules]` or exempted per source with a mandatory written reason — the [rules reference](../configuration/rules.md) covers each one.

## Run

**`run` executes a source through the full pipeline and loads its rows into the destination.**

```bash
dlt-ops pipeline run -s demo_events -y
```

The CLI prints the resolved configuration before running — source, resources, destination, dataset, and the destination's capability tier, each traced to where it came from:

```text
Pipeline Configuration
----------------------------------------
  Source: demo_events
  Function: demo_events_source
  Resources: all (1 total)
  Destination: duckdb
  Dataset: demo_data (from .dlt/config.toml)
  Capabilities: full
```

Destination and dataset resolve from config only: `[dlt_ops].default_destination` / `default_dataset`, overridden per source by `[sources.<X>.dlt_ops].destination` / `dataset`. No environment-variable fallbacks, no silent defaults — an unresolved destination is an error.

`Capabilities: full` is the tier line: DuckDB ships a first-party `DestinationAdapter`, so the ledger, checkpoint, and backfill sections below all work against it. Any other destination dlt can resolve (for example a local `filesystem` bucket, which needs no extra) runs the same pipeline at **core tier**: `run` and its assertions work, while the adapter-gated features refuse loudly instead. The [destinations reference](../reference/destinations.md) has the full feature × tier matrix.

Where the data lands: DuckDB writes `demo_events_pipeline.duckdb` in the project directory (one file per source — the underlying dlt pipeline is named `<source>_pipeline`). dlt's own working state (schemas, incremental state) lives in its standard per-user location (`~/.dlt/pipelines/...`, overridable with `DLT_DATA_DIR`). The run loads six typed rows into `demo_data.events`:

```text
1 load package(s) were loaded to destination duckdb and into dataset demo_data
Load package 1784212832.897951 is LOADED and contains no failed jobs
```

`-y` skips the confirmation prompt (that's the flag orchestrators and scripts use). Run without it — or with `-I` — for interactive source/resource selection.

## Status

**Every run and backfill writes one row to a `_dlt_ops_runs` table in the destination itself — inserted at start, updated with the outcome at the end — so the ledger lives where the data lands, not on the machine that triggered the run.**

```bash
dlt-ops pipeline status
```

```text
Source: demo_events
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  completed  2026-07-16 14:40:32  2026-07-16 14:40:33  6         cli        -               c06b0c691238
```

`pipeline status` reads it back; `--json` gives the machine-readable form (one object per source with a `ledger` state and its `runs`).

Two properties of the ledger worth knowing before you rely on it operationally:

- **Absence states are distinct.** A source that never ran shows `no runs recorded`; a ledger `status` cannot read (unreachable destination, unresolved config) shows `ledger unreadable` with the reason; a core-tier destination that cannot carry a ledger at all (no `DestinationAdapter`) shows `ledger unsupported`. An outage never masquerades as an empty history, and a capability gap never masquerades as an outage.
- **Writes are best-effort.** A ledger write failure is logged loudly but never fails the data run — the data landing is the priority; the ledger is observability. See [failure semantics](../concepts/failure-semantics.md) for the full run contract.

Also useful for orientation:

```bash
dlt-ops pipeline list                       # every discovered source + schedule
dlt-ops pipeline resources -s demo_events   # a source's resources
```

```text
Found 1 source(s)

Name                           Pipeline        Schedule   Resources
----------------------------------------------------------------------
demo_events                    my_pipeline     @daily     1
```

Both use the Phase-1 static scan only — they never import your code.

## Pre-load assertions

**Assertions are per-resource data-quality gates, declared in the same config file and enforced between extract and load — failing data never reaches the destination.** Still in the `demo` project, append to `.dlt/config.toml`:

```toml
[sources.demo_events.dlt_ops.assertions.events]
min_rows_per_load = 1
unique_columns = { value = ["id"], on_failure = "quarantine" }
```

`validate` checks the block statically — unknown assertion types, column references that don't exist on the resource's Pydantic model, unresolvable custom predicates (probed in the same import-safety sandbox as your source modules). The run enforces it:

```bash
dlt-ops pipeline run -s demo_events -y    # 6 rows, unique ids: loads normally
```

Now make a gate trip — demand more rows than the example emits (`min_rows_per_load = 10`) and run again:

```text
dlt_ops.assertions.models.AssertionFailedError: assertion 'min_rows_per_load' failed on demo_events.events: row count 6 is below min_rows_per_load 10
```

The run exits 1 **before the load step**: nothing lands in the destination, the extracted batch is dropped (never auto-loaded by a later run), and the ledger records the outcome:

```text
Source: demo_events
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  failed     2026-07-16 14:41:34  2026-07-16 14:41:34  -         cli        -               9c9214e98440
    ✗ AssertionFailedError: assertion 'min_rows_per_load' failed on demo_events.events: row count 6 is below min_rows_per_load 10
  completed  2026-07-16 14:41:22  2026-07-16 14:41:23  6         cli        -               ba86a9215dd1
  completed  2026-07-16 14:40:32  2026-07-16 14:40:33  6         cli        -               c06b0c691238
```

`on_failure` picks the policy per assertion (or per resource): `fail` aborts the run, `quarantine` removes just the failing rows and writes them — full row JSON, violation, assertion params — to a `_dlt_rejected` table in the run's own dataset (joinable to the ledger on `run_id`), and `warn` logs, counts, and loads anyway. Custom row predicates plug in as `[[...custom]]` entries pointing at your own functions. The [config reference](../configuration/reference.md) covers the full syntax; [assertions](../concepts/assertions.md) covers the execution model.

## Checkpoint resume

**Checkpoints persist pagination progress mid-run, so a failed run resumes from the last checkpoint instead of restarting the window.** The scaffolded example is deliberately minimal; the repository's `examples/basic_project` is the fuller demo: an incremental source with mid-run checkpoints, a fault-injection hook, and two sources sharing one pipeline directory. From a repository checkout:

```bash
cp -R examples/basic_project /tmp/dlt-demo && cd /tmp/dlt-demo
```

Its `events` resource pairs an incremental cursor with checkpoints (`with_checkpoints` is a public export of `dlt_ops`):

```python
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
```

Every second page, the maximum cursor value seen so far is persisted to a `_dlt_custom_checkpoints` table in the destination. Simulate an API dying mid-pagination:

```bash
GITHUB_EVENTS_FAIL_AFTER_PAGE=3 dlt-ops pipeline run -s github_events_api -y
```

```text
[events] Checkpoint saved: page 2, 6 records, value: 2026-01-01T05:00:00+00:00
...
RuntimeError: injected API failure after page 3 (GITHUB_EVENTS_FAIL_AFTER_PAGE)
```

The run fails after page 3 (exit code 1), nothing is loaded — but the page-2 checkpoint is already persisted in the destination. Run it again, without the fault:

```bash
dlt-ops pipeline run -s github_events_api -y
```

```text
[events] Resuming from checkpoint: 2026-01-01T05:00:00+00:00 (adjusted: 2026-01-01 04:59:59+00:00)
```

```bash
dlt-ops pipeline status
```

```text
Source: github_events_api
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  completed  2026-07-16 14:42:15  2026-07-16 14:42:16  20        cli        -               d3d89de89c0c
  failed     2026-07-16 14:42:01  2026-07-16 14:42:01  -         cli        -               b4b741780c16
    ✗ PipelineStepFailed: Pipeline execution failed at `step=extract` ... injected API failure after page 3 (GITHUB_EVENTS_FAIL_AFTER_PAGE)

Source: github_events_full
  no runs recorded
```

The second run resumed from the checkpointed cursor (minus the one-second safety overlap visible in the `adjusted:` log line) instead of re-extracting the whole window, and the ledger keeps both outcomes. Resume trusts the checkpoint, not the destination, so it is deliberately not a full replay — rows the failed run extracted below the checkpoint but never loaded are skipped, and a windowed [backfill](../concepts/backfill.md) is the recovery path for them.

The untouched sibling source shows `no runs recorded` — one of the three distinct absence states. [Checkpoints](../concepts/checkpoints.md) covers the semantics.

## Drift detection

**The reconciler diffs the live destination schema against your declared Pydantic models:**

```bash
dlt-ops pipeline reconcile -s github_events_api --dry-run
```

```text
Dry-run: alert emission suppressed

Source: github_events_api  |  Findings: 0  |  Duration: 1.06s
  ✓ No drift
```

If a column appears in the destination behind the model's back (someone `ALTER TABLE`s, or an evolving contract lets a surprise field through), `reconcile` reports it — resource, columns, inferred types, first-seen timestamp, and a reproduce query — and emits an alert event through the configured alert sinks (`[dlt_ops] alert_sinks`; the default sink writes structured log lines). `--dry-run` prints findings without emitting.

Additive drift is half the story. A column can also silently go dark — the provider stops sending a field, and because ingestion accepts nulls under every contract mode, nothing fails. With a `load_timestamp_column` configured (the example project sets it; see the [config reference](../configuration/reference.md)), `--include-removal` adds a windowed coverage scan that flags model columns whose recent non-null coverage collapsed against a baseline window:

```bash
dlt-ops pipeline reconcile -s github_events_api --include-removal --dry-run
```

```text
Dry-run: alert emission suppressed

Source: github_events_api  |  Findings: 0  |  Duration: 0.84s
  ✓ No drift

Source: github_events_api (removal)  |  Findings: 0  |  Duration: 0.61s
  ✓ No drift
```

Without the timestamp column the removal scan is skipped with a warning; the additive scan still runs.

## Selective cleanup

**`clean` is the selective delete — scope it to a single resource or a whole source, removed from the live destination.**

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

`clean` removes data tables, incremental state entries, checkpoint rows, and local working state — for the whole source or a single resource (`-r events`). dlt's own `dlt pipeline <name> drop <resource>` already covers the destination tables and the incremental state; what `clean` adds is the dry run, the local working directory, dlt-ops' checkpoint rows, and the source-level unit. `--dry-run` shows the plan; the real thing asks for confirmation unless you pass `--auto-approve`.

## Backfill

**`pipeline backfill` splits a window into sequential chunks, each with injected `[from, to)` bounds, its own ledger row, and its own entry in a `_dlt_backfills` state table — so re-running the same window skips completed chunks and retries failed ones:**

```bash
dlt-ops pipeline backfill github_events_api --from 2026-01-01T00:00:00Z --to 2026-01-02T00:00:00Z --chunk 6h
```

```text
Error: backfill bounds were supplied but resource(s) without an incremental cursor are selected: actors. Declare a dlt.sources.incremental cursor or deselect them.
```

That refusal is Tier 2 of the enforcement model working as intended: the example's `actors` resource has no incremental cursor, so injected bounds would be silently ignored for it — the runtime hard-fails instead of quietly re-extracting everything per chunk. Backfill a source whose selected resources all declare cursors and the chunks run through. [Backfill](../concepts/backfill.md) covers the chunk state machine.

## Where next

- [Add a source](../guides/add-a-source.md) — from empty project to green `validate` and `run`, step by step
- [Config reference](../configuration/reference.md) — every key, with types, defaults, and precedence
- [Rules reference](../configuration/rules.md) — every `validate` rule and how to override it

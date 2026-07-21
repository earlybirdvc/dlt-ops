<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/earlybirdvc/dlt-ops/main/docs/assets/wordmark-dark.svg">
    <img src="https://raw.githubusercontent.com/earlybirdvc/dlt-ops/main/docs/assets/wordmark.svg" alt="dlt-ops" width="320">
  </picture>
</h1>

**A ready-made structure, toolchain, and set of guides for running many [dlt](https://dlthub.com) sources in production.**

[![PyPI](https://img.shields.io/pypi/v/dlt-ops)](https://pypi.org/project/dlt-ops/)
[![Python](https://img.shields.io/pypi/pyversions/dlt-ops)](https://pypi.org/project/dlt-ops/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/earlybirdvc/dlt-ops/blob/main/LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/earlybirdvc/dlt-ops/ci.yml?branch=main&label=ci)](https://github.com/earlybirdvc/dlt-ops/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-latest-047857)](https://earlybirdvc.github.io/dlt-ops/)

Adopt a worked-out layout, scheduling contract, validation and observability story instead of designing one per project. `dlt-ops` is a wrapper around dlt, the way dbt wrapped SQL: the primitive stays in charge of the core job (moving data), the wrapper decides how a project is laid out, validated, gated, scheduled, and operated day to day.

It is a convenience toolchain for the common case — scheduled batch ingestion into a warehouse, lake, or local engine at moderate volume. It adds guardrails and ergonomics, not throughput: nothing here makes dlt faster, and high-load pipelines with hard SLAs are better served by purpose-built infrastructure around plain dlt.

## What this is — and is not

- **A toolchain for dlt specifically.** Not a generic ingestion framework: it ships zero connectors and owns no part of the ingest write path — your `@dlt.source` code and dlt do all the ingesting. It writes your rows itself in exactly one place: rows an assertion rejects are diverted out of the load into a `_dlt_rejected` table. Drift alerts also carry up to five sample values per drifted column, so a data-governance review has those two paths to account for beyond dlt.
- **Not a replacement for dlt, Airbyte, or Meltano.** Those ship connector catalogs and execution engines; this package ships structure and operations for the dlt code you write.
- **Not an orchestrator.** It declares schedule metadata and generates DAGs for one (Airflow first); it never runs a scheduler loop itself.
- **Narrow on purpose, opinionated within scope.** One mandatory layout, one validation model, one operational toolchain — one thing done thoroughly rather than many things done flexibly.

`dlt-ops` is a third-party project, not affiliated with or endorsed by dltHub.

## Why this exists

dlt is an excellent ingestion primitive, and deliberately unopinionated — it moves data and leaves the surrounding shape to you. Running one source that way is a script. Running twenty of them on a schedule, across a team, turns that freedom into a set of design decisions: where source code lives, what a reviewer checks, what has to be true before a run may start, what data is allowed to land, and where you look afterwards to find out what happened. Every team answers those, usually more than once. `dlt-ops` answers them once, one way, and ships the tooling that holds the answer in place.

- **What data is allowed to land.** Pre-load assertions declared in TOML run between extract and load: row-count floors and ceilings, required columns, in-batch uniqueness, custom predicates. A violating load fails, warns, or has its bad rows diverted into a `_dlt_rejected` table you can query. dlt's per-row Pydantic validation and schema contracts judge a row's *shape*; assertions judge a load's *content*, and the two compose.
- **What a project looks like.** One mandatory layout, so discovery is a filesystem scan rather than registration code and every source in every repo reads the same way. New sources arrive by convention instead of by wiring.
- **What must be true before a run starts.** A rule framework checks layout, naming, config, schedules, schema contracts, column typing, and assertion config in CI, and the runtime re-checks the critical subset on every run, because a production scheduler does not run your CLI steps first.
- **What importing your code is allowed to do.** Source modules are imported in a sandbox that fails on network I/O or disk writes at import time, so a module-level `requests.get(...)` is caught before it can fire on every scheduler heartbeat that parses the file.
- **How you find out what happened.** Every run writes a row to a `_dlt_ops_runs` table beside the data, opened at start and closed with its outcome, and `pipeline status` reads it back. Schema drift is reconciled in both directions: columns that appeared, and columns your model still declares that quietly stopped carrying data.

`dlt-ops` does not replace dlt — sources and resources stay plain `@dlt.source` / `@dlt.resource` code. It wraps them in pre-load gates, a strict project layout, static validation, and a runtime that fails fast instead of degrading silently.

## What you get

- **Pre-load assertions** — per-resource data-quality gates (`min_rows_per_load`, `max_rows_per_load`, `required_columns`, `unique_columns`, custom predicates) declared in TOML and enforced between extract and load: fail the run, quarantine rows to a `_dlt_rejected` table, or warn — bad data never loads by default.
- **`pipeline validate`** — a rule framework (21 core rules, plus plugin-owned ones) that statically checks layout, naming, config, schedules, schema contracts, column typing, assertion config, destination capability, and import safety before anything runs.
- **Filesystem discovery** — a mandatory project layout; sources are found by scanning, not by registration code. Phase 1 is a pure AST scan (never imports your code); Phase 2 imports inside a sandbox that fails on import-time network I/O or disk writes.
- **Schema-drift reconciler** — `pipeline reconcile` diffs the live destination schema against your declared Pydantic models (additive drift), optionally detects model columns whose data went dark (`--include-removal`), and routes findings to pluggable alert sinks.
- **Checkpoints** — `@with_checkpoints` persists pagination progress to the destination mid-run; a failed run resumes from the last checkpoint instead of the window start.
- **Chunked backfill** — `pipeline backfill --from --to --chunk` splits a window into resumable chunks with per-chunk state; re-running skips completed chunks.
- **Run ledger** — every run/backfill writes one row to a `_dlt_ops_runs` table in the destination, opened at start and closed with its outcome; `pipeline status` reads it back.
- **Scheduling metadata** — every source declares a `schedule` in TOML; orchestrator adapters (Airflow first) turn discovery output into DAGs.
- **Selective cleanup** — `pipeline clean` removes a resource's tables, incremental state, and checkpoints (or a whole source) from the destination *and* the local working directory, with a dry run. dlt's own `pipeline_drop` does the destination-side drop, nested child tables included; `clean` wraps it and adds the rest.
- **Plugin axes** — destinations, orchestrators, validators, secret backends, alert sinks, and assertion types all extend through one entry-points mechanism. Anyone can ship a plugin; no blessing required.
- **Capability tiers** — every destination dlt can resolve runs the core loop (core tier); registering a `DestinationAdapter` for it upgrades to full tier, which adds the runs ledger and status, checkpoints, backfill, clean (remote), reconcile, and assertion quarantine. Adapter-less destinations degrade loudly, never silently. See the [destinations reference](https://earlybirdvc.github.io/dlt-ops/reference/destinations/).
- **Two-tier enforcement** — everything statically checkable is checked by `validate` (Tier 1); every `run`/`backfill` re-checks a small set of critical preconditions at runtime (Tier 2), because production schedulers don't run CLI steps first.

## The opinions you're signing up for

This package is strict and layout-mandatory. Deviate from the layout and discovery refuses to find you — there is no flexible mode. In exchange for roughly nine conventions per source (vanilla dlt asks for three), you get everything in the list above.

| # | Convention |
|---|---|
| 1 | One directory per pipeline directly under the project root (no leading `.` or `_`) |
| 2 | Source modules live in `<pipeline>/source/`, shared resources in `<pipeline>/resource/` (exact singular names) |
| 3 | Module stem equals the config section: `source/<X>.py` ↔ `[sources.<X>]` |
| 4 | The source function is named `<X>_source` |
| 5 | The decorator names the section explicitly: `@dlt.source(name="<X>")` |
| 6 | Every source has a `[sources.<X>]` section in `.dlt/config.toml` |
| 7 | Every source declares a `schedule` under `[sources.<X>.dlt_ops]` |
| 8 | Resource names are unique within a pipeline directory |
| 9 | Every `@dlt.resource` declares `columns=` as a Pydantic model |

Two more opinions are enforced without you writing anything:

- **Schema contracts**: a resource that declares no `schema_contract` gets the canonical freeze contract (`{"tables": "evolve", "columns": "freeze", "data_type": "freeze"}`) auto-applied at runtime. Evolving contracts require an explicit, justified opt-in in config.
- **Import safety**: `validate` imports each source module in a sandbox and fails on network I/O or disk writes at import time — the orchestrator-parse foot-gun, caught before deploy.

Every rule can be switched off per project (`[dlt_ops.rules]`) or exempted per source with a mandatory written reason (`rule_exemptions`) — with one exception: `import_safety` takes no per-source exemption, and naming it under a source's `rule_exemptions` is a config error that fails `validate`. Whether project code is imported into this process is not a per-source decision, so the project-wide `[dlt_ops.rules] import_safety = false` switch is its only opt-out. The defaults are the point, though. See the [rules reference](https://earlybirdvc.github.io/dlt-ops/configuration/rules/).

## Failure semantics

`run` and `backfill` wrap the data write in several layers, and the contract between them is asymmetric on purpose: **gates that decide what data loads fail hard; observability that merely records what happened never takes a healthy run down with it.** A violated precondition, a `fail` assertion, or an adapter-gated feature (checkpoints, quarantine, backfill) engaged on a core-tier destination aborts loudly *before* any data moves — dlt-ops refuses rather than degrading silently. Observability sits on the other side of that line: a runs-ledger or trace write that fails is logged, never fatal, and `status` keeps three absences honestly distinct — `no runs recorded` (the source never ran), `ledger unreadable` (an outage), and `ledger unsupported` (a core-tier destination with no `DestinationAdapter` for a ledger to live in). Which verbs a destination gets, and which fail hard versus go quiet, is the core-vs-full [capability tier](https://earlybirdvc.github.io/dlt-ops/reference/destinations/) split.

The full contract — every layer, at both tiers — is the [failure-semantics page](https://earlybirdvc.github.io/dlt-ops/concepts/failure-semantics/), which is canonical.

## Install

```
pip install dlt-ops              # the CLI + plugin registry: discovery, validate,
                                       # run, and scheduling on any dlt destination
pip install "dlt-ops[duckdb]"    # + DuckDB destination + adapter — full tier (dev loop)
pip install "dlt-ops[postgres]"  # + Postgres destination + adapter — full tier
pip install "dlt-ops[bigquery]"  # + BigQuery destination + adapter — full tier
pip install "dlt-ops[airflow]"   # + Airflow adapter: DAG factory, Variable secret
                                       #   backend, Airflow-specific validate rules
pip install "dlt-ops[sentry]"    # + Sentry alert sink for the reconciler
pip install "dlt-ops[snowflake]" # + dlt's Snowflake destination — core tier*
pip install "dlt-ops[databricks]"# + dlt's Databricks destination — core tier*
```

Which destination you point at decides the **capability tier**. Full tier ships a `DestinationAdapter` and unlocks the adapter-routed features — runs ledger and status, checkpoints, backfill, clean (remote), reconcile, assertion quarantine; core tier runs the pipeline and everything that does not speak SQL to the destination directly.

| Extra | Destination | Tier |
|---|---|---|
| `[duckdb]` / `[postgres]` / `[bigquery]` | DuckDB / Postgres / BigQuery | **full** — first-party adapter ships |
| `[snowflake]` / `[databricks]` | Snowflake / Databricks | **core** — dlt destination support, no adapter registered |
| (none) | any destination dlt resolves, e.g. a local `filesystem` bucket | **core** — no extra needed |
| `[filesystem]` / `[s3]` / `[gs]` / `[az]` | filesystem / S3 / GCS / Azure object stores | **core** — a remote bucket needs the matching extra; a local `file://` bucket needs none |

\* `snowflake` / `databricks` install dlt's destination support only — no first-party `DestinationAdapter` is registered for them, so they run at **core tier**: `run` and scheduling work; the adapter-routed features listed above are unavailable and degrade loudly until an adapter is registered.

First-party adapters ship for DuckDB, Postgres, and BigQuery, and CI exercises DuckDB and Postgres against live instances on every commit. Two other routes to full tier exist. Third-party adapters plug in via entry points. And because dlt publishes most of what an adapter needs per destination, `register_derived_adapter("<engine>")` builds one at runtime for any destination that declares a sqlglot dialect — deliberately opt-in, and it logs a warning, because deriving an adapter shapes the SQL for the right dialect without proving anyone has run it there. Read [derived is not the same as tested](https://earlybirdvc.github.io/dlt-ops/reference/destinations/#derived-is-not-the-same-as-tested) before relying on one; the [destinations reference](https://earlybirdvc.github.io/dlt-ops/reference/destinations/) and the [adapter-authoring guide](https://earlybirdvc.github.io/dlt-ops/guides/write-a-destination-adapter/) cover all three routes.

## Quickstart

```bash
pip install "dlt-ops[duckdb]"

dlt-ops init demo --example   # scaffold a project with a runnable example source
cd demo

dlt-ops pipeline validate     # static checks: layout, config, contracts, import safety
dlt-ops pipeline run -s demo_events -y
dlt-ops pipeline status       # the run ledger, read back from the destination
```

The example source is fixture-backed and network-free; the run lands six typed rows in a local DuckDB file (`demo_events_pipeline.duckdb`) — no credentials, no cloud. The [quickstart](https://earlybirdvc.github.io/dlt-ops/getting-started/quickstart/) walks through the scaffold, what `validate` checks, pre-load assertions, checkpoint resume, drift detection, and selective cleanup.

## Documentation

The full documentation site lives under [`docs/`](https://earlybirdvc.github.io/dlt-ops/) (published via GitHub Pages):

- [Quickstart](https://earlybirdvc.github.io/dlt-ops/getting-started/quickstart/) — the full first-run tour; [project layout](https://earlybirdvc.github.io/dlt-ops/getting-started/project-layout/) — the nine conventions and why
- [Concepts](https://earlybirdvc.github.io/dlt-ops/concepts/discovery/) — discovery, validation, failure semantics, capability tiers, runs ledger, checkpoints, assertions, reconciler, backfill, scheduling, plugins
- [Guides](https://earlybirdvc.github.io/dlt-ops/guides/add-a-source/) — task recipes: add a source, assertions, checkpoint resume, backfill, cleanup, drift detection, [deployment & scheduling](https://earlybirdvc.github.io/dlt-ops/guides/deployment/), Airflow, and [plugin authoring](https://earlybirdvc.github.io/dlt-ops/guides/write-a-destination-adapter/)
- [Configuration](https://earlybirdvc.github.io/dlt-ops/configuration/reference/) — every `[dlt_ops]` and `[sources.<X>.dlt_ops]` key; [rules reference](https://earlybirdvc.github.io/dlt-ops/configuration/rules/)
- [Reference](https://earlybirdvc.github.io/dlt-ops/reference/cli/) — CLI, [Python API](https://earlybirdvc.github.io/dlt-ops/reference/api/), [destinations matrix](https://earlybirdvc.github.io/dlt-ops/reference/destinations/) (the CLI/API pages are autogenerated — they render on the published site, not in the raw markdown)
- [CONTRIBUTING](https://github.com/earlybirdvc/dlt-ops/blob/main/CONTRIBUTING.md) — dev setup, test lanes, PR expectations
- [SECURITY](https://github.com/earlybirdvc/dlt-ops/blob/main/SECURITY.md) — how to report vulnerabilities

## Stability and compatibility

`dlt-ops` is pre-1.0: the API and plugin surface are still settling, and 0.x minor releases may break them. The versioning and deprecation policy lives in [VERSIONING.md](https://github.com/earlybirdvc/dlt-ops/blob/main/VERSIONING.md); releases are tracked in the [CHANGELOG](https://github.com/earlybirdvc/dlt-ops/blob/main/CHANGELOG.md).

The package never caps dlt: the dependency is a floor (`dlt>=1.27`) and you own your project's dlt version. Nothing gates on a list of dlt minors — every feature runs on any dlt at or above the floor, including a release newer than the tested matrix. That matrix records what CI exercised per dlt minor × destination and lives in [COMPATIBILITY.md](https://github.com/earlybirdvc/dlt-ops/blob/main/COMPATIBILITY.md).

## License

[Apache-2.0](https://github.com/earlybirdvc/dlt-ops/blob/main/LICENSE).

<h1 align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/earlybirdvc/dlt-ops/main/docs/assets/wordmark-dark.svg">
    <img src="https://raw.githubusercontent.com/earlybirdvc/dlt-ops/main/docs/assets/wordmark.svg" alt="dlt-ops" width="320">
  </picture>
</h1>

**An opinionated project layout and toolchain for [dlt](https://dlthub.com) pipelines — start structured, stay structured.**

[![PyPI](https://img.shields.io/pypi/v/dlt-ops)](https://pypi.org/project/dlt-ops/)
[![Python](https://img.shields.io/pypi/pyversions/dlt-ops)](https://pypi.org/project/dlt-ops/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://github.com/earlybirdvc/dlt-ops/blob/main/LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/earlybirdvc/dlt-ops/ci.yml?branch=main&label=ci)](https://github.com/earlybirdvc/dlt-ops/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-latest-047857)](https://earlybirdvc.github.io/dlt-ops/)

`dlt-ops` is a wrapper around dlt, the way dbt wrapped SQL: the primitive stays in charge of the core job (moving data), the wrapper decides how a project is laid out, validated, scheduled, and operated day to day.

It is a convenience toolchain for the common case — scheduled batch ingestion into a warehouse, lake, or local engine at moderate volume. It adds guardrails and ergonomics, not throughput: nothing here makes dlt faster, and high-load pipelines with hard SLAs are better served by purpose-built infrastructure around plain dlt.

## What this is — and is not

- **A toolchain for dlt specifically.** Not a generic ingestion framework: it ships zero connectors and moves zero rows itself — your `@dlt.source` code and dlt do all the ingesting.
- **Not a replacement for dlt, Airbyte, or Meltano.** Those ship connector catalogs and execution engines; this package ships structure and operations for the dlt code you write.
- **Not an orchestrator.** It declares schedule metadata and generates DAGs for one (Airflow first); it never runs a scheduler loop itself.
- **Narrow on purpose, opinionated within scope.** One mandatory layout, one validation model, one operational toolchain — one thing done thoroughly rather than many things done flexibly.

`dlt-ops` is a third-party project, not affiliated with or endorsed by dltHub.

## Why this exists

dlt is the best open-source ingestion primitive available. It is also, deliberately, unopinionated — and that flexibility is exactly what makes a dlt codebase hard to keep consistent once it grows past one person and one script:

- **Config sprawl.** Environment variables can override almost anything from anywhere, so the effective configuration of a pipeline depends on the machine it runs on.
- **No enforced layout.** Every project invents its own structure; discovery, tooling, and review conventions have to be rebuilt each time.
- **No selective state cleanup.** Removing one resource's data *and* its incremental state from a live destination has no supported path.
- **No run ledger.** dlt tells you what the last run on this machine did; nothing records what ran, when, and with what outcome where the data actually lands.
- **Import-time foot-guns.** A module-level `requests.get(...)` in a source file works fine locally and then fires on every scheduler heartbeat once the file is parsed by an orchestrator.
- **Silent schema drift.** Columns appearing in (or vanishing from) the destination behind your model's back go unnoticed until a consumer breaks.

`dlt-ops` fills that gap. It does not replace dlt — sources and resources are plain `@dlt.source` / `@dlt.resource` code — it wraps them in a strict project layout, static validation, and a runtime that fails fast instead of degrading silently.

## What you get

- **Filesystem discovery** — a mandatory project layout; sources are found by scanning, not by registration code. Phase 1 is a pure AST scan (never imports your code); Phase 2 imports inside a sandbox.
- **`pipeline validate`** — a rule framework (19 core rules, plus plugin-owned ones) that statically checks layout, naming, config, schedules, schema contracts, column typing, assertion config, destination capability, and import safety before anything runs.
- **Scheduling metadata** — every source declares a `schedule` in TOML; orchestrator adapters (Airflow first) turn discovery output into DAGs.
- **Checkpoints** — `@with_checkpoints` persists pagination progress to the destination mid-run; a failed run resumes from the last checkpoint instead of the window start.
- **Chunked backfill** — `pipeline backfill --from --to --chunk` splits a window into resumable chunks with per-chunk state; re-running skips completed chunks.
- **Selective cleanup** — `pipeline clean` removes one resource's tables, incremental state, and checkpoints (or a whole source) from the live destination, surgically.
- **Run ledger** — every run/backfill writes start and outcome rows to a `_dlt_ops_runs` table in the destination; `pipeline status` reads it back.
- **Schema-drift reconciler** — `pipeline reconcile` diffs the live destination schema against your declared Pydantic models (additive drift), optionally detects model columns whose data went dark (`--include-removal`), and routes findings to pluggable alert sinks.
- **Pre-load assertions** — per-resource data-quality gates (`min_rows_per_load`, `max_rows_per_load`, `required_columns`, `unique_columns`, custom predicates) declared in TOML and enforced between extract and load: fail the run, quarantine rows to a `_dlt_rejected` table, or warn — bad data never loads by default.
- **Plugin axes** — destinations, orchestrators, validators, secret backends, alert sinks, and assertion types all extend through one entry-points mechanism. Anyone can ship a plugin; no blessing required.
- **Capability tiers** — every destination dlt can resolve runs the core loop (core tier); registering a `DestinationAdapter` for it upgrades to full tier, which adds the runs ledger and status, checkpoints, backfill, clean (remote), reconcile, and assertion quarantine. Adapter-less destinations degrade loudly, never silently. See the [destinations reference](docs/reference/destinations.md).
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

Every rule can be switched off per project (`[dlt_ops.rules]`) or exempted per source with a mandatory written reason (`rule_exemptions`). The defaults are the point, though. See the [rules reference](docs/configuration/rules.md).

## Failure semantics

`run` and `backfill` wrap the data write in several layers, and the contract between them is asymmetric on purpose: **gates that decide what data loads fail hard; observability that merely records what happened never takes a healthy run down with it.** A violated precondition, a `fail` assertion, or an adapter-gated feature (checkpoints, quarantine, backfill) engaged on a core-tier destination aborts loudly *before* any data moves — dlt-ops refuses rather than degrading silently. Observability sits on the other side of that line: a runs-ledger or trace write that fails is logged, never fatal, and `status` keeps three absences honestly distinct — `no runs recorded` (the source never ran), `ledger unreadable` (an outage), and `ledger unsupported` (a core-tier destination with no `DestinationAdapter` for a ledger to live in). Which verbs a destination gets, and which fail hard versus go quiet, is the core-vs-full [capability tier](docs/reference/destinations.md) split.

The full contract — every layer, at both tiers — is the [failure-semantics page](docs/concepts/failure-semantics.md), which is canonical.

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

Which destination you point at decides the **capability tier**. Full tier ships a `DestinationAdapter` and unlocks the adapter-routed features — runs ledger and status, checkpoints, backfill, clean (remote), reconcile, assertion quarantine; core tier runs the pipeline and everything else.

| Extra | Destination | Tier |
|---|---|---|
| `[duckdb]` / `[postgres]` / `[bigquery]` | DuckDB / Postgres / BigQuery | **full** — first-party adapter ships |
| `[snowflake]` / `[databricks]` | Snowflake / Databricks | **core** — dlt destination support, no adapter registered |
| (none) | any destination dlt resolves, e.g. a local `filesystem` bucket | **core** — no extra needed |
| `[filesystem]` / `[s3]` / `[gs]` / `[az]` | filesystem / S3 / GCS / Azure object stores | **core** — a remote bucket needs the matching extra; a local `file://` bucket needs none |

\* `snowflake` / `databricks` install dlt's destination support only — no first-party `DestinationAdapter` is registered for them, so they run at **core tier**: `run` and scheduling work; the adapter-routed features listed above are unavailable and degrade loudly until an adapter is registered. First-party adapters ship for DuckDB, Postgres, and BigQuery; third-party adapters plug in via entry points — see the [destinations reference](docs/reference/destinations.md) and the [adapter-authoring guide](docs/guides/write-a-destination-adapter.md).

## Quickstart

```bash
pip install "dlt-ops[duckdb]"

dlt-ops init demo --example   # scaffold a project with a runnable example source
cd demo

dlt-ops pipeline validate     # static checks: layout, config, contracts, import safety
dlt-ops pipeline run -s demo_events -y
dlt-ops pipeline status       # the run ledger, read back from the destination
```

The example source is fixture-backed and network-free; the run lands six typed rows in a local DuckDB file (`demo_events_pipeline.duckdb`) — no credentials, no cloud. The [quickstart](docs/getting-started/quickstart.md) walks through the scaffold, what `validate` checks, pre-load assertions, checkpoint resume, drift detection, and selective cleanup.

## Documentation

The full documentation site lives under [`docs/`](docs/) (published via GitHub Pages):

- [Quickstart](docs/getting-started/quickstart.md) — the full first-run tour; [project layout](docs/getting-started/project-layout.md) — the nine conventions and why
- [Concepts](docs/concepts/discovery.md) — discovery, validation, failure semantics, capability tiers, runs ledger, checkpoints, assertions, reconciler, backfill, scheduling, plugins
- [Guides](docs/guides/add-a-source.md) — task recipes: add a source, assertions, checkpoint resume, backfill, cleanup, drift detection, [deployment & scheduling](docs/guides/deployment.md), Airflow, and [plugin authoring](docs/guides/write-a-destination-adapter.md)
- [Configuration](docs/configuration/reference.md) — every `[dlt_ops]` and `[sources.<X>.dlt_ops]` key; [rules reference](docs/configuration/rules.md)
- [Reference](docs/reference/cli.md) — CLI, [Python API](docs/reference/api.md), [destinations matrix](docs/reference/destinations.md) (the CLI/API pages are autogenerated — they render on the published site, not in the raw markdown)
- [CONTRIBUTING](CONTRIBUTING.md) — dev setup, test lanes, PR expectations
- [SECURITY](SECURITY.md) — how to report vulnerabilities

## Stability and compatibility

`dlt-ops` is pre-1.0: the API and plugin surface are still settling, and 0.x minor releases may break them. The versioning and deprecation policy lives in [VERSIONING.md](VERSIONING.md); releases are tracked in the [CHANGELOG](CHANGELOG.md).

The package never caps dlt: the dependency is a floor (`dlt>=1.27`) and you own your project's dlt version. Features that touch dlt internals are verified per minor, and the one destructive feature (remote `clean`) refuses to run on unverified minors rather than guess. The verified matrix of dlt versions and destinations is in [COMPATIBILITY.md](COMPATIBILITY.md).

## License

[Apache-2.0](LICENSE).

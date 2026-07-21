---
description: The dlt-ops configuration model — the [dlt_ops] project table and per-source [sources.<X>.dlt_ops] tables inside dlt's own .dlt/config.toml, how destination/dataset/rules resolve, where secrets live, and the plugin-registry side effect of loading a project.
---

# Configuration

`dlt-ops` keeps all of its configuration in the same `.dlt/config.toml` that dlt already reads — one `[dlt_ops]` table for the project, one `[sources.<X>.dlt_ops]` table per source, and nothing else. This page covers the config model: the two namespaces, how destination, dataset, and rules resolve, where secrets go, and the one side effect config loading has on the plugin registry. For every key with its type and default, see the [config reference](reference.md); for the rule catalog, the [rules reference](rules.md).

**At a glance**

| Where | What lives there |
|---|---|
| `[dlt_ops]` in `.dlt/config.toml` | Project-wide settings; also the **project marker** that makes a directory a `dlt-ops` project. |
| `[sources.<X>.dlt_ops]` | Everything `dlt-ops` adds per source — `schedule`, destination/dataset overrides, assertions, rule exemptions. |
| `.dlt/secrets.toml` | Credentials and alert-sink secrets, along dlt's existing secret / non-secret split. |

## The `[dlt_ops]` namespace and the project marker

**The top-level `[dlt_ops]` table is the project marker: a directory is a `dlt-ops` project iff `.dlt/config.toml` exists, parses, and contains a `[dlt_ops]` table.** Commands walk up from the current directory until a directory qualifies, or take an explicit `--root`.

The check fails loudly rather than guessing: a broken `config.toml` raises a parse error instead of silently widening the search to a parent, and a directory with no `[dlt_ops]` table is reported as "not a project" with a `dlt-ops init` hint. TOML is canonical **for the `dlt_ops` namespace**: every `[dlt_ops]` and `[sources.<X>.dlt_ops]` key is parsed straight out of this file, with no environment-variable override and no decorator-level config on top of dlt's own `@dlt.source` / `@dlt.resource`. That scope is the honest one — dlt's own keys keep dlt's full provider chain, environment variables included, and credentials live in `secrets.toml`.

## Per-source settings: `[sources.<X>.dlt_ops]`

**Everything `dlt-ops` adds for a source nests under `[sources.<X>.dlt_ops]`, one level deeper than dlt-native source config, so it can never collide with a dlt-native key.** `<X>` is the source's config section, which by convention (enforced by `validate`) equals the source module stem and the explicit `@dlt.source(name="<X>")` value.

dlt-native source config — `base_url`, credentials sections, incremental values — lives directly under `[sources.<X>]` and is dlt's territory. Everything `dlt-ops` adds for that source (its `schedule`, destination and dataset overrides, assertions, rule exemptions) sits one level deeper under `[sources.<X>.dlt_ops]`.

## Why namespaced inside dlt's config file

**`dlt-ops` adds its `[dlt_ops]` and `[sources.<X>.dlt_ops]` tables to dlt's existing `.dlt/config.toml` instead of introducing a second config file of its own.** dlt already requires that file for secrets and source config; one file means no duplicated surface and parity with how dlt is already configured.

The `dlt_ops` prefix keeps every added key out of dlt's namespace and marks each site as "this is `dlt-ops`, not dlt". The rest of the file stays exactly what dlt expects.

## Resolution precedence

**Destination and dataset each resolve up a short ladder, lowest to highest; there is no silent default at the bottom — unresolved after the chain is a hard error, because the package is destination-agnostic and refuses to guess.**

- **Destination**: `[dlt_ops].default_destination` → `[sources.<X>.dlt_ops].destination`. No CLI override — the destination always comes from config.
- **Dataset**: `[dlt_ops].default_dataset` → `[sources.<X>.dlt_ops].dataset` → an explicit `--dataset` on `run` / `clean`.

Rules resolve on a parallel ladder: each rule's registered default (on for every shipped rule except `incremental_cursor_required`, which ships off) → the `[dlt_ops.rules]` project-wide on/off knob → a `[sources.<X>.dlt_ops.rule_exemptions]` entry that suppresses one rule's findings for one source (the rule still runs everywhere else). See the [rules reference](rules.md) for the catalog and the [config reference](reference.md#resolution-precedence) for the canonical ladders.

## Secrets: config.toml vs secrets.toml

**`dlt-ops` adds no secret file of its own — it splits along dlt's existing line: credentials in `.dlt/secrets.toml`, non-secret settings in `.dlt/config.toml`.** Destination credentials and source API keys go in `secrets.toml` per [dlt's conventions](https://dlthub.com/docs/general-usage/credentials/setup), and alert-sink secrets (for example a Sentry DSN) under `[alert_sinks.<name>]`; non-secret alert-sink constructor options stay in `config.toml` under `[dlt_ops.alert_sink.<name>]`.

Secret **backends** such as Airflow Variables are plugins that fetch values at runtime and write them into `dlt.secrets`, so the secret itself never lands in either file — see [plugins](../concepts/plugins.md).

## What `run` writes into the environment

**Three dlt-native settings are passed to dlt as environment variables rather than through config, because that is how a per-invocation override reaches dlt.** Every `run` (and every backfill chunk) applies them before the pipeline is constructed:

| CLI flag | Environment variable it sets | Local default when the flag is omitted |
|---|---|---|
| `-n` / `--normalize-workers` | `NORMALIZE__WORKERS` | `4` |
| `-l` / `--load-workers` | `LOAD__WORKERS` | `3` |
| `-f` / `--file-max-items` | `NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS` | none |

An explicit flag is written unconditionally and outranks every provider — that is what a per-invocation override is for. The defaults in the third column behave differently in two ways worth knowing. They apply **only when the resolved destination is DuckDB** — the local dev loop — so a run against Postgres, BigQuery, or anything else leaves all three variables untouched. And they are a floor, not an override: before writing one, `run` asks dlt's own config chain whether the key already resolves from any provider — environment, `secrets.toml`, or `config.toml` — and writes nothing if it does. `NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS` has no local default at all; without `-f` it is never set.

The keys are dlt's own, spelled dlt's way (sections joined by `__`, uppercased), so every setting you would put in `[normalize]` or `[load]` in `.dlt/config.toml` still applies — this only pre-fills two of them, and only for a dev loop that configured neither.

!!! tip "Configuring the two keys yourself is enough"
    `[normalize] workers = 9` in `.dlt/config.toml` resolves to 9 on every destination, DuckDB included: the local default fills a gap, it does not close one. The same holds for a value already exported in the environment, which stays untouched. The run log names which of the three applied — `Normalize workers: 4 (local default)` versus `Normalize workers: 9 (configured, local default not applied)` — so you can read the resolved number straight out of the run.

## Plugin disambiguation, installed at config load

**Loading the project config does one side-effecting thing beyond parsing: it installs the `[dlt_ops.plugins.<axis>]` collision-disambiguation mapping into the process-wide plugin registry, so plugin resolution follows the loaded project.** Re-loading is idempotent, and loading a different project replaces the mapping — the registry is process-global, one project per process.

The mapping is validated as it is installed. `<axis>` must be one of the six known axes — `destination`, `orchestrator`, `assertion`, `validators`, `secret_backend`, `alert_sink` — and a typo'd axis is a hard `ProjectConfigError` at load time, never a silent no-op. A `[dlt_ops.plugins.storage]` table, for instance, fails with `[dlt_ops.plugins]: unknown plugin axes in disambiguation mapping: storage`. See [plugins](../concepts/plugins.md) for the registry and collision model.

## Where next

- [Config reference](reference.md) — every key, with types, defaults, and the parser that reads it.
- [Rules reference](rules.md) — the full validation catalog and how to switch rules off.
- [Plugins](../concepts/plugins.md) — the six axes, entry-point groups, and collision resolution.

---
description: Every .dlt/config.toml key dlt-ops reads — the [dlt_ops] project table and per-source [sources.<X>.dlt_ops] tables — with type, default, and the parser that consumes it, plus the destination, dataset, and rule resolution ladders.
---

# Config reference

Every key `dlt-ops` reads from `.dlt/config.toml`, with its type, default, and the parser that consumes it. All of it lives in the same file dlt itself reads, and every key below is parsed straight out of that file — no environment-variable override, no decorator-level config. The scope is the `dlt_ops` namespace only: dlt's own keys keep dlt's provider chain, environment variables included. For the model behind these keys (namespaces, precedence, secrets split) see the [configuration overview](index.md); for the rule IDs the `[dlt_ops.rules]` and `rule_exemptions` tables key on, see the [rules reference](rules.md).

The top-level `[dlt_ops]` table doubles as the **project marker**: a directory is a `dlt-ops` project iff `.dlt/config.toml` exists, parses, and contains a `[dlt_ops]` table. Two namespaces carry `dlt-ops` config; everything else in the file is dlt's own territory.

- `[dlt_ops]` — project-level settings.
- `[sources.<X>.dlt_ops]` — per-source settings. Everything else under `[sources.<X>]` is dlt-native source config (`base_url`, credentials sections, ...) and is not documented here.

## Project-level: `[dlt_ops]`

**The `[dlt_ops]` table carries project-wide settings and doubles as the project marker; every key it accepts:**

| Key | Type | Default | Description |
|---|---|---|---|
| `default_destination` | string | unset | Project-wide destination name (`"duckdb"`, `"postgres"`, `"bigquery"` run at full tier; any other destination dlt can resolve runs at core tier — see the [destinations reference](../reference/destinations.md)). Overridden per source by `[sources.<X>.dlt_ops].destination`. If neither is set, any verb that needs a destination fails with a hard error — there is no silent fallback. |
| `default_dataset` | string | unset | Project-wide dataset (schema) name. Overridden per source by `[sources.<X>.dlt_ops].dataset`, and by an explicit `--dataset` on `run` / `clean`. Same no-fallback rule. |
| `require_destination_adapter` | boolean | `false` | When `true`, a resolved destination with no registered `DestinationAdapter` is a hard failure instead of core-mode degradation: every `run` / `backfill` fails its Tier-2 preflight and the `destination_capability` rule reports an error. Default `false` — the core run loop works on any destination dlt can resolve (core tier); only the adapter-gated features are unavailable there — the six named in the [destinations reference](../reference/destinations.md). Only the literal `true` engages it; any other value reads as `false`. |
| `load_timestamp_column` | string | unset (feature off) | Name of a per-run load-timestamp column; also the time axis for removal-drift detection (`pipeline reconcile --include-removal`). Read from the raw table by the runner, the reconciler, and the `cursor_not_load_timestamp` rule. See the warning below before setting it. |
| `injected_columns` | array of strings | `[]` | Column names your own code stamps on every row of **every** source (infrastructure keys that are not part of any upstream payload). The schema-drift reconciler ignores them instead of reporting them as drift. Merged (union) with each source's own `injected_columns` and the `load_timestamp_column`. Non-string entries are dropped. |
| `staleness_days` | positive integer | `7` | Threshold for the `stale_sources` rule: a source **with run history** whose last run started more than this many days ago gets a validation warning, printed by every `validate` run and fatal under `--strict`. Non-integer or non-positive values fall back to the default. |
| `alert_sinks` | array of strings | unset → `["logging"]` | Alert-sink plugin names the reconciler emits events through. Unset means the built-in `logging` sink. **Unset vs. explicit matters**: `validate` and the runtime preflight only enforce names you explicitly configured. An explicitly empty list (`alert_sinks = []`) disables emission on purpose. |

Every key above is parsed today — three of them (`load_timestamp_column`, `injected_columns`, `staleness_days`) are read directly from the raw table rather than through a typed accessor, but all have live consumers. Any top-level key not in this table (and not one of the sub-tables below) is collected as an unknown key.

!!! warning "`load_timestamp_column` mutates your destination schema"
    Setting this key changes what lands in the destination. When it is set, every `run` and `backfill` injects a transform that stamps one UTC timestamp — captured once per run, identical on every row of that run — onto **every row of every resource** in the project. The column appears in your destination tables even though no Pydantic model declares it: the load timestamp is extraction metadata, deliberately kept out of the models. Two guards come with it automatically — the reconciler auto-registers the column as injected so it never reads as drift, and the `cursor_not_load_timestamp` rule fails `validate` if any resource cursors on it (it advances every run, so cursoring on it silently skips in-window updates). It also unlocks removal-drift detection: `pipeline reconcile --include-removal` uses this column as its time axis, and skips the scan with a warning when the key is unset. Unset (or empty, or non-string) means off: nothing is stamped.

### `[dlt_ops.rules]`

**Per-rule on/off knob for the validation framework.** A missing entry means the rule's registered default — on for every shipped rule except `incremental_cursor_required`, which ships off and is adopted with `= true`. Values must be `true` or `false` (anything else is a `validate` error); an unknown rule ID is an error in **both** tiers (`validate` fails, and every `run` / `backfill` fails its runtime preflight — the typo guard). Rule IDs are stable within a major version; the full catalog is in the [rules reference](rules.md).

```toml
[dlt_ops.rules]
import_safety = true          # explicit on (no behavior change; missing entry = on)
stale_sources = false         # opt out of one rule project-wide
```

### `[dlt_ops.plugins.<axis>]`

**Collision disambiguation for the plugin registry.** When two installed distributions register the same `<axis>/<name>`, lookups hard-fail (no silent first-wins) until you pick a winner by distribution name or object qualname. `<axis>` must be one of the six known axes — `destination`, `orchestrator`, `assertion`, `validators`, `secret_backend`, `alert_sink` — an unknown axis is a `ProjectConfigError` raised when the project config loads (see the [configuration overview](index.md#plugin-disambiguation-installed-at-config-load)).

```toml
[dlt_ops.plugins.destination]
snowflake = "acme-dlt-snowflake"
```

### `[dlt_ops.alert_sink.<name>]`

**Non-secret constructor options for one configured alert sink, passed to the sink class as keyword arguments.** The table key is singular (`alert_sink`) — TOML forbids `alert_sinks` being both the list above and a table. Secrets (e.g. the Sentry DSN) do **not** go here; they live in `.dlt/secrets.toml` under `[alert_sinks.<name>]`.

```toml
[dlt_ops]
alert_sinks = ["logging", "sentry"]

[dlt_ops.alert_sink.sentry]
environment = "prod"
```

### Unknown keys

**Any other top-level key under `[dlt_ops]` is collected as unknown (a probable typo) and ignored by the parsers.** `validate` surfaces the collected names as typo warnings; nothing is raised at load time.

## Per-source: `[sources.<X>.dlt_ops]`

**`<X>` is the source's config section — which, by convention (enforced by `validate`), equals the source module stem and the explicit `@dlt.source(name="<X>")` value.**

| Key | Type | Default | Description |
|---|---|---|---|
| `schedule` | string | — (**required**) | One of `@hourly`, `@2hourly`, `@daily`, `@weekly`, `@monthly`, `@manual`. Missing or invalid values fail the `schedule_required` rule, and the source is treated as having no valid config (grouped under `@manual` by scheduling helpers). |
| `destination` | string | unset | Per-source destination override. Wins over `[dlt_ops].default_destination`. |
| `dataset` | string | unset | Per-source dataset override. Wins over `[dlt_ops].default_dataset`; an explicit `--dataset` on `run` / `clean` wins over both. |
| `airflow_var` | string | unset | Airflow Variable name holding this source's secrets. Setting it is what makes the Airflow secret backend claim the source (the key is owned by the `[airflow]` extra's plugin; core parses it but never reads it for its own behavior). With the Airflow plugin active, the `airflow_var_required` rule demands it for any source whose signature uses `dlt.secrets.value`. |
| `airflow_var_key` | string | `"api_secret_key"` | The `dlt.secrets` leaf key the fetched Airflow Variable value is written to (`sources.<X>.<airflow_var_key>`). Only meaningful together with `airflow_var`. |
| `schema_contract_evolve_reason` | string | unset | Opt-in justification for the evolve schema contract. A non-empty string permits this source's resources to declare `schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "freeze"}`. Absent, empty, or non-string = no opt-in; the canonical freeze contract is the only accepted declaration (and is auto-applied to resources that declare none). |
| `injected_columns` | array of strings | `[]` | Same as the project-level key, scoped to this source: columns this source's own code stamps on its rows, ignored by the reconciler. Merged (union) with the project-level list and the `load_timestamp_column`. Non-array values collapse to empty. |

### `[sources.<X>.dlt_ops.assertions.<resource>]`

**Pre-load data-quality assertions, declared per resource.** Assertions run between extract and destination write: a failing assertion stops bad data from loading (and the rejected batch is discarded, never auto-resumed on the next run). See the [assertions concept](../concepts/assertions.md) for the execution model and the [assertions guide](../guides/assertions.md) for a worked run; this section is the declaration syntax.

```toml
[sources.my_api.dlt_ops.assertions.events]
on_failure = "fail"                          # resource-level default; optional (default "fail")
min_rows_per_load = 1                        # shorthand form
required_columns = ["id", "updated_at"]
unique_columns = { value = ["id"], on_failure = "quarantine" }   # table form = per-assertion override

[[sources.my_api.dlt_ops.assertions.events.custom]]
predicate = "my_project.assertions:events_business_rule"
on_failure = "warn"                          # optional; falls back to the resource default
```

- **Keys are assertion type names** — each non-reserved key must name a registered `assertion` plugin. Built-ins: `min_rows_per_load` (batch scope; int >= 0), `max_rows_per_load` (batch scope; int > 0), `required_columns` (row scope; non-empty column list, key presence), `unique_columns` (row scope; non-empty column list, uniqueness **within the load batch only** — cross-run dedupe is dlt merge/primary-key territory). Unknown keys fail `validate` **and** the runtime preflight.
- **Value forms**: shorthand (scalar/array, normalized to `{ value = ... }`) or an inline table whose `on_failure` key is the per-assertion override; the rest of the table is the type's params.
- **`on_failure`** ∈ `"fail"` / `"quarantine"` / `"warn"`. Precedence (lowest → highest): built-in default `"fail"` → resource-level `on_failure` → per-assertion `on_failure`. `fail` aborts the run: nothing loads, the runs ledger records `failed` with the assertion message, and the pending extracted package is dropped. `quarantine` removes failing rows from the stream and writes them to the `_dlt_rejected` table in the run's own dataset (one JSON-payload table; joins `_dlt_ops_runs` on `run_id`); a quarantine-write failure fails the run. `warn` logs, counts, and loads anyway. `quarantine` on a batch-scoped type is a config error — there are no specific rows to quarantine when a batch verdict fails.
- **Reserved keys**: `on_failure` and `custom`. `[[...custom]]` entries reference a row predicate by import path (`module:attr`; dotted `module.attr` also accepted) — a callable taking one row and returning `True` (pass) / `False` (fail). Project-local predicate modules resolve relative to the project root. Predicates are row-scope only; batch-scope custom checks are written as assertion-type plugins.
- Everything statically checkable is checked by the three always-on `assertion_*` rules (see the [rules reference](rules.md)); referencing an unregistered type also hard-fails every `run` / `backfill` at the Tier-2 preflight, even when `validate` was skipped.

### `[sources.<X>.dlt_ops.rule_exemptions]`

**Per-source, per-rule exemptions with a mandatory reason.** Every entry must name a known rule ID and carry a non-empty string reason — a misspelled rule or an empty reason is a `validate` error, never a silently weaker exemption. An exemption suppresses that rule's findings for this source only; the rule still runs for every other source.

```toml
[sources.legacy_api.dlt_ops.rule_exemptions]
pydantic_columns_required = "third-party generator yields untyped rows; typed model tracked upstream"
```

## Resolution precedence

**Destination, dataset, and rules each resolve up a short ladder, lowest to highest; unresolved after the chain is a hard error.**

For **destination** (lowest to highest):

1. `[dlt_ops].default_destination`
2. `[sources.<X>.dlt_ops].destination`

The destination has no CLI override — it always comes from config. Unresolved after the chain = hard error, by design: the package is destination-agnostic and refuses to guess.

For **dataset** (lowest to highest):

1. `[dlt_ops].default_dataset`
2. `[sources.<X>.dlt_ops].dataset`
3. An explicit CLI flag (`run --dataset`, `clean --dataset`).

Unresolved = hard error, same rule.

For **rules** (lowest to highest):

1. The rule's registered default (on for all shipped rules except `incremental_cursor_required`)
2. `[dlt_ops.rules]` project-wide override
3. `[sources.<X>.dlt_ops.rule_exemptions]` per-source suppression (findings filtered, rule still runs)

## Secrets

**`dlt-ops` adds no secret file of its own.** Destination credentials and source API keys live in `.dlt/secrets.toml` per [dlt's own conventions](https://dlthub.com/docs/general-usage/credentials/setup); alert-sink secrets live there too, under `[alert_sinks.<name>]`. Non-secret alert-sink options stay in `[dlt_ops.alert_sink.<name>]` in `config.toml`. Secret **backends** (e.g. Airflow Variables) are plugins that fetch values at runtime and write them into `dlt.secrets` — see [Write plugins](../guides/write-plugins.md).

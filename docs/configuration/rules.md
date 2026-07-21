---
description: Every validation rule dlt-ops pipeline validate can run — the exact rule IDs from --show-resolved-rules — with owning plugin, enforcement tier, and what each checks. The catalog the [dlt_ops.rules] and rule_exemptions tables key on.
---

# Rules reference

`dlt-ops pipeline validate` runs a resolved set of rules against the discovered sources and `.dlt/config.toml`. This page lists every rule ID shipped by the package and its first-party plugins, with what each checks, why, and which tier enforces it. For the tier model itself, see [validation](../concepts/validation.md).

## How rules resolve

**Rules resolve from provider defaults, overlaid by the `[dlt_ops.rules]` on/off knob and per-source exemptions; an unknown rule ID is a typo-guard error in both tiers.**

- Rules arrive from **providers** registered in the `dlt_ops.validators` entry-point group. The package's own rules ship through the same mechanism (`core`); installing a plugin distribution with a validator provider auto-activates its rules. A provider that fails to load is reported (its rules are unavailable that run), never silently skipped.
- Every shipped rule defaults to **on**. `[dlt_ops.rules]` flips individual rules per project (`rule_id = false`); a missing entry means the registered default.
- `[sources.<X>.dlt_ops.rule_exemptions]` suppresses one rule's findings for one source, with a mandatory non-empty reason string. The rule still runs for every other source.
- Rule IDs are **stable within a major version** — the knob and exemption tables key on them.
- Unknown rule IDs in either table are errors: `validate` fails, and so does the Tier-2 runtime preflight on every `run` / `backfill` (the typo guard).

Inspect the resolution for your project — the command prints every known rule with its on/off state and owning plugin:

```console
$ dlt-ops pipeline validate --show-resolved-rules
Resolved rules (21):
  bigquery_partitioning                on   bigquery
  bigquery_partition_hints             on   bigquery
  import_safety                        on   core
  config_section_required              on   core
  schedule_required                    on   core
  explicit_source_name                 on   core
  module_name_matches_section          on   core
  orphan_config_sections               on   core
  no_resource_overlap                  on   core
  json_hints_for_dict_fields           on   core
  pydantic_columns_required            on   core
  schema_contract_declared             on   core
  explicit_resource_name_multi_source  on   core
  cursor_not_load_timestamp            on   core
  secret_backend_registered            on   core
  alert_sink_registered                on   core
  destination_capability               on   core
  stale_sources                        on   core
  assertion_config_valid               on   core
  assertion_columns_exist              on   core
  assertion_predicate_resolvable       on   core
```

A bare environment resolves **21 rules**: 19 from `core` and 2 from `bigquery`. The `airflow` provider adds one more (`airflow_var_required`) only when the `[airflow]` extra is installed — it loads either way, so `plugins doctor` stays green on a bare install, but contributes no rules until `airflow` is importable, and so does not appear above.

To switch a rule off, set it to `false` under `[dlt_ops.rules]`; to exempt one source, name the rule under that source's `[dlt_ops.rule_exemptions]` with a reason. Both key on the rule ID:

```toml
[dlt_ops.rules]
stale_sources = false                        # off project-wide

[sources.legacy_api.dlt_ops.rule_exemptions]
pydantic_columns_required = "third-party generator yields untyped rows"   # off for one source
```

Findings are errors unless tagged **warning** below; warnings fail the run only under `validate --strict`.

## Rule catalog

**Every rule this page documents, at a glance — owning plugin, enforcement, and what it checks.** Rule IDs are the exact strings `--show-resolved-rules` emits; the detailed sections below carry the rationale and edge cases, and `airflow_var_required` is active only with the `[airflow]` extra.

| Rule ID | Plugin | Enforcement | What it checks |
|---|---|---|---|
| [`import_safety`](#import_safety) | core | Tier 1 | Source modules do no network I/O, disk writes, pipeline runs, or process spawns at import (disk reads OK); enforced in the Phase-2 sandbox. |
| [`config_section_required`](#config_section_required) | core | Tier 1 | Every discovered source has a `[sources.<X>]` section in `.dlt/config.toml`. |
| [`schedule_required`](#schedule_required) | core | Tier 1 | Every source declares a valid `schedule` (`@hourly` … `@manual`). |
| [`explicit_source_name`](#explicit_source_name) | core | Tier 1 | `@dlt.source(name="<X>")` names its section explicitly. |
| [`module_name_matches_section`](#module_name_matches_section) | core | Tier 1 | Source module filename equals its config section (`source/<X>.py` ↔ `[sources.<X>]`). |
| [`orphan_config_sections`](#orphan_config_sections) | core | Tier 1 · warning | A `[sources.<X>]` section with no matching discovered source is flagged. |
| [`no_resource_overlap`](#no_resource_overlap) | core | Tier 1 | No two sources in a pipeline directory declare a resource with the same name. |
| [`json_hints_for_dict_fields`](#json_hints_for_dict_fields) | core | Tier 1 | Every Pydantic `dict` / `list[dict]` field carries a `data_type="json"` column hint. |
| [`pydantic_columns_required`](#pydantic_columns_required) | core | Tier 1 | Every `@dlt.resource` declares `columns=` resolving to a Pydantic model. |
| [`schema_contract_declared`](#schema_contract_declared) | core | Tier 1 | A declared `schema_contract` is exactly the canonical freeze literal (or the opted-in evolve literal); none = auto-applied. |
| [`explicit_resource_name_multi_source`](#explicit_resource_name_multi_source) | core | Tier 1 | In multi-source directories, every `@dlt.resource` passes an explicit `name=`. |
| [`cursor_not_load_timestamp`](#cursor_not_load_timestamp) | core | Tier 1 | No incremental cursor uses the configured `load_timestamp_column`. |
| [`secret_backend_registered`](#secret_backend_registered) | core | Tier 1 + Tier-2 twin | Every engaged secret backend resolves to a registered, healthy `secret_backend` plugin. |
| [`alert_sink_registered`](#alert_sink_registered) | core | Tier 1 + Tier-2 twin | Every configured `alert_sinks` name is a registered, constructible `alert_sink` plugin. |
| [`destination_capability`](#destination_capability) | core | Tier 1 + Tier-2 twin | The resolved destination supports each adapter-gated feature the source engages (else error, or a core-tier warning). |
| [`stale_sources`](#stale_sources) | core | Tier 1 · warning | A source with run history whose last run exceeds `staleness_days` (default 7) is flagged. |
| [`assertion_config_valid`](#assertion_config_valid) | core | Tier 1 + Tier-2 twin | Every `assertions.<resource>` table is structurally valid and references real resources and types. |
| [`assertion_columns_exist`](#assertion_columns_exist) | core | Tier 1 | Columns named in assertion params exist on the resource's Pydantic model. |
| [`assertion_predicate_resolvable`](#assertion_predicate_resolvable) | core | Tier 1 | Every custom assertion predicate imports and resolves to a callable (sandboxed). |
| [`bigquery_partitioning`](#bigquery_partitioning) | bigquery | Tier 1 | Every `bigquery_adapter(...)` call passes `partition=` and `cluster=` (per-call `# no-partition:` / `# no-cluster:` escape). |
| [`bigquery_partition_hints`](#bigquery_partition_hints) | bigquery | Tier 1 | Every resource resolving to BigQuery carries a real partition column hint at runtime (`_dlt_load_id` doesn't count). |
| [`airflow_var_required`](#airflow_var_required) | airflow | Tier 1 · `[airflow]` extra | A source using `dlt.secrets.value` configures `airflow_var` (only when `[airflow]` is installed). |

## Tiers

**Every rule on this page is Tier 1: statically checkable, run by `validate` (pre-deploy, CI).** The runtime does not trust that `validate` ever ran — every `run` / `backfill` additionally executes a narrow **Tier-2 preflight** that hard-fails on critical preconditions: a referenced plugin not registered (secret backend, alert sink, assertion type), a destination that dlt cannot resolve or that engages an adapter-gated feature it cannot provide (a registered-but-broken or capability-incomplete adapter fails here too), a plugin that soft-failed at load, an unknown rule ID in `[dlt_ops.rules]`, and backfill bounds supplied for resources without an incremental cursor. The overlap with Tier 1 (`secret_backend_registered`, `alert_sink_registered`, `destination_capability`, the `assertion_*` rules, the typo guard) is deliberate redundancy: orchestrator-triggered runs must fail fast rather than degrade silently. Tier 2 is not configurable and has no rule IDs — the per-rule tags below mark which rules have a Tier-2 twin.

One check sits outside the rule framework entirely: **import-error surfacing**. A source module that cannot be imported cannot run, so it is always reported by `validate` — no rule ID, no knob.

## Core rules (plugin: `core`)

**The core rules ship with the base distribution's `core` provider and are on by default.**

### `import_safety`

*Tier 1 (`validate`).*

**Source modules must be import-safe: no network I/O, no disk writes, no pipeline runs, no process spawns at module load** (disk **reads** are fine). Findings come from the Phase-2 sandbox, which imports each module under CPython audit hooks. This catches the orchestrator foot-gun where a module-level `requests.get(...)` fires on every scheduler heartbeat that parses the file. Disabling the rule (`import_safety = false`) also skips the sandbox entirely; per-module import errors are still isolated so a broken module never breaks sibling discovery.

### `config_section_required`

*Tier 1 (`validate`).*

**Every discovered source has a `[sources.<X>]` section in `.dlt/config.toml`.** A source without config cannot resolve a schedule, destination, or secrets.

### `schedule_required`

*Tier 1 (`validate`).*

**Every source declares `schedule` under `[sources.<X>.dlt_ops]`, and its value is one of `@hourly`, `@2hourly`, `@daily`, `@weekly`, `@monthly`, `@manual`.** The schedule is what orchestrator adapters build DAGs from.

### `explicit_source_name`

*Tier 1 (`validate`).*

**The `@dlt.source` decorator names its section explicitly: `@dlt.source(name="<X>")`.** Discovery and config resolution key on that name; relying on dlt's function-name fallback makes renames silently break the config link.

### `module_name_matches_section`

*Tier 1 (`validate`).*

**The source module's filename equals its config section (`source/<X>.py` ↔ `[sources.<X>]`).** This is load-bearing for dlt itself: `dlt.secrets.value` resolution uses the module name to find the right config section.

### `orphan_config_sections`

*Tier 1 (`validate`) · warning.*

**A `[sources.<X>]` section with no matching discovered source is flagged** — usually a leftover from a deleted source or a typo'd section name. Known dlt-native sections (`data_writer`, `normalize`, `load`, `extract`) are excluded.

### `no_resource_overlap`

*Tier 1 (`validate`).*

**No two sources within the same pipeline directory declare a resource with the same name.** Overlapping names make table ownership and cleanup ambiguous.

### `json_hints_for_dict_fields`

*Tier 1 (`validate`).*

**Every Pydantic model field typed `dict` / `list[dict]` carries a `data_type="json"` column hint.** Without it, dlt normalizes such fields into nested child tables instead of a single JSON column — on any destination.

### `pydantic_columns_required`

*Tier 1 (`validate`).*

**Every `@dlt.resource` declares `columns=` resolving to a Pydantic model.** Without a declared schema, dlt infers types at load time — and a column whose values are all NULL in the first load is silently dropped, then can never be added under a frozen-columns contract. Attribute references (`columns=cfg.model`, the factory pattern) are accepted.

### `schema_contract_declared`

*Tier 1 (`validate`).*

**A resource that declares no `schema_contract` passes — the runtime auto-applies the canonical contract (`{"tables": "evolve", "columns": "freeze", "data_type": "freeze"}`).** A declared contract must be exactly the canonical literal, or the evolve literal (`columns: "evolve"`) on a source that opted in with a non-empty `schema_contract_evolve_reason` in config. Anything else is an error: contracts are a project policy, not a per-resource preference.

### `explicit_resource_name_multi_source`

*Tier 1 (`validate`).*

**In a pipeline directory hosting more than one source, every `@dlt.resource` must pass an explicit `name=` kwarg (any expression form) so resource-to-source ownership can be attributed for the schema-contract check.** Single-source directories are unaffected.

### `cursor_not_load_timestamp`

*Tier 1 (`validate`).*

**No `dlt.sources.incremental(...)` uses the configured `[dlt_ops] load_timestamp_column` as its cursor.** That column advances on every run, so cursoring on it silently skips in-window source updates — use the provider's business timestamp instead. Inert when `load_timestamp_column` is unset.

### `secret_backend_registered`

*Tier 1 (`validate`) · Tier-2 twin.*

**Every source that engages a secret backend (or uses `dlt.secrets.value` in its signature) resolves to a registered, healthy backend plugin on the `secret_backend` axis.** The runtime preflight repeats the check.

### `alert_sink_registered`

*Tier 1 (`validate`) · Tier-2 twin.*

**Every name in `[dlt_ops] alert_sinks` is a registered `alert_sink` plugin that loads and constructs with its `[dlt_ops.alert_sink.<name>]` options.** Only explicitly configured names are enforced (an unset key means the built-in `logging` default). The runtime preflight repeats the check.

### `destination_capability`

*Tier 1 (`validate`) · Tier-2 twin.*

**Every source's resolved destination must support each adapter-gated feature the source engages.** The destination resolves through the config chain (per-source override, then `default_destination`); the rule then runs the same capability check as the Tier-2 preflight, so the tiers can't drift. It is an **error** when the destination is unresolvable through the config chain, is not a destination dlt can resolve (typo guard), or has a registered `DestinationAdapter` that fails to load or is capability-incomplete. It is also an **error** when the destination has no adapter *and* the source engages an adapter-gated feature that acts as a gate — assertion `quarantine` on a selected resource, `@with_checkpoints`, or `[dlt_ops] require_destination_adapter = true`. Otherwise an adapter-less destination is a **warning** naming the darkened adapter-gated features, because the source runs at core tier, which is allowed by design. The [destinations reference](../reference/destinations.md) has the full tier model. The runtime preflight repeats the capability check on every `run` / `backfill` and, unlike this rule, is not configurable.

Checkpoint engagement is detected by the Phase-1 **AST scan**: `@with_checkpoints` is seen when its terminal name appears as a decorator (bare or attribute form) in the source module or a shared `resource/*.py` sibling. An aliased import (`... import with_checkpoints as wc`) escapes the name match — such a source passes this rule as a core-tier warning, and the runtime `CheckpointManager` raises a typed error mid-run as the backstop.

### `stale_sources`

*Tier 1 (`validate`) · warning.*

**A source with run history in the `_dlt_ops_runs` ledger whose last run started more than `staleness_days` ago (default 7) is flagged as ingested-then-orphaned.** Sources with zero history are skipped — they have nothing to be stale relative to. Degrades gracefully: without destination access (unresolved destination, unreachable ledger) the rule stays quiet, so `validate` never requires credentials.

### `assertion_config_valid`

*Tier 1 (`validate`) · Tier-2 twin.*

**Every `[sources.<X>.dlt_ops.assertions.<resource>]` table is structurally sound and references real things.** The checks:

- the assertions value and every resource entry are tables;
- every resource key names a resource of the source (the live Phase-2 list);
- every non-reserved key names a registered `assertion` plugin (the error lists registered names);
- `on_failure` is one of `fail` / `quarantine` / `warn` at every level;
- `quarantine` is not set on a batch-scoped type;
- params pass the type's own `check_config` (shape/domain — e.g. `min_rows_per_load = -1`);
- `custom` entries are tables with a `predicate` in `module:attr` (or dotted) form;
- no plugin registers a reserved name (`on_failure`, `custom`).

Always-on in bare `validate` — there is no `--include-assertions` flag and no dry-run: facts that require extracting data are `run`'s job, not `validate`'s. The runtime preflight has a twin for unregistered types.

### `assertion_columns_exist`

*Tier 1 (`validate`).*

**Every column referenced by assertion params (`required_columns`, `unique_columns`, third-party types checking columns) exists on the resource's declared Pydantic `columns=` model.** Skipped (not failed) for a resource whose model is unresolvable — `pydantic_columns_required` already polices that separately. A separate rule ID from `assertion_config_valid` on purpose: a source with intentionally dynamic columns can exempt this check without disabling structural config validation.

### `assertion_predicate_resolvable`

*Tier 1 (`validate`).*

**Every `[[...custom]]` assertion predicate imports and resolves to a callable: module importable, attribute present, attribute callable.** The probe runs in the same audit-hook sandbox child as `import_safety`, calling the runtime's own predicate resolver — so a failing predicate fails `validate` with exactly the message `run` would produce, import side effects never run inside the `validate` process, and import-time network I/O, disk writes, pipeline construction, or process spawns in a predicate module are reported as import-safety findings under this rule.

## BigQuery rules (plugin: `bigquery`)

**These two rules ship with the core distribution's `bigquery` validator provider and are active by default; both no-op for projects that never touch BigQuery.**

### `bigquery_partitioning`

*Tier 1 (`validate`).*

**The AST half: every `bigquery_adapter(...)` call in a pipeline directory passes `partition=` and `cluster=`.** Unpartitioned BigQuery tables scan-charge the full table on every query. Escape hatch per call site: a `# no-partition: <reason>` / `# no-cluster: <reason>` comment on the line above the call.

### `bigquery_partition_hints`

*Tier 1 (`validate`).*

**The runtime half: every resource whose resolved destination is BigQuery carries a real partition column hint at runtime** — catching resources that never went through a `bigquery_adapter()` call and are invisible to the AST check. `_dlt_load_id` does not count (STRING; BigQuery silently ignores STRING partition keys). Sources resolving to any other destination are skipped.

## Airflow rules (plugin: `airflow`)

**Registered only when the `[airflow]` extra is installed.** The provider loads either way, so `plugins doctor` stays green on a bare install, but it contributes no rules — and `airflow_var_required` does not appear in `--show-resolved-rules` — unless `airflow` is importable.

### `airflow_var_required`

*Tier 1 (`validate`) · requires the `[airflow]` extra.*

**A source whose signature uses `dlt.secrets.value` must configure `airflow_var` in `[sources.<X>.dlt_ops]`** — otherwise the Airflow secret backend has nothing to fetch from and the DAG fails at runtime instead of at review time. Only meaningful for Airflow-orchestrated projects, which is exactly why it lives in the Airflow plugin and not core.

## Third-party rules

**Any installed distribution can register a validator provider (a zero-argument callable returning rule specs) under the `dlt_ops.validators` entry-point group.** Its rules join this resolution with the same knob and exemption mechanics, and appear in `--show-resolved-rules` with their plugin name. See [Write plugins](../guides/write-plugins.md).

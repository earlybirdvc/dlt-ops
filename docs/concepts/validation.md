---
description: The two-tier enforcement model behind dlt-ops — static pipeline validate (Tier 1) with a provider-supplied rule framework you switch off per project or per source, plus a non-configurable runtime preflight (Tier 2) that re-checks critical preconditions on every run and backfill.
---

# Validation

The enforcement model behind `pipeline validate` and the runtime preflight that `run` and `backfill` execute: two tiers, one rule framework, and the switches for turning individual rules off. Read this to understand what is checked when — and why some checks run twice on purpose.

**At a glance**

| What it is | When each runs | Rules from | On failure | Canonical detail |
|---|---|---|---|---|
| Two enforcement tiers over discovery output: static `validate` (Tier 1) and a runtime preflight (Tier 2) | Tier 1 in CI / before deploy (no destination credentials); Tier 2 at the top of every `run` and `backfill` | Providers on the `dlt_ops.validators` axis — 21 `core` rules plus `bigquery`/`airflow` | Tier 1 renders every finding and fails on errors (`--strict` promotes warnings to errors); Tier 2 hard-fails before extract and is not configurable | [Rules reference](../configuration/rules.md) — every rule ID, tier, and message |

## Two tiers of enforcement

**Tier 1 is `pipeline validate`**: everything statically checkable — layout and naming, config sections and schedules, schema contracts, Pydantic column declarations, assertion config, destination capability, plugin registration, import safety — checked before anything runs. It is the command you put in CI and run before deploy. Findings are errors or warnings, and `--json` emits them machine-readably for CI. Tier 1 can also surface purely operational facts without gating on them: the `stale_sources` rule warns about sources that had run history and then stopped, and stays quiet when the ledger is unreachable — `validate` never requires destination credentials.

**`--strict` decides whether warnings are fatal, not whether they are visible.** Every run renders every finding, in both the human and the `--json` output; the severity is what differs. A plain run prints warnings under `⚠` and exits 0 as long as no finding is an error, so a project whose only findings are warnings ends on `✓ No errors (N warning(s))`. `--strict` promotes those warnings to errors — same messages, rendered under `✗` — and the command exits 1. Three core rules emit warnings: `orphan_config_sections`, `stale_sources`, and `destination_capability` when it reports core-tier degradation. Run `validate` plain to see the operational picture without gating on it; add `--strict` in CI when a warning should block the pipeline.

**Tier 2 is the runtime preflight**: a narrow re-check at the top of every `run` and `backfill`, because the runtime does not trust that `validate` ever ran. Production schedulers execute the run entry point directly — there is no CLI step in front of an orchestrator-triggered run, so a violated precondition must fail fast at runtime rather than degrade silently. The preflight hard-fails, before any pipeline work, on five conditions:

1. A referenced plugin is not registered on its axis — secret backends, alert sinks, assertion types.
2. The destination fails the capability check: the name does not resolve as a dlt destination (the typo guard), a registered `DestinationAdapter` fails to load or is missing part of the required capability surface, or no adapter is registered while the run engages an adapter-gated feature — checkpoints, assertion quarantine on a selected resource, backfill's chunk state, or `[dlt_ops] require_destination_adapter = true`.
3. A referenced plugin soft-failed at load — the run would otherwise silently lose the feature.
4. `[dlt_ops.rules]` references an unknown rule ID.
5. Backfill bounds were supplied but a selected resource declares no incremental cursor — without one, the injected window is silently ignored and every chunk re-extracts everything.

The overlap with Tier 1 is deliberate — the redundancy is the point — and the two tiers share their check implementations (the unknown-rule-ID guard and the destination capability check are literally the same functions), so they cannot drift. Tier 2 is not configurable and has no rule IDs: the five conditions, nothing else. What each failure does to the run is specified in [failure semantics](failure-semantics.md).

The typo guard, caught at Tier 2 with `stale_source` (missing `s`) configured in `[dlt_ops.rules]`:

```bash
dlt-ops pipeline run -s demo_events -y
```

```text
dlt_ops.preflight.UnknownRuleIdError: unknown rule id(s) in [dlt_ops.rules]: stale_source; valid rule ids: alert_sink_registered, assertion_columns_exist, assertion_config_valid, ...
```

The run exits 1 before extract. A config entry that silently did nothing would be worse than the failure.

## Rules come from providers

**Rules are not hard-coded into `validate`.** They arrive as specs from **providers** registered in the `dlt_ops.validators` entry-point group; a provider is a zero-argument callable returning rule specs, and installing a distribution that registers one auto-activates its rules. The package's own rules ship through the same mechanism — three first-party providers:

- **`core`** — 21 rules, destination- and orchestrator-agnostic; all on by default except `incremental_cursor_required`.
- **`bigquery`** — 2 rules that ship in the main distribution: AST and column-hint checks with no BigQuery SDK involved, so they resolve without the `[bigquery]` extra installed, and no-op for projects that never touch BigQuery.
- **`airflow`** — contributes its rule only when Airflow is importable, i.e. with the `[airflow]` extra.

Inspect exactly what resolved for your environment — on a bare install, 23 rules:

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
  explicit_source_name                 on   core
  module_name_matches_section          on   core
  orphan_config_sections               on   core
  no_resource_overlap                  on   core
  json_hints_for_dict_fields           on   core
  pydantic_columns_required            on   core
  pydantic_model_forbids_extra         on   core
  schema_contract_declared             on   core
  explicit_resource_name_multi_source  on   core
  cursor_not_load_timestamp            on   core
  incremental_cursor_required          off  core
  secret_backend_registered            on   core
  alert_sink_registered                on   core
  destination_capability               on   core
  stale_sources                        off  core
  assertion_config_valid               on   core
  assertion_columns_exist              on   core
  assertion_predicate_resolvable       on   core
```

(`stale_sources` shows `off` here because this project disabled it — see the next section. `incremental_cursor_required` shows `off` because that is how it ships: it is the one core rule a project opts into rather than out of.) The [rules reference](../configuration/rules.md) documents every rule: what it checks, why, and its error-versus-warning status.

The provider mechanism follows the plugin loader's soft-fail policy in one direction only: a provider that raises on load is recorded rather than crashing the process, but it is not tolerated. Its rules are missing, so `validate` reports it as an error on an ordinary run — a project silently validating against a shrunken rule set is the failure this prevents. `--show-resolved-rules` also lists it under "Unavailable rule providers", and `plugins doctor` shows the load error. A rule ID already claimed by an earlier provider is skipped and recorded — rule IDs are globally unique and stable within a major version, because the config switches key on them. Third-party distributions register providers the same way; see [plugins](plugins.md).

## Switching rules off

**Two switches exist, at two scopes, and both are typo-guarded.**

**Per project** — `[dlt_ops.rules]` overlays the registry defaults. A missing entry means the rule's registered default — on for every shipped rule except `incremental_cursor_required`, which ships off. `false` disables a rule project-wide, `true` adopts an opt-in one:

```toml
[dlt_ops.rules]
stale_sources = false
```

An unknown rule ID in this table is an error at both tiers (`validate` fails, and so does every `run`, as shown above), and a non-boolean value is a config error too.

**Per source** — `[sources.<X>.dlt_ops.rule_exemptions]` suppresses one rule's findings for one source, and every exemption carries a mandatory, non-empty written reason. The rule still runs for every other source. An empty or missing reason is a config error, never a silently weaker exemption:

```toml
[sources.demo_events.dlt_ops.rule_exemptions]
orphan_config_sections = ""
```

```text
✗ 1 error(s):
  [demo_events] rule_exemptions.orphan_config_sections: exemption for rule 'orphan_config_sections' in [sources.demo_events.dlt_ops.rule_exemptions] requires a non-empty reason string: orphan_config_sections = "<why this source is exempt>"
```

The reason string is for your reviewers, not for the tool — it turns "we disabled a check" into a documented decision that survives in config next to the source it covers.

Know the limits of the switches. Import health sits outside the rule framework: a source module that cannot import cannot run, so `validate` always reports it — no rule ID, no knob — and alongside it a `validation_coverage` error names, per excluded source, the rule coverage that exclusion cost, so a shrunken pass never renders as a clean one. The Tier-2 preflight is not configurable at all. And switches silence findings without changing behavior: disabling `schema_contract_declared` does not stop the runtime from applying the canonical freeze contract to the resources it applies it to, and no switch teaches [discovery](discovery.md) a different layout.

The inverse also holds, and it is the one case where silencing a rule does cost you enforcement. A Pydantic `columns=` model's contract comes from the model's own `extra` setting, which dlt reads at decoration time — the runtime deliberately does not overwrite it, since doing so would equally overrule an author's opted-in `extra="allow"`. So exempting `pydantic_model_forbids_extra` for a source leaves its models on Pydantic's default, and unknown columns on that source are dropped silently rather than failing. That is a real decision, not a formality: write the reason string accordingly.

## What `validate` refuses to do

**`validate` is static analysis, and it stays that way: there is no assertion dry-run mode and no flag to "also execute the data checks".** The three `assertion_*` rules verify structure (tables well-formed, types registered, `on_failure` values valid), column references against the declared Pydantic models, and custom-predicate resolvability — probed in the same audit-hook sandbox as source modules, so a predicate's import side effects never run inside the `validate` process. Facts that require extracting data are `run`'s job; the gates themselves execute between extract and load, per [assertions](assertions.md).

## Where next

- [Rules reference](../configuration/rules.md) — every rule ID, what it checks, and how to override it
- [Failure semantics](failure-semantics.md) — what a Tier-2 refusal does to the run, and the full failure contract
- [Discovery](discovery.md) — the two-phase scan that produces what `validate` checks

---
description: How everything pluggable in dlt-ops extends through one mechanism — Python entry points in six fixed axes (destination, orchestrator, assertion, validators, secret_backend, alert_sink) — with a lazy soft-fail-but-hard-on-collision loader, plugins doctor diagnostics, and two-tier enforcement of every plugin reference.
---

# Plugins

Everything extensible in `dlt-ops` extends through one mechanism: Python entry points, in six fixed axes. Installing a distribution that registers an entry point is the whole integration — no config to enable it, no registry to apply to, no blessing from this project. Read this to understand the axes, the loader's exact policy (lazy, soft-fail, hard on collisions), and how `plugins doctor` and the enforcement tiers keep a plugin-assembled toolchain diagnosable.

**At a glance**

| What it is | The mechanism | The six axes | Loader policy | Where next |
|---|---|---|---|---|
| One extension mechanism for everything pluggable in `dlt-ops` — no config to enable, no blessing from this project | Python entry points in groups named `dlt_ops.<axis>` (frozen public API); installing a distribution that registers one is the whole integration | Six fixed extension points, one entry-point group each — named and specced in the table below | Lazy, metadata-only scans; soft-fail on load errors (hard once a run references one); hard error on name collisions | [Write plugins](../guides/write-plugins.md); inspect with `plugins doctor` |

## The six axes

**An axis is one extension point, with an entry-point group named `dlt_ops.<axis>`.** The group names are frozen public API — renaming one breaks every plugin — and the set of axes is closed until a later major:

| Axis | Entry-point group | Contract | Ships in the box |
|---|---|---|---|
| `destination` | `dlt_ops.destination` | `DestinationAdapter` Protocol | `duckdb`, `postgres`, `bigquery` |
| `orchestrator` | `dlt_ops.orchestrator` | reserved | — (see note below) |
| `assertion` | `dlt_ops.assertion` | `AssertionType` Protocol | `min_rows_per_load`, `max_rows_per_load`, `required_columns`, `unique_columns` |
| `validators` | `dlt_ops.validators` | zero-arg callable returning `RuleSpec`s | `core`, `bigquery`, `airflow` |
| `secret_backend` | `dlt_ops.secret_backend` | `SecretBackend` Protocol | `secrets_toml`, `airflow` |
| `alert_sink` | `dlt_ops.alert_sink` | `AlertSink` Protocol | `logging`, `sentry` |

What lands on each axis: a registered `DestinationAdapter` takes its destination to [full tier](destinations-and-tiers.md); an [assertion](assertions.md) type becomes declarable in every project's assertion config; a validator provider's rules auto-activate in [`validate`](validation.md); a secret backend can claim sources and feed `dlt.secrets`; an alert sink receives [reconciler](reconciler.md) findings. The `orchestrator` axis is reserved: nothing registers under it in v0.1 — the first-party Airflow adapter ships its pluggable pieces (secret backend, validator provider) through their own axes and its DAG factory as a direct import from the `[airflow]` extra ([scheduling and orchestration](scheduling-and-orchestration.md)).

First-party plugins go through exactly the same groups — the package's own `pyproject.toml` registers `duckdb = "dlt_ops.destinations.duckdb:DuckDBAdapter"` the same way a third party would — so there is one lookup path and no privileged registry. For quick experiments and tests, a runtime twin feeds the same registry without packaging:

```python
import dlt_ops

@dlt_ops.register("alert_sink", "my_sink")
class MySink: ...
```

## Loader policy

**The registry's behavior is a small set of deliberate rules:**

- **Discovery is automatic.** An installed entry point in a `dlt_ops.<axis>` group is registered, full stop. There is no `plugins = [...]` allow-list to maintain.
- **Scans are lazy and metadata-only.** Enumerating an axis reads entry-point metadata without importing anything; a plugin's module is imported on the first lookup of that specific plugin, and the loaded object is cached for the process. Installing a plugin with heavy imports does not tax every CLI invocation that never uses it.
- **Load failures are soft — until the plugin is referenced.** A plugin whose import raises is recorded instead of crashing the process: the runtime continues, `plugins doctor` reports the failure, and `validate` flags it wherever config references it. The moment a *run* actually engages a soft-failed plugin, Tier-2 preflight hard-fails — a run that kept going would silently lose the feature, which the [failure-semantics contract](failure-semantics.md) forbids.
- **Collisions are hard errors.** Two distinct plugins claiming the same `<axis>/<name>` fail every lookup of that name until config picks a winner. There is no silent first-wins — which of two adapters handles your destination is not a thing to leave to import order.

## `plugins doctor`

**One verb reports the whole registry: every axis, every registered name with its origin (distribution and object path), every load failure, every collision.** On a bare install:

```bash
dlt-ops plugins doctor
```

```text
destination:
  bigquery  [dlt-ops]  dlt_ops.destinations.bigquery:BigQueryAdapter
  duckdb  [dlt-ops]  dlt_ops.destinations.duckdb:DuckDBAdapter
  postgres  [dlt-ops]  dlt_ops.destinations.postgres:PostgresAdapter
orchestrator: (none)
assertion:
  max_rows_per_load  [dlt-ops]  dlt_ops.assertions.builtin:MaxRowsPerLoad
  min_rows_per_load  [dlt-ops]  dlt_ops.assertions.builtin:MinRowsPerLoad
  required_columns  [dlt-ops]  dlt_ops.assertions.builtin:RequiredColumns
  unique_columns  [dlt-ops]  dlt_ops.assertions.builtin:UniqueColumns
validators:
  airflow  [dlt-ops]  dlt_ops.airflow.validators:airflow_rules
  bigquery  [dlt-ops]  dlt_ops.bigquery.validators:bigquery_rules
  core  [dlt-ops]  dlt_ops.discovery.validators:core_rules
secret_backend:
  airflow  [dlt-ops]  dlt_ops.airflow.secrets:AirflowVariableBackend
  secrets_toml  [dlt-ops]  dlt_ops.secrets.default:SecretsTomlBackend
alert_sink:
  logging  [dlt-ops]  dlt_ops.reconciler._emission:LoggingAlertSink
  sentry  [dlt-ops]  dlt_ops.sentry:SentryAlertSink
plugins doctor: OK
```

Doctor loads each plugin to prove it imports, so it is the diagnostic to run after installing or upgrading anything: a broken plugin shows as `FAILED: <error>` on its line, a contested name shows as `COLLISION` with the claimants and the exact fix. The exit code is CI-usable — 0 only when every registered plugin loads cleanly and no name is contested. Note what doctor checks: registration and importability, not extras — the `sentry` sink loads without `sentry-sdk` installed and fails later, at construction, if you configure it without the `[sentry]` extra.

## Collisions and `[dlt_ops.plugins]`

**A collision means two *distinct* plugins claim one `<axis>/<name>`** — for example, two Snowflake adapter distributions both registering `destination/snowflake`. (The same object re-exported by the same distribution twice is deduplicated, not a collision.) Every lookup of a contested name raises with the exact config block that resolves it. Reproduced here with two runtime registrations — installed distributions produce the same message with their distribution names in place of `<runtime>`:

```python
from dlt_ops import register
from dlt_ops.plugins import get, set_disambiguation

@register("destination", "snowflake")
class AcmeSnowflakeAdapter: ...

@register("destination", "snowflake")
class OtherSnowflakeAdapter: ...

get("destination", "snowflake")
```

```text
PluginCollisionError: multiple plugins register destination/snowflake: '<runtime>' (__main__:AcmeSnowflakeAdapter), '<runtime>' (__main__:OtherSnowflakeAdapter). Pick a winner (distribution name or qualified object path) in .dlt/config.toml:

[dlt_ops.plugins.destination]
snowflake = "__main__:AcmeSnowflakeAdapter"
```

The `[dlt_ops.plugins.<axis>]` table in `.dlt/config.toml` is the disambiguation mapping — one entry per contested name, its value either the winning **distribution name** or the winning **qualified object path** (dotted and `module:attr` forms both match). Loading the project config installs the table into the process registry, and every project-scoped CLI verb and runtime path loads the config — so the TOML entry alone resolves the collision for `validate`, `run`, `reconcile`, and friends.

The one exception is `plugins doctor`: it inspects raw registry state without loading a project, so it keeps reporting the collision as information — doctor is the detector, a green `validate` proves the pick. `dlt_ops.plugins.set_disambiguation` is the same mechanism as a Python API; continuing the session above resolves the name and clears the collision:

```python
set_disambiguation({"destination": {"snowflake": "__main__.AcmeSnowflakeAdapter"}})
print("resolved:", get("destination", "snowflake").__name__)
```

```text
resolved: AcmeSnowflakeAdapter
```

A disambiguation entry that matches none of the claimants leaves the collision standing — the mapping selects among real candidates, it cannot invent one. An entry naming an unknown *axis* is a config error: `load_project_config` raises immediately, so a typo in the table fails loudly instead of rotting as a dead knob.

## Enforcement: two tiers, same policy as everything else

**The plugin system is wired into both [enforcement tiers](validation.md), so a plugin reference in config is never a silent no-op:**

- **Tier 1 (`validate`)** checks that every plugin referenced from config is registered and loadable: `alert_sink_registered` for each name in `[dlt_ops] alert_sinks`, `secret_backend_registered` for each source's engaged backend, `destination_capability` for the destination axis, and the three `assertion_*` rules for assertion types. A typo fails fast with a pointer at the diagnostic:

    ```text
    ✗ 1 error(s):
      [dlt_ops.alert_sinks] alert_sinks: alert_sinks references 'pagerduty' but no such plugin is registered under the 'dlt_ops.alert_sink' entry-point group; inspect with `dlt-ops plugins doctor`
    ```

- **Tier 2 (runtime preflight)** re-checks the plugins the run actually engages — the secret backend each source resolved to, every configured alert sink (including that it constructs with its options), every assertion type the selected resources reference — because a scheduler-triggered run never ran `validate` first. Registered-but-soft-failed is as fatal as unregistered.

Validator plugins add one more property: rules auto-activate when their provider is installed, and deactivate coherently when it cannot apply — the `airflow` provider always loads (so a bare install's doctor stays green) but contributes its rule only when Airflow is importable. Every plugin-owned rule remains switchable through the same `[dlt_ops.rules]` knob and per-source exemptions as core rules, keyed by rule IDs that are globally unique and stable within a major version.

## Packaging model

**First-party adapters ship as optional extras of the main distribution** — `dlt-ops[airflow]`, `dlt-ops[bigquery]` — one release cycle, one import path, with hard imports of the extra's dependencies confined to the subpackage so the core never pays for them at import time. Community plugins are **separate distributions** registering the same entry-point groups: `pip install acme-dlt-snowflake` next to `dlt-ops` is a complete integration. Two naming rules keep the shared namespace usable: pick names that identify your implementation rather than generic terms, and never squat the names first-party plugins already claim (`duckdb`, `bigquery`, `postgres`, `logging`, `sentry`, `secrets_toml`, `airflow`, `core`) — genuine collisions are user-resolvable, but shipping one on purpose helps nobody.

## Where next

- [Write plugins](../guides/write-plugins.md) — worked examples for alert sinks, assertion types, validator providers, and secret backends
- [Write a destination adapter](../guides/write-a-destination-adapter.md) — the full-tier axis, end to end
- [Validation](validation.md) — how plugin-owned rules join the resolved rule set

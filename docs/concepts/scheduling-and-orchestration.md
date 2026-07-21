---
description: How dlt-ops handles scheduling without being an orchestrator — every source declares a schedule tag in TOML, discovery carries it, and an orchestrator adapter (Airflow first) turns the discovered project into native DAGs, plus the orchestrator-neutral core interface that keeps a future self-scheduler from forcing a rewrite.
---

# Scheduling and orchestration

`dlt-ops` is not an orchestrator and never runs a scheduler loop. What it owns is the metadata and the machinery around one: every source declares a `schedule` in TOML, discovery carries it, and an orchestrator adapter (Airflow first) turns the discovered project into the orchestrator's native objects. Read this to understand the schedule contract, what stays orchestrator-neutral in core, what the Airflow adapter does — and the design ladder that keeps a future self-scheduler from requiring a rewrite.

**At a glance**

| What it is | What owns scheduling | The schedule contract | Orchestrator adapters | Canonical detail |
|---|---|---|---|---|
| Schedule metadata plus the orchestrator-neutral machinery around a scheduler — `dlt-ops` runs no scheduler loop itself | The orchestrator (Airflow first); core owns discovery, schedule tags, and window/selection decisions as plain data | Every source declares one `schedule` tag from a closed set (below), enforced by the `schedule_required` rule | Translate discovery output into native objects — one DAG per tag; ship on the `[airflow]` extra | [Airflow guide](../guides/airflow.md); [deployment](../guides/deployment.md) |

## The `schedule` key

**Every source must declare a `schedule` under its `[sources.<X>.dlt_ops]` table** — it is one of the nine [layout conventions](../getting-started/project-layout.md), enforced by the `schedule_required` rule. The value comes from a closed set:

| Value | Meaning |
|---|---|
| `@hourly` | Every hour |
| `@2hourly` | Every two hours |
| `@daily` | Every day |
| `@weekly` | Every week |
| `@monthly` | Every month |
| `@manual` | Never scheduled; runs only when explicitly triggered |

`validate` rejects both a missing key and a value outside the set:

```text
✗ 1 error(s):
  [web_events] schedule: Missing 'schedule' field in [sources.web_events.dlt_ops]
```

```text
✗ 1 error(s):
  [web_events] schedule: Invalid schedule '@fortnightly'. Valid: ['@hourly', '@2hourly', '@daily', '@weekly', '@monthly', '@manual']
```

The set is deliberately coarse — tags, not cron strings. A schedule value is a **grouping key**: every source sharing a tag runs together (one DAG per tag under Airflow), and the adapter owns turning the tag into the orchestrator's native scheduling syntax. Cron-precision scheduling in project config would push orchestrator policy into every project and fragment sources into one-off groups; the closed set covers the cadences of the moderate-volume scheduled batch ingestion this package targets, and `@manual` covers everything event-shaped. `pipeline list` shows the resolved tag per source — here the scaffolded demo project (`dlt-ops init demo --example`) plus the `@manual` source the [backfill](backfill.md) page adds to it:

```text
Found 2 source(s)

Name                           Pipeline        Schedule   Resources
----------------------------------------------------------------------
demo_events                    my_pipeline     @daily     1
web_events                     web             @manual    1
```

## The core orchestrator interface

**Everything an orchestrator needs that is *not* orchestrator-specific lives in `dlt_ops.orchestration`, importable without any orchestrator installed:**

- **`scheduled_sources(project_root)`** groups Phase-1 sources by `Schedule`. It is pure AST — it never imports project code, so an adapter may call it where project code must never execute: DAG-parse time. Sources with no (or invalid) config group under `@manual` rather than disappearing. Against the same two-source demo project:

    ```python
    from pathlib import Path
    from dlt_ops.orchestration import scheduled_sources

    for schedule, sources in scheduled_sources(Path(".")).items():
        print(schedule.value, [s.name for s in sources])
    ```

    ```text
    @daily ['demo_events']
    @manual ['web_events']
    ```

- **`filtering_decision(...)`** and **`resolve_window(...)`** compute the manual-trigger *decisions* as plain data: should this (source, resource) unit run under a trigger's selection, and what `[start, end)` window applies — explicit overrides outranking the orchestrator's native interval, a partial override replacing just its edge. Adapters map the verdicts onto native mechanics (a skip exception, a data interval) but never re-implement the logic.
- **`run_source(...)`** is the run entry: Phase-2 introspection of the one named source, secrets through the [secret-backend axis](plugins.md), then the same shared runner the CLI uses — Tier-2 preflight, the runs ledger, trace persistence, and window injection via `TimeIntervalContext` all live in the runner, not per adapter.

An adapter is therefore thin by construction: it keeps only what is genuinely native — task shapes, skip exceptions, its own data intervals, and the materialization of `Schedule` tags into its scheduling syntax.

## The Airflow adapter

**The adapter ships as the `[airflow]` extra (`pip install "dlt-ops[airflow]"`).** Its plugin surface — the Variable secret backend and the Airflow validator provider — is importable *without* Airflow, so a bare install's `validate` and `plugins doctor` stay healthy; the adapter surface hard-requires it and says so:

```python
from dlt_ops.airflow import build_schedule_dags
```

```text
ImportError: Apache Airflow is not installed; the dlt-ops Airflow adapter requires it. Install the extra: pip install 'dlt-ops[airflow]'
```

!!! note
    Airflow is not installed in the environment these docs are verified in; the snippets below show the adapter's API contract as pinned by its test suite (`tests/test_airflow_runtime.py`), not a pasted Airflow session.

### The DAG factory

**One DAG file in your dags folder builds every DAG from discovery output:**

```python
# dags/ingestion_dags.py — the dlt-ops project lives inside the dags folder
from pathlib import Path

from dlt_ops.airflow import build_schedule_dags

globals().update(build_schedule_dags(Path(__file__).parent / "my_project"))
```

`build_schedule_dags` produces one DAG per discovered schedule group, id `{dag_prefix}_{schedule-minus-@}` (`dlt_daily`, `dlt_2hourly`, ...; the prefix and extra `DAG(...)` kwargs are parameters). `@manual` sources build a trigger-only DAG (`schedule=None`).

Inside each DAG, every source becomes a task group (dlt's own `PipelineTasksGroup`, keyed by the source's config section) with one task per Phase-1 static resource, task id `{source}.{source}_{resource}`; a source whose resources only materialize dynamically gets a single whole-source task. `catchup` defaults to off — missed windows are re-runnable through the manual-trigger conf instead — and an optional `cleanup_data_dir` appends a scratch-directory hygiene task that sweeps stale `dlt_*` working directories off the workers.

Keeping the project root inside the dags folder is deliberate: `PipelineTasksGroup` points dlt's own config/secrets resolution at the dags folder, so the project's `.dlt/` directory serves both parse time and run time. Per-environment deployments are config, not code — point prod and staging at their own project roots, each with a `config.toml` naming the right destination and dataset.

Two schedule tags materialize as explicit cron, fixed by design with no config knob: `@2hourly` becomes `0 */2 * * *` (Airflow has no such preset), and `@weekly` becomes `0 0 * * 1` — Monday 00:00 UTC, not Airflow's Sunday preset, because a scheduler fires when the interval closes and Monday is what closes the ISO week. The Monday pin is intentional, not a bug.

### Parse purity

**The factory's contract is that DAG parsing never executes your project code**: `build_schedule_dags` calls only Phase-1 discovery, and Phase-2 introspection, secret setup, and the actual run all happen inside the task body via `run_source`. This is the [orchestrator-parse foot-gun](discovery.md) closed at the second end — the scheduler re-parses DAG files continuously, and a source module with an import-time side effect would otherwise fire on every heartbeat. The adapter's tests pin it structurally: building DAGs over a project containing a boobytrapped module leaves the side effect unexecuted and the module unimported.

### Manual triggers

**Every generated DAG accepts a conf JSON with the shared selection contract:**

```json
{
  "start_date": "2024-01-01T00:00:00Z",
  "end_date": "2024-02-01T00:00:00Z",
  "source": "my_api",
  "resources": ["events", "users"]
}
```

`source` selects one source by config section; unselected units end in Airflow's *skipped* state, and a `source` value naming no known source fails the run — a typo must not skip the world silently. `resources` narrows within the selected source (per-resource tasks skip; a whole-source task passes the narrowing to the runner's resource filter). `start_date`/`end_date` override the run window over Airflow's native `data_interval_start/end`; a partial override replaces just its edge, and manual-schedule DAGs carry no native window, so a partial override there is an error. Runs triggered this way land in the [runs ledger](runs-ledger.md) with `trigger_source = "airflow"`.

### Secrets from Airflow Variables

**The `airflow` secret backend claims any source whose `[sources.<X>.dlt_ops]` table sets `airflow_var`**: at run start it fetches `Variable.get(<airflow_var>)` and writes the value to `dlt.secrets["sources.<X>.<airflow_var_key>"]` (key defaults to `api_secret_key`). Sources without the trigger key fall back to the default `secrets_toml` backend — dlt's native secrets file. Airflow itself is imported lazily at fetch time, and a Variable that does not exist raises a `SecretNotFoundError` naming the fix. One Airflow-specific `validate` rule rides along: `airflow_var_required` flags a source whose signature takes `dlt.secrets.value` but configures no `airflow_var` — it is auto-active exactly when Airflow is importable (meaningless on projects not orchestrated by Airflow) and switchable like any rule via `[dlt_ops.rules] airflow_var_required = false`.

## The design ladder: adapters first, self-scheduler later

**Orchestrator adapters are the first rung of a deliberate two-rung ladder: adapters now, an optional core self-scheduler later.** Today (X), orchestrator adapters translate discovery output into native primitives, and the orchestrator owns scheduling, retries, and the UI — maximum reuse of infrastructure you already run. The planned second rung (Y) inverts ownership: a `run-due` verb in core would consult each source's `schedule`, the runs ledger, and the clock to decide what is due and run it — reducing "orchestration" to any dumb trigger that can invoke a CLI every few minutes (cron, CI schedulers, Cloud Scheduler).

`run-due` does not exist in v0.1 and ships only if adoption justifies it; the ledger's reserved `y-scheduler` trigger vocabulary and the core interface above are the down payment. That interface is why the ladder is climbable: because the Airflow adapter already consumes plain-data decisions from core instead of owning discovery and filtering itself, a self-scheduler implements the same interface rather than forcing a rewrite — and the Airflow adapter would eventually become a thin wrapper delegating to the same verb.

## Where next

- [Airflow guide](../guides/airflow.md) — deploying a project on Airflow end to end
- [Deployment](../guides/deployment.md) — the honest ladder from dev loop to cron to orchestrator
- [Discovery](discovery.md) — the Phase-1 scan that makes parse-time DAG building safe

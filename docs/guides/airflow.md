---
description: Task guide — deploy a dlt-ops project on Apache Airflow; install the [airflow] extra, place the project in the dags folder, build one DAG per schedule tag from a single build_schedule_dags factory call, wire source secrets through Airflow Variables, and use manual-trigger conf to re-run a window, one source, or one resource.
---

# Schedule a project on Airflow

This guide takes a dlt-ops project from `schedule` tags in TOML to running Airflow DAGs: install the adapter, place the project in the dags folder, build every DAG from one factory call, wire secrets through Airflow Variables, and know which validate rule rides along. The [scheduling and orchestration concept page](../concepts/scheduling-and-orchestration.md) covers the schedule contract and the adapter design; this page is the deployment task.

**Prerequisites**

- A dlt-ops project whose sources carry `schedule` tags — this guide deploys the repository's `examples/basic_project` (one `@hourly` source, one `@daily`).
- The `dlt-ops[airflow]` extra (installed in step 1) and a running Apache Airflow ≥ 2.9 for the deployment itself.

**Steps at a glance**

1. [Install the adapter — and see what a bare install already has](#1-install-the-adapter-and-see-what-a-bare-install-already-has)
2. [Put the project inside the dags folder](#2-put-the-project-inside-the-dags-folder)
3. [One DAG file builds everything](#3-one-dag-file-builds-everything)
4. [What parse time is allowed to do: nothing of yours](#4-what-parse-time-is-allowed-to-do-nothing-of-yours)
5. [Secrets from Airflow Variables](#5-secrets-from-airflow-variables)
6. [Manual triggers: re-run a window, one source, or one resource](#6-manual-triggers-re-run-a-window-one-source-or-one-resource)

!!! note
    Airflow is not installed in the environment these docs are verified in. Every command below that runs without Airflow was executed and its output pasted; the Airflow-side snippets are shown against the adapter's API contract as pinned by its test suite (`tests/test_airflow_runtime.py`) and carry a caveat instead of fabricated output.

## 1. Install the adapter — and see what a bare install already has

**Install the adapter's `[airflow]` extra:**

```bash
pip install "dlt-ops[airflow]"
```

The extra pulls `apache-airflow>=2.9` (the oldest version the adapter is CI-tested against) on top of the core package. The adapter has two surfaces with different import requirements, and the split is what keeps laptops and CI honest: the **plugin surface** — the Variable secret backend and the Airflow validator provider — is importable *without* Airflow, so `validate` and `plugins doctor` stay healthy on a bare install; the **adapter surface** — the DAG factory and the cleanup task — hard-requires it. On the bare install this guide is verified in:

```bash
dlt-ops plugins doctor
```

```text
validators:
  airflow  [dlt-ops]  dlt_ops.airflow.validators:airflow_rules
  bigquery  [dlt-ops]  dlt_ops.bigquery.validators:bigquery_rules
  core  [dlt-ops]  dlt_ops.discovery.validators:core_rules
secret_backend:
  airflow  [dlt-ops]  dlt_ops.airflow.secrets:AirflowVariableBackend
  secrets_toml  [dlt-ops]  dlt_ops.secrets.default:SecretsTomlBackend
...
plugins doctor: OK
```

Both Airflow plugins are registered and healthy with no Airflow anywhere. Touch the adapter surface without the extra and the boundary announces itself:

```bash
python -c "from dlt_ops.airflow import build_schedule_dags"
```

```text
ImportError: Apache Airflow is not installed; the dlt-ops Airflow adapter requires it. Install the extra: pip install 'dlt-ops[airflow]'
```

## 2. Put the project inside the dags folder

**The deployment layout is one DAG file plus the project tree next to it:**

```text
dags/
├── ingestion_dags.py        # the one DAG file (step 3)
└── my_project/              # the dlt-ops project, unchanged
    ├── .dlt/
    │   ├── config.toml
    │   └── secrets.toml
    └── github_events/
        ├── source/
        └── resource/
```

Keeping the project root inside the dags folder is deliberate, not incidental: the adapter builds on dlt's own `PipelineTasksGroup`, which points dlt's config/secrets resolution at the dags folder — so the project's `.dlt/` directory serves both DAG-parse time and task run time from one place. Per-environment deployments are config, not code: prod and staging each get their own project root whose `config.toml` names the right destination and dataset through the normal [config chain](../configuration/index.md); the adapter adds no environment policy of its own.

## 3. One DAG file builds everything

**One DAG file builds every scheduled DAG from a single factory call:**

```python
# dags/ingestion_dags.py
from pathlib import Path

from dlt_ops.airflow import build_schedule_dags

globals().update(build_schedule_dags(Path(__file__).parent / "my_project"))
```

*(Not executed here — this file runs inside a live Airflow scheduler.)*

`build_schedule_dags` produces one DAG per discovered schedule group. Which groups your project has is answerable without Airflow, because the factory's grouping call is the importable core interface — against the example project:

```python
from pathlib import Path
from dlt_ops.orchestration import scheduled_sources

for schedule, sources in scheduled_sources(Path(".")).items():
    print(schedule.value, [s.name for s in sources])
```

```text
@hourly ['github_events_api']
@daily ['github_events_full']
```

So this project materializes as two DAGs, `dlt_hourly` and `dlt_daily` — ids are `{dag_prefix}_{schedule-minus-@}`, prefix `dlt` by default. Per the factory contract (pinned by the adapter's tests):

- Inside each DAG, every source becomes a task group keyed by its config section, with one task per Phase-1 static resource, task id `{source}.{source}_{resource}` — here `github_events_api.github_events_api_events`, and so on. A source whose resources only materialize dynamically gets a single whole-source task.
- `@manual` sources build a trigger-only DAG (`schedule=None`).
- Two tags materialize as explicit cron, fixed with no config knob: `@2hourly` becomes `0 */2 * * *`, and `@weekly` becomes `0 0 * * 1` — Monday 00:00 UTC, intentionally, because Monday is what closes the ISO week (Airflow's Sunday preset would fire with the week's own Sunday uncaptured).
- `catchup` defaults to off; missed windows are re-runnable through the manual-trigger conf instead.

The factory takes `dag_prefix`, `start_date`, `catchup`, and `dag_kwargs` (extra keyword arguments — tags, `default_args` — applied to every `DAG(...)`) for fleet-wide policy, and `cleanup_data_dir` / `cleanup_after_days` to append a worker-hygiene task: dlt's `PipelineTasksGroup` provisions a fresh `dlt_*` scratch directory per parse, stale ones bloat worker storage, and the appended `cleanup_old_dlt_files` task sweeps entries older than the threshold (default 3 days) after the pipeline groups finish.

## 4. What parse time is allowed to do: nothing of yours

**The factory's contract is that DAG parsing never executes project code:** `build_schedule_dags` calls only Phase-1 discovery — the pure AST scan — and Phase-2 introspection, secret setup, and the actual run all happen inside the task body via `run_source`. This closes the [orchestrator-parse foot-gun](../concepts/discovery.md) at its second end: the scheduler re-parses DAG files continuously, and a module-level `requests.get(...)` in a source file would otherwise fire on every heartbeat. The adapter's tests pin the guarantee structurally — building DAGs over a project containing a boobytrapped source module leaves the side effect unexecuted and the module unimported. Inside the task body, `run_source` goes through the same shared runner as the CLI: Tier-2 preflight, assertions, checkpoints, and a [runs ledger](../concepts/runs-ledger.md) row with `trigger_source = "airflow"`.

## 5. Secrets from Airflow Variables

**The `airflow` secret backend claims any source whose `[sources.<X>.dlt_ops]` table sets `airflow_var`:**

```toml
[sources.github_events_api.dlt_ops]
schedule = "@hourly"
airflow_var = "github_events_api_key"
# airflow_var_key = "api_secret_key"   # the default leaf it writes to
```

At task start — never at parse time; Airflow is imported lazily at fetch time — the backend runs `Variable.get("github_events_api_key")` and writes the value to `dlt.secrets["sources.github_events_api.api_secret_key"]`, where a source signature taking `api_secret_key=dlt.secrets.value` picks it up. Sources without `airflow_var` fall back to the default `secrets_toml` backend, dlt's native secrets file — the two coexist per source in one project. Other stores (Vault, cloud secret managers) plug into the same `secret_backend` axis; see [plugins](../concepts/plugins.md).

One Airflow-specific `validate` rule rides along with the extra: `airflow_var_required` flags a source whose signature uses `dlt.secrets.value` but whose config sets no `airflow_var` — a pipeline that works on your laptop (secrets.toml) and dies on the scheduler (no file there). The rule is auto-active exactly when Airflow is importable, because it is meaningless on projects not orchestrated by Airflow. On this bare install it is absent from the resolved set:

```bash
dlt-ops pipeline validate --show-resolved-rules
```

```text
Resolved rules (21):
  bigquery_partitioning                on   bigquery
  bigquery_partition_hints             on   bigquery
  import_safety                        on   core
  config_section_required              on   core
  schedule_required                    on   core
  ...
  assertion_predicate_resolvable       on   core
```

Install the `[airflow]` extra and `airflow_var_required` joins the list (provider `airflow`), reporting per offending source: `Source uses secrets but 'airflow_var' not configured in [sources.<X>.dlt_ops]` (the rule's message, quoted from its source). It is switchable like any rule — `[dlt_ops.rules] airflow_var_required = false` project-wide, or a per-source exemption with a written reason ([rules reference](../configuration/rules.md)).

## 6. Manual triggers: re-run a window, one source, or one resource

**Every generated DAG accepts a conf JSON with the shared selection contract:**

```json
{
  "start_date": "2024-01-01T00:00:00Z",
  "end_date": "2024-02-01T00:00:00Z",
  "source": "github_events_api",
  "resources": ["events"]
}
```

*(Triggering requires a live Airflow; the semantics below are the adapter contract, pinned by its tests.)* All keys are optional. `source` selects one source by config section — unselected units end in Airflow's *skipped* state, and a value naming no known source **fails** the run, because a typo must not skip the world silently. `resources` narrows within the selected source. `start_date`/`end_date` override the run window over Airflow's native `data_interval_start/end`, a partial override replacing just its edge — the injected window reaches every incremental resource through the runner, no per-source code. An empty conf runs everything with the native interval: a plain "clear and re-run" on a failed task re-extracts its own window. The [concept page](../concepts/scheduling-and-orchestration.md#manual-triggers) has the full decision rules; for large historical windows prefer `pipeline backfill`, which adds resumable [chunk state](backfill.md) no trigger conf carries.

## Troubleshooting: `SecretNotFoundError` at task start

**A source claimed by the Variable backend whose Variable does not exist fails its task at secret-setup time — before extract — with the fix in the message** (quoted from the backend's source):

```text
SecretNotFoundError: Airflow Variable 'github_events_api_key' does not exist; create it or fix the 'airflow_var' key in the source's [sources.<X>.dlt_ops] table
```

Create the Variable in the Airflow UI (Admin → Variables) or correct the `airflow_var` value. The failure is per source: sibling sources in the same DAG run normally.

## Where next

- [Scheduling and orchestration](../concepts/scheduling-and-orchestration.md) — the schedule contract, the core interface, and the adapter design ladder
- [Deployment](deployment.md) — where Airflow sits on the honest ladder from dev loop to cron
- [Runs ledger](../concepts/runs-ledger.md) — reading Airflow-triggered runs back with `pipeline status`

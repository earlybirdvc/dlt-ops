---
description: Task hub — the honest deployment ladder for a dlt-ops project; the dev loop, scheduled cron / GitHub Actions / GitLab CI triggers over one dlt-ops command per source (with a verified account of which state survives a fresh stateless runner), fanning out with discover_sources(), and the orchestrator rung pointing to the Airflow, Dagster, and Prefect guides.
---

# Deployment and scheduling

This page is the deployment ladder: how a dlt-ops project runs somewhere other than your terminal — three rungs from the dev loop, to a scheduled trigger (cron, GitHub Actions, or GitLab CI), to an orchestrator (Airflow, Dagster, or Prefect). Every rung rides the same one command per source, so this page is the router: the contract they all share, the concrete cron and CI recipes, and pointers to the orchestrator guides. The scheduled rung is the moderate-volume batch ingestion this toolchain targets; it carries the part people guess wrong, so most of the page lives there: which state survives a runner that starts from a fresh checkout every time, and which does not.

**The ladder at a glance**

| Rung | What triggers a run | What it adds | Covered in |
|---|---|---|---|
| Dev loop | you, at a terminal | the baseline: `validate` → `run` → `status` against local DuckDB | [The dev loop](#the-dev-loop) |
| Scheduled trigger | cron, GitHub Actions, GitLab CI — any scheduler firing one command | unattended runs; you mirror the `schedule` tag into the trigger by hand | [Simple scheduled ingestion](#simple-scheduled-ingestion-cron-or-a-ci-runner) |
| Orchestrator | Airflow, Dagster, or Prefect, from your `schedule` tags | native retries and alerting, per-source scheduling, window re-runs | [Graduate to an orchestrator](#graduate-to-an-orchestrator) |

## Every target rides one command

**Whatever fires it — cron, GitHub Actions, GitLab CI, or an orchestrator — a scheduled run is one command per source, and the trigger reads its exit code:** `dlt-ops pipeline run -s <source> -y` returns `0` on success and `1` on any failure. Two facts turn that single command into every recipe below.

**Fan out from discovery, not a hand-kept list.** The public [`dlt_ops.discover_sources()`](../reference/api.md) returns every source keyed by name, each carrying its `schedule` tag — so a CI matrix, a crontab, or an orchestrator's asset list is generated from the project, never transcribed by hand. Run it from the project root:

```python
from pathlib import Path
from dlt_ops import discover_sources

for name, info in discover_sources(Path(".")).items():
    print(name, info.config.schedule.value)
```

```text
github_events_api @hourly
github_events_full @daily
```

**The `schedule` tag is the cadence source of truth — but only the Airflow adapter reads it for you.** Every source declares one preset tag from a [closed set](../concepts/scheduling-and-orchestration.md) (`@hourly` through `@manual`); on cron and CI you mirror that tag into the trigger's own cron expression by hand, one trigger or matrix cadence per group. The orchestrators below turn the tag into their native scheduling syntax.

## The dev loop

**Your machine is the first deployment target, and the [quickstart](../getting-started/quickstart.md) already covers it: `validate` → `run -s <source> -y` → `status` against local DuckDB.** Everything above this rung is the same CLI with the interactivity removed and the credentials externalized — there is no separate production mode and no build artifact beyond your checked-out project.

## Simple scheduled ingestion: cron or a CI runner

**This is the 90% case for this toolchain's audience: something invokes one shell command on a schedule inside a checkout of your project.** `dlt-ops` asks nothing else from the trigger — no agent, no daemon, no callback endpoint. Cron on a machine you keep, a GitHub Actions `schedule:` workflow, and a GitLab CI pipeline schedule are the three shapes shown here; anything equivalent (a systemd timer, a cloud scheduler poking a container) holds the same rung with the same command.

### The command the trigger runs

**`-y` is the whole non-interactive story:** it skips the source picker, the resource picker, and the start confirmation, and it makes `-s` mandatory — a scheduler must never sit on a prompt:

```text
Error: --source/-s required in non-interactive mode (--yes)
```

The exit code is the trigger's failure signal (above), and the [failure-semantics contract](../concepts/failure-semantics.md) defines exactly what fails a run — a preflight refusal, a tripped `fail` assertion, dlt's own load error. Every run whose destination and dataset resolve also lands in the [runs ledger](../concepts/runs-ledger.md) destination-side, failed outcomes with a one-line error summary — so a red cron mail and `pipeline status` tell one story, preflight refusals included. The exception is a run that cannot resolve its destination or dataset at all: the ledger row lives in that destination, so such a run shows up only in the trigger's log. Two more facts to settle before the first scheduled run:

- **Overlap control belongs to the trigger.** `dlt-ops` is a CLI, not a resident scheduler: nothing serializes two simultaneous `run`s of the same source (`backfill` is the one verb with cross-invocation coordination, via [chunk claims](../concepts/backfill.md)). Schedule with headroom and make the trigger queue rather than stack — the workflow below does it with a `concurrency` group.
- **`--root` replaces `cd`.** Every command takes the project root explicitly (`dlt-ops --root /opt/ingest/shop pipeline run -s orders -y`), which reads better in a crontab than a `cd &&` chain.

A daily source is then one crontab line — shown as config, since this page cannot run your cron daemon. Cron starts commands with a minimal environment, so the binary path must be absolute or on cron's `PATH`, and credentials come from an env file or a wrapper script (next section):

```text
# mirror the source's `schedule` tag by hand — nothing reads the TOML here
0 5 * * *  dlt-ops --root /opt/ingest/shop pipeline run -s orders -y >> /var/log/dlt-ops-orders.log 2>&1
```

### Credentials on a stateless runner

**`.dlt/config.toml` is code: destination, dataset, schedules, and rules ship with the checkout, and that is the point** — the effective configuration of a scheduled run is whatever is in git ([config model](../configuration/index.md)). Secrets are the one thing a fresh runner must be handed, through one of three doors:

- **Environment variables.** dlt's environment provider reads uppercase keys with `__` between segments, no dlt-ops involvement: `SOURCES__ORDERS__API_SECRET_KEY` resolves as `sources.orders.api_secret_key`, and `DESTINATION__POSTGRES__CREDENTIALS` carries the destination's connection string. This is the route for CI runners — scheduler secret store → env → dlt.
- **A `secrets.toml` written at deploy time.** `.dlt/secrets.toml` is never committed; a provisioning step can materialize it from your secret store before the run. On a machine you keep (the cron case) it can simply live on disk like any other credential file.
- **A secret-backend plugin.** The `secret_backend` [plugin axis](../concepts/plugins.md) fetches secrets at run start and writes them into `dlt.secrets` — the Airflow Variable backend is the shipped example, and a vault or cloud secret manager plugs into the same axis.

The scaffolded example and `examples/basic_project` need none of this — fixture-backed, local DuckDB, zero credentials — which is why every transcript on this page runs without a secret in sight.

Which of those doors actually wins is dlt's provider ordering, and it is worth knowing before you pick one: a `.dlt/secrets.toml` present in the working directory outranks Google Secret Manager, AWS Secrets Manager, and any provider you register yourself. [Production readiness](production-readiness.md) has the full chain, the Docker and Kubernetes file-secret paths, the Airflow Variables story, and dlt's telemetry setting — the two things worth deciding on purpose before this goes to production.

### State: what survives a fresh runner

**A cron machine keeps its disk; a CI runner starts from nothing every time.** The design answer is the same either way: **everything that must survive lives in the destination, next to the data** — for the dlt-ops tables that requires a [full-tier destination](../concepts/destinations-and-tiers.md); on core tier the ledger, checkpoints, and backfill have nowhere to live and degrade or refuse per the failure-semantics contract.

| State | Lives in | A fresh runner... |
|---|---|---|
| Runs ledger (`_dlt_ops_runs`) | the destination dataset | reads the full history — `status` works from any machine |
| Checkpoints (`_dlt_custom_checkpoints`) | the destination dataset | resumes a failed extract it never saw (shown below) |
| Backfill chunk state (`_dlt_backfills`) | the destination dataset | re-runs the same `--from --to --chunk` as a resume — completed chunks skip |
| dlt's pipeline state, incremental cursors included | the destination dataset (`_dlt_pipeline_state`) plus a local cache | restores the cursor before extracting — nothing re-ingested |
| Pending load packages | the local working directory only | never sees them — the one real loss, below |
| The local working directory (schemas, state cache) | `~/.dlt/pipelines/...`, relocatable via `DLT_DATA_DIR` | rebuilds it on first run |

The claim worth distrusting is the fourth row, so here it is verified: two runs of the repository's `examples/basic_project`, each given its own empty `DLT_DATA_DIR` — two simulated stateless runners sharing nothing but the project checkout and its DuckDB file. The first runner extracts and loads the full window:

```bash
cp -R examples/basic_project /tmp/dlt-demo && cd /tmp/dlt-demo

DLT_DATA_DIR=/tmp/runner-a/.dlt-data dlt-ops pipeline run -s github_events_api -y
```

```text
Pipeline working directory: /tmp/runner-a/.dlt-data/pipelines/github_events_api_pipeline
...
events: 20  | Time: 0.08s | Rate: 259.15/s
actors: 5  | Time: 0.08s | Rate: 65.99/s
...
1 load package(s) were loaded to destination duckdb and into dataset github_events_raw
```

The second runner has an empty working directory — as far as its local disk knows, this pipeline has never run. dlt persists pipeline state to the destination alongside the data (the `_dlt_pipeline_state` table, visible in the first run's load output) and restores it on the first run against an existing dataset — dlt's own behavior, not a dlt-ops layer — so the incremental `events` resource comes back knowing its cursor and extracts nothing new; only `actors` (`write_disposition="replace"`, re-extracted every run by design) moves:

```bash
DLT_DATA_DIR=/tmp/runner-b/.dlt-data dlt-ops pipeline run -s github_events_api -y
```

```text
Pipeline working directory: /tmp/runner-b/.dlt-data/pipelines/github_events_api_pipeline
...
Normalized data for the following tables:
- actors: 5 row(s)
...
1 load package(s) were loaded to destination duckdb and into dataset github_events_raw
```

No duplicate ingestion — the table still holds exactly the first run's 20 rows, and the ledger (read here from runner B, which never wrote the first row) keeps both outcomes:

```bash
python - <<'PY'
import duckdb
con = duckdb.connect("github_events_api_pipeline.duckdb", read_only=True)
print(con.sql("SELECT count(*) AS events_rows FROM github_events_raw.events"))
con.close()
PY
```

```text
┌─────────────┐
│ events_rows │
├─────────────┤
│          20 │
└─────────────┘
```

```bash
DLT_DATA_DIR=/tmp/runner-b/.dlt-data dlt-ops pipeline status
```

```text
Source: github_events_api
  Status     Started              Completed            Records   Trigger    Resource        Run ID
  completed  2026-07-17 10:14:26  2026-07-17 10:14:27  5         cli        -               e35dc79d9632
  completed  2026-07-17 10:14:24  2026-07-17 10:14:25  25        cli        -               881accf42f01
```

[Checkpoints](../concepts/checkpoints.md) pass the same test, because their state is destination-side too. Against a fresh copy of the project, kill the first runner mid-pagination with the example's fault-injection hook, then run a second, equally fresh runner:

```bash
cp -R examples/basic_project /tmp/dlt-demo2 && cd /tmp/dlt-demo2

GITHUB_EVENTS_FAIL_AFTER_PAGE=3 DLT_DATA_DIR=/tmp/runner-c/.dlt-data dlt-ops pipeline run -s github_events_api -y
```

```text
[events] Checkpoint saved: page 2, 6 records, value: 2026-01-01T05:00:00+00:00
RuntimeError: injected API failure after page 3 (GITHUB_EVENTS_FAIL_AFTER_PAGE)
```

```bash
DLT_DATA_DIR=/tmp/runner-d/.dlt-data dlt-ops pipeline run -s github_events_api -y
```

```text
[events] Resuming from checkpoint: 2026-01-01T05:00:00+00:00 (adjusted: 2026-01-01 04:59:59+00:00)
...
events: 15  | Time: 0.06s | Rate: 247.89/s
...
1 load package(s) were loaded to destination duckdb and into dataset github_events_raw
```

Runner D never saw the failed run, and resumes it anyway — the checkpoint semantics, boundary overlap and skipped-span trade included, are on the [checkpoints page](../concepts/checkpoints.md).

What a fresh runner loses is the local pipeline working directory, and the cost is specific: **pending load packages**. A run that fails during normalize or load leaves its fully extracted package on local disk, and dlt retries that package on the next run *from the same machine* ([failure semantics](../concepts/failure-semantics.md)); a fresh runner has no package to retry, so the next run re-extracts from the last destination-committed state instead.

No data is lost and no gate is bypassed — but the extraction is paid twice, which matters exactly where extraction is expensive: rate-limited APIs, paid-per-call sources, multi-hour paginations. That is the checkpoint use case, and it is why checkpoint state deliberately does not live in the working directory.

### A GitHub Actions workflow

**The same command under a CI scheduler, end to end.** GitHub-hosted runners are the fully stateless case — every state property verified above is load-bearing here.

!!! note
    This workflow is a template to adapt — source names, destination extra, secret names, cadence. It is presented as config, not as a pasted run: GitHub Actions is not executed in the environment these docs are verified in, but every `dlt-ops` command in it is, on this page and the [quickstart](../getting-started/quickstart.md).

```yaml
# .github/workflows/ingest.yml
name: scheduled-ingestion

on:
  schedule:
    - cron: "0 5 * * *" # mirror the source's `schedule` tag by hand — nothing reads the TOML here
  workflow_dispatch:

permissions: {}

jobs:
  ingest:
    runs-on: ubuntu-latest
    timeout-minutes: 60
    permissions:
      contents: read
    # One matrix leg per source. `fail-fast: false` keeps one source's failure
    # from cancelling its siblings; generate the list from `discover_sources()`
    # so it never drifts from the project.
    strategy:
      fail-fast: false
      matrix:
        source: [orders, github_events_api]
    # One ingestion at a time PER SOURCE: a delayed run queues behind the
    # previous run of the same source instead of extracting the same window
    # next to it. Never cancel a run mid-load.
    concurrency:
      group: ingest-${{ matrix.source }}
      cancel-in-progress: false
    steps:
      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0
        with:
          persist-credentials: false
      - uses: astral-sh/setup-uv@11f9893b081a58869d3b5fccaea48c9e9e46f990 # v8.3.2
        with:
          enable-cache: true
      - run: uv tool install "dlt-ops[postgres]" # pick the extra your destination needs

      # Credential-free by design: validate never opens the destination.
      # Plain validate is the pass/fail gate. Warnings — orphan config
      # sections, staleness, core-tier notices — are filtered out of this
      # form entirely; add --strict to a separate step to see or enforce them.
      - run: dlt-ops pipeline validate

      - run: dlt-ops pipeline run -s "${{ matrix.source }}" -y
        env:
          DESTINATION__POSTGRES__CREDENTIALS: ${{ secrets.PG_CREDENTIALS }}
          SOURCES__ORDERS__API_SECRET_KEY: ${{ secrets.ORDERS_API_KEY }}

      # Optional: print the ledger tail into the job log — reads the same
      # destination the run just wrote to.
      - run: dlt-ops pipeline status --limit 3
        env:
          DESTINATION__POSTGRES__CREDENTIALS: ${{ secrets.PG_CREDENTIALS }}
```

Reading notes, top to bottom: the `strategy.matrix.source` list is the fan-out — one job per source, and it is exactly the `discover_sources()` enumeration from the top of this page rather than a hand-kept list. The checkout *is* the project (`.dlt/config.toml` at the repo root — pass `--root` on each command if it lives in a subdirectory). `uv tool install` pulls `dlt-ops` from PyPI with the destination extra your config names ([installation](../getting-started/installation.md)) and puts the binary on `PATH`; once you deploy for real, pin `dlt-ops==<version>` — and your dlt version with it, because the [dependency policy](../reference/compatibility.md) sets floors only and leaves the dlt version to you. `validate` runs before credentials exist in the job on purpose: it never needs the destination. The secrets flow GitHub → env → dlt's environment provider, exactly the first door from the credentials section. And GitHub's `schedule` trigger is best-effort — its cron granularity is roughly five minutes at best and queued runs can start 15–20 minutes late under load, so the per-source `concurrency` group turns a late run into queueing instead of overlap.

### A GitLab CI pipeline

**The same command under GitLab's scheduler, fanned out with `parallel:matrix`.** A pipeline schedule supplies the cron; a `rules` gate keeps the ingestion job off ordinary pushes, and `parallel:matrix` runs one job per source — again the `discover_sources()` list, not a hand-kept one:

```yaml
# .gitlab-ci.yml
run-ingestion:
  image: python:3.12
  rules:
    - if: $CI_PIPELINE_SOURCE == "schedule" # only on a scheduled pipeline, never on push
  parallel:
    matrix:
      - SOURCE: [orders, github_events_api]
  script:
    - pip install "dlt-ops[postgres]" # pick the extra your destination needs
    - dlt-ops pipeline validate
    - dlt-ops pipeline run -s "$SOURCE" -y
  variables:
    DESTINATION__POSTGRES__CREDENTIALS: $PG_CREDENTIALS
```

Same shape as the workflow above: a non-zero `dlt-ops` exit fails the job, secrets arrive as masked, protected CI/CD variables mapped straight into dlt's environment provider, and cadence comes from one or more GitLab pipeline schedules (give each a distinct `SCHEDULE_NAME` variable and branch on it in `rules` when different sources need different crons). The `schedule` tag still maps by hand — GitLab reads nothing from your TOML.

## Graduate to an orchestrator

**Graduate to the third rung when the job outgrows "run one command on a timer":** when per-source schedules should come from the `schedule` tags instead of hand-mirrored cron lines, when re-running a window should be a first-class action, and when retries, alerting, and run history belong to a platform your team already operates. Three orchestrators have a guide, and each one's recommended recipe still runs through the same `dlt-ops pipeline run -s <source> -y` under the hood — so the exit-code and state guarantees above carry over unchanged. (Dagster's guide also documents an advanced native-asset path that trades those guarantees away for per-table lineage, and says so up front.)

| Orchestrator | Integration | What the guide covers |
|---|---|---|
| [Airflow](airflow.md) | First-party adapter (`[airflow]` extra) | One DAG per `schedule` tag from a single `build_schedule_dags` call; Variable-backed secrets; manual-trigger window re-runs |
| [Dagster](dagster.md) | Documented recipe, no adapter | One asset per source from `discover_sources()`, scheduled with `AutomationCondition.on_cron`, each asset subprocessing the CLI — plus native `@dlt_assets` for per-table lineage |
| [Prefect](prefect.md) | Documented recipe, no adapter | A thin `@flow` per source deployed on a cron schedule, surfacing the CLI's non-zero exit as a Prefect failure |

[Scheduling and orchestration](../concepts/scheduling-and-orchestration.md) is the conceptual model behind all three: the schedule contract, the orchestrator-neutral core interface, and why an adapter stays thin.

## What this ladder deliberately omits

**No Kubernetes operators, no serverless recipes, no always-on service — omitted on purpose, twice over.** First, scope: `dlt-ops` is a convenience toolchain for moderate-volume scheduled batch ingestion, and a batch CLI that runs for minutes and exits has no need for a resident runtime; high-load pipelines with hard SLAs are better served by purpose-built infrastructure around plain dlt. Second, honesty: every `dlt-ops` command on this page ran before it was written, and container-platform recipes this project cannot run and verify would be decoration. If your platform can invoke a CLI on a schedule — and every platform can — it holds the second rung.

## Where next

- [Scheduling and orchestration](../concepts/scheduling-and-orchestration.md) — the schedule contract every recipe here mirrors, and the core interface the orchestrator guides build on
- [Failure semantics](../concepts/failure-semantics.md) — what your trigger's exit code 1 actually means, layer by layer
- [Runs ledger](../concepts/runs-ledger.md) — the destination-side history `status` reads from any machine

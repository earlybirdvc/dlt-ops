---
description: Schedule a dlt-ops project on Prefect — a thin @flow per source that shells out to dlt-ops pipeline run, deployed on a cron schedule from each source's schedule tag with to_deployment() and serve(), surfacing the CLI's non-zero exit as a Prefect failure.
---

# Schedule a project on Prefect

`dlt-ops` ships no Prefect adapter, and Prefect does not need one: a thin `@flow` shells out to `dlt-ops pipeline run -s <source> -y` and lets Prefect schedule, retry, and observe it. This is the well-worn "one generic flow, one deployment per source" pattern, with the dlt-ops CLI as the unit of work — so the runs ledger, assertions, checkpoints, and backfill all stay in force.

**Prerequisites**

- A dlt-ops project whose sources carry `schedule` tags — this page uses the repository's `examples/basic_project`.
- `dlt-ops` installed and on `PATH` wherever the flow runs — the task shells out to the CLI.
- `pip install prefect` (Prefect 3.x — `.serve()` / `to_deployment()` take a `cron` argument).

!!! note
    Prefect is not installed in the environment these docs are verified in. The Prefect snippets are shown against the current Prefect 3 API and carry no pasted Prefect run; the `dlt-ops pipeline run` command they wrap is executed and its output shown on the [deployment page](deployment.md).

## A thin flow over the CLI

**One generic flow runs one source, and a non-zero CLI exit fails the run.** Keep the orchestration layer thin — the flow does nothing but invoke the command and let its exit code speak:

```python title="prefect_ingestion.py"
# at (or beside) your dlt-ops project root
import subprocess
from pathlib import Path

from prefect import flow, task

PROJECT_ROOT = Path(__file__).resolve().parent


@task
def run_source(source: str) -> None:
    # Non-zero exit -> CalledProcessError -> the task and the flow run fail.
    subprocess.run(
        ["dlt-ops", "--root", str(PROJECT_ROOT), "pipeline", "run", "-s", source, "-y"],
        check=True,
    )


@flow(name="dlt-ops-ingestion")
def ingest(source: str) -> None:
    run_source(source)
```

Because the work is `dlt-ops pipeline run`, every dlt-ops guarantee holds — the runs ledger, pre-load assertions, checkpoint resume, and backfill window injection live in the CLI's runner, not in the flow (secrets reach dlt via env or `secrets.toml`, exactly as on the [deployment page](deployment.md), where the command's exit-code contract and real output also live).

## One scheduled deployment per source

**Fan out over `discover_sources()` and give each source its own cron deployment** — add this to the same `prefect_ingestion.py`, mapping the `schedule` tag to a cron expression you own:

```python title="prefect_ingestion.py"
from prefect import serve

from dlt_ops import discover_sources

# You own the preset -> cron mapping; dlt-ops keeps schedule tags coarse.
TAG_TO_CRON = {
    "@hourly": "0 * * * *",
    "@2hourly": "0 */2 * * *",
    "@daily": "0 5 * * *",
    "@weekly": "0 0 * * 1",
    "@monthly": "0 0 1 * *",
}

if __name__ == "__main__":
    deployments = [
        ingest.to_deployment(
            name=f"ingest-{name}",
            cron=TAG_TO_CRON[info.config.schedule.value],
            parameters={"source": name},
        )
        for name, info in discover_sources(PROJECT_ROOT).items()
        if info.config and info.config.schedule.value != "@manual"
    ]
    serve(*deployments)
```

`discover_sources(PROJECT_ROOT)` returns each source with its `schedule` tag (the enumeration shown on the [deployment page](deployment.md)), so the deployment list tracks the project instead of a hand-kept copy. `serve(*deployments)` runs them all from one process; for containerized or remote infrastructure, swap `.to_deployment(...)` / `serve(...)` for `flow.deploy(work_pool_name=..., cron=...)` against a work pool — the `cron` argument is identical. `@manual` sources get no cron; trigger them on demand.

## Where next

- [Deployment](deployment.md) — where Prefect sits on the ladder, and the cron / GitHub Actions / GitLab CI recipes that share the same command
- [Scheduling and orchestration](../concepts/scheduling-and-orchestration.md) — the schedule contract and the orchestrator-neutral core interface
- [Dagster](dagster.md) — the same CLI-driven pattern, plus native per-table assets

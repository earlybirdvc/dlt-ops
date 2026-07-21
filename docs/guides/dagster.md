---
description: Schedule a dlt-ops project on Dagster — a CLI-driven asset per source from discover_sources() scheduled with AutomationCondition.on_cron that keeps the dlt-ops ledger, assertions, and checkpoints, plus the advanced native @dlt_assets recipe for per-table lineage and the operational trade-off it makes.
---

# Schedule a project on Dagster

`dlt-ops` ships no Dagster adapter, and for the recommended path it does not need one. This guide gives you two recipes: a CLI-driven asset per source that keeps every dlt-ops guarantee — lead with this — and native `@dlt_assets` for per-table lineage when you are willing to trade that operational layer away. Both enumerate your project through the public `discover_sources()` API.

**Prerequisites**

- A dlt-ops project whose sources carry `schedule` tags — this page uses the repository's `examples/basic_project` (one `@hourly` source, one `@daily`).
- `dlt-ops` installed and on `PATH` in the environment Dagster runs in — the recommended recipe shells out to the CLI.
- A current Dagster release with Declarative Automation (`pip install dagster`), plus `pip install dagster-dlt` (never the deprecated `dagster-embedded-elt`) for the advanced native recipe only.

**Choose a recipe**

| Recipe | Unit in Dagster | Keeps dlt-ops ledger, assertions, checkpoints | Per-table lineage | API surface it needs |
|---|---|---|---|---|
| CLI-driven asset (below) | one asset per source | Yes — the run goes through `dlt-ops pipeline run` | No | Public and stable — `discover_sources()` plus the CLI |
| Native `@dlt_assets` (advanced) | one asset per dlt table | No — Dagster owns the run | Yes | Internal, no-stability helpers |

!!! note
    Dagster is not installed in the environment these docs are verified in. The Dagster and `dagster-dlt` snippets are shown against their current APIs and carry no pasted Dagster run; every `dlt-ops` command they wrap was executed and its output pasted.

## Start from discovery

**Both recipes begin by enumerating the project through the public `discover_sources()` API**, which returns each source keyed by name with its `schedule` tag and its live `@dlt.source` callable. Run it from the project root:

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

That mapping is the input to everything below: the source names become asset keys, and the `schedule` tags become cron expressions.

## One asset per source, subprocessing the CLI

**The recommended recipe builds one Dagster asset per source, each subprocessing `dlt-ops pipeline run -s <name> -y`.** Because the run goes through the CLI, it goes through the same dlt-ops runner as everything else — the runs ledger, pre-load assertions, and checkpoint resume all stay in force. A per-source `AutomationCondition.on_cron` gives each asset its own cadence, mapped from the `schedule` tag:

Backfill is the exception, and it is a different command. `pipeline run` takes no window flags; a bounded, chunked, resumable window is `dlt-ops pipeline backfill --from --to --chunk`, so a Dagster partition that must re-ingest a specific window subprocesses that verb instead. See the [backfill guide](backfill.md).

```python title="dagster_defs.py"
# at (or beside) your dlt-ops project root
import subprocess
from pathlib import Path

import dagster as dg

from dlt_ops import discover_sources

PROJECT_ROOT = Path(__file__).resolve().parent

# You own the preset -> cron mapping; dlt-ops keeps schedule tags coarse.
TAG_TO_CRON = {
    "@hourly": "0 * * * *",
    "@2hourly": "0 */2 * * *",
    "@daily": "0 5 * * *",
    "@weekly": "0 0 * * 1",
    "@monthly": "0 0 1 * *",
}


def source_asset(name: str, cron: str) -> dg.AssetsDefinition:
    @dg.asset(
        name=name,
        group_name="dlt_ops",
        automation_condition=dg.AutomationCondition.on_cron(cron),
    )
    def _asset(context: dg.AssetExecutionContext) -> None:
        # Non-zero exit -> CalledProcessError -> failed materialization.
        subprocess.run(
            ["dlt-ops", "--root", str(PROJECT_ROOT), "pipeline", "run", "-s", name, "-y"],
            check=True,
        )

    return _asset


assets = [
    source_asset(name, TAG_TO_CRON[info.config.schedule.value])
    for name, info in discover_sources(PROJECT_ROOT).items()
    if info.config and info.config.schedule.value != "@manual"
]
```

Wire the assets — and the sensor that makes `on_cron` fire — into `Definitions`:

```python title="dagster_defs.py"
defs = dg.Definitions(
    assets=assets,
    sensors=[
        # Declarative Automation only runs assets when this sensor is on. Ship
        # it enabled so `on_cron` works on deploy without a UI toggle.
        dg.AutomationConditionSensorDefinition(
            name="dlt_ops_automation",
            target=dg.AssetSelection.all(),
            default_status=dg.DefaultSensorStatus.RUNNING,
        ),
    ],
)
```

Each asset shells out to the CLI you already run by hand; here is that command against the example source — exit `0`, with every dlt-ops guarantee applied:

```text
Pipeline Configuration
----------------------------------------
  Source: github_events_api
  Function: github_events_api_source
  Resources: all (2 total)
  Destination: duckdb
  Dataset: github_events_raw (from .dlt/config.toml)
  Capabilities: full
...
events: 20  | Time: 0.09s | Rate: 212.93/s
actors: 5  | Time: 0.09s | Rate: 54.15/s
...
1 load package(s) were loaded to destination duckdb and into dataset github_events_raw
```

Two things this recipe gets right by construction:

- **Discovery imports your import-safe source modules once at code-location load — never per sensor tick — and the run itself is subprocessed**, so heavy work stays out of the Dagster daemon and the dlt-ops runner keeps ownership of the pipeline. `@manual` sources are filtered out of the cron list; expose them as un-scheduled assets to materialize on demand.
- **Non-zero exit fails the materialization.** `subprocess.run(..., check=True)` raises `CalledProcessError` on exit `1`, which Dagster records as a failed materialization — the same exit-code contract cron and CI rely on ([deployment](deployment.md)).

## Advanced: native `@dlt_assets` for per-table lineage

**Native `@dlt_assets` turns one source into one Dagster asset per dlt table**, with column-level schema metadata in the asset graph — the reason `dagster-dlt` exists. It is the right choice only in a narrow case, because it makes two trades you must accept up front:

- **It needs internal, no-stability helpers.** Building the matching `dlt.pipeline(...)` needs the effective destination, dataset, and the runtime `<source>_pipeline` name — none of them public: `SourceInfo.config` carries only per-source *overrides* (`None` when the source uses project defaults), and `SourceInfo.pipeline_name` is the pipeline *directory* name, not the runtime pipeline name. The recipe imports `load_project_config`, `resolve_destination`, `resolve_dataset`, and `pipeline_name_for_source`, none of which are in `dlt_ops.__all__` — so they carry no stability promise across minors.
- **Dagster owns the run, so the dlt-ops operational layer is bypassed.** `@dlt_assets` calls `dlt.run` itself; it never goes through `dlt-ops pipeline run`. That means no runs-ledger row, no pre-load `fail`/`warn` assertions, and no checkpoint resume — everything that lives in the dlt-ops runner. You gain per-table lineage and give up the operational guarantees the CLI-driven asset keeps.

**Choose native `@dlt_assets` only when per-table lineage matters more than the dlt-ops operational layer.** The internal helpers do resolve to real values — here against the example source:

```python
from pathlib import Path
from dlt_ops import discover_sources
from dlt_ops.config import load_project_config, resolve_destination, resolve_dataset
from dlt_ops.runs.writer import pipeline_name_for_source

root = Path(".")
project = load_project_config(root)
info = discover_sources(root)["github_events_api"]
print(pipeline_name_for_source(info.name),
      resolve_destination(info.config, project),
      resolve_dataset(info.config, project))
```

```text
github_events_api_pipeline duckdb github_events_raw
```

Feed those three values into `dlt.pipeline(...)`, and the live source from `info.source_fn()` into `@dlt_assets`:

```python title="dagster_native.py"
from pathlib import Path

import dlt
import dagster as dg
from dagster_dlt import DagsterDltResource, dlt_assets

from dlt_ops import discover_sources
# Internal — not in dlt_ops.__all__, no stability promise across minors:
from dlt_ops.config import load_project_config, resolve_destination, resolve_dataset
from dlt_ops.runs.writer import pipeline_name_for_source

PROJECT_ROOT = Path(__file__).resolve().parent
project = load_project_config(PROJECT_ROOT)
info = discover_sources(PROJECT_ROOT)["github_events_api"]


@dlt_assets(
    dlt_source=info.source_fn(),  # the live @dlt.source — this half is public
    dlt_pipeline=dlt.pipeline(
        pipeline_name=pipeline_name_for_source(info.name),      # github_events_api_pipeline
        destination=resolve_destination(info.config, project),  # duckdb
        dataset_name=resolve_dataset(info.config, project),     # github_events_raw
    ),
    name=info.name,
    group_name="dlt_ops",
)
def github_events_api_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


defs = dg.Definitions(
    assets=[github_events_api_assets],
    resources={"dlt": DagsterDltResource()},
)
```

Schedule these exactly like the CLI-driven assets — attach an `AutomationCondition.on_cron` through a `DagsterDltTranslator`, or add the same automation-condition sensor to `Definitions`.

## Troubleshooting: assets never materialize on schedule

**If assets with `on_cron` never run, the automation-condition sensor is off.** Declarative Automation is evaluated by a sensor, not by the scheduler directly: Dagster auto-creates a `default_automation_condition_sensor` for any code location that has automation conditions, but it starts **stopped** and must be toggled on under the Automation tab. The `Definitions` above ship an `AutomationConditionSensorDefinition` with `default_status=RUNNING` so this is handled at deploy time; if you rely on the default sensor instead, enable it in the UI.

## Where next

- [Deployment](deployment.md) — where Dagster sits on the ladder, and the cron / GitHub Actions / GitLab CI recipes that share the same command
- [Scheduling and orchestration](../concepts/scheduling-and-orchestration.md) — the schedule contract and the orchestrator-neutral core interface
- [API reference](../reference/api.md) — `discover_sources()` and the `SourceInfo` fields these recipes read

"""Generic DAG factory: one DAG per ``Schedule`` group, Phase-1 discovery only at parse time.

Parse-time contract: :func:`build_schedule_dags` calls only Phase-1 discovery
(pure AST) — no project source module is imported while the Airflow scheduler
parses DAG files. Phase-2 introspection, secret setup and the actual run all
happen inside the task through ``dlt_ops.orchestration.run_source``,
which delegates to the shared runner (``run_pipeline(bounds=...)``).

Adapter-internal contracts:

- One DAG per schedule group, id ``{dag_prefix}_{schedule-minus-@}``
  (``dlt_daily``, ``dlt_2hourly``, ...). ``@manual`` sources (and sources
  without valid config) build a trigger-only DAG (``schedule=None``).
- Task ids: ``{source}.{source}_{resource}`` where the ``PipelineTasksGroup``
  id is the source's config section (the same value the manual-trigger
  ``source`` key selects), with one task per Phase-1 static resource. A source
  whose resources only materialize dynamically (Phase 1 sees none) gets a
  single whole-source task ``{source}.{source}``.
- Manual-trigger conf JSON::

      {
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-02-01T00:00:00Z",
        "source": "my_api",
        "resources": ["events", "users"]
      }

  ``source``/``resources`` filter via ``AirflowSkipException`` on
  per-resource tasks (a whole-source task passes the selection to the
  runner's resource filter instead); ``resources`` applies only when
  ``source`` is given. ``start_date``/``end_date`` override the run window.
- Run window: the resolved window — conf overrides over Airflow's
  ``data_interval_start``/``data_interval_end``; manual-schedule DAGs carry
  no native window — feeds ``run_pipeline(bounds=...)``. No per-source flags
  and no Airflow-context mutation: the runner's injected
  ``TimeIntervalContext`` applies the bounds to every incremental resource.

``project_root`` should live inside the Airflow dags folder: dlt's
``PipelineTasksGroup`` points dlt's own config/secrets resolution at the dags
folder, so keeping the project's ``.dlt/`` there gives one consistent config
tree at parse and run time. Per-environment deployments point each
environment at its own project root (see the package docstring).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import pendulum

from dlt_ops.airflow import _INSTALL_HINT

try:
    from airflow import DAG
    from airflow.exceptions import AirflowSkipException
    from airflow.operators.python import PythonOperator, get_current_context
except ModuleNotFoundError as exc:
    raise ImportError(_INSTALL_HINT) from exc

from dlt.helpers.airflow_helper import PipelineTasksGroup

from dlt_ops import orchestration
from dlt_ops.airflow.tasks import cleanup_old_dlt_files
from dlt_ops.discovery.models import Schedule
from dlt_ops.runs.writer import TriggerSource

__all__ = ["SCHEDULE_CRON_MAP", "build_schedule_dags", "schedule_to_airflow"]

logger = logging.getLogger(__name__)

# Cron materialization of Schedule values is orchestrator policy, fixed by
# design (opinionated, no config knob):
# - "@2hourly" is not an Airflow preset; it materializes as `0 */2 * * *`
#   (every even hour at :00).
# - "@weekly" pins Monday 00:00 UTC: a scheduler fires when the interval
#   closes, and the Sunday-00:00 preset would leave the week's own Sunday
#   uncaptured — Monday closes the ISO week. The Monday pin is intentional,
#   not a bug.
SCHEDULE_CRON_MAP: dict[str, str] = {
    "@2hourly": "0 */2 * * *",
    "@weekly": "0 0 * * 1",
}


def schedule_to_airflow(schedule: Schedule) -> str | None:
    """Airflow ``schedule`` value for a Schedule: cron overrides first, presets pass through."""
    if schedule is Schedule.MANUAL:
        return None
    return SCHEDULE_CRON_MAP.get(schedule.value, schedule.value)


def _execute_unit(
    project_root: str,
    source_name: str,
    resource: str | None,
    known_sources: tuple[str, ...],
    has_native_window: bool,
) -> None:
    """Task body for one (source, resource) unit: decide, resolve window, run.

    All decisions are core (`orchestration`); this function only maps them
    onto Airflow mechanics — ``AirflowSkipException`` for a negative
    filtering decision, ``data_interval_start/end`` as the native window.
    """
    context = get_current_context()
    dag_run = context.get("dag_run")
    conf: dict[str, Any] = dict(dag_run.conf) if dag_run is not None and dag_run.conf else {}
    if conf:
        logger.info(f"Manual-trigger conf: {conf}")

    decision = orchestration.filtering_decision(
        conf, source_name=source_name, resource=resource, known_sources=known_sources
    )
    if not decision.run:
        raise AirflowSkipException(decision.reason)

    native = None
    if has_native_window:
        native = (context["data_interval_start"], context["data_interval_end"])
    window = orchestration.resolve_window(conf, native=native)

    if resource is not None:
        resources: tuple[str, ...] | None = (resource,)
    elif conf.get("source") == source_name and conf.get("resources"):
        # Whole-source task: resource narrowing happens in the runner, not
        # via task skips (there is no per-resource task to skip).
        resources = tuple(conf["resources"])
    else:
        resources = None

    orchestration.run_source(
        source_name,
        project_root=Path(project_root),
        resources=resources,
        window=window,
        trigger_source=TriggerSource.AIRFLOW,
    )


def build_schedule_dags(
    project_root: Path | str,
    *,
    dag_prefix: str = "dlt",
    start_date: datetime | None = None,
    catchup: bool = False,
    cleanup_data_dir: str | Path | None = None,
    cleanup_after_days: int = 3,
    dag_kwargs: Mapping[str, Any] | None = None,
) -> dict[str, DAG]:
    """Build one DAG per discovered schedule group (Phase-1 AST scan only).

    Args:
        project_root: dlt-ops project root (``.dlt/config.toml`` +
            pipeline directories), normally inside the Airflow dags folder.
        dag_prefix: dag-id prefix; ids are ``{dag_prefix}_{schedule-minus-@}``.
        start_date: DAG start date; defaults to 2024-01-01 UTC.
        catchup: Airflow catchup flag (off by default — missed windows are
            re-runnable through the manual-trigger conf).
        cleanup_data_dir: when set, append a ``cleanup_old_dlt_files`` task
            (scratch-dir hygiene) for this directory to every built DAG.
        cleanup_after_days: age threshold for the cleanup task.
        dag_kwargs: extra keyword arguments applied to every ``DAG(...)``
            (tags, default_args, ...).

    Returns:
        ``dag_id -> DAG``. Register them in the DAG file's module globals::

            globals().update(build_schedule_dags(Path(__file__).parent))
    """
    root = Path(project_root)
    groups = orchestration.scheduled_sources(root)
    resolved_start = start_date or pendulum.datetime(2024, 1, 1, tz="UTC")
    extra_dag_kwargs = dict(dag_kwargs) if dag_kwargs else {}

    dags: dict[str, DAG] = {}
    for schedule in sorted(groups, key=lambda s: s.value):
        sources = sorted(groups[schedule], key=lambda s: s.name)
        known = tuple(source.name for source in sources)
        dag_id = f"{dag_prefix}_{schedule.value.removeprefix('@')}"
        with DAG(
            dag_id=dag_id,
            schedule=schedule_to_airflow(schedule),
            start_date=resolved_start,
            catchup=catchup,
            **extra_dag_kwargs,
        ) as dag:
            task_groups = []
            for source in sources:
                with PipelineTasksGroup(pipeline_name=source.name) as task_group:
                    for resource in source.resources or (None,):
                        task_id = source.name if resource is None else f"{source.name}_{resource}"
                        PythonOperator(
                            task_id=task_id,
                            python_callable=_execute_unit,
                            op_kwargs={
                                "project_root": str(root),
                                "source_name": source.name,
                                "resource": resource,
                                "known_sources": known,
                                "has_native_window": schedule is not Schedule.MANUAL,
                            },
                        )
                task_groups.append(task_group)
            if cleanup_data_dir is not None:
                cleanup = cleanup_old_dlt_files(data_dir=str(cleanup_data_dir), days=cleanup_after_days)
                for task_group in task_groups:
                    task_group >> cleanup
        dags[dag_id] = dag
    return dags

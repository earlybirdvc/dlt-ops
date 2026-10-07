"""Keep dlt's own config resolution on the dlt-ops project root under Airflow.

This module must not import Airflow: the package exports it eagerly so a
bare install can load it.
"""

from __future__ import annotations

import os
from pathlib import Path

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext
from dlt.common.known_env import DLT_PROJECT_DIR

__all__ = ["pin_dlt_project_dir"]


def pin_dlt_project_dir(project_root: Path | str) -> None:
    """Point dlt's ``.dlt/`` lookup at ``project_root`` and rebuild its providers.

    dlt's ``PipelineTasksGroup`` assigns ``DLT_PROJECT_DIR`` to the Airflow
    dags folder while constructing itself, which makes dlt read
    ``config.toml`` and ``secrets.toml`` from the dags folder instead of the
    project. Call this after constructing the task group and before anything
    resolves dlt config.

    Setting the variable is not enough on its own: dlt builds its config
    providers once and each one caches the absolute file paths it was built
    with, so an already-loaded provider keeps reading the dags folder.

    ``DLT_DATA_DIR`` and ``DLT_LOCAL_DIR`` are left exactly as the task-group
    constructor set them, because ``PipelineTasksGroup.add_run`` requires a
    pipeline's working directory to sit inside ``DLT_DATA_DIR``.

    Args:
        project_root: dlt-ops project root — the directory holding ``.dlt/``.
    """
    os.environ[DLT_PROJECT_DIR] = str(project_root)

    container = Container()
    if PluggableRunContext in container:
        container[PluggableRunContext].reload_providers()

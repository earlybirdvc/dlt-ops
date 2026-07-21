"""Operational helper tasks for the Airflow adapter."""

from __future__ import annotations

import datetime as dt
import logging
import shutil
import tempfile
from pathlib import Path

from dlt_ops.airflow import _INSTALL_HINT

try:
    from airflow.decorators import task
    from airflow.utils.trigger_rule import TriggerRule
except ModuleNotFoundError as exc:
    raise ImportError(_INSTALL_HINT) from exc

logger = logging.getLogger(__name__)

__all__ = ["cleanup_old_dlt_files"]


@task(trigger_rule=TriggerRule.NONE_FAILED)
def cleanup_old_dlt_files(data_dir: str | None = None, days: int = 3, enabled: bool = True) -> None:
    """Delete dlt scratch files and directories older than ``days`` under ``data_dir``.

    Generic scratch-dir hygiene: dlt's ``PipelineTasksGroup`` provisions a
    fresh ``dlt_*`` working directory on the worker per parse; stale ones
    accumulate and bloat worker storage. This task removes ``dlt_*`` entries
    older than the threshold.

    Args:
        data_dir: Scratch directory to sweep. Defaults to the system temp
            directory (where ``PipelineTasksGroup`` puts its data folders by
            default); pass the ``local_data_folder`` you configured if you
            moved it.
        days: Age threshold in days; entries modified more recently survive.
        enabled: Explicit on/off switch so deployments can wire their own
            environment policy (e.g. sweep only in production) without any
            environment variable convention in this package.
    """
    if not enabled:
        logger.info("Cleanup disabled; skipping")
        return

    if data_dir is None:
        data_dir = tempfile.gettempdir()

    data_path = Path(data_dir)
    if not data_path.exists():
        raise ValueError(f"data_dir does not exist: {data_dir}")

    cutoff_time = dt.datetime.now(dt.UTC) - dt.timedelta(days=days)

    logger.info(f"Starting cleanup of dlt files in {data_dir}")
    logger.info(f"Deleting files older than {cutoff_time.isoformat()} ({days} days)")

    deleted_files = 0
    deleted_dirs = 0

    for path in data_path.rglob("dlt_*"):
        # Handle race condition: parent directory may have been deleted in a previous iteration
        try:
            path_mtime = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.UTC)
        except FileNotFoundError:
            continue

        if path_mtime < cutoff_time:
            if path.is_dir():
                logger.info(f"Deleting directory {path} (modified: {path_mtime.isoformat()})")
                shutil.rmtree(path, ignore_errors=True)
                deleted_dirs += 1
            elif path.is_file():
                logger.info(f"Deleting file {path} (modified: {path_mtime.isoformat()})")
                path.unlink(missing_ok=True)
                deleted_files += 1
            else:
                logger.info(f"Skipping {path} (modified: {path_mtime.isoformat()})")
        else:
            logger.info(f"Skipping {path} (modified: {path_mtime.isoformat()})")

    logger.info(f"Cleanup complete: Deleted {deleted_files} files and {deleted_dirs} directories")

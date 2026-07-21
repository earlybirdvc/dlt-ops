"""Utility functions for checkpoint management."""

import logging
from typing import Any

import dlt

from dlt_ops.checkpoints.manager import _CHECKPOINT_COLUMNS, DEFAULT_CHECKPOINT_TABLE
from dlt_ops.destinations import adapter_for_pipeline, open_client


def _resolve_pipeline(pipeline: Any, pipeline_name: str | None, caller: str) -> tuple[Any, str]:
    """Attach by name or accept a live pipeline; returns (pipeline, pipeline_name)."""
    if pipeline is None:
        if pipeline_name is None:
            raise ValueError("pipeline_name is required when pipeline not provided")
        if not pipeline_name.strip():
            raise ValueError("pipeline_name cannot be empty")

        logging.info(f"[{caller}] Attaching to pipeline: {pipeline_name}")
        pipeline = dlt.attach(pipeline_name=pipeline_name)
    elif pipeline_name is None:
        pipeline_name = pipeline.pipeline_name
    return pipeline, pipeline_name


def _is_missing_table_error(error: Exception) -> bool:
    message = str(error).lower()
    return "not found" in message or "does not exist" in message


# `status` values written by CheckpointManager: rows start 'active' and a
# successful run flips them to 'completed'. Only 'active' rows are resume state.
_COMPLETED = "status = 'completed'"
_ACTIVE = "status = 'active'"


def _scope(pipeline_name: str, resource_name: str | None) -> tuple[list[str], list[str]]:
    """WHERE fragments + bound params selecting one pipeline, optionally one resource."""
    conditions = ["pipeline_name = ?"]
    params = [pipeline_name]
    if resource_name:
        conditions.append("resource_name = ?")
        params.append(resource_name)
    return conditions, params


def cleanup_checkpoints(
    pipeline_name: str | None = None,
    resource_name: str | None = None,
    checkpoint_table: str = DEFAULT_CHECKPOINT_TABLE,
    pipeline=None,
    *,
    include_active: bool = False,
):
    """Delete completed checkpoint rows for a pipeline or resource.

    Retention housekeeping by default: only rows a successful run already
    marked `completed` are deleted. `active` rows are live resume state — the
    row a crashed or still-running extract resumes from — so deleting one
    restarts that resource at its window start and silently re-extracts
    everything the previous run already loaded. Active rows found in scope are
    kept and reported at WARNING.

    Pass `include_active=True` for the destructive form: every checkpoint row
    in scope regardless of status. That is the surgical escape hatch —
    abandoning a poisoned resume point, or clearing state for a pipeline
    dropped outside `dlt-ops` — and the next run of an affected resource
    restarts its window from the beginning.

    Args:
        pipeline_name: Pipeline name to clean (required if pipeline not provided)
        resource_name: Specific resource to clean (None = all resources in pipeline)
        checkpoint_table: Name of checkpoint table (default: _dlt_custom_checkpoints)
        pipeline: Optional pipeline object (avoids dlt.attach() call)
        include_active: Also delete `active` rows, destroying resume state
            (default: False)

    Examples:
        # Prune completed checkpoints for a pipeline
        cleanup_checkpoints(pipeline_name="my_pipeline")

        # Prune completed checkpoints for one resource
        cleanup_checkpoints(pipeline_name="my_pipeline", resource_name="companies_bulk")

        # Wipe every row, resume state included
        cleanup_checkpoints(pipeline_name="my_pipeline", include_active=True)
    """
    pipeline, pipeline_name = _resolve_pipeline(pipeline, pipeline_name, "cleanup_checkpoints")

    adapter = adapter_for_pipeline(pipeline)
    table_ref = adapter.render_table_ref(pipeline.dataset_name, checkpoint_table)
    logging.info(f"[cleanup_checkpoints] Attached to dataset: {pipeline.dataset_name}")

    conditions, params = _scope(pipeline_name, resource_name)
    delete_conditions = conditions if include_active else [*conditions, _COMPLETED]
    delete_sql = f"DELETE FROM {table_ref} WHERE {' AND '.join(delete_conditions)}"
    kept_sql = f"SELECT COUNT(*) FROM {table_ref} WHERE {' AND '.join([*conditions, _ACTIVE])}"

    scope = f"pipeline='{pipeline_name}'" + (f", resource='{resource_name}'" if resource_name else " (all resources)")

    try:
        with open_client(pipeline) as client:
            adapter.execute_sql(client, delete_sql, *params)
            # Counted after the delete, which never touches active rows: this
            # is what survived, not what was at risk.
            kept = 0 if include_active else adapter.execute_query(client, kept_sql, *params).fetchone()[0]
    except Exception as e:
        # Table might not exist, which is fine
        if _is_missing_table_error(e):
            logging.info("[cleanup_checkpoints] Checkpoint table does not exist, nothing to clean")
            return
        logging.error(f"[cleanup_checkpoints] Failed to delete checkpoints: {e}")
        raise

    deleted = "all checkpoints" if include_active else "completed checkpoints"
    logging.info(f"[cleanup_checkpoints] Deleted {deleted} for {scope}")
    if kept:
        logging.warning(
            f"[cleanup_checkpoints] Kept {kept} active checkpoint row(s) for {scope}: they are live resume state. "
            f"Pass include_active=True to delete them too — affected resources then restart from their window start."
        )


def list_checkpoints(
    pipeline_name: str | None = None,
    checkpoint_table: str = DEFAULT_CHECKPOINT_TABLE,
    pipeline=None,
) -> list[dict[str, Any]]:
    """List all checkpoints for debugging.

    Args:
        pipeline_name: Pipeline name to filter (required if pipeline not provided)
        checkpoint_table: Name of checkpoint table (default: _dlt_custom_checkpoints)
        pipeline: Optional pipeline object (avoids dlt.attach() call)

    Returns:
        List of checkpoint records as dicts keyed by column name, newest first
    """
    pipeline, pipeline_name = _resolve_pipeline(pipeline, pipeline_name, "list_checkpoints")

    adapter = adapter_for_pipeline(pipeline)
    table_ref = adapter.render_table_ref(pipeline.dataset_name, checkpoint_table)
    logging.info(f"[list_checkpoints] Attached to dataset: {pipeline.dataset_name}")

    query = f"SELECT {', '.join(_CHECKPOINT_COLUMNS)} FROM {table_ref} WHERE pipeline_name = ? ORDER BY created_at DESC"

    try:
        with open_client(pipeline) as client:
            cursor = adapter.execute_query(client, query, pipeline_name)
        return [dict(zip(_CHECKPOINT_COLUMNS, row, strict=True)) for row in cursor.fetchall()]
    except Exception as e:
        if _is_missing_table_error(e):
            logging.info("[list_checkpoints] Checkpoint table does not exist")
            return []
        logging.error(f"[list_checkpoints] Failed to query checkpoints: {e}")
        raise

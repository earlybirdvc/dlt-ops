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


def cleanup_checkpoints(
    pipeline_name: str | None = None,
    resource_name: str | None = None,
    checkpoint_table: str = DEFAULT_CHECKPOINT_TABLE,
    pipeline=None,
):
    """Clean up checkpoints for a pipeline or resource.

    Use this when you run `dlt pipeline drop` or similar cleanup commands
    to remove associated checkpoint data.

    Args:
        pipeline_name: Pipeline name to clean (required if pipeline not provided)
        resource_name: Specific resource to clean (None = all resources in pipeline)
        checkpoint_table: Name of checkpoint table (default: _dlt_custom_checkpoints)
        pipeline: Optional pipeline object (avoids dlt.attach() call)

    Examples:
        # Clean all checkpoints for current pipeline
        cleanup_checkpoints(pipeline_name="my_pipeline")

        # Clean checkpoints for specific resource
        cleanup_checkpoints(pipeline_name="my_pipeline", resource_name="companies_bulk")

        # Pass pipeline directly (e.g., in tests)
        cleanup_checkpoints(pipeline=my_pipeline)
    """
    pipeline, pipeline_name = _resolve_pipeline(pipeline, pipeline_name, "cleanup_checkpoints")

    adapter = adapter_for_pipeline(pipeline)
    table_ref = adapter.render_table_ref(pipeline.dataset_name, checkpoint_table)
    logging.info(f"[cleanup_checkpoints] Attached to dataset: {pipeline.dataset_name}")

    conditions = ["pipeline_name = ?"]
    params: list[str] = [pipeline_name]
    if resource_name:
        conditions.append("resource_name = ?")
        params.append(resource_name)

    delete_sql = f"DELETE FROM {table_ref} WHERE {' AND '.join(conditions)}"

    try:
        with open_client(pipeline) as client:
            adapter.execute_sql(client, delete_sql, *params)
        logging.info(
            f"[cleanup_checkpoints] Deleted checkpoints for "
            f"pipeline='{pipeline_name}'" + (f", resource='{resource_name}'" if resource_name else " (all resources)")
        )
    except Exception as e:
        # Table might not exist, which is fine
        if _is_missing_table_error(e):
            logging.info("[cleanup_checkpoints] Checkpoint table does not exist, nothing to clean")
        else:
            logging.error(f"[cleanup_checkpoints] Failed to delete checkpoints: {e}")
            raise


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

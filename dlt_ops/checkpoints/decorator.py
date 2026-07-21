"""Decorator for transparent checkpoint management in dlt resources."""

import datetime as dt
import hashlib
import inspect
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any

import dlt
from dlt.common.pendulum import pendulum
from dlt.extract import Incremental
from dlt.sources import DltResource

from dlt_ops.checkpoints.manager import DEFAULT_CHECKPOINT_TABLE, CheckpointManager


def with_checkpoints(
    cursor_field: str,
    frequency: int = 10,
    offset_seconds: int = 1,
    checkpoint_table: str = DEFAULT_CHECKPOINT_TABLE,
    cleanup_days: int = 7,
    value_parser: Callable[[str], Any] | None = None,
) -> Callable:
    """Decorator to add transparent checkpointing to dlt resources.

    Saves progress to destination during pagination and resumes from last
    checkpoint on failure. Works with any incremental cursor field.

    **Concurrent Run Isolation**: Automatically isolates checkpoints for different
    intervals (e.g., hourly vs backfill) using the incremental's initial_value.
    Runs with different initial_value get separate checkpoint namespaces.
    Scheduler-agnostic (works locally and in Airflow).

    **Decorator order**: ``@with_checkpoints`` must sit UNDER ``@dlt.resource``
    (closest to the generator function). Applied on top of ``@dlt.resource`` it
    would replace the ``DltResource`` with a plain generator function, silently
    dropping the resource's name, write disposition, and hints — that order
    raises ``TypeError`` at decoration time.

    Args:
        cursor_field: Name of incremental field (e.g., "last_updated_utc")
        frequency: Checkpoint every N pages (default: 10)
        offset_seconds: Safety overlap on resume in seconds (default: 1)
        checkpoint_table: Name of checkpoint table (default: _dlt_custom_checkpoints)
        cleanup_days: Days to keep completed checkpoints (default: 7)
        value_parser: Optional function to parse checkpoint string back to original type.
                     If not provided, uses default parser for datetime/timestamp fields.

    Raises:
        ValueError: If frequency <= 0 or cleanup_days < 0
        TypeError: If applied on top of ``@dlt.resource`` instead of under it

    Usage:
        @dlt.resource(...)
        @with_checkpoints(cursor_field="last_updated_utc")
        def my_resource(last_updated_utc=dlt.sources.incremental(...)):
            for page in paginate(...):
                yield page

    Example with custom parser:
        @dlt.resource(...)
        @with_checkpoints(
            cursor_field="cursor_id",
            value_parser=lambda s: int(s)
        )
        def my_resource(cursor_id=dlt.sources.incremental(...)):
            for page in paginate(...):
                yield page
    """
    # Validate inputs
    if frequency <= 0:
        raise ValueError(f"frequency must be positive, got {frequency}")
    if cleanup_days < 0:
        raise ValueError(f"cleanup_days must be non-negative, got {cleanup_days}")

    def decorator(resource_func: Callable) -> Callable:
        if isinstance(resource_func, DltResource):
            raise TypeError(
                "@with_checkpoints must be applied under @dlt.resource, not on top of it: "
                "decorate the plain generator function and let @dlt.resource wrap the result. "
                "Applied on top, it replaces the DltResource with a plain generator function, "
                "dropping the resource's name, write disposition, and hints."
            )

        @wraps(resource_func)
        def wrapper(*args, **kwargs):
            # Get pipeline context
            try:
                pipeline = dlt.current.pipeline()
            except Exception as e:
                logging.error(f"Failed to get dlt pipeline context: {e}")
                raise RuntimeError(
                    "Cannot use checkpoints outside dlt pipeline context. "
                    "Ensure decorator is used within a dlt.pipeline().run() call."
                ) from e

            # Find incremental argument by inspecting function signature (before CheckpointManager)
            incremental_arg: Incremental | None = None
            sig = inspect.signature(resource_func)

            # Try kwargs first
            if cursor_field in kwargs:
                incremental_arg = kwargs[cursor_field]
            else:
                # Find in args by position using signature inspection
                param_names = list(sig.parameters.keys())
                if cursor_field in param_names:
                    param_index = param_names.index(cursor_field)
                    if param_index < len(args):
                        incremental_arg = args[param_index]

            # Fallback to parameter default if not found in args/kwargs
            if incremental_arg is None and cursor_field in sig.parameters:
                param_default = sig.parameters[cursor_field].default
                if param_default is not inspect.Parameter.empty:
                    # Verify it's an incremental-like object
                    if hasattr(param_default, "initial_value") or isinstance(param_default, Incremental):
                        incremental_arg = param_default

            # Derive run_id from incremental initial_value for concurrent run isolation
            # This isolates checkpoints for different intervals (e.g., hourly vs backfill)
            run_id = None
            if incremental_arg:
                # Try initial_value first
                isolation_val = None
                if hasattr(incremental_arg, "initial_value") and incremental_arg.initial_value is not None:
                    isolation_val = incremental_arg.initial_value
                # Fallback to start_value if initial_value not set
                elif hasattr(incremental_arg, "start_value") and incremental_arg.start_value is not None:
                    isolation_val = incremental_arg.start_value

                if isolation_val is not None:
                    # Hash value to create compact run_id
                    isolation_str = _serialize_checkpoint_value(isolation_val)
                    run_id = hashlib.sha256(isolation_str.encode()).hexdigest()[:16]
                    logging.info(f"[{resource_func.__name__}] Run isolation: value={isolation_str}, run_id={run_id}")
                else:
                    # No initial_value or start_value - use default run_id
                    run_id = "default"
                    logging.warning(f"[{resource_func.__name__}] No initial_value/start_value, using run_id='default'")

            with CheckpointManager(
                pipeline_name=pipeline.pipeline_name,
                resource_name=resource_func.__name__,
                frequency=frequency,
                checkpoint_table=checkpoint_table,
                cleanup_days=cleanup_days,
                run_id=run_id,
            ) as checkpoint_mgr:
                # Get actual cursor path from incremental arg (field name in data)
                data_cursor_path = cursor_field
                if incremental_arg and hasattr(incremental_arg, "cursor_path"):
                    data_cursor_path = incremental_arg.cursor_path

                # Override incremental start_value if checkpoint exists
                if incremental_arg and hasattr(incremental_arg, "start_value"):
                    last_checkpoint = checkpoint_mgr.get_last_checkpoint()

                    if last_checkpoint:
                        # Parse checkpoint value back to appropriate type
                        effective_start = _parse_checkpoint_value(last_checkpoint, value_parser)

                        # Apply offset (subtract offset_seconds for safety overlap)
                        if isinstance(effective_start, dt.datetime | pendulum.DateTime):
                            effective_start = effective_start - dt.timedelta(seconds=offset_seconds)

                        # Inject checkpoint by modifying incremental object
                        # Note: This directly modifies the internal state
                        incremental_arg.start_value = effective_start

                        logging.info(
                            f"[{resource_func.__name__}] Resuming from checkpoint: "
                            f"{last_checkpoint} (adjusted: {effective_start})"
                        )

                # Run original resource with checkpoint tracking
                for page in resource_func(*args, **kwargs):
                    # Extract max cursor value from page using actual data field name
                    cursor_value = _extract_cursor_value(page, data_cursor_path)

                    if cursor_value is not None:
                        # Convert to string for storage
                        checkpoint_str = _serialize_checkpoint_value(cursor_value)
                        checkpoint_mgr.save_checkpoint(checkpoint_str, page)

                    yield page

        return wrapper

    return decorator


def _parse_checkpoint_value(checkpoint_str: str, custom_parser: Callable[[str], Any] | None) -> Any:
    """Parse checkpoint string back to original type.

    Args:
        checkpoint_str: String representation of checkpoint
        custom_parser: Optional custom parsing function

    Returns:
        Parsed checkpoint value
    """
    if custom_parser:
        return custom_parser(checkpoint_str)

    # Default: try to parse as datetime/timestamp
    try:
        return pendulum.parse(checkpoint_str)
    except (ValueError, TypeError, AttributeError):
        # If parsing fails, return as string
        return checkpoint_str


def _serialize_checkpoint_value(value: Any) -> str:
    """Convert checkpoint value to string for storage.

    Args:
        value: Checkpoint value (datetime, int, str, etc.)

    Returns:
        String representation
    """
    if isinstance(value, dt.datetime | pendulum.DateTime):
        iso_str: str = value.isoformat()
        return iso_str
    # Explicit type cast for the type checker
    result: str = str(value)
    return result


def _extract_cursor_value(page: Any, cursor_field: str) -> Any | None:
    """Extract max cursor value from a page of data.

    Handles various page formats:
    - List of dicts: [{"cursor_field": value, ...}, ...]
    - List of objects with attributes
    - Single dict
    - Single object

    Args:
        page: Page data
        cursor_field: Name of cursor field

    Returns:
        Maximum cursor value or None
    """
    if not page:
        return None

    # Handle list of items
    if isinstance(page, list):
        cursor_values = []
        for item in page:
            value = _get_field_value(item, cursor_field)
            if value is not None:
                cursor_values.append(value)

        if cursor_values:
            try:
                return max(cursor_values)
            except TypeError as e:
                logging.warning(
                    f"Cannot compare cursor values of type {type(cursor_values[0]).__name__}. "
                    f"Using last value instead. Error: {e}"
                )
                return cursor_values[-1]

    # Handle single item
    else:
        return _get_field_value(page, cursor_field)

    return None


def _get_field_value(item: Any, field_name: str) -> Any | None:
    """Get field value from dict or object.

    Args:
        item: Dict or object
        field_name: Field name

    Returns:
        Field value or None
    """
    # Try dict access
    if isinstance(item, dict):
        return item.get(field_name)

    # Try attribute access
    if hasattr(item, field_name):
        return getattr(item, field_name)

    return None

"""CheckpointManager for mid-run checkpoint persistence."""

import logging
from typing import Any

import dlt

from dlt_ops.destinations import DestinationAdapter, adapter_for_pipeline, open_client

DEFAULT_CHECKPOINT_TABLE = "_dlt_custom_checkpoints"
"""Default checkpoint table name — the single copy other modules import."""

_CHECKPOINT_COLUMNS = (
    "pipeline_name",
    "resource_name",
    "run_id",
    "checkpoint_value",
    "page_number",
    "records_processed",
    "status",
    "created_at",
    "updated_at",
)
"""Checkpoint table columns in DDL order; list_checkpoints keys rows by these."""


def checkpoint_table_ddl(adapter: DestinationAdapter, dataset: str, table: str) -> str:
    """One canonical (DuckDB-dialect) checkpoint-table DDL for every destination.

    No PARTITION BY / CLUSTER BY: checkpoint volume is tiny (one row per N
    pages x resources), a full scan is fine, and those clauses don't transpile.
    Timestamp defaults use the adapter's canonical-dialect fragment because
    sqlglot transpiles syntax, not every function idiom.
    """
    now = adapter.timestamp_now_sql
    return (
        f"CREATE TABLE IF NOT EXISTS {adapter.render_table_ref(dataset, table)} ("
        "pipeline_name VARCHAR NOT NULL, "
        "resource_name VARCHAR NOT NULL, "
        "run_id VARCHAR, "
        "checkpoint_value VARCHAR NOT NULL, "
        "page_number BIGINT, "
        "records_processed BIGINT, "
        "status VARCHAR DEFAULT 'active', "
        f"created_at TIMESTAMPTZ DEFAULT {now}, "
        f"updated_at TIMESTAMPTZ DEFAULT {now})"
    )


class CheckpointManager:
    """Context manager for transparent mid-run checkpointing to destination DB.

    Tracks progress during pagination and persists checkpoints to a separate
    table. Enables resume from last checkpoint on failure.

    **Concurrent Run Isolation**: Use run_id to isolate checkpoints for different
    intervals. Derived from incremental's initial_value by decorator.

    Args:
        pipeline_name: Name of the dlt pipeline
        resource_name: Name of the resource being checkpointed
        frequency: Checkpoint every N pages (default: 10)
        checkpoint_table: Name of checkpoint table (default: _dlt_custom_checkpoints)
        cleanup_days: Days to keep completed checkpoints (default: 7)
        run_id: Optional run identifier for concurrent run isolation (default: None)

    Example:
        with CheckpointManager("my_pipeline", "my_resource", run_id="abc123") as mgr:
            for page in paginate():
                mgr.save_checkpoint(str(cursor_value), page)
                yield page
    """

    def __init__(
        self,
        pipeline_name: str,
        resource_name: str,
        frequency: int = 10,
        checkpoint_table: str = DEFAULT_CHECKPOINT_TABLE,
        cleanup_days: int = 7,
        run_id: str | None = None,
    ):
        # Validate inputs
        if not pipeline_name or not pipeline_name.strip():
            raise ValueError("pipeline_name cannot be empty")
        if not resource_name or not resource_name.strip():
            raise ValueError("resource_name cannot be empty")
        if frequency <= 0:
            raise ValueError(f"frequency must be positive, got {frequency}")
        if cleanup_days < 0:
            raise ValueError(f"cleanup_days must be non-negative, got {cleanup_days}")

        self.pipeline_name = pipeline_name
        self.resource_name = resource_name
        self.frequency = frequency
        self.checkpoint_table = checkpoint_table
        self.cleanup_days = cleanup_days
        self.run_id = run_id  # None = backwards compatible (no isolation)

        self.page_count = 0
        self.records_count = 0
        self.last_checkpoint: str | None = None
        self._pipeline: Any = None
        self._adapter: DestinationAdapter | None = None
        self._table_ref: str | None = None

    def _boundary(self) -> tuple[DestinationAdapter, str]:
        """Adapter + validated table ref, available once __enter__ has run."""
        if self._adapter is None or self._table_ref is None:
            raise RuntimeError("CheckpointManager must be entered before running SQL")
        return self._adapter, self._table_ref

    def _run_id_condition(self) -> tuple[str, tuple[str, ...]]:
        """WHERE fragment isolating this run; NULL-run rows stay isolated too."""
        if self.run_id:
            return "run_id = ?", (self.run_id,)
        return "run_id IS NULL", ()

    def __enter__(self):
        """Resolve pipeline context + destination adapter, load last checkpoint."""
        try:
            self._pipeline = dlt.current.pipeline()
            dataset: str = self._pipeline.dataset_name
        except Exception as e:
            logging.error(f"Failed to get dlt pipeline context: {e}")
            raise RuntimeError("Cannot initialize CheckpointManager outside dlt pipeline context") from e

        # Typed errors propagate as-is: UnregisteredDestinationError when the
        # destination runs in core mode (checkpoints are adapter-gated;
        # preflight refuses statically-detectable usage before extract — this
        # is the backstop for dynamic application), ValueError for names
        # outside the destination's identifier grammar.
        adapter = adapter_for_pipeline(self._pipeline)
        self._adapter = adapter
        self._table_ref = adapter.render_table_ref(dataset, self.checkpoint_table)

        # Ensure checkpoint table exists
        try:
            self._ensure_checkpoint_table(dataset)
        except Exception as e:
            logging.error(f"Failed to create checkpoint table: {e}")
            raise RuntimeError(f"Failed to initialize checkpoint table: {e}") from e

        # Load latest checkpoint
        try:
            self.last_checkpoint = self._load_latest_checkpoint()
        except Exception as e:
            logging.warning(f"Failed to load checkpoint, starting fresh: {e}")
            self.last_checkpoint = None

        if self.last_checkpoint:
            logging.info(f"[{self.resource_name}] Resuming from checkpoint: {self.last_checkpoint}")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Mark completed and cleanup old checkpoints."""
        if exc_type is None:
            # Only mark completed and cleanup if we actually created checkpoints
            if self.page_count > 0:
                try:
                    self._mark_completed()
                except Exception as e:
                    logging.error(f"Failed to mark checkpoints as completed: {e}")

                try:
                    self._cleanup_old()
                except Exception as e:
                    logging.warning(f"Failed to cleanup old checkpoints: {e}")

        return False

    def should_checkpoint(self) -> bool:
        """Check if current page should trigger checkpoint."""
        return self.page_count > 0 and self.page_count % self.frequency == 0

    def save_checkpoint(self, checkpoint_value: str, page_data: Any) -> None:
        """Save checkpoint to the destination.

        Args:
            checkpoint_value: String representation of checkpoint (e.g., timestamp, cursor)
            page_data: Page data to count records
        """
        self.page_count += 1

        # Count records (check list first to avoid strings matching __len__)
        if isinstance(page_data, list):
            self.records_count += len(page_data)
        elif hasattr(page_data, "__len__") and not isinstance(page_data, str):
            self.records_count += len(page_data)
        else:
            self.records_count += 1

        if self.should_checkpoint():
            try:
                self._write_checkpoint(checkpoint_value)
                logging.info(
                    f"[{self.resource_name}] Checkpoint saved: "
                    f"page {self.page_count}, {self.records_count} records, "
                    f"value: {checkpoint_value}"
                )
            except Exception as e:
                logging.error(f"Failed to save checkpoint: {e}. Continuing without checkpoint.")
                # Don't raise - checkpoint save failure shouldn't stop pipeline

    def get_last_checkpoint(self) -> str | None:
        """Get the last saved checkpoint value."""
        return self.last_checkpoint

    def _ensure_checkpoint_table(self, dataset: str):
        """Create checkpoint table (and, where supported, its schema) if not exists."""
        adapter, _ = self._boundary()
        ddl = checkpoint_table_ddl(adapter, dataset, self.checkpoint_table)
        with open_client(self._pipeline) as client:
            adapter.ensure_schema(client, dataset)
            adapter.execute_sql(client, ddl)

    def _load_latest_checkpoint(self) -> str | None:
        """Load most recent active checkpoint."""
        adapter, table_ref = self._boundary()
        run_id_condition, run_id_params = self._run_id_condition()
        query = (
            f"SELECT checkpoint_value FROM {table_ref} "
            f"WHERE pipeline_name = ? AND resource_name = ? AND status = 'active' AND {run_id_condition} "
            "ORDER BY created_at DESC LIMIT 1"
        )

        with open_client(self._pipeline) as client:
            cursor = adapter.execute_query(client, query, self.pipeline_name, self.resource_name, *run_id_params)
        row = cursor.fetchone()
        return str(row[0]) if row is not None else None

    def _write_checkpoint(self, checkpoint_value: str):
        """Insert new checkpoint record."""
        adapter, table_ref = self._boundary()
        insert_sql = (
            f"INSERT INTO {table_ref} "
            "(pipeline_name, resource_name, run_id, checkpoint_value, page_number, records_processed, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'active')"
        )

        with open_client(self._pipeline) as client:
            adapter.execute_sql(
                client,
                insert_sql,
                self.pipeline_name,
                self.resource_name,
                self.run_id,
                checkpoint_value,
                self.page_count,
                self.records_count,
            )

    def _mark_completed(self):
        """Mark all active checkpoints as completed."""
        adapter, table_ref = self._boundary()
        run_id_condition, run_id_params = self._run_id_condition()
        update_sql = (
            f"UPDATE {table_ref} SET status = 'completed', updated_at = {adapter.timestamp_now_sql} "
            f"WHERE pipeline_name = ? AND resource_name = ? AND status = 'active' AND {run_id_condition}"
        )

        with open_client(self._pipeline) as client:
            adapter.execute_sql(client, update_sql, self.pipeline_name, self.resource_name, *run_id_params)

        logging.info(f"[{self.resource_name}] Checkpoints marked as completed")

    def _cleanup_old(self):
        """Delete completed checkpoints older than N days."""
        adapter, table_ref = self._boundary()
        delete_sql = (
            f"DELETE FROM {table_ref} WHERE status = 'completed' "
            f"AND created_at < {adapter.timestamp_sub_days_sql(self.cleanup_days)}"
        )

        with open_client(self._pipeline) as client:
            adapter.execute_sql(client, delete_sql)

        logging.info(f"[{self.resource_name}] Cleaned up checkpoints older than {self.cleanup_days} days")

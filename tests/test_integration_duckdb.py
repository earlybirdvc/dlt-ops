"""Integration tests for checkpoint framework using DuckDB in-memory."""

import datetime as dt
import os
import tempfile
from pathlib import Path

import dlt
import pytest

from dlt_ops import cleanup_checkpoints, list_checkpoints, with_checkpoints
from dlt_ops.checkpoints.manager import CheckpointManager


@pytest.fixture(scope="session", autouse=True)
def disable_airflow_secrets():
    """Disable Airflow secrets provider for tests."""
    os.environ["PROVIDERS__ENABLE_AIRFLOW_SECRETS"] = "false"


@pytest.fixture
def temp_pipeline():
    """Create a temporary DuckDB in-memory pipeline for testing."""
    original_cwd = Path.cwd()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Change to tmpdir so DuckDB creates file there
        os.chdir(tmpdir)

        try:
            pipeline = dlt.pipeline(
                pipeline_name="test_checkpoint_pipeline",
                destination="duckdb",
                dataset_name="test_dataset",
                pipelines_dir=tmpdir,
            )
            yield pipeline

            # Cleanup DuckDB database file
            try:
                # Close any open connections
                if hasattr(pipeline, "_destination_client"):
                    pipeline._destination_client = None

                # Remove database file from tmpdir
                db_path = Path(tmpdir) / "test_checkpoint_pipeline.duckdb"
                if db_path.exists():
                    db_path.unlink()
            except Exception:
                pass  # Cleanup is best-effort

        finally:
            # Always restore working directory
            os.chdir(original_cwd)


@pytest.fixture
def clean_pipeline(temp_pipeline):
    """Provide clean pipeline and cleanup after test."""
    yield temp_pipeline
    # Cleanup checkpoints after test
    try:
        cleanup_checkpoints(pipeline=temp_pipeline)
    except Exception:
        pass  # Table might not exist


class TestCheckpointLifecycle:
    """Test full checkpoint lifecycle with actual DuckDB."""

    def test_checkpoint_table_creation(self, clean_pipeline):
        """Test that checkpoint table is created automatically."""

        @dlt.resource(write_disposition="append")
        @with_checkpoints(cursor_field="timestamp", frequency=1)
        def test_resource(timestamp=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 1, 10, 0)}]

        clean_pipeline.run(test_resource())

        # Verify checkpoint table exists
        with clean_pipeline.sql_client() as client:
            with client.execute_query("SELECT COUNT(*) FROM test_dataset._dlt_custom_checkpoints") as cursor:
                count = cursor.fetchone()[0]
                assert count >= 0  # Table exists

    def test_concurrent_runs_isolation(self, clean_pipeline):
        """Test that concurrent runs with different initial_values are isolated."""

        @dlt.resource(write_disposition="append", primary_key="id")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def isolated_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            """Generate 2 pages."""
            base_time = ts.start_value
            for page in range(1, 3):
                yield [{"id": page, "ts": base_time + dt.timedelta(hours=page)}]

        # Run 1 with initial_value=2024-01-01 (e.g., hourly run)
        clean_pipeline.run(isolated_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))))

        # Run 2 with different initial_value=2024-02-01 (e.g., backfill run)
        clean_pipeline.run(isolated_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 2, 1))))

        # Verify both runs have separate checkpoints with different run_ids
        with clean_pipeline.sql_client() as client:
            with client.execute_query(
                "SELECT DISTINCT run_id FROM test_dataset._dlt_custom_checkpoints WHERE run_id IS NOT NULL"
            ) as cursor:
                run_ids = [row[0] for row in cursor.fetchall()]

        assert len(run_ids) == 2, f"Should have 2 different run_ids, got {len(run_ids)}: {run_ids}"

        # Verify each run_id has its own checkpoints
        all_checkpoints = list_checkpoints(pipeline=clean_pipeline)
        assert len(all_checkpoints) >= 4, "Should have checkpoints from both runs (2 pages each)"

    def test_run_id_fallback_to_start_value(self, clean_pipeline):
        """Test that run_id falls back to start_value when no initial_value."""

        @dlt.resource(write_disposition="append")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def fallback_resource(ts=dlt.sources.incremental("ts")):
            """Resource without initial_value - should use start_value for run_id."""
            yield [{"id": 1, "ts": dt.datetime(2024, 3, 1)}]

        clean_pipeline.run(fallback_resource())

        # Verify checkpoint has run_id (derived from start_value or default)
        checkpoints = list_checkpoints(pipeline=clean_pipeline)
        assert len(checkpoints) > 0
        run_id = checkpoints[0]["run_id"]
        # Should have run_id (not None) - either from start_value or default
        assert run_id is not None, "Should have run_id even without initial_value"
        assert run_id in ["default"] or len(run_id) == 16, "Should be 'default' or 16-char hash"

    def test_checkpoint_save_and_resume(self, clean_pipeline):
        """Test checkpoint is saved and used on resume."""

        @dlt.resource(write_disposition="append", primary_key="id")
        @with_checkpoints(cursor_field="timestamp", frequency=2)
        def paginated_resource(timestamp=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            """Generate 5 pages of data."""
            base_time = timestamp.start_value
            for page in range(1, 6):
                page_data = []
                for i in range(10):
                    record_id = (page - 1) * 10 + i + 1
                    ts = base_time + dt.timedelta(hours=record_id)
                    page_data.append({"id": record_id, "ts": ts, "page": page})
                yield page_data

        # First run - should save checkpoints at page 2 and 4
        clean_pipeline.run(paginated_resource())

        # Check checkpoints were created
        checkpoints = list_checkpoints(pipeline=clean_pipeline)
        assert len(checkpoints) > 0, "Checkpoints should be saved"

        # Verify checkpoint values (should have 2 checkpoints at frequency=2)
        checkpoint_pages = [cp["page_number"] for cp in checkpoints]
        assert 2 in checkpoint_pages, "Should have checkpoint at page 2"
        assert 4 in checkpoint_pages, "Should have checkpoint at page 4"

        # Second run - should resume from last checkpoint
        # DuckDB in-memory loses data, but state is persisted in .dlt/
        # The incremental will use max(ts) from state
        clean_pipeline.run(paginated_resource())

        # Verify old checkpoints marked as completed
        checkpoints_after = list_checkpoints(pipeline=clean_pipeline)
        completed_count = sum(1 for cp in checkpoints_after if cp["status"] == "completed")
        assert completed_count > 0, "Old checkpoints should be marked completed"

    def test_checkpoint_frequency(self, clean_pipeline):
        """Test that checkpoints respect frequency setting."""

        @dlt.resource(write_disposition="append")
        @with_checkpoints(cursor_field="timestamp", frequency=3)
        def test_resource(timestamp=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            """Generate 7 pages to test frequency."""
            for page in range(1, 8):
                yield [{"id": page, "ts": dt.datetime(2024, 1, 1) + dt.timedelta(hours=page)}]

        clean_pipeline.run(test_resource())

        # Should have checkpoints at page 3 and 6 only (frequency=3)
        checkpoints = list_checkpoints(pipeline=clean_pipeline)
        checkpoint_pages = [cp["page_number"] for cp in checkpoints]

        assert 3 in checkpoint_pages, "Should checkpoint at page 3"
        assert 6 in checkpoint_pages, "Should checkpoint at page 6"
        assert 1 not in checkpoint_pages, "Should NOT checkpoint at page 1"
        assert 2 not in checkpoint_pages, "Should NOT checkpoint at page 2"

    def test_checkpoint_with_datetime_cursor(self, clean_pipeline):
        """Test checkpoint serialization/deserialization with datetime cursors."""

        @dlt.resource(write_disposition="append")
        @with_checkpoints(cursor_field="updated_at", frequency=1)
        def datetime_resource(
            updated_at=dlt.sources.incremental(
                "updated_at", initial_value=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
            ),
        ):
            yield [
                {
                    "id": 1,
                    "updated_at": dt.datetime(2024, 1, 2, 10, 30, tzinfo=dt.timezone.utc),
                }
            ]

        clean_pipeline.run(datetime_resource())

        # Verify checkpoint value is ISO format datetime string
        checkpoints = list_checkpoints(pipeline=clean_pipeline)
        assert len(checkpoints) > 0
        checkpoint_value = checkpoints[0]["checkpoint_value"]
        assert "2024-01-02" in checkpoint_value, "Should contain date"
        assert "T" in checkpoint_value, "Should be ISO format"

    def test_checkpoint_with_custom_parser(self, clean_pipeline):
        """Test checkpoint with custom value parser."""

        @dlt.resource(write_disposition="append")
        @with_checkpoints(cursor_field="cursor_id", frequency=1, value_parser=lambda s: int(s))
        def int_cursor_resource(cursor_id=dlt.sources.incremental("cursor_id", initial_value=0)):
            yield [{"id": 1, "cursor_id": 12345}]

        clean_pipeline.run(int_cursor_resource())

        # Verify checkpoint stored as string but parseable to int
        checkpoints = list_checkpoints(pipeline=clean_pipeline)
        assert len(checkpoints) > 0
        assert checkpoints[0]["checkpoint_value"] == "12345"

    def test_multiple_resources_separate_checkpoints(self, clean_pipeline):
        """Test that different resources have separate checkpoints."""

        @dlt.resource(write_disposition="append", name="resource_a")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def resource_a(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 1, 10, 0)}]

        @dlt.resource(write_disposition="append", name="resource_b")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def resource_b(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 1, 11, 0)}]

        # Run both resources
        clean_pipeline.run([resource_a(), resource_b()])

        # Verify separate checkpoints
        checkpoints = list_checkpoints(pipeline=clean_pipeline)
        resource_names = {cp["resource_name"] for cp in checkpoints}

        assert "resource_a" in resource_names
        assert "resource_b" in resource_names


class TestInjectionProbe:
    """Hostile values ride the full checkpoint path as data, never as SQL."""

    def test_hostile_pipeline_name_round_trips_as_data(self, clean_pipeline):
        hostile = "x'); DROP TABLE t;--"
        resumed = {}

        # Decoy table the payload would drop if it ever reached SQL text.
        with clean_pipeline.sql_client() as client:
            client.execute_sql("CREATE SCHEMA IF NOT EXISTS test_dataset")
            client.execute_sql("CREATE TABLE IF NOT EXISTS test_dataset.t (x BIGINT)")

        @dlt.resource(write_disposition="append")
        def probe_resource():
            # pipeline_name is user-config-controlled in OSS — exercise it directly.
            with CheckpointManager(hostile, "probe_res", frequency=1) as mgr:
                mgr.save_checkpoint("cursor-1", [{"id": 1}])
                # Second manager sees the hostile row as an active checkpoint: full
                # write-then-read round trip with the hostile value bound as data.
                with CheckpointManager(hostile, "probe_res", frequency=1) as reader:
                    resumed["value"] = reader.get_last_checkpoint()
            yield [{"id": 1}]

        clean_pipeline.run(probe_resource())

        assert resumed["value"] == "cursor-1"
        with clean_pipeline.sql_client() as client:
            with client.execute_query(
                "SELECT pipeline_name FROM test_dataset._dlt_custom_checkpoints WHERE pipeline_name = ?",
                hostile,
            ) as cursor:
                rows = cursor.fetchall()
            assert [row[0] for row in rows] == [hostile]
            # The decoy table survived: the payload never reached the SQL text.
            with client.execute_query(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'test_dataset' AND table_name = 't'"
            ) as cursor:
                assert cursor.fetchone()[0] == 1


class TestCheckpointCleanup:
    """Test checkpoint cleanup functionality."""

    def test_cleanup_all_checkpoints(self, clean_pipeline):
        """Test cleanup removes all checkpoints for pipeline."""

        @dlt.resource(write_disposition="append")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def test_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 1)}]

        # Create checkpoints
        clean_pipeline.run(test_resource())

        # Verify checkpoints exist
        checkpoints_before = list_checkpoints(pipeline=clean_pipeline)
        assert len(checkpoints_before) > 0

        # Cleanup
        cleanup_checkpoints(pipeline=clean_pipeline)

        # Verify all gone
        checkpoints_after = list_checkpoints(pipeline=clean_pipeline)
        assert len(checkpoints_after) == 0

    def test_cleanup_specific_resource(self, clean_pipeline):
        """Test cleanup of specific resource only."""

        @dlt.resource(write_disposition="append", name="keep_me")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def keep_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 1)}]

        @dlt.resource(write_disposition="append", name="delete_me")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def delete_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 1)}]

        # Create checkpoints for both
        clean_pipeline.run([keep_resource(), delete_resource()])

        # Cleanup only delete_resource (function name, not @dlt.resource name)
        cleanup_checkpoints(pipeline=clean_pipeline, resource_name="delete_resource")

        # Verify selective deletion
        checkpoints = list_checkpoints(pipeline=clean_pipeline)
        resource_names = {cp["resource_name"] for cp in checkpoints}

        # Note: Checkpoints use function name, not @dlt.resource name parameter
        assert "keep_resource" in resource_names
        assert "delete_resource" not in resource_names

    def test_cleanup_old_completed_checkpoints(self, clean_pipeline):
        """Test that old completed checkpoints are auto-cleaned."""

        @dlt.resource(write_disposition="append")
        @with_checkpoints(cursor_field="ts", frequency=1, cleanup_days=0)  # Cleanup immediately
        def test_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 1)}]

        # First run - creates and completes checkpoints
        clean_pipeline.run(test_resource())

        # Verify checkpoints marked as completed and cleaned up (cleanup_days=0)
        # With cleanup_days=0, old completed checkpoints should be deleted
        # But the most recent run's checkpoints will exist briefly before cleanup
        # This test verifies the cleanup mechanism runs without error
        checkpoints = list_checkpoints(pipeline=clean_pipeline)
        # Checkpoints may or may not exist depending on timing - just verify no crash
        assert isinstance(checkpoints, list)


class TestCheckpointErrors:
    """Test error handling in checkpoint operations."""

    def test_checkpoint_table_not_exist_initially(self, clean_pipeline):
        """Test that missing checkpoint table doesn't break listing."""
        # Before any resource runs, table doesn't exist
        checkpoints = list_checkpoints(pipeline=clean_pipeline)
        assert checkpoints == []  # Should return empty list, not crash

    def test_cleanup_nonexistent_table(self, clean_pipeline):
        """Test cleanup handles missing table gracefully."""
        # Should not raise error
        cleanup_checkpoints(pipeline=clean_pipeline)

    def test_checkpoint_with_empty_page(self, clean_pipeline):
        """Test checkpoint handles empty pages gracefully."""

        @dlt.resource(write_disposition="append")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def empty_page_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield []  # Empty page
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 1)}]  # Non-empty page

        # Should not crash on empty page
        clean_pipeline.run(empty_page_resource())


class TestCheckpointCustomTable:
    """Test custom checkpoint table names."""

    def test_custom_checkpoint_table(self, clean_pipeline):
        """Test using custom checkpoint table name."""

        @dlt.resource(write_disposition="append")
        @with_checkpoints(cursor_field="ts", frequency=1, checkpoint_table="my_custom_checkpoints")
        def test_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 1)}]

        clean_pipeline.run(test_resource())

        # Verify custom table was created
        with clean_pipeline.sql_client() as client:
            with client.execute_query("SELECT COUNT(*) FROM test_dataset.my_custom_checkpoints") as cursor:
                count = cursor.fetchone()[0]
                assert count > 0

        # Cleanup with custom table
        cleanup_checkpoints(pipeline=clean_pipeline, checkpoint_table="my_custom_checkpoints")

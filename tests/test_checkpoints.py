"""Tests for checkpoint management framework."""

import datetime as dt
import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import dlt
import pytest
from dlt.common.pendulum import pendulum

from dlt_ops import cleanup_checkpoints, list_checkpoints, with_checkpoints
from dlt_ops.checkpoints import DEFAULT_CHECKPOINT_TABLE, decorator
from dlt_ops.checkpoints.manager import CheckpointManager, CheckpointStateError, checkpoint_table_ddl
from dlt_ops.destinations import UnregisteredDestinationError
from dlt_ops.destinations.bigquery import BigQueryAdapter
from dlt_ops.destinations.duckdb import DuckDBAdapter

CKPT_PIPELINE = "ckpt_pipeline"
CKPT_DATASET = "analytics"


@pytest.fixture
def duckdb_pipeline(tmp_path, monkeypatch):
    """A real DuckDB-backed pipeline installed as the current dlt pipeline.

    CheckpointManager resolves adapter, dataset and client from
    ``dlt.current.pipeline()``, so this runs the checkpoint path end-to-end
    against a real destination file instead of a mock.
    """
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
    monkeypatch.chdir(tmp_path)
    pipeline = dlt.pipeline(
        pipeline_name=CKPT_PIPELINE,
        destination="duckdb",
        dataset_name=CKPT_DATASET,
        pipelines_dir=str(tmp_path / "pipelines"),
    )
    monkeypatch.setattr(dlt.current, "pipeline", lambda: pipeline)
    return pipeline


def _fail_sql_matching(monkeypatch, method: str, fragment: str) -> None:
    """Make the adapter call `method` raise for canonical SQL containing `fragment`.

    Simulates one statement hitting a transient destination failure (outage,
    rate limit) while the rest of the checkpoint path stays real.
    """
    real = getattr(DuckDBAdapter, method)

    def patched(self: Any, client: Any, canonical_sql: str, *params: Any) -> Any:
        if fragment in canonical_sql:
            raise RuntimeError("destination unavailable")
        return real(self, client, canonical_sql, *params)

    monkeypatch.setattr(DuckDBAdapter, method, patched)


class TestValueSerialization:
    """Test checkpoint value serialization/deserialization."""

    def test_serialize_datetime(self):
        """Test datetime serialization."""
        dt_value = dt.datetime(2025, 11, 7, 10, 30, 0)
        result = decorator._serialize_checkpoint_value(dt_value)
        assert isinstance(result, str)
        assert "2025-11-07" in result

    def test_serialize_pendulum(self):
        """Test pendulum datetime serialization."""
        pdt_value = pendulum.parse("2025-11-07 10:30:00")
        result = decorator._serialize_checkpoint_value(pdt_value)
        assert isinstance(result, str)
        assert "2025-11-07" in result

    def test_serialize_string(self):
        """Test string serialization."""
        result = decorator._serialize_checkpoint_value("cursor_123")
        assert result == "cursor_123"

    def test_serialize_int(self):
        """Test int serialization."""
        result = decorator._serialize_checkpoint_value(12345)
        assert result == "12345"

    def test_parse_datetime_default(self):
        """Test datetime parsing with default parser."""
        checkpoint_str = "2025-11-07T10:30:00"
        result = decorator._parse_checkpoint_value(checkpoint_str, None)
        assert isinstance(result, pendulum.DateTime)
        assert result.year == 2025

    def test_parse_with_custom_parser(self):
        """Test parsing with custom parser."""
        result = decorator._parse_checkpoint_value("12345", lambda s: int(s))
        assert result == 12345

    def test_parse_fallback_to_string(self):
        """Test parsing fallback for non-parseable strings."""
        result = decorator._parse_checkpoint_value("invalid_datetime", None)
        assert result == "invalid_datetime"


class TestCursorExtraction:
    """Test cursor value extraction from different page formats."""

    def test_extract_from_list_of_dicts(self):
        """Test extraction from list of dicts."""
        page = [
            {"id": 1, "timestamp": "2025-11-07 10:00:00"},
            {"id": 2, "timestamp": "2025-11-07 11:00:00"},
            {"id": 3, "timestamp": "2025-11-07 09:00:00"},
        ]
        result = decorator._extract_cursor_value(page, "timestamp")
        assert result == "2025-11-07 11:00:00"

    def test_extract_from_list_of_objects(self):
        """Test extraction from list of objects with attributes."""

        class Item:
            def __init__(self, timestamp):
                self.timestamp = timestamp

        page = [
            Item("2025-11-07 10:00:00"),
            Item("2025-11-07 11:00:00"),
            Item("2025-11-07 09:00:00"),
        ]
        result = decorator._extract_cursor_value(page, "timestamp")
        assert result == "2025-11-07 11:00:00"

    def test_extract_from_single_dict(self):
        """Test extraction from single dict."""
        page = {"id": 1, "timestamp": "2025-11-07 10:00:00"}
        result = decorator._extract_cursor_value(page, "timestamp")
        assert result == "2025-11-07 10:00:00"

    def test_extract_from_empty_page(self):
        """Test extraction from empty page."""
        assert decorator._extract_cursor_value([], "timestamp") is None
        assert decorator._extract_cursor_value(None, "timestamp") is None

    def test_extract_missing_field(self):
        """Test extraction when field doesn't exist."""
        page = [{"id": 1}, {"id": 2}]
        result = decorator._extract_cursor_value(page, "timestamp")
        assert result is None

    def test_get_field_from_dict(self):
        """Test field access from dict."""
        item = {"id": 1, "name": "test"}
        assert decorator._get_field_value(item, "name") == "test"
        assert decorator._get_field_value(item, "missing") is None

    def test_get_field_from_object(self):
        """Test field access from object."""

        class Item:
            name = "test"

        item = Item()
        assert decorator._get_field_value(item, "name") == "test"
        assert decorator._get_field_value(item, "missing") is None

    def test_extract_handles_non_comparable_values(self):
        """Test extraction handles non-comparable cursor values gracefully."""
        # Mixed types that can't be compared
        page = [
            {"id": 1, "cursor": {"nested": "value1"}},
            {"id": 2, "cursor": {"nested": "value2"}},
        ]
        # Should return last value instead of crashing
        result = decorator._extract_cursor_value(page, "cursor")
        assert result == {"nested": "value2"}


class TestCheckpointManager:
    """Test CheckpointManager functionality."""

    def test_init_validates_inputs(self):
        """Test that init validates inputs."""
        # Empty pipeline name
        with pytest.raises(ValueError, match="pipeline_name cannot be empty"):
            CheckpointManager("", "resource")

        # Empty resource name
        with pytest.raises(ValueError, match="resource_name cannot be empty"):
            CheckpointManager("pipeline", "")

        # Negative frequency
        with pytest.raises(ValueError, match="frequency must be positive"):
            CheckpointManager("pipeline", "resource", frequency=0)

        # Negative cleanup days
        with pytest.raises(ValueError, match="cleanup_days must be non-negative"):
            CheckpointManager("pipeline", "resource", cleanup_days=-1)

    def test_should_checkpoint_frequency(self):
        """Test checkpoint frequency logic."""
        mgr = CheckpointManager("test_pipeline", "test_resource", frequency=10)

        # First 9 pages shouldn't checkpoint
        for i in range(1, 10):
            mgr.page_count = i
            assert mgr.should_checkpoint() is False

        # 10th page should checkpoint
        mgr.page_count = 10
        assert mgr.should_checkpoint() is True

        # 20th page should checkpoint
        mgr.page_count = 20
        assert mgr.should_checkpoint() is True

        # 21st shouldn't
        mgr.page_count = 21
        assert mgr.should_checkpoint() is False

    def test_save_checkpoint_counts_records(self):
        """Test that save_checkpoint correctly counts records."""
        mgr = CheckpointManager("test_pipeline", "test_resource", frequency=1)
        mgr._write_checkpoint = MagicMock()  # Mock the write method

        # Test with list
        page_data = [{"id": 1}, {"id": 2}, {"id": 3}]
        mgr.save_checkpoint("checkpoint_1", page_data)
        assert mgr.page_count == 1
        assert mgr.records_count == 3

        # Test with another list
        page_data = [{"id": 4}, {"id": 5}]
        mgr.save_checkpoint("checkpoint_2", page_data)
        assert mgr.page_count == 2
        assert mgr.records_count == 5

    def test_get_last_checkpoint(self):
        """Test getting last checkpoint."""
        mgr = CheckpointManager("test_pipeline", "test_resource")
        mgr.last_checkpoint = "2025-11-07T10:00:00"

        assert mgr.get_last_checkpoint() == "2025-11-07T10:00:00"


class TestDecoratorIntegration:
    """Test decorator integration (requires actual dlt setup)."""

    def test_decorator_validates_inputs(self):
        """Test that decorator validates inputs."""
        # Negative frequency
        with pytest.raises(ValueError, match="frequency must be positive"):

            @with_checkpoints(cursor_field="timestamp", frequency=0)
            def test_resource():
                yield []

        # Negative cleanup days
        with pytest.raises(ValueError, match="cleanup_days must be non-negative"):

            @with_checkpoints(cursor_field="timestamp", cleanup_days=-1)
            def test_resource():
                yield []

    def test_decorator_wraps_function(self):
        """Test that decorator properly wraps the function."""

        @with_checkpoints(cursor_field="timestamp")
        def test_resource():
            yield [{"id": 1, "timestamp": "2025-11-07T10:00:00"}]

        # Check that function name is preserved
        assert test_resource.__name__ == "test_resource"


class TestDecoratorOrder:
    """@with_checkpoints must sit UNDER @dlt.resource (probed both ways)."""

    def test_under_dlt_resource_preserves_resource_semantics(self):
        @dlt.resource(write_disposition="replace", name="custom_rows")
        @with_checkpoints(cursor_field="ts")
        def my_rows(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 2)}]

        assert isinstance(my_rows, dlt.sources.DltResource)
        assert my_rows.name == "custom_rows"
        assert my_rows.write_disposition == "replace"

    def test_on_top_of_dlt_resource_raises(self):
        """On top it would swap the DltResource for a bare generator function."""
        with pytest.raises(TypeError, match="under @dlt.resource"):

            @with_checkpoints(cursor_field="ts")
            @dlt.resource(write_disposition="replace", name="custom_rows")
            def my_rows(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
                yield [{"id": 1, "ts": dt.datetime(2024, 1, 2)}]


class TestAdapterResolution:
    """CheckpointManager resolves the destination adapter from the live pipeline."""

    @staticmethod
    def _stub_pipeline(monkeypatch, destination_type: str):
        stub = SimpleNamespace(
            pipeline_name="stub_pipeline",
            dataset_name="stub_dataset",
            destination=SimpleNamespace(destination_type=destination_type),
        )
        monkeypatch.setattr(dlt.current, "pipeline", lambda: stub)
        return stub

    def test_unregistered_destination_raises_typed_error(self, monkeypatch):
        self._stub_pipeline(monkeypatch, "dlt.destinations.motherduck")
        with pytest.raises(UnregisteredDestinationError, match="'motherduck'.*core mode"):
            CheckpointManager("stub_pipeline", "resource").__enter__()

    def test_invalid_checkpoint_table_name_rejected(self, monkeypatch):
        """Custom table names go through the adapter's identifier grammar."""
        self._stub_pipeline(monkeypatch, "dlt.destinations.duckdb")
        with pytest.raises(ValueError, match="identifier"):
            CheckpointManager("stub_pipeline", "resource", checkpoint_table="bad name; DROP").__enter__()


class TestCheckpointDDL:
    """One canonical DDL drives every adapter."""

    @pytest.mark.parametrize("adapter", [DuckDBAdapter(), BigQueryAdapter()], ids=["duckdb", "bigquery"])
    def test_no_partition_or_cluster_clauses(self, adapter):
        ddl = checkpoint_table_ddl(adapter, "ds", "cp")
        assert "PARTITION" not in ddl.upper()
        assert "CLUSTER" not in ddl.upper()

    def test_same_canonical_shape_for_both_adapters(self):
        duckdb_adapter, bigquery_adapter = DuckDBAdapter(), BigQueryAdapter()
        normalized = {
            checkpoint_table_ddl(adapter, "ds", "cp").replace(adapter.timestamp_now_sql, "<now>")
            for adapter in (duckdb_adapter, bigquery_adapter)
        }
        assert len(normalized) == 1

    def test_bigquery_transpile_snapshot(self):
        """The canonical DDL must keep rendering valid GoogleSQL."""
        calls = []
        client = SimpleNamespace(execute_sql=lambda sql, *args: calls.append((sql, args)))
        adapter = BigQueryAdapter()
        adapter.execute_sql(client, checkpoint_table_ddl(adapter, "ds", "cp"))
        assert calls == [
            (
                "CREATE TABLE IF NOT EXISTS `ds`.`cp` (pipeline_name STRING NOT NULL, "
                "resource_name STRING NOT NULL, run_id STRING, checkpoint_value STRING NOT NULL, "
                "page_number INT64, records_processed INT64, status STRING DEFAULT 'active', "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(), "
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP())",
                (),
            )
        ]


class TestResumeStateFailures:
    """Resume state is a gate, not observability — it fails loudly, never quietly.

    The regression these guard: an unreadable checkpoint used to be downgraded
    to "no checkpoint", which restarts the resource at the window start and
    silently duplicates every already-loaded row on an append-only destination.
    """

    def test_unreadable_checkpoint_fails_the_run_instead_of_starting_fresh(self, duckdb_pipeline, monkeypatch):
        _fail_sql_matching(monkeypatch, "execute_query", "SELECT checkpoint_value")

        with pytest.raises(CheckpointStateError, match="resume state"):
            CheckpointManager(CKPT_PIPELINE, "events", frequency=1).__enter__()

    def test_failed_completion_write_fails_the_run(self, duckdb_pipeline, monkeypatch):
        """Checkpoints left 'active' by a finished run are resume state too: the
        NEXT run would resume from this one's checkpoint instead of its bounds."""
        _fail_sql_matching(monkeypatch, "execute_sql", "SET status = 'completed'")

        with pytest.raises(CheckpointStateError, match="mark checkpoints completed"):
            with CheckpointManager(CKPT_PIPELINE, "events", frequency=1) as mgr:
                mgr.save_checkpoint("cursor-1", [{"id": 1}])

    def test_failed_run_keeps_its_checkpoints_active(self, duckdb_pipeline):
        """The mirror image: a run that raised must NOT mark completed — those
        rows are exactly what its retry resumes from."""
        with pytest.raises(ValueError, match="resource blew up"):
            with CheckpointManager(CKPT_PIPELINE, "events", frequency=1) as mgr:
                mgr.save_checkpoint("cursor-1", [{"id": 1}])
                raise ValueError("resource blew up")

        with CheckpointManager(CKPT_PIPELINE, "events", frequency=1) as reader:
            assert reader.get_last_checkpoint() == "cursor-1"

    def test_checkpoint_write_failure_is_loud_but_not_fatal(self, duckdb_pipeline, monkeypatch, caplog):
        """The one deliberate asymmetry: a lost checkpoint WRITE only costs
        re-work on a future resume and never changes what this run extracts, so
        the healthy extract continues — at ERROR, never swallowed."""
        _fail_sql_matching(monkeypatch, "execute_sql", "INSERT INTO")

        with caplog.at_level(logging.ERROR):
            with CheckpointManager(CKPT_PIPELINE, "events", frequency=1) as mgr:
                mgr.save_checkpoint("cursor-1", [{"id": 1}])
                assert mgr.page_count == 1

        failures = [record for record in caplog.records if "Failed to save checkpoint" in record.getMessage()]
        assert [record.levelname for record in failures] == ["ERROR"]


class TestResumePointSelection:
    def test_resume_uses_the_highest_page_not_the_newest_row(self, duckdb_pipeline):
        """page_number is the run's monotonic key. Ordering on created_at alone
        ties whenever two checkpoints land inside one timestamp tick, and can
        then resume from a checkpoint behind the one the run reached."""
        with CheckpointManager(CKPT_PIPELINE, "events", frequency=1) as mgr:
            mgr.page_count = 2
            mgr._write_checkpoint("page-2")
            mgr.page_count = 1
            mgr._write_checkpoint("page-1")

            # Make the *lower* page unambiguously the newest row by created_at,
            # so a created_at ordering has to pick the wrong checkpoint.
            with duckdb_pipeline.sql_client() as client:
                client.execute_sql(
                    f"UPDATE {CKPT_DATASET}.{DEFAULT_CHECKPOINT_TABLE} "
                    "SET created_at = TIMESTAMPTZ '2000-01-01 00:00:00+00' WHERE page_number = 2"
                )

            assert mgr._load_latest_checkpoint() == "page-2"


class TestCheckpointTableSchema:
    """Test checkpoint table schema and SQL operations."""

    def test_checkpoint_table_name_default(self):
        """Test default checkpoint table name."""
        mgr = CheckpointManager("test_pipeline", "test_resource")
        assert mgr.checkpoint_table == DEFAULT_CHECKPOINT_TABLE
        assert DEFAULT_CHECKPOINT_TABLE == "_dlt_custom_checkpoints"

    def test_checkpoint_table_name_custom(self):
        """Test custom checkpoint table name."""
        mgr = CheckpointManager("test_pipeline", "test_resource", checkpoint_table="custom_checkpoints")
        assert mgr.checkpoint_table == "custom_checkpoints"

    def test_cleanup_days_default(self):
        """Test default cleanup days."""
        mgr = CheckpointManager("test_pipeline", "test_resource")
        assert mgr.cleanup_days == 7

    def test_cleanup_days_custom(self):
        """Test custom cleanup days."""
        mgr = CheckpointManager("test_pipeline", "test_resource", cleanup_days=30)
        assert mgr.cleanup_days == 30


class TestCleanupCheckpointScope:
    """`cleanup_checkpoints` prunes completed rows; active ones are resume state.

    The regression these guard: the DELETE carried no status filter, so the
    manual cleanup verb destroyed `active` rows — the row a crashed extract
    resumes from — and the next run restarted at its window start, silently
    re-extracting everything already loaded. Same failure class the manager
    refuses to cause on a read (TestResumeStateFailures), reached from the
    other side.
    """

    @staticmethod
    def _failed_run(resource: str) -> None:
        """Leave one `active` checkpoint behind, the way a crashed extract does."""
        with pytest.raises(ValueError, match="resource blew up"):
            with CheckpointManager(CKPT_PIPELINE, resource, frequency=1) as mgr:
                mgr.save_checkpoint(f"{resource}-cursor", [{"id": 1}])
                raise ValueError("resource blew up")

    @staticmethod
    def _successful_run(resource: str) -> None:
        """Leave one `completed` checkpoint behind."""
        with CheckpointManager(CKPT_PIPELINE, resource, frequency=1) as mgr:
            mgr.save_checkpoint(f"{resource}-cursor", [{"id": 1}])

    @staticmethod
    def _statuses(pipeline) -> dict[str, str]:
        return {cp["resource_name"]: cp["status"] for cp in list_checkpoints(pipeline=pipeline)}

    def test_active_rows_survive_the_default_cleanup(self, duckdb_pipeline):
        self._failed_run("crashed")
        self._successful_run("finished")

        cleanup_checkpoints(pipeline=duckdb_pipeline)

        assert self._statuses(duckdb_pipeline) == {"crashed": "active"}

    def test_resume_point_still_reads_after_cleanup(self, duckdb_pipeline):
        """The property that actually matters: the crashed resource resumes
        where it stopped instead of re-extracting its whole window."""
        self._failed_run("crashed")

        cleanup_checkpoints(pipeline=duckdb_pipeline)

        with CheckpointManager(CKPT_PIPELINE, "crashed", frequency=1) as resumed:
            assert resumed.get_last_checkpoint() == "crashed-cursor"

    def test_include_active_deletes_everything_in_scope(self, duckdb_pipeline):
        """The destructive form stays reachable — abandoning a poisoned resume
        point is a real recovery path. It just has to be asked for."""
        self._failed_run("crashed")
        self._successful_run("finished")

        cleanup_checkpoints(pipeline=duckdb_pipeline, include_active=True)

        assert list_checkpoints(pipeline=duckdb_pipeline) == []

    def test_kept_active_rows_are_reported(self, duckdb_pipeline, caplog):
        """Keeping them silently would be its own defect: the operator asked for
        a cleanup and has to learn what it declined to delete."""
        self._failed_run("crashed")

        with caplog.at_level(logging.WARNING):
            cleanup_checkpoints(pipeline=duckdb_pipeline)

        kept = [r for r in caplog.records if "active checkpoint row(s)" in r.getMessage()]
        assert [r.levelname for r in kept] == ["WARNING"]
        assert "include_active=True" in kept[0].getMessage()

    def test_no_warning_when_nothing_was_withheld(self, duckdb_pipeline, caplog):
        self._successful_run("finished")

        with caplog.at_level(logging.WARNING):
            cleanup_checkpoints(pipeline=duckdb_pipeline)

        assert [r for r in caplog.records if "active checkpoint row(s)" in r.getMessage()] == []

    def test_resource_scope_narrows_both_forms(self, duckdb_pipeline):
        self._failed_run("crashed")
        self._failed_run("other")

        cleanup_checkpoints(pipeline=duckdb_pipeline, resource_name="crashed", include_active=True)

        assert self._statuses(duckdb_pipeline) == {"other": "active"}

    def test_missing_table_is_not_an_error(self, duckdb_pipeline):
        """Nothing has run, so the checkpoint table does not exist yet."""
        cleanup_checkpoints(pipeline=duckdb_pipeline)
        cleanup_checkpoints(pipeline=duckdb_pipeline, include_active=True)

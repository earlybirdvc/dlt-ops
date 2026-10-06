"""Live-table handling shared by both reconciler detectors.

Both detectors read the live destination columns first. A declared resource
whose table never landed is skipped. Each table gets its own time column: the
configured ``load_timestamp_column`` (normalized with the source's naming
convention) when the table has it, else the column the destination reports as
the partition column, else none. The time column drives removal's coverage
windows, additive sampling, and the ``reproduce_sql`` on every finding.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pydantic
import pytest
from dlt.common.normalizers.naming.snake_case import NamingConvention

from dlt_ops.destinations import ColumnInfo
from dlt_ops.reconciler import additive as additive_mod
from dlt_ops.reconciler import removal as removal_mod
from tests.test_reconciler import (
    ORDER_ITEM_LIVE,
    FakeQueryRunner,
    FakeSchemaFetcher,
    OrderItemModel,
    RecordingSink,
    _cols,
    _make_source,
    _project_config,
    _seed,
    duckdb_home,  # noqa: F401 - pytest fixture, requested through usefixtures below
)


class EventModel(pydantic.BaseModel):
    """A resource whose table is partitioned on one of its own model columns."""

    event_id: str
    event_ts: datetime
    payload: str | None = None


def _partition(name: str, data_type: str = "TIMESTAMP") -> ColumnInfo:
    """A column the destination reports as the table's partition column."""
    return ColumnInfo(name=name, data_type=data_type, is_partition_column=True)


def _event_columns(*extra: str, partitioned: bool = True) -> tuple[ColumnInfo, ...]:
    event_ts = _partition("event_ts") if partitioned else ColumnInfo(name="event_ts", data_type="TIMESTAMP")
    return (*_cols("event_id", "payload"), event_ts, *_cols(*extra))


def _coverage_queries(runner: FakeQueryRunner) -> list[str]:
    return [sql for sql, _params in runner.queries if "AS recent_" in sql]


def _detect_removal(
    source: Any, *, fetcher: Any, runner: Any, sink: Any = None, project_config: Any = None, dataset: str = "raw"
):
    return removal_mod.detect_removal(
        source.name,
        dry_run=sink is None,
        runner=runner,
        fetcher=fetcher,
        dataset=dataset,
        sources={source.name: source},
        project_config=project_config if project_config is not None else _project_config(),
        sink=sink,
    )


def _reconcile_additive(source: Any, *, fetcher: Any, runner: Any, project_config: Any = None, dataset: str = "raw"):
    return additive_mod.reconcile_source(
        source.name,
        dry_run=True,
        fetcher=fetcher,
        runner=runner,
        dataset=dataset,
        sources={source.name: source},
        project_config=project_config if project_config is not None else _project_config(),
    )


# ---------------------------------------------------------------------------
# Time-column rule (unit)
# ---------------------------------------------------------------------------


@pytest.fixture
def resolve_time_column():
    from dlt_ops.reconciler._live_tables import resolve_time_column

    return resolve_time_column


class TestResolveTimeColumn:
    NAMING = NamingConvention()

    def test_configured_column_wins_over_partition_column(self, resolve_time_column):
        assert resolve_time_column(_event_columns("loaded_at"), "loaded_at", self.NAMING) == "loaded_at"

    def test_configured_name_is_normalized_before_the_lookup(self, resolve_time_column):
        assert resolve_time_column(_event_columns("loaded_at"), "loadedAt", self.NAMING) == "loaded_at"

    def test_configured_path_separator_is_kept(self, resolve_time_column):
        assert resolve_time_column(_event_columns("loaded__at"), "loaded__at", self.NAMING) == "loaded__at"

    def test_configured_path_separator_is_kept_without_a_partition_column(self, resolve_time_column):
        columns = _event_columns("loaded__at", partitioned=False)
        assert resolve_time_column(columns, "loaded__at", self.NAMING) == "loaded__at"

    def test_partition_column_when_configured_column_is_absent(self, resolve_time_column):
        assert resolve_time_column(_event_columns(), "loaded_at", self.NAMING) == "event_ts"

    def test_partition_column_when_nothing_is_configured(self, resolve_time_column):
        assert resolve_time_column(_event_columns(), None, self.NAMING) == "event_ts"

    def test_none_when_table_has_neither(self, resolve_time_column):
        assert resolve_time_column(_event_columns(partitioned=False), "loaded_at", self.NAMING) is None


# ---------------------------------------------------------------------------
# Removal
# ---------------------------------------------------------------------------


class TestRemovalLiveTables:
    def test_missing_table_is_skipped(self):
        """A declared resource that never landed: no query, no error, no finding."""
        source = _make_source(resources={"order_items": OrderItemModel, "events": EventModel})
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE, "events": None})
        runner = FakeQueryRunner()
        sink = RecordingSink()

        result = _detect_removal(source, fetcher=fetcher, runner=runner, sink=sink)

        assert result.error is None
        assert result.findings == ()
        assert sink.errors == []
        [sql] = _coverage_queries(runner)
        assert '"raw"."order_items"' in sql
        assert all('"events"' not in sql for sql, _params in runner.queries)

    def test_configured_column_wins_over_partition_column(self):
        source = _make_source(resources={"events": EventModel})
        fetcher = FakeSchemaFetcher({"events": _event_columns("loaded_at")})
        runner = FakeQueryRunner(coverage={"payload": (0.0, 0.9)})

        result = _detect_removal(source, fetcher=fetcher, runner=runner)

        [sql] = _coverage_queries(runner)
        assert 'WHERE "loaded_at" >= ?' in sql
        assert 'WHERE "event_ts"' not in sql
        [finding] = result.findings
        assert '"loaded_at" >= TIMESTAMP' in finding.reproduce_sql

    def test_partition_column_used_when_configured_column_is_absent(self):
        source = _make_source(resources={"events": EventModel})
        fetcher = FakeSchemaFetcher({"events": _event_columns()})
        runner = FakeQueryRunner(coverage={"payload": (0.0, 0.9)})

        result = _detect_removal(source, fetcher=fetcher, runner=runner)

        assert result.error is None
        [sql] = _coverage_queries(runner)
        assert 'WHERE "event_ts" >= ?' in sql
        assert '"loaded_at"' not in sql
        [finding] = result.findings
        assert finding.columns == ("payload",)
        assert '"event_ts" >= TIMESTAMP' in finding.reproduce_sql
        assert '"loaded_at"' not in finding.reproduce_sql

    def test_configured_name_is_normalized_with_the_source_naming(self):
        """`loadedAt` in config lands as `loaded_at`; the SQL must use the landed name."""
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE})
        runner = FakeQueryRunner()

        result = _detect_removal(
            source, fetcher=fetcher, runner=runner, project_config=_project_config(load_timestamp_column="loadedAt")
        )

        assert result.error is None
        [sql] = _coverage_queries(runner)
        assert 'WHERE "loaded_at" >= ?' in sql
        assert '"loadedAt"' not in sql

    def test_configured_path_separator_is_kept_in_the_sql(self):
        """dlt keeps the `__` separator in a column name, so `loaded__at` lands as `loaded__at`."""
        source = _make_source(resources={"order_items": OrderItemModel})
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE[:-1] + _cols("loaded__at")})
        runner = FakeQueryRunner(default_coverage=(0.0, 0.9))

        result = _detect_removal(
            source, fetcher=fetcher, runner=runner, project_config=_project_config(load_timestamp_column="loaded__at")
        )

        assert result.error is None
        [sql] = _coverage_queries(runner)
        assert 'WHERE "loaded__at" >= ?' in sql
        [finding] = result.findings
        assert '"loaded__at" >= TIMESTAMP' in finding.reproduce_sql

    def test_no_time_column_skips_the_table_with_one_info_log(self, caplog):
        source = _make_source(resources={"events": EventModel})
        fetcher = FakeSchemaFetcher({"events": _event_columns(partitioned=False)})
        runner = FakeQueryRunner()
        sink = RecordingSink()

        with caplog.at_level(logging.INFO, logger="dlt_ops.reconciler"):
            result = _detect_removal(source, fetcher=fetcher, runner=runner, sink=sink)

        assert result.error is None
        assert result.findings == ()
        assert sink.errors == []
        assert runner.queries == []
        info = [r for r in caplog.records if r.name.startswith("dlt_ops.reconciler") and r.levelno == logging.INFO]
        assert len(info) == 1
        assert "events" in info[0].getMessage()

    def test_schema_fetch_failure_reports_once_and_sets_error(self):
        source = _make_source(resources={"order_items": OrderItemModel})

        class BoomFetcher:
            def fetch(self, refs):
                raise RuntimeError("credentials not found")

        runner = FakeQueryRunner()
        sink = RecordingSink()

        result = _detect_removal(source, fetcher=BoomFetcher(), runner=runner, sink=sink)

        assert result.error is not None
        assert "credentials not found" in result.error
        assert result.findings == ()
        assert runner.queries == []
        assert sink.errors == [("orders_api", None, "fetch_schemas")]


# ---------------------------------------------------------------------------
# Additive sampling
# ---------------------------------------------------------------------------


class TestAdditiveTimeColumn:
    def test_configured_column_wins_over_partition_column(self):
        source = _make_source(resources={"events": EventModel}, injected_columns=())
        fetcher = FakeSchemaFetcher({"events": _event_columns("loaded_at", "surprise_column")})
        runner = FakeQueryRunner(sample_rows=[("v",)])

        result = _reconcile_additive(source, fetcher=fetcher, runner=runner)

        [finding] = result.findings
        assert finding.columns == ("surprise_column",)
        [(sample_sql, _params)] = runner.queries
        assert 'ORDER BY "loaded_at" DESC' in sample_sql
        assert '"loaded_at" >= TIMESTAMP' in finding.reproduce_sql

    def test_partition_column_used_when_configured_column_is_absent(self):
        source = _make_source(resources={"events": EventModel}, injected_columns=())
        fetcher = FakeSchemaFetcher({"events": _event_columns("surprise_column")})
        runner = FakeQueryRunner(sample_rows=[("v",)])

        result = _reconcile_additive(source, fetcher=fetcher, runner=runner)

        [finding] = result.findings
        assert finding.columns == ("surprise_column",)
        [(sample_sql, sample_params)] = runner.queries
        assert 'WHERE "event_ts" >= ?' in sample_sql
        assert 'ORDER BY "event_ts" DESC' in sample_sql
        assert '"loaded_at"' not in sample_sql
        assert len(sample_params) == 1
        assert '"event_ts" >= TIMESTAMP' in finding.reproduce_sql

    def test_no_time_column_falls_back_to_unordered_sample(self):
        source = _make_source(resources={"events": EventModel}, injected_columns=())
        fetcher = FakeSchemaFetcher({"events": _event_columns("surprise_column", partitioned=False)})
        runner = FakeQueryRunner(sample_rows=[("v",)])

        result = _reconcile_additive(source, fetcher=fetcher, runner=runner)

        [finding] = result.findings
        assert finding.sample_values["surprise_column"] == ["v"]
        [(sample_sql, sample_params)] = runner.queries
        assert "WHERE" not in sample_sql
        assert "ORDER BY" not in sample_sql
        assert sample_sql.endswith("LIMIT 5")
        assert sample_params == ()
        assert "WHERE" not in finding.reproduce_sql

    def test_configured_name_is_normalized_with_the_source_naming(self):
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=())
        fetcher = FakeSchemaFetcher({"order_items": ORDER_ITEM_LIVE + _cols("surprise_column")})
        runner = FakeQueryRunner(sample_rows=[("v",)])

        result = _reconcile_additive(
            source, fetcher=fetcher, runner=runner, project_config=_project_config(load_timestamp_column="loadedAt")
        )

        [finding] = result.findings
        assert finding.columns == ("surprise_column",)
        [(sample_sql, _params)] = runner.queries
        assert 'ORDER BY "loaded_at" DESC' in sample_sql
        assert '"loadedAt"' not in sample_sql
        assert '"loaded_at" >= TIMESTAMP' in finding.reproduce_sql


# ---------------------------------------------------------------------------
# DuckDB end-to-end (real DestinationAdapter boundary)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.usefixtures("duckdb_home")
class TestRemovalLiveTablesDuckDB:
    PROJECT_CONFIG = _project_config(default_destination="duckdb", default_dataset="raw_orders")

    def test_declared_resource_that_never_landed_is_not_an_error(self):
        now = datetime.now(tz=UTC)
        rows = [
            {"api_id": f"a{i}", "order_id": f"o{i}", "name": "n", "discount_code": "D", "loaded_at": now - age}
            for i, age in enumerate((timedelta(minutes=10), timedelta(hours=24)))
        ]
        _seed("orders_api_pipeline", "raw_orders", "order_items", rows)
        source = _make_source(resources={"order_items": OrderItemModel, "events": EventModel}, injected_columns=())
        sink = RecordingSink()

        result = removal_mod.detect_removal(
            "orders_api", sources={"orders_api": source}, project_config=self.PROJECT_CONFIG, sink=sink
        )

        assert result.error is None
        assert result.findings == ()
        assert sink.errors == []

    def test_table_without_a_time_column_is_not_an_error(self):
        """DuckDB reports no partition column, so a table without the configured column has no time column."""
        _seed(
            "orders_api_pipeline",
            "raw_orders",
            "order_items",
            [{"api_id": "a1", "order_id": "o1", "name": "n", "discount_code": "D"}],
        )
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=())
        sink = RecordingSink()

        result = removal_mod.detect_removal(
            "orders_api", sources={"orders_api": source}, project_config=self.PROJECT_CONFIG, sink=sink
        )

        assert result.error is None
        assert result.findings == ()
        assert sink.errors == []

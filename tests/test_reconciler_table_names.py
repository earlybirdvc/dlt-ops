"""Where both reconciler detectors look for a resource's table.

dlt writes a resource's table under the resource name, and its dataset under
the configured dataset name, each normalized by the source's naming
convention. Both detectors must look up and query those physical names, while
findings keep the original resource name.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

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
from tests.test_reconciler_live_tables import _coverage_queries, _detect_removal, _reconcile_additive


# ---------------------------------------------------------------------------
# Resource name vs. table name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("resource_name", "table_name"),
    [
        ("EVENTS", "events"),
        ("OrderItems", "order_items"),
        # dlt keeps the `__` path separator in a table name; normalizing the
        # whole name as one identifier would collapse it to `orders_archive`.
        ("orders__archive", "orders__archive"),
    ],
)
class TestResourceTableName:
    """dlt writes a resource's table under the resource name normalized by the source's naming convention."""

    def test_removal_queries_the_normalized_table(self, resource_name, table_name):
        source = _make_source(resources={resource_name: OrderItemModel})
        fetcher = FakeSchemaFetcher({table_name: ORDER_ITEM_LIVE})
        runner = FakeQueryRunner(coverage={"discount_code": (0.0, 0.9)})

        result = _detect_removal(source, fetcher=fetcher, runner=runner)

        assert [ref.table for ref in fetcher.requested] == [table_name]
        [sql] = _coverage_queries(runner)
        assert f'FROM "raw"."{table_name}"' in sql
        [finding] = result.findings
        assert finding.resource_name == resource_name
        assert f'FROM "raw"."{table_name}"' in finding.reproduce_sql

    def test_additive_samples_the_normalized_table(self, resource_name, table_name):
        source = _make_source(resources={resource_name: OrderItemModel}, injected_columns=())
        fetcher = FakeSchemaFetcher({table_name: ORDER_ITEM_LIVE + _cols("surprise_column")})
        runner = FakeQueryRunner(sample_rows=[("v",)])

        result = _reconcile_additive(source, fetcher=fetcher, runner=runner)

        assert [ref.table for ref in fetcher.requested] == [table_name]
        [finding] = result.findings
        assert finding.resource_name == resource_name
        assert finding.columns == ("surprise_column",)
        [(sample_sql, _params)] = runner.queries
        assert f'FROM "raw"."{table_name}"' in sample_sql
        assert f'FROM "raw"."{table_name}"' in finding.reproduce_sql


@pytest.mark.integration
@pytest.mark.usefixtures("duckdb_home")
@pytest.mark.parametrize("resource_name", ["EVENTS", "OrderItems"])
class TestResourceTableNameDuckDB:
    """A dlt run names the table after the normalized resource name; both detectors must find it."""

    PROJECT_CONFIG = _project_config(default_destination="duckdb", default_dataset="raw_orders")

    def test_removal_finds_the_table(self, resource_name):
        now = datetime.now(tz=UTC)
        rows = [
            {
                "api_id": f"a{i}",
                "order_id": f"o{i}",
                "name": "n",
                "discount_code": "D" if age > timedelta(hours=6) else None,
                "loaded_at": now - age,
            }
            for i, age in enumerate((timedelta(minutes=10), timedelta(hours=24)))
        ]
        # A dlt run normalizes this table name the same way it normalizes a resource name.
        _seed("orders_api_pipeline", "raw_orders", resource_name, rows)
        source = _make_source(resources={resource_name: OrderItemModel}, injected_columns=())
        sink = RecordingSink()

        result = removal_mod.detect_removal(
            "orders_api", dry_run=True, sources={"orders_api": source}, project_config=self.PROJECT_CONFIG, sink=sink
        )

        assert result.error is None
        assert sink.errors == []
        [finding] = result.findings
        assert finding.resource_name == resource_name
        assert finding.columns == ("discount_code",)

    def test_additive_finds_the_table(self, resource_name):
        row = {
            "api_id": "a1",
            "order_id": "o1",
            "name": "n",
            "discount_code": "D",
            "loaded_at": datetime.now(tz=UTC),
            "surprise_column": "hello",
        }
        _seed("orders_api_pipeline", "raw_orders", resource_name, [row])
        source = _make_source(resources={resource_name: OrderItemModel}, injected_columns=())

        result = additive_mod.reconcile_source(
            "orders_api", dry_run=True, sources={"orders_api": source}, project_config=self.PROJECT_CONFIG
        )

        assert result.error is None
        [finding] = result.findings
        assert finding.resource_name == resource_name
        assert finding.sample_values["surprise_column"] == ["hello"]


# ---------------------------------------------------------------------------
# Configured dataset vs. physical dataset
# ---------------------------------------------------------------------------


class _UppercaseNaming:
    def normalize_identifier(self, identifier: str) -> str:
        return identifier.upper()

    normalize_path = normalize_identifier

    normalize_tables_path = normalize_identifier
    normalize_table_identifier = normalize_identifier


@pytest.mark.parametrize(
    ("configured", "naming", "physical", "table"),
    [
        ("RAW_ORDERS", None, "raw_orders", "order_items"),
        ("rawOrders", None, "raw_orders", "order_items"),
        # The source's own naming convention decides, not a fixed lowercase rule.
        ("raw_orders", _UppercaseNaming(), "RAW_ORDERS", "ORDER_ITEMS"),
    ],
)
class TestPhysicalDataset:
    """dlt writes to the configured dataset normalized by the source's naming convention."""

    @staticmethod
    def _live(naming: Any, *extra: str) -> tuple[ColumnInfo, ...]:
        columns = ORDER_ITEM_LIVE + _cols(*extra)
        if naming is None:
            return columns
        return tuple(ColumnInfo(name=naming.normalize_identifier(c.name), data_type=c.data_type) for c in columns)

    def test_removal_looks_up_and_queries_the_normalized_dataset(self, configured, naming, physical, table):
        source = _make_source(resources={"order_items": OrderItemModel}, naming=naming)
        fetcher = FakeSchemaFetcher({table: self._live(naming)})
        runner = FakeQueryRunner(default_coverage=(0.0, 0.9))

        result = _detect_removal(source, fetcher=fetcher, runner=runner, dataset=configured)

        assert [ref.dataset for ref in fetcher.requested] == [physical]
        [sql] = _coverage_queries(runner)
        assert f'FROM "{physical}"."{table}"' in sql
        [finding] = result.findings
        assert f'FROM "{physical}"."{table}"' in finding.reproduce_sql

    def test_additive_looks_up_and_samples_the_normalized_dataset(self, configured, naming, physical, table):
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=(), naming=naming)
        fetcher = FakeSchemaFetcher({table: self._live(naming, "surprise_column")})
        runner = FakeQueryRunner(sample_rows=[("v",)])

        result = _reconcile_additive(source, fetcher=fetcher, runner=runner, dataset=configured)

        assert [ref.dataset for ref in fetcher.requested] == [physical]
        [finding] = result.findings
        [(sample_sql, _params)] = runner.queries
        assert f'FROM "{physical}"."{table}"' in sample_sql
        assert f'FROM "{physical}"."{table}"' in finding.reproduce_sql


@pytest.mark.integration
@pytest.mark.usefixtures("duckdb_home")
class TestPhysicalDatasetDuckDB:
    """A dlt run lands dataset ``RAW_ORDERS`` as schema ``raw_orders``; both detectors must find its tables."""

    PROJECT_CONFIG = _project_config(default_destination="duckdb", default_dataset="RAW_ORDERS")

    def test_removal_finds_the_table(self):
        now = datetime.now(tz=UTC)
        rows = [
            {
                "api_id": f"a{i}",
                "order_id": f"o{i}",
                "name": "n",
                "discount_code": "D" if age > timedelta(hours=6) else None,
                "loaded_at": now - age,
            }
            for i, age in enumerate((timedelta(minutes=10), timedelta(hours=24)))
        ]
        _seed("orders_api_pipeline", "RAW_ORDERS", "order_items", rows)
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=())
        sink = RecordingSink()

        result = removal_mod.detect_removal(
            "orders_api", dry_run=True, sources={"orders_api": source}, project_config=self.PROJECT_CONFIG, sink=sink
        )

        assert result.error is None
        assert sink.errors == []
        [finding] = result.findings
        assert finding.columns == ("discount_code",)
        assert 'FROM "raw_orders"."order_items"' in finding.reproduce_sql

    def test_additive_finds_the_table(self):
        row = {
            "api_id": "a1",
            "order_id": "o1",
            "name": "n",
            "discount_code": "D",
            "loaded_at": datetime.now(tz=UTC),
            "surprise_column": "hello",
        }
        _seed("orders_api_pipeline", "RAW_ORDERS", "order_items", [row])
        source = _make_source(resources={"order_items": OrderItemModel}, injected_columns=())

        result = additive_mod.reconcile_source(
            "orders_api", dry_run=True, sources={"orders_api": source}, project_config=self.PROJECT_CONFIG
        )

        assert result.error is None
        [finding] = result.findings
        assert finding.columns == ("surprise_column",)
        assert finding.sample_values["surprise_column"] == ["hello"]

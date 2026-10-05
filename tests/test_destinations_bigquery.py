"""BigQuery adapter tests for the partition flag ``fetch_columns`` reports.

Offline: the columns query selects ``is_partitioning_column`` and its
``YES``/``NO`` values map onto ``ColumnInfo.is_partition_column``.

Integration (@pytest.mark.integration, skips without the BigQuery SDK or
credentials): a table partitioned on a column reports that column, and only
that column, as the partition column.
"""

import os

import dlt
import pytest

from dlt_ops.destinations import ColumnInfo, get_adapter
from dlt_ops.destinations.bigquery import BigQueryAdapter
from tests.test_destinations import RecordingClient


class TestPartitionFlagOffline:
    def test_columns_query_selects_the_partitioning_flag(self):
        client = RecordingClient(rows=[("created_at", "TIMESTAMP", "YES")])
        BigQueryAdapter().fetch_columns(client, "ds", "orders_api")
        (method, sql, args) = client.calls[0]
        assert method == "execute_query"
        assert sql.startswith(
            "SELECT column_name, data_type, is_partitioning_column FROM `ds`.INFORMATION_SCHEMA.COLUMNS"
        )
        assert args == ("orders_api",)

    def test_yes_and_no_map_onto_the_flag(self):
        client = RecordingClient(rows=[("id", "INT64", "NO"), ("created_at", "TIMESTAMP", "YES")])
        columns = BigQueryAdapter().fetch_columns(client, "ds", "orders_api")
        assert columns == [
            ColumnInfo(name="id", data_type="INT64", is_partition_column=False),
            ColumnInfo(name="created_at", data_type="TIMESTAMP", is_partition_column=True),
        ]

    def test_flag_comparison_ignores_case(self):
        client = RecordingClient(rows=[("created_at", "TIMESTAMP", "yes"), ("id", "INT64", "no")])
        columns = BigQueryAdapter().fetch_columns(client, "ds", "orders_api")
        assert columns is not None
        assert [column.is_partition_column for column in columns] == [True, False]

    def test_two_column_row_reports_no_partition_column(self):
        client = RecordingClient(rows=[("created_at", "TIMESTAMP")])
        columns = BigQueryAdapter().fetch_columns(client, "ds", "orders_api")
        assert columns == [ColumnInfo(name="created_at", data_type="TIMESTAMP", is_partition_column=False)]


@pytest.mark.integration
def test_bigquery_live_partition_column_is_reported(tmp_path):
    """Live BigQuery lane: skips cleanly without the SDK or credentials."""
    pytest.importorskip("google.cloud.bigquery")
    if not (os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("BQ_SERVICE_ACCOUNT_JSON")):
        pytest.skip("BigQuery credentials not configured")

    dataset, table = "dlt_ext_adapter_ci", "partition_probe"
    adapter = get_adapter("bigquery")
    pipeline = dlt.pipeline(
        pipeline_name="dest_adapter_bq_partition_ci",
        destination="bigquery",
        dataset_name=dataset,
        pipelines_dir=str(tmp_path),
    )
    pipeline.run([{"seed": 1}], table_name="seed_rows")
    with pipeline.sql_client() as client:
        table_ref = adapter.render_table_ref(dataset, table)
        try:
            adapter.execute_sql(
                client,
                f"CREATE TABLE IF NOT EXISTS {table_ref} "
                "(order_id VARCHAR, created_at TIMESTAMPTZ) PARTITION BY DATE(created_at)",
            )
            columns = adapter.fetch_columns(client, dataset, table)
            assert columns is not None
            assert {column.name: column.is_partition_column for column in columns} == {
                "order_id": False,
                "created_at": True,
            }
        finally:
            adapter.drop_table_if_exists(client, dataset, table)

"""Postgres adapter tests.

Offline: plugin registration, capability surface, snapshot-locked transpiles of
the shared canonical-SQL fixture suite (imported from tests/test_destinations.py,
not copied), adapter-owned placeholder conversion, and import hygiene (no
psycopg at import time).

Integration (@pytest.mark.integration, live Postgres via the ``postgres_url``
conftest fixture — POSTGRES_URL or a throwaway docker pg16): the same canonical
suite executed through dlt's real sql_client, the checkpoint lifecycle through
``dlt_ops.checkpoints``, a pipeline.run equivalence probe against DuckDB,
and the SQL-injection regression. Cleanup's state surgery is covered by the
cleanup suite; the equivalence probe below stands in for a runner end-to-end.
"""

import datetime as dt
import json
import subprocess
import sys
import uuid

import dlt
import pytest
import sqlglot

from dlt_ops import DestinationAdapter, cleanup_checkpoints, list_checkpoints, with_checkpoints
from dlt_ops.checkpoints.manager import CheckpointManager
from dlt_ops.destinations import ColumnInfo, adapter_for_pipeline, get_adapter
from dlt_ops.destinations.postgres import PostgresAdapter
from dlt_ops.plugins import names
from dlt_ops.plugins import registry as registry_mod
from tests.test_destinations import RecordingClient, canonical_shapes

# Snapshot-locked native renderings of the shared canonical shapes: they must
# stay valid Postgres (double quotes, %s placeholders, CURRENT_TIMESTAMP,
# TEXT/BIGINT types, interval arithmetic). `DESC NULLS LAST` is sqlglot
# preserving DuckDB's DESC null ordering — identical here (NOT NULL column).
EXPECTED_SQL = {
    "create_table": (
        'CREATE TABLE IF NOT EXISTS "ds"."cp" (pipeline_name TEXT NOT NULL, resource_name TEXT NOT NULL, '
        "run_id TEXT, checkpoint_value TEXT NOT NULL, page_number BIGINT, records_processed BIGINT, "
        "status TEXT DEFAULT 'active', created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP, "
        "updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)"
    ),
    "insert": (
        'INSERT INTO "ds"."cp" (pipeline_name, resource_name, run_id, checkpoint_value, page_number, '
        "records_processed, status) VALUES (%s, %s, %s, %s, %s, %s, 'active')"
    ),
    "update": (
        'UPDATE "ds"."cp" SET status = \'completed\', updated_at = CURRENT_TIMESTAMP '
        "WHERE pipeline_name = %s AND resource_name = %s AND status = 'active'"
    ),
    "delete_old": (
        "DELETE FROM \"ds\".\"cp\" WHERE status = 'completed' AND created_at < CURRENT_TIMESTAMP - INTERVAL '7 DAYS'"
    ),
    "select_latest": (
        'SELECT checkpoint_value FROM "ds"."cp" WHERE pipeline_name = %s AND resource_name = %s '
        "AND status = 'active' AND run_id IS NULL ORDER BY created_at DESC NULLS LAST LIMIT 1"
    ),
    "alter_add_column": 'ALTER TABLE "ds"."cp" ADD COLUMN IF NOT EXISTS run_id TEXT',
}

EXPECTED_COLUMNS_SQL = (
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position"
)

HOSTILE = "x'); DROP TABLE t;--"


@pytest.fixture
def adapter():
    return get_adapter("postgres")


class TestRegistrationAndCapabilities:
    def test_registered_via_entry_point(self):
        assert "postgres" in names("destination")
        assert registry_mod.source("destination", "postgres").dist == "dlt-ops"

    def test_get_adapter_resolves_and_satisfies_protocol(self, adapter):
        assert isinstance(adapter, PostgresAdapter)
        assert isinstance(adapter, DestinationAdapter)

    def test_capability_surface(self, adapter):
        # Capability surface verified against live Postgres across the supported dlt range.
        assert (adapter.name, adapter.placeholder_style) == ("postgres", "%s")
        assert adapter.supports_if_exists is True
        assert adapter.supports_alter_add_column_if_not_exists is True
        assert adapter.supports_create_schema_if_not_exists is True
        assert adapter.timestamp_now_sql == "CURRENT_TIMESTAMP"


class TestTranspileSnapshots:
    @pytest.mark.parametrize(
        "shape", ["create_table", "insert", "update", "delete_old", "select_latest", "alter_add_column"]
    )
    def test_canonical_shape_renders_native_sql(self, adapter, shape):
        canonical_sql, param_count = canonical_shapes(adapter)[shape]
        client = RecordingClient()
        params = tuple(f"p{i}" for i in range(param_count))
        adapter.execute_sql(client, canonical_sql, *params)
        (method, sql, args) = client.calls[0]
        assert method == "execute_sql"
        assert sql == EXPECTED_SQL[shape]
        assert args == params

    def test_information_schema_columns_query(self, adapter):
        client = RecordingClient(rows=[("pipeline_name", "text")])
        adapter.fetch_columns(client, "ds", "cp")
        (method, sql, args) = client.calls[0]
        assert method == "execute_query"
        assert sql == EXPECTED_COLUMNS_SQL
        assert args == ("ds", "cp")


class TestPlaceholderConversion:
    def test_sqlglot_postgres_writer_rewrites_question_mark(self):
        """The postgres write dialect rewrites ? -> %s on its own."""
        assert (
            sqlglot.transpile("SELECT * FROM t WHERE a = ?", read="duckdb", write="postgres")[0]
            == "SELECT * FROM t WHERE a = %s"
        )

    def test_conversion_is_adapter_owned_and_count_matched(self, adapter):
        """Placeholders are swapped as AST nodes before rendering: the output style
        never depends on sqlglot's per-dialect rewrite, and the %s count matches
        the bound params (a '?' string literal is not a placeholder)."""
        client = RecordingClient()
        adapter.execute_sql(client, 'SELECT \'?\' AS q, a FROM "ds"."t" WHERE a = ? AND b = ?', "x", "y")
        sql = client.calls[0][1]
        assert "'?'" in sql
        assert sql.count("%s") == 2
        assert sql.replace("'?'", "").count("?") == 0

    def test_param_count_mismatch_raises_before_execution(self, adapter):
        client = RecordingClient()
        with pytest.raises(ValueError, match="placeholder/param mismatch"):
            adapter.execute_sql(client, 'SELECT * FROM "ds"."t" WHERE a = ? AND b = ?', "only-one")
        assert client.calls == []

    def test_injection_value_stays_out_of_sql_text(self, adapter):
        client = RecordingClient()
        adapter.execute_sql(client, 'INSERT INTO "ds"."cp" (pipeline_name) VALUES (?)', HOSTILE)
        (_, sql, args) = client.calls[0]
        assert HOSTILE not in sql
        assert args == (HOSTILE,)


class TestImportHygiene:
    def test_adapter_import_pulls_no_psycopg(self):
        """Core install hygiene: the adapter (and dlt_ops) load without psycopg."""
        code = "import json, sys, dlt_ops, dlt_ops.destinations.postgres; print(json.dumps(sorted(sys.modules)))"
        proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
        loaded = json.loads(proc.stdout)
        offenders = [
            module
            for module in loaded
            if any(module == pkg or module.startswith(f"{pkg}.") for pkg in ("psycopg2", "psycopg"))
        ]
        assert offenders == []


@pytest.fixture(scope="module")
def pg_pipeline(postgres_url, tmp_path_factory):
    """A real dlt pipeline on Postgres — the adapter must work through dlt's own sql_client."""
    tmp = tmp_path_factory.mktemp("dest_postgres")
    dataset = f"dltx_pg_{uuid.uuid4().hex[:8]}"
    pipeline = dlt.pipeline(
        pipeline_name="dest_adapter_pg_test",
        destination=dlt.destinations.postgres(postgres_url),
        dataset_name=dataset,
        pipelines_dir=str(tmp / "pipelines"),
    )
    pipeline.run([{"seed": 1}], table_name="seed_rows")
    yield pipeline
    with pipeline.sql_client() as client:
        for schema in (dataset, f"{dataset}_extra"):
            client.execute_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture
def pg_client(pg_pipeline):
    with pg_pipeline.sql_client() as client:
        yield client


@pytest.fixture
def pg_dataset(pg_pipeline):
    return pg_pipeline.dataset_name


@pytest.mark.integration
class TestPostgresLive:
    """The shared canonical-SQL fixture suite, executed against live Postgres."""

    def _create(self, adapter, client, dataset, table):
        ddl, _ = canonical_shapes(adapter)["create_table"]
        adapter.execute_sql(client, ddl.replace('"ds"."cp"', adapter.render_table_ref(dataset, table)))

    def _shape(self, adapter, dataset, table, name):
        sql, _ = canonical_shapes(adapter)[name]
        return sql.replace('"ds"."cp"', adapter.render_table_ref(dataset, table))

    def test_insert_select_round_trip(self, adapter, pg_client, pg_dataset):
        self._create(adapter, pg_client, pg_dataset, "cp_roundtrip")
        insert = self._shape(adapter, pg_dataset, "cp_roundtrip", "insert")
        adapter.execute_sql(pg_client, insert, "pipe", "res", None, "cursor-1", 1, 10)
        adapter.execute_sql(pg_client, insert, "pipe", "res", None, "cursor-2", 2, 20)
        cursor = adapter.execute_query(
            pg_client, self._shape(adapter, pg_dataset, "cp_roundtrip", "select_latest"), "pipe", "res"
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] in ("cursor-1", "cursor-2")  # same-timestamp tie; both are valid "latest"
        assert cursor.fetchone() is None  # LIMIT 1

    def test_injection_probe_round_trips_as_data(self, adapter, pg_client, pg_dataset):
        adapter.execute_sql(
            pg_client, f"CREATE TABLE IF NOT EXISTS {adapter.render_table_ref(pg_dataset, 't')} (x BIGINT)"
        )
        self._create(adapter, pg_client, pg_dataset, "cp_probe")
        adapter.execute_sql(
            pg_client, self._shape(adapter, pg_dataset, "cp_probe", "insert"), HOSTILE, "res", None, "v", 1, 1
        )
        cursor = adapter.execute_query(
            pg_client,
            f"SELECT pipeline_name FROM {adapter.render_table_ref(pg_dataset, 'cp_probe')} WHERE pipeline_name = ?",
            HOSTILE,
        )
        assert cursor.fetchall() == [(HOSTILE,)]
        # The decoy table survived: the hostile value never reached the SQL text.
        assert adapter.table_exists(pg_client, pg_dataset, "t")

    def test_update_and_delete_old_shapes(self, adapter, pg_client, pg_dataset):
        self._create(adapter, pg_client, pg_dataset, "cp_lifecycle")
        table_ref = adapter.render_table_ref(pg_dataset, "cp_lifecycle")
        adapter.execute_sql(
            pg_client, self._shape(adapter, pg_dataset, "cp_lifecycle", "insert"), "pipe", "res", None, "v", 1, 1
        )
        # Backdate a second, already-completed checkpoint so delete_old has a target.
        adapter.execute_sql(
            pg_client,
            f"INSERT INTO {table_ref} (pipeline_name, resource_name, checkpoint_value, status, created_at) "
            "VALUES (?, ?, ?, 'completed', ?)",
            "pipe",
            "res",
            "old",
            dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
        )
        adapter.execute_sql(pg_client, self._shape(adapter, pg_dataset, "cp_lifecycle", "update"), "pipe", "res")
        adapter.execute_sql(pg_client, self._shape(adapter, pg_dataset, "cp_lifecycle", "delete_old"))
        cursor = adapter.execute_query(pg_client, f"SELECT checkpoint_value, status FROM {table_ref}")
        assert cursor.fetchall() == [("v", "completed")]  # fresh row completed, backdated row purged

    def test_fetch_columns_existing_table(self, adapter, pg_client, pg_dataset):
        self._create(adapter, pg_client, pg_dataset, "cp_columns")
        columns = adapter.fetch_columns(pg_client, pg_dataset, "cp_columns")
        assert columns is not None
        assert [column.name for column in columns][:4] == [
            "pipeline_name",
            "resource_name",
            "run_id",
            "checkpoint_value",
        ]
        assert columns[0] == ColumnInfo(name="pipeline_name", data_type="text")
        assert ColumnInfo(name="page_number", data_type="bigint") in columns
        assert ColumnInfo(name="created_at", data_type="timestamp with time zone") in columns

    def test_fetch_columns_absent_returns_none(self, adapter, pg_client, pg_dataset):
        assert adapter.fetch_columns(pg_client, pg_dataset, "no_such_table") is None
        assert adapter.fetch_columns(pg_client, "no_such_dataset", "no_such_table") is None

    def test_table_exists_and_drop_if_exists(self, adapter, pg_client, pg_dataset):
        self._create(adapter, pg_client, pg_dataset, "cp_drop")
        assert adapter.table_exists(pg_client, pg_dataset, "cp_drop") is True
        adapter.drop_table_if_exists(pg_client, pg_dataset, "cp_drop")
        assert adapter.table_exists(pg_client, pg_dataset, "cp_drop") is False
        adapter.drop_table_if_exists(pg_client, pg_dataset, "cp_drop")  # idempotent

    def test_ensure_schema_then_create_table(self, adapter, pg_client, pg_dataset):
        extra = f"{pg_dataset}_extra"
        adapter.ensure_schema(pg_client, extra)
        adapter.ensure_schema(pg_client, extra)  # idempotent
        adapter.execute_sql(
            pg_client, f"CREATE TABLE IF NOT EXISTS {adapter.render_table_ref(extra, 'probe')} (x BIGINT)"
        )
        assert adapter.table_exists(pg_client, extra, "probe")

    def test_alter_add_column_if_not_exists(self, adapter, pg_client, pg_dataset):
        self._create(adapter, pg_client, pg_dataset, "cp_alter")
        alter = self._shape(adapter, pg_dataset, "cp_alter", "alter_add_column")
        adapter.execute_sql(pg_client, alter)  # column already exists; IF NOT EXISTS absorbs it
        columns = adapter.fetch_columns(pg_client, pg_dataset, "cp_alter")
        assert columns is not None
        assert sum(1 for column in columns if column.name == "run_id") == 1


@pytest.mark.integration
class TestCheckpointLifecyclePostgres:
    """create / save / resume / mark-completed / cleanup-old through dlt_ops.checkpoints."""

    def test_create_save_and_mark_completed(self, pg_pipeline):
        @dlt.resource(write_disposition="append", primary_key="id")
        @with_checkpoints(cursor_field="ts", frequency=2)
        def paginated_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            base = ts.start_value
            for page in range(1, 6):
                yield [
                    {"id": (page - 1) * 10 + i, "ts": base + dt.timedelta(hours=(page - 1) * 10 + i)}
                    for i in range(1, 11)
                ]

        pg_pipeline.run(paginated_resource())

        checkpoints = [
            cp for cp in list_checkpoints(pipeline=pg_pipeline) if cp["resource_name"] == "paginated_resource"
        ]
        pages = [cp["page_number"] for cp in checkpoints]
        assert 2 in pages and 4 in pages  # frequency=2 over 5 pages
        # Successful run exit marks every active checkpoint of the run completed.
        assert {cp["status"] for cp in checkpoints} == {"completed"}

    def test_save_then_resume_within_run(self, pg_pipeline):
        resumed = {}

        @dlt.resource(write_disposition="append")
        def resume_probe():
            with CheckpointManager("resume_pipe", "resume_res", frequency=1) as mgr:
                mgr.save_checkpoint("cursor-42", [{"id": 1}])
                # A second manager sees the active row: live write-then-read resume on pg.
                with CheckpointManager("resume_pipe", "resume_res", frequency=1) as reader:
                    resumed["value"] = reader.get_last_checkpoint()
            yield [{"id": 1}]

        pg_pipeline.run(resume_probe())
        assert resumed["value"] == "cursor-42"

    def test_cleanup_old_purges_backdated_completed(self, pg_pipeline, adapter):
        @dlt.resource(write_disposition="append")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def cleanup_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 2)}]

        pg_pipeline.run(cleanup_resource())  # creates the table + a completed row
        table_ref = adapter.render_table_ref(pg_pipeline.dataset_name, "_dlt_custom_checkpoints")
        with pg_pipeline.sql_client() as client:
            adapter.execute_sql(
                client,
                f"INSERT INTO {table_ref} (pipeline_name, resource_name, checkpoint_value, status, created_at) "
                "VALUES (?, ?, ?, 'completed', ?)",
                pg_pipeline.pipeline_name,
                "cleanup_resource",
                "stale",
                dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            )
        # Next successful run's exit triggers _cleanup_old (default 7 days).
        pg_pipeline.run(cleanup_resource())
        values = [cp["checkpoint_value"] for cp in list_checkpoints(pipeline=pg_pipeline)]
        assert "stale" not in values

    def test_cleanup_checkpoints_selective_then_full(self, pg_pipeline):
        @dlt.resource(write_disposition="append", name="keep_me")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def keep_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 2)}]

        @dlt.resource(write_disposition="append", name="delete_me")
        @with_checkpoints(cursor_field="ts", frequency=1)
        def delete_resource(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2024, 1, 1))):
            yield [{"id": 1, "ts": dt.datetime(2024, 1, 2)}]

        pg_pipeline.run([keep_resource(), delete_resource()])

        # Selective: checkpoints key on the function name, not the @dlt.resource name.
        cleanup_checkpoints(pipeline=pg_pipeline, resource_name="delete_resource")
        remaining = {cp["resource_name"] for cp in list_checkpoints(pipeline=pg_pipeline)}
        assert "keep_resource" in remaining
        assert "delete_resource" not in remaining

        cleanup_checkpoints(pipeline=pg_pipeline)
        assert list_checkpoints(pipeline=pg_pipeline) == []

    def test_hostile_pipeline_name_round_trips_as_data(self, pg_pipeline, adapter):
        resumed = {}
        dataset = pg_pipeline.dataset_name
        with pg_pipeline.sql_client() as client:
            adapter.execute_sql(
                client, f"CREATE TABLE IF NOT EXISTS {adapter.render_table_ref(dataset, 'decoy')} (x BIGINT)"
            )

        @dlt.resource(write_disposition="append")
        def probe_resource():
            # pipeline_name is user-config-controlled in OSS — exercise it directly.
            with CheckpointManager(HOSTILE, "probe_res", frequency=1) as mgr:
                mgr.save_checkpoint("cursor-1", [{"id": 1}])
                with CheckpointManager(HOSTILE, "probe_res", frequency=1) as reader:
                    resumed["value"] = reader.get_last_checkpoint()
            yield [{"id": 1}]

        pg_pipeline.run(probe_resource())

        assert resumed["value"] == "cursor-1"
        table_ref = adapter.render_table_ref(dataset, "_dlt_custom_checkpoints")
        with pg_pipeline.sql_client() as client:
            cursor = adapter.execute_query(
                client, f"SELECT pipeline_name FROM {table_ref} WHERE pipeline_name = ?", HOSTILE
            )
            assert [row[0] for row in cursor.fetchall()] == [HOSTILE]
            # The decoy table survived: the payload never reached the SQL text.
            assert adapter.table_exists(client, dataset, "decoy")


@pytest.mark.integration
class TestRunEquivalencePostgres:
    """pipeline.run-based DuckDB/Postgres equivalence.

    Stands in for a runner end-to-end test (the runner's modules are not
    imported here); the load path and the canonical read path are identical
    either way.
    """

    def test_same_rows_load_and_read_back_identically(self, pg_pipeline, adapter, tmp_path):
        rows = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}, {"id": 3, "val": HOSTILE}]

        duck_pipeline = dlt.pipeline(
            pipeline_name="dest_adapter_pg_equiv",
            destination=dlt.destinations.duckdb(str(tmp_path / "equiv.duckdb")),
            dataset_name="equiv_ds",
            pipelines_dir=str(tmp_path / "pipelines"),
        )
        duck_pipeline.run(rows, table_name="equiv_events")
        pg_pipeline.run(rows, table_name="equiv_events")

        def read_back(pipeline, dataset):
            dest_adapter = adapter_for_pipeline(pipeline)
            table_ref = dest_adapter.render_table_ref(dataset, "equiv_events")
            canonical = f"SELECT id, val FROM {table_ref} WHERE id >= ? ORDER BY id"
            with pipeline.sql_client() as client:
                cursor = dest_adapter.execute_query(client, canonical, 1)
                return [(row[0], row[1]) for row in cursor.fetchall()]

        expected = [(1, "a"), (2, "b"), (3, HOSTILE)]
        assert read_back(duck_pipeline, "equiv_ds") == expected
        assert read_back(pg_pipeline, pg_pipeline.dataset_name) == expected

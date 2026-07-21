"""Cross-system integration flows: a real source system feeding a real destination through the real CLI.

Three lanes, each a staged class driving ``uv run --no-sync dlt-ops --root <root> ...`` as a
subprocess against a project tree scaffolded to the README layout conventions (pipeline dir,
``source/`` subdir, module stem = config section, ``<X>_source`` function, explicit
``@dlt.source(name=...)``, ``[sources.<X>]`` section, ``schedule`` key, unique resource names,
pydantic ``columns=``):

1. filesystem -> postgres  (full tier): seeded JSONL read via dlt's built-in filesystem source,
   loaded to a live Postgres (conftest ``postgres_url``: POSTGRES_URL or throwaway docker;
   skips cleanly without either).
2. postgres  -> duckdb     (full tier): seeded Postgres table read via
   ``dlt.sources.sql_database.sql_table`` (the dev group's ``dlt[sql_database]``), loaded to a
   local ``.duckdb`` file.
3. duckdb    -> filesystem (core tier): seeded ``.duckdb`` file read via a plain duckdb
   connection, loaded to a local ``file://`` bucket — no DestinationAdapter, proving the loud
   core-tier degradation path end to end.

Methods within a class run in definition order against one shared project (the
``test_e2e_example.py`` staged-suite pattern); running a single step with ``-k`` is unsupported.
Seed locations reach the source modules through ``DLTX_IT_*`` env vars read at call time, so the
sandboxed validate import (Rule 15) never touches the source system.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

RUNS_TABLE = "_dlt_ops_runs"


def _run_cli(root: Path, *args: str, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Invoke the installed ``dlt-ops`` console script exactly as a user would."""
    full_env = {
        **os.environ,
        **env,
        "RUNTIME__DLTHUB_TELEMETRY": "false",
        "DLT_DATA_DIR": str(root / ".dlt-data"),
    }
    return subprocess.run(
        ["uv", "run", "--no-sync", "dlt-ops", "--root", str(root), *args],
        cwd=REPO_ROOT,
        env=full_env,
        capture_output=True,
        text=True,
        timeout=600,
    )


def _out(proc: subprocess.CompletedProcess[str]) -> str:
    return proc.stdout + proc.stderr


def _scaffold(root: Path, source: str, destination: str, dataset: str, source_code: str) -> None:
    """One-source project tree satisfying every README layout convention."""
    (root / ".dlt").mkdir(parents=True)
    (root / ".dlt" / "config.toml").write_text(
        dedent(
            f"""\
            [dlt_ops]
            default_destination = "{destination}"
            default_dataset = "{dataset}"

            [sources.{source}.dlt_ops]
            schedule = "@daily"
            """
        )
    )
    src_dir = root / source / "source"
    src_dir.mkdir(parents=True)
    (src_dir / f"{source}.py").write_text(source_code)


def _pg_execute(url: str, *statements: str) -> None:
    import psycopg2

    conn = psycopg2.connect(url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
    finally:
        conn.close()


def _pg_query(url: str, sql: str) -> list[tuple]:
    import psycopg2

    conn = psycopg2.connect(url)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def _duckdb_query(path: Path, sql: str) -> list[tuple]:
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


FS_PG_SOURCE = "file_events"
FS_PG_DATASET = "dltx_it_fs_pg"

FILE_EVENTS_ROWS = [
    {"id": 1, "name": "signup", "amount": 1.5},
    {"id": 2, "name": "login", "amount": 2.5},
    {"id": 3, "name": "purchase", "amount": 3.5},
    {"id": 4, "name": "logout", "amount": 4.5},
]

FILE_EVENTS_SOURCE = """\
import os

import dlt
import pydantic
from dlt.sources.filesystem import filesystem, read_jsonl


class Event(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    id: int
    name: str
    amount: float


@dlt.resource(name="events", columns=Event, write_disposition="replace")
def events():
    files = filesystem(bucket_url=os.environ["DLTX_IT_EVENTS_DIR"], file_glob="events.jsonl")
    yield from (files | read_jsonl())


@dlt.source(name="file_events")
def file_events_source():
    return events
"""

PG_DUCK_SOURCE = "pg_customers"
PG_DUCK_DATASET = "dltx_it_pg_duck"

PG_CUSTOMERS_ROWS = [
    (1, "Ada", "ada@example.com"),
    (2, "Alan", "alan@example.com"),
    (3, "Grace", "grace@example.com"),
]

PG_CUSTOMERS_SOURCE = """\
import os

import dlt
import pydantic
from dlt.sources.sql_database import sql_table


class Customer(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    id: int
    name: str
    email: str


@dlt.resource(name="customers", columns=Customer, write_disposition="replace")
def customers():
    url = os.environ["DLTX_IT_SOURCE_DB_URL"]
    yield from sql_table(credentials=url, table="it_customers", schema="public")


@dlt.source(name="pg_customers")
def pg_customers_source():
    return customers
"""

DUCK_FS_SOURCE = "duck_metrics"
DUCK_FS_DATASET = "dltx_it_duck_fs"

DUCK_METRICS_ROWS = [
    (1, "cpu", 0.42),
    (2, "mem", 0.58),
    (3, "disk", 0.13),
    (4, "net", 0.77),
    (5, "gpu", 0.91),
]

DUCK_METRICS_SOURCE = """\
import os

import dlt
import duckdb
import pydantic


class Metric(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    id: int
    name: str
    value: float


@dlt.resource(name="metrics", columns=Metric, write_disposition="append")
def metrics():
    con = duckdb.connect(os.environ["DLTX_IT_METRICS_DB"], read_only=True)
    try:
        for row_id, name, value in con.execute("SELECT id, name, value FROM metrics ORDER BY id").fetchall():
            yield {"id": row_id, "name": name, "value": value}
    finally:
        con.close()


@dlt.source(name="duck_metrics")
def duck_metrics_source():
    return metrics
"""


@pytest.fixture(scope="class")
def fs_pg_lane(tmp_path_factory, postgres_url):
    base = tmp_path_factory.mktemp("it-fs-pg")
    data_dir = base / "source-data"
    data_dir.mkdir()
    (data_dir / "events.jsonl").write_text("\n".join(json.dumps(row) for row in FILE_EVENTS_ROWS) + "\n")
    root = base / "project"
    _scaffold(root, FS_PG_SOURCE, "postgres", FS_PG_DATASET, FILE_EVENTS_SOURCE)
    # POSTGRES_URL may point at a persistent server: drop the lane's schema so counts are exact.
    _pg_execute(postgres_url, f'DROP SCHEMA IF EXISTS "{FS_PG_DATASET}" CASCADE')
    return SimpleNamespace(
        root=root,
        pg_url=postgres_url,
        env={
            "DESTINATION__POSTGRES__CREDENTIALS": postgres_url,
            "DLTX_IT_EVENTS_DIR": str(data_dir),
        },
    )


@pytest.mark.integration
class TestFilesystemToPostgres:
    """Full tier: JSONL on local disk -> dlt's filesystem source -> live Postgres."""

    def test_validate_exits_zero(self, fs_pg_lane):
        proc = _run_cli(fs_pg_lane.root, "pipeline", "validate", env=fs_pg_lane.env)
        assert proc.returncode == 0, _out(proc)
        assert "validated successfully" in proc.stdout

    def test_run_lands_seeded_rows_in_postgres(self, fs_pg_lane):
        proc = _run_cli(fs_pg_lane.root, "pipeline", "run", "-s", FS_PG_SOURCE, "-y", env=fs_pg_lane.env)
        assert proc.returncode == 0, _out(proc)
        rows = _pg_query(fs_pg_lane.pg_url, f'SELECT count(*) FROM "{FS_PG_DATASET}".events')
        assert rows[0][0] == len(FILE_EVENTS_ROWS)

    def test_status_shows_the_run(self, fs_pg_lane):
        proc = _run_cli(fs_pg_lane.root, "pipeline", "status", env=fs_pg_lane.env)
        assert proc.returncode == 0, _out(proc)
        assert f"Source: {FS_PG_SOURCE}" in proc.stdout
        assert "completed" in proc.stdout
        assert "no runs recorded" not in proc.stdout
        assert "ledger unsupported" not in proc.stdout
        assert "ledger unreadable" not in proc.stdout

    def test_ledger_row_exists(self, fs_pg_lane):
        rows = _pg_query(
            fs_pg_lane.pg_url,
            f'SELECT pipeline_name, status, records_loaded FROM "{FS_PG_DATASET}".{RUNS_TABLE}',
        )
        assert rows == [(f"{FS_PG_SOURCE}_pipeline", "completed", len(FILE_EVENTS_ROWS))]


@pytest.fixture(scope="class")
def pg_duck_lane(tmp_path_factory, postgres_url):
    base = tmp_path_factory.mktemp("it-pg-duck")
    values = ", ".join(f"({row_id}, '{name}', '{email}')" for row_id, name, email in PG_CUSTOMERS_ROWS)
    _pg_execute(
        postgres_url,
        "DROP TABLE IF EXISTS public.it_customers",
        "CREATE TABLE public.it_customers (id int PRIMARY KEY, name text, email text)",
        f"INSERT INTO public.it_customers (id, name, email) VALUES {values}",
    )
    root = base / "project"
    _scaffold(root, PG_DUCK_SOURCE, "duckdb", PG_DUCK_DATASET, PG_CUSTOMERS_SOURCE)
    warehouse = base / "warehouse.duckdb"
    return SimpleNamespace(
        root=root,
        warehouse=warehouse,
        env={
            "DESTINATION__DUCKDB__CREDENTIALS": str(warehouse),
            "DLTX_IT_SOURCE_DB_URL": postgres_url,
        },
    )


@pytest.mark.integration
@pytest.mark.skipif(
    importlib.util.find_spec("sqlalchemy") is None,
    reason="sqlalchemy not installed — sync the dev group (dlt[sql_database]) to run the SQL-source lane",
)
class TestPostgresToDuckDB:
    """Full tier: live Postgres table -> dlt.sources.sql_database.sql_table -> local DuckDB file."""

    def test_validate_exits_zero(self, pg_duck_lane):
        proc = _run_cli(pg_duck_lane.root, "pipeline", "validate", env=pg_duck_lane.env)
        assert proc.returncode == 0, _out(proc)
        assert "validated successfully" in proc.stdout

    def test_run_lands_seeded_rows_in_duckdb(self, pg_duck_lane):
        proc = _run_cli(pg_duck_lane.root, "pipeline", "run", "-s", PG_DUCK_SOURCE, "-y", env=pg_duck_lane.env)
        assert proc.returncode == 0, _out(proc)
        rows = _duckdb_query(pg_duck_lane.warehouse, f'SELECT count(*) FROM "{PG_DUCK_DATASET}".customers')
        assert rows[0][0] == len(PG_CUSTOMERS_ROWS)

    def test_status_shows_the_run(self, pg_duck_lane):
        proc = _run_cli(pg_duck_lane.root, "pipeline", "status", env=pg_duck_lane.env)
        assert proc.returncode == 0, _out(proc)
        assert f"Source: {PG_DUCK_SOURCE}" in proc.stdout
        assert "completed" in proc.stdout
        assert "no runs recorded" not in proc.stdout
        assert "ledger unsupported" not in proc.stdout
        assert "ledger unreadable" not in proc.stdout

    def test_ledger_row_exists(self, pg_duck_lane):
        rows = _duckdb_query(
            pg_duck_lane.warehouse,
            f'SELECT pipeline_name, status, records_loaded FROM "{PG_DUCK_DATASET}".{RUNS_TABLE}',
        )
        assert rows == [(f"{PG_DUCK_SOURCE}_pipeline", "completed", len(PG_CUSTOMERS_ROWS))]


@pytest.fixture(scope="class")
def duck_fs_lane(tmp_path_factory):
    base = tmp_path_factory.mktemp("it-duck-fs")
    source_db = base / "metrics.duckdb"
    con = duckdb.connect(str(source_db))
    try:
        con.execute("CREATE TABLE metrics (id INTEGER, name VARCHAR, value DOUBLE)")
        con.executemany("INSERT INTO metrics VALUES (?, ?, ?)", DUCK_METRICS_ROWS)
    finally:
        con.close()
    bucket = base / "bucket"
    bucket.mkdir()
    root = base / "project"
    _scaffold(root, DUCK_FS_SOURCE, "filesystem", DUCK_FS_DATASET, DUCK_METRICS_SOURCE)
    return SimpleNamespace(
        root=root,
        bucket=bucket,
        env={
            "DESTINATION__FILESYSTEM__BUCKET_URL": f"file://{bucket}",
            "DLTX_IT_METRICS_DB": str(source_db),
            # plain-text jsonl so the row-count assert below can parse the landed files
            "NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION": "true",
        },
    )


@pytest.mark.integration
class TestDuckDBToFilesystem:
    """Core tier: .duckdb file -> plain duckdb connection -> local file:// bucket, no adapter."""

    def test_validate_exits_zero_on_the_core_tier(self, duck_fs_lane):
        """Core mode is reported, not blocked: the warning renders and the gate stays open."""
        proc = _run_cli(duck_fs_lane.root, "pipeline", "validate", env=duck_fs_lane.env)
        assert proc.returncode == 0, _out(proc)
        assert "core mode" in proc.stdout
        assert "1 warning(s)" in proc.stdout

    def test_run_lands_seeded_rows_as_bucket_files(self, duck_fs_lane):
        proc = _run_cli(duck_fs_lane.root, "pipeline", "run", "-s", DUCK_FS_SOURCE, "-y", env=duck_fs_lane.env)
        assert proc.returncode == 0, _out(proc)
        assert "core (no adapter" in _out(proc)
        data_files = list((duck_fs_lane.bucket / DUCK_FS_DATASET / "metrics").glob("*"))
        assert data_files, "resource rows must land as files under <bucket>/<dataset>/metrics/"
        landed = [json.loads(line) for f in data_files for line in f.read_text().splitlines() if line.strip()]
        assert len(landed) == len(DUCK_METRICS_ROWS)
        assert {row["name"] for row in landed} == {name for _, name, _ in DUCK_METRICS_ROWS}

    def test_status_reports_ledger_unsupported(self, duck_fs_lane):
        proc = _run_cli(duck_fs_lane.root, "pipeline", "status", env=duck_fs_lane.env)
        assert proc.returncode == 0, _out(proc)
        assert f"Source: {DUCK_FS_SOURCE}" in proc.stdout
        assert "! ledger unsupported: destination 'filesystem' has no DestinationAdapter (core mode)" in proc.stdout

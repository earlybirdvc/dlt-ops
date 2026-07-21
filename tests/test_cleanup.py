"""Tests for pipeline cleanup — dlt's drop for the remote half, dlt-ops for the rest.

Unit layers pin the contract at two seams: what cleanup asks *dlt* to drop
(``pipeline_drop`` stubbed at its import site) and what cleanup itself writes
through the ``DestinationAdapter`` boundary (a FakeAdapter). Integration layers
run real end-to-end cleanup against DuckDB (credential-free) and, when
POSTGRES_URL is set, against Postgres — those are the ones that prove the drop
actually happened.
"""

import importlib
import json
import logging
import re
import shutil
import uuid
from contextlib import contextmanager
from os import environ
from pathlib import Path
from types import SimpleNamespace

import dlt
import pytest

from dlt_ops import SourceInfo
from dlt_ops.destinations import UnregisteredDestinationError
from dlt_ops.discovery import cleanup as cleanup_module
from dlt_ops.discovery.cleanup import (
    DLT_SYSTEM_TABLES,
    _clean_local_state_selective,
    clean_pipeline,
    get_cleanup_plan,
)

REPO_ROOT = Path(__file__).parent.parent


def make_source(
    name="test_source",
    resources=("organizations", "lists"),
    source_fn=None,
    path=None,
    module_path=None,
):
    """Real SourceInfo record; Phase-2-enriched when source_fn is given."""
    return SourceInfo(
        name=name,
        pipeline_name=name,
        path=path or Path("/nonexistent") / name,
        function_name=f"{name}_source",
        resources=tuple(resources),
        module_stem=name,
        source_fn=source_fn,
        module_path=module_path,
    )


class _FakeCursor:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class FakeAdapter:
    """In-memory DestinationAdapter double recording every boundary call."""

    name = "fake"
    placeholder_style = "?"
    supports_if_exists = True
    supports_create_schema_if_not_exists = True
    timestamp_now_sql = "CURRENT_TIMESTAMP"
    _identifier_re = re.compile(r"[A-Za-z0-9_]+")

    def __init__(self, existing_tables=()):
        self.existing_tables = set(existing_tables)
        self.executed: list[tuple[str, tuple]] = []
        self.queried: list[tuple[str, tuple]] = []
        self.dropped: list[str] = []

    def render_identifier(self, ident):
        if not isinstance(ident, str) or not self._identifier_re.fullmatch(ident):
            raise ValueError(f"invalid fake identifier {ident!r}")
        return f'"{ident}"'

    def render_table_ref(self, dataset, table):
        return f"{self.render_identifier(dataset)}.{self.render_identifier(table)}"

    def timestamp_sub_days_sql(self, days):
        return f"CURRENT_TIMESTAMP - INTERVAL '{days} days'"

    def execute_sql(self, client, sql, *params):
        self.executed.append((sql, params))

    def execute_query(self, client, sql, *params):
        self.queried.append((sql, params))
        return _FakeCursor([])

    def table_exists(self, client, dataset, table):
        return table in self.existing_tables

    def drop_table_if_exists(self, client, dataset, table):
        self.render_table_ref(dataset, table)  # same grammar gate as real adapters
        self.dropped.append(table)

    def ensure_schema(self, client, dataset):
        pass

    def fetch_columns(self, client, dataset, table):
        return None


@pytest.fixture
def dlt_home(tmp_path, monkeypatch):
    """Point dlt's data dir (and thus cleanup's working-dir resolution) at tmp."""
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-home"))
    return tmp_path / "dlt-home" / "pipelines"


@pytest.fixture
def local_pipeline_dir(dlt_home):
    """A local pipeline working directory with state + schema."""
    pipeline_dir = dlt_home / "test_source_pipeline"
    pipeline_dir.mkdir(parents=True)

    state = {
        "_state_version": 5,
        "_state_engine_version": 4,
        "_version_hash": "abc123",
        "pipeline_name": "test_source_pipeline",
        "dataset_name": "test_dataset",
        "default_schema_name": "test_source",
        "schema_names": ["test_source"],
        "sources": {
            "test_source": {
                "resources": {
                    "organizations": {"incremental": {"updated_at": {"last_value": "2025-01-01T00:00:00Z"}}},
                    "lists": {"incremental": {"updated_at": {"last_value": "2025-01-02T00:00:00Z"}}},
                }
            }
        },
        "_local": {"first_run": False},
    }
    (pipeline_dir / "state.json").write_text(json.dumps(state))

    schemas_dir = pipeline_dir / "schemas"
    schemas_dir.mkdir()
    schema = {
        "tables": {
            "_dlt_version": {"resource": "_dlt_version"},
            "_dlt_loads": {"resource": "_dlt_loads"},
            "_dlt_pipeline_state": {"resource": "_dlt_pipeline_state"},
            "test_organizations": {"resource": "organizations"},
            "test_lists": {"resource": "lists"},
        }
    }
    (schemas_dir / "test_source.schema.json").write_text(json.dumps(schema))

    return pipeline_dir


@pytest.fixture
def fake_boundary(monkeypatch):
    """Route cleanup's destination boundary to a FakeAdapter; yields the fake."""
    fake = FakeAdapter()

    @contextmanager
    def _boundary(pipeline_name, destination, dataset_name):
        yield fake, None

    monkeypatch.setattr(cleanup_module, "open_destination_boundary", _boundary)
    return fake


@pytest.fixture
def no_boundary(monkeypatch):
    """Fail the test if cleanup tries to open a destination boundary."""

    @contextmanager
    def _boundary(pipeline_name, destination, dataset_name):
        raise AssertionError("destination boundary must not be opened")
        yield  # pragma: no cover

    monkeypatch.setattr(cleanup_module, "open_destination_boundary", _boundary)


class _FakeSchema:
    """dlt Schema stand-in exposing exactly what cleanup derives its SQL from.

    ``naming`` defaults to identity; a test can pass a folding one to prove the
    bookkeeping SQL follows dlt's naming convention instead of a hardcoded
    snake_case literal.
    """

    def __init__(self, name, naming=None, prefix="_dlt_"):
        self.name = name
        self.state_table_name = f"{prefix}pipeline_state"
        self.loads_table_name = f"{prefix}loads"
        self.version_table_name = f"{prefix}version"
        self.naming = naming or SimpleNamespace(normalize_path=lambda column: column)


class _FakeDrop:
    """Stand-in for a prepared ``pipeline_drop``: inspectable, and callable to execute."""

    def __init__(self, recorder, **kwargs):
        self._recorder = recorder
        self.info = dict(recorder.info)
        recorder.constructed.append(kwargs)

    @property
    def is_empty(self):
        return not (self.info["tables"] or self.info["resource_states"])

    def __call__(self):
        self._recorder.executed += 1


@pytest.fixture
def dlt_drop(monkeypatch):
    """Stub dlt's drop at the seam cleanup resolves it through.

    ``_prepare_drop`` imports ``pipeline_drop`` from ``dlt.pipeline.helpers`` at
    call time, so patching the helper module intercepts the real lookup. The
    synced pipeline is stubbed alongside it because building one means talking
    to a destination.
    """
    recorder = SimpleNamespace(
        constructed=[],
        executed=0,
        default_schema_name="test_source",
        schema=None,  # set to a _FakeSchema to override the default
        info={
            "tables": ["test_organizations"],
            "tables_with_data": ["test_organizations"],
            "resource_states": ["organizations"],
            "warnings": [],
            "schema_name": "test_source",
        },
    )

    @contextmanager
    def _synced(pipeline_name, destination, dataset_name):
        name = recorder.default_schema_name
        schema = recorder.schema or (_FakeSchema(name) if name else None)
        yield SimpleNamespace(
            pipeline_name=pipeline_name,
            dataset_name=dataset_name,
            default_schema_name=name,
            schemas={schema.name: schema} if schema else {},
        )

    def _factory(pipeline, **kwargs):
        return _FakeDrop(recorder, pipeline=pipeline, **kwargs)

    # `dlt.pipeline` is a function shadowing the module of the same name, so the
    # helper module has to be resolved through importlib rather than attribute
    # lookup — which is exactly how `_prepare_drop`'s local import resolves it.
    helpers = importlib.import_module("dlt.pipeline.helpers")
    monkeypatch.setattr(cleanup_module, "_synced_drop_pipeline", _synced)
    monkeypatch.setattr(helpers, "pipeline_drop", _factory)
    return recorder


# --- What cleanup asks dlt to drop ---


class TestRemoteDropDelegation:
    def test_full_clean_asks_dlt_for_drop_all(self, fake_boundary, dlt_drop, dlt_home):
        clean_pipeline(
            source=make_source(),
            resources=None,
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        [call] = dlt_drop.constructed
        assert call["drop_all"] is True
        assert call["resources"] == []
        assert call["schema_name"] == "test_source"
        assert dlt_drop.executed == 1

    def test_selective_clean_passes_exact_resource_names(self, fake_boundary, dlt_drop, dlt_home):
        clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        [call] = dlt_drop.constructed
        assert call["drop_all"] is False
        assert call["resources"] == ["organizations"]
        assert dlt_drop.executed == 1

    def test_result_reports_dropped_tables_and_reset_states(self, fake_boundary, dlt_drop, dlt_home):
        result = clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert "table: test_organizations" in result["remote"]
        assert "state: reset organizations" in result["remote"]

    def test_schema_only_table_is_not_reported_as_dropped(self, fake_boundary, dlt_drop, dlt_home):
        """A table dlt removes from the schema but that never materialized is not a drop."""
        dlt_drop.info = {**dlt_drop.info, "tables": ["test_organizations", "never_loaded"]}

        result = clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert "table: test_organizations" in result["remote"]
        assert not any("never_loaded" in item for item in result["remote"])

    def test_empty_drop_is_never_executed(self, fake_boundary, dlt_drop, dlt_home):
        """Nothing selected means nothing runs — no empty load package against the destination."""
        dlt_drop.info = {**dlt_drop.info, "tables": [], "tables_with_data": [], "resource_states": []}

        result = clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert dlt_drop.executed == 0
        assert not any(item.startswith("table:") for item in result["remote"])

    def test_absent_remote_state_skips_the_drop_but_still_cleans_dlt_ops_rows(self, fake_boundary, dlt_drop, dlt_home):
        """A destination dlt knows nothing about is a no-op for dlt, not an error."""
        dlt_drop.default_schema_name = None
        fake_boundary.existing_tables = {"_dlt_custom_checkpoints"}

        result = clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert dlt_drop.constructed == []
        assert dlt_drop.executed == 0
        assert "checkpoint: organizations" in result["remote"]

    def test_drop_warnings_are_surfaced(self, fake_boundary, dlt_drop, dlt_home, caplog):
        dlt_drop.info = {**dlt_drop.info, "warnings": ["resource matched no tables"]}

        with caplog.at_level(logging.WARNING, logger="dlt_ops.discovery.cleanup"):
            clean_pipeline(
                source=make_source(),
                resources=["organizations"],
                local=False,
                remote=True,
                dataset_name="test_dataset",
                destination="fake",
            )

        assert any("resource matched no tables" in record.message for record in caplog.records)

    def test_drop_runs_on_a_throwaway_dir_so_remote_only_keeps_local_state(
        self, fake_boundary, dlt_drop, local_pipeline_dir
    ):
        """--remote-only must not rewrite local state, and dlt's drop is a pipeline run."""
        clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        state = json.loads((local_pipeline_dir / "state.json").read_text())
        assert sorted(state["sources"]["test_source"]["resources"]) == ["lists", "organizations"]
        assert (local_pipeline_dir / "schemas" / "test_source.schema.json").exists()


# --- Local state modification ---


class TestLocalStateModification:
    def test_selective_state_removal(self, local_pipeline_dir):
        cleaned = _clean_local_state_selective(local_pipeline_dir, "test_source", ["organizations"])

        state = json.loads((local_pipeline_dir / "state.json").read_text())
        assert "organizations" not in state["sources"]["test_source"]["resources"]
        assert "lists" in state["sources"]["test_source"]["resources"]
        assert state["_state_version"] == 6  # bumped from 5

        # Schema file deleted (dlt re-derives on next run; surgical edits break its hash check)
        assert not (local_pipeline_dir / "schemas" / "test_source.schema.json").exists()
        assert len(cleaned) == 2  # state + schema

    def test_selective_removes_only_target(self, local_pipeline_dir):
        _clean_local_state_selective(local_pipeline_dir, "test_source", ["lists"])

        state = json.loads((local_pipeline_dir / "state.json").read_text())
        assert "organizations" in state["sources"]["test_source"]["resources"]
        assert "lists" not in state["sources"]["test_source"]["resources"]

    def test_missing_state_file(self, tmp_path):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()
        assert _clean_local_state_selective(empty_dir, "test_source", ["organizations"]) == []

    def test_unknown_state_engine_version_refuses(self, local_pipeline_dir):
        """The one dlt-owned file cleanup still hand-edits checks the layout dlt stamped.

        A silent pass here would report a reset that never happened, so the
        refusal is loud and names the way out.
        """
        state_path = local_pipeline_dir / "state.json"
        state = json.loads(state_path.read_text())
        state["_state_engine_version"] = 99
        state_path.write_text(json.dumps(state))

        with pytest.raises(RuntimeError, match="state engine version"):
            _clean_local_state_selective(local_pipeline_dir, "test_source", ["organizations"])

        # Nothing was half-done: the resource is still there and so is the schema.
        after = json.loads(state_path.read_text())
        assert "organizations" in after["sources"]["test_source"]["resources"]
        assert (local_pipeline_dir / "schemas" / "test_source.schema.json").exists()

    def test_real_dlt_state_carries_the_engine_version_we_check(self, tmp_path):
        """The guard reads a key dlt actually writes — not one invented here."""
        pipeline = dlt.pipeline(
            pipeline_name="engine_probe_pipeline",
            destination=dlt.destinations.duckdb(str(tmp_path / "probe.duckdb")),
            dataset_name="probe_ds",
            pipelines_dir=str(tmp_path / "home"),
        )
        pipeline.run([{"id": 1}], table_name="rows")

        state = json.loads((tmp_path / "home" / "engine_probe_pipeline" / "state.json").read_text())
        assert state["_state_engine_version"] == cleanup_module._STATE_ENGINE_VERSION


# --- Full cleanup ---


class TestFullCleanup:
    def test_full_local_and_remote(self, fake_boundary, dlt_drop, local_pipeline_dir):
        fake_boundary.existing_tables = set(DLT_SYSTEM_TABLES)

        result = clean_pipeline(
            source=make_source(),
            resources=None,
            local=True,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        # Local dir removed
        assert not local_pipeline_dir.exists()
        assert result["local"] == [str(local_pipeline_dir)]

        # dlt dropped the data tables; cleanup never issues its own DROP
        assert dlt_drop.executed == 1
        assert fake_boundary.dropped == []

        # System tables: DELETE rows, never DROP
        deletes = [(sql, params) for sql, params in fake_boundary.executed if sql.startswith("DELETE")]
        delete_text = " | ".join(sql for sql, _ in deletes)
        assert '"test_dataset"."_dlt_pipeline_state"' in delete_text
        assert '"test_dataset"."_dlt_version"' in delete_text
        assert '"test_dataset"."_dlt_loads"' in delete_text
        assert ("test_source_pipeline",) in [params for _, params in deletes]  # pipeline-scoped filter
        assert ("test_source",) in [params for _, params in deletes]  # schema-scoped filter
        assert any("state: _dlt_pipeline_state (rows deleted)" == item for item in result["remote"])

    def test_full_deletes_checkpoint_rows_when_table_exists(self, fake_boundary, dlt_drop, local_pipeline_dir):
        fake_boundary.existing_tables = {*DLT_SYSTEM_TABLES, "_dlt_custom_checkpoints"}

        result = clean_pipeline(
            source=make_source(),
            resources=None,
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        checkpoint_deletes = [
            (sql, params) for sql, params in fake_boundary.executed if "_dlt_custom_checkpoints" in sql
        ]
        assert checkpoint_deletes == [
            (
                'DELETE FROM "test_dataset"."_dlt_custom_checkpoints" WHERE "pipeline_name" = ?',
                ("test_source_pipeline",),
            )
        ]
        assert "state: _dlt_custom_checkpoints (rows deleted)" in result["remote"]

    def test_absent_system_tables_are_skipped(self, fake_boundary, dlt_drop, dlt_home):
        """Missing tables are detected via table_exists, not error-string matching."""
        fake_boundary.existing_tables = set()  # nothing exists remotely

        result = clean_pipeline(
            source=make_source(source_fn=None, resources=("organizations",)),
            resources=None,
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert [item for item in result["remote"] if item.startswith("state: _dlt")] == []
        assert all(not sql.startswith("DELETE") for sql, _ in fake_boundary.executed)

    def test_system_table_sql_follows_dlts_naming_convention(self, fake_boundary, dlt_drop, dlt_home):
        """dlt normalizes its own identifiers before writing them, so cleanup asks
        the schema for them instead of hardcoding snake_case."""
        dlt_drop.schema = _FakeSchema(
            "test_source",
            naming=SimpleNamespace(normalize_path=str.upper),
            prefix="XDLT_",
        )
        fake_boundary.existing_tables = {"XDLT_pipeline_state", "XDLT_loads", "XDLT_version"}

        clean_pipeline(
            source=make_source(),
            resources=None,
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        deletes = [sql for sql, _ in fake_boundary.executed if sql.startswith("DELETE")]
        assert any('"XDLT_pipeline_state" WHERE "PIPELINE_NAME"' in sql for sql in deletes)
        assert any('"XDLT_loads" WHERE "SCHEMA_NAME"' in sql for sql in deletes)
        assert any('"XDLT_version" WHERE "SCHEMA_NAME"' in sql for sql in deletes)
        # No hardcoded snake_case leaked through alongside the derived names.
        assert not any("_dlt_pipeline_state" in sql for sql in deletes)

    def test_local_only_never_opens_destination(self, no_boundary, local_pipeline_dir):
        result = clean_pipeline(
            source=make_source(),
            resources=None,
            local=True,
            remote=False,
            dataset_name=None,
        )
        assert not local_pipeline_dir.exists()
        assert len(result["local"]) == 1
        assert result["remote"] == []

    def test_missing_local_dir_graceful(self, dlt_home):
        result = clean_pipeline(
            source=make_source(),
            resources=None,
            local=True,
            remote=False,
            dataset_name=None,
        )
        assert result["local"] == []

    def test_remote_requires_dataset(self, dlt_home):
        with pytest.raises(ValueError, match="dataset_name is required"):
            clean_pipeline(make_source(), None, local=False, remote=True, dataset_name=None, destination="duckdb")

    def test_remote_requires_destination(self, dlt_home):
        with pytest.raises(ValueError, match="destination is required"):
            clean_pipeline(make_source(), None, local=False, remote=True, dataset_name="ds")


# --- Selective cleanup ---


class TestSelectiveCleanup:
    def test_selective_deletes_checkpoints_per_resource(self, fake_boundary, dlt_drop, local_pipeline_dir):
        fake_boundary.existing_tables = {"_dlt_custom_checkpoints"}

        result = clean_pipeline(
            source=make_source(),
            resources=["organizations", "lists"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        checkpoint_deletes = [params for sql, params in fake_boundary.executed if "_dlt_custom_checkpoints" in sql]
        assert checkpoint_deletes == [
            ("test_source_pipeline", "organizations"),
            ("test_source_pipeline", "lists"),
        ]
        assert "checkpoint: organizations" in result["remote"]
        assert "checkpoint: lists" in result["remote"]

    def test_selective_leaves_dlt_system_tables_alone(self, fake_boundary, dlt_drop, local_pipeline_dir):
        """Surviving resources still own the dataset's load history."""
        fake_boundary.existing_tables = {*DLT_SYSTEM_TABLES, "_dlt_custom_checkpoints"}

        clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        touched = " | ".join(sql for sql, _ in fake_boundary.executed)
        for table in DLT_SYSTEM_TABLES:
            assert table not in touched

    def test_selective_local_updates_state_keeps_dir(self, no_boundary, local_pipeline_dir):
        result = clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=True,
            remote=False,
            dataset_name=None,
        )

        assert local_pipeline_dir.exists()
        assert len(result["local"]) > 0
        state = json.loads((local_pipeline_dir / "state.json").read_text())
        assert "organizations" not in state["sources"]["test_source"]["resources"]
        assert "lists" in state["sources"]["test_source"]["resources"]


# --- Validation ---


class TestValidation:
    def test_invalid_resource_name_raises(self, dlt_home):
        with pytest.raises(ValueError, match="Unknown resources"):
            clean_pipeline(
                source=make_source(),
                resources=["nonexistent"],
                local=False,
                remote=True,
                dataset_name="test_dataset",
                destination="fake",
            )

    def test_regex_selector_is_refused(self, dlt_home):
        """dlt reads `re:` as a pattern; clean promises exact names, so it refuses."""
        source = make_source(resources=("organizations", "re:.*"))
        with pytest.raises(ValueError, match="exact names, not patterns"):
            clean_pipeline(
                source=source,
                resources=["re:.*"],
                local=False,
                remote=True,
                dataset_name="test_dataset",
                destination="fake",
            )

    def test_validation_runs_before_anything_is_touched(self, no_boundary, local_pipeline_dir):
        with pytest.raises(ValueError):
            clean_pipeline(
                source=make_source(),
                resources=["nonexistent"],
                local=True,
                remote=True,
                dataset_name="test_dataset",
                destination="fake",
            )
        assert local_pipeline_dir.exists()

    def test_unknown_local_state_engine_refuses_before_the_remote_drop(
        self, fake_boundary, dlt_drop, local_pipeline_dir
    ):
        """A refusal that fired after the drop would leave a half-cleaned pipeline."""
        state_path = local_pipeline_dir / "state.json"
        state = json.loads(state_path.read_text())
        state["_state_engine_version"] = 99
        state_path.write_text(json.dumps(state))

        with pytest.raises(RuntimeError, match="state engine version"):
            clean_pipeline(
                source=make_source(),
                resources=["organizations"],
                local=True,
                remote=True,
                dataset_name="test_dataset",
                destination="fake",
            )

        assert dlt_drop.constructed == []
        assert dlt_drop.executed == 0
        assert fake_boundary.executed == []

    def test_full_clean_ignores_the_state_engine_version(self, fake_boundary, dlt_drop, local_pipeline_dir):
        """A full clean deletes the working dir outright, so it never reads the layout."""
        state_path = local_pipeline_dir / "state.json"
        state = json.loads(state_path.read_text())
        state["_state_engine_version"] = 99
        state_path.write_text(json.dumps(state))

        result = clean_pipeline(
            source=make_source(),
            resources=None,
            local=True,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert not local_pipeline_dir.exists()
        assert result["local"] == [str(local_pipeline_dir)]


# --- Cleanup plan ---


class TestCleanupPlan:
    def test_full_plan(self, fake_boundary, dlt_drop, local_pipeline_dir):
        plan = get_cleanup_plan(
            source=make_source(),
            resources=None,
            local=True,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert plan["pipeline_name"] == "test_source_pipeline"
        assert plan["schema_name"] == "test_source"
        assert plan["is_full"] is True
        assert plan["local_exists"] is True
        assert plan["data_tables"] == ["test_organizations"]
        assert plan["resource_states"] == ["organizations"]
        assert plan["system_tables"] == list(DLT_SYSTEM_TABLES)

    def test_selective_plan(self, fake_boundary, dlt_drop, local_pipeline_dir):
        plan = get_cleanup_plan(
            source=make_source(),
            resources=["organizations"],
            local=True,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert plan["is_full"] is False
        assert plan["target_resources"] == ["organizations"]
        assert plan["data_tables"] == ["test_organizations"]
        assert plan["system_tables"] == []  # No system tables for selective

    def test_plan_never_executes_the_drop(self, fake_boundary, dlt_drop, local_pipeline_dir):
        """The dry run is a constructed pipeline_drop that is read, never called."""
        get_cleanup_plan(
            source=make_source(),
            resources=None,
            local=True,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert len(dlt_drop.constructed) == 1
        assert dlt_drop.executed == 0
        assert fake_boundary.executed == []  # and no dlt-ops SQL either

    def test_plan_lists_the_tables_dlt_would_really_drop(self, fake_boundary, dlt_drop, dlt_home):
        """Nested child tables come from dlt's schema, not from a naming guess."""
        dlt_drop.info = {
            **dlt_drop.info,
            "tables": ["test_organizations__tags", "test_organizations"],
            "tables_with_data": ["test_organizations__tags", "test_organizations"],
        }

        plan = get_cleanup_plan(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert plan["data_tables"] == ["test_organizations__tags", "test_organizations"]

    def test_plan_degrades_when_destination_unreachable(self, fake_boundary, monkeypatch, dlt_home, caplog):
        """A boundary failure downgrades the plan to its local half plus a warning."""

        @contextmanager
        def _broken(pipeline_name, destination, dataset_name):
            raise RuntimeError("no credentials")
            yield  # pragma: no cover

        monkeypatch.setattr(cleanup_module, "_synced_drop_pipeline", _broken)
        source = make_source(resources=("organizations",))

        with caplog.at_level(logging.WARNING, logger="dlt_ops.discovery.cleanup"):
            plan = get_cleanup_plan(source, None, local=False, remote=True, dataset_name="ds", destination="fake")

        assert plan["data_tables"] == []
        assert any("no credentials" in warning for warning in plan["warnings"])
        assert any("Failed to open destination for cleanup plan" in record.message for record in caplog.records)

    def test_plan_degrades_when_the_boundary_itself_fails(self, monkeypatch, dlt_home):
        """A credentials/network failure is transient — warn, don't kill the dry run."""

        @contextmanager
        def _broken(pipeline_name, destination, dataset_name):
            raise RuntimeError("connection refused")
            yield  # pragma: no cover

        monkeypatch.setattr(cleanup_module, "open_destination_boundary", _broken)

        plan = get_cleanup_plan(
            source=make_source(), resources=None, local=False, remote=True, dataset_name="ds", destination="fake"
        )

        assert plan["data_tables"] == []
        assert any("connection refused" in warning for warning in plan["warnings"])

    def test_plan_warns_when_the_destination_holds_no_state(self, fake_boundary, dlt_drop, dlt_home):
        dlt_drop.default_schema_name = None

        plan = get_cleanup_plan(
            source=make_source(),
            resources=None,
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert plan["data_tables"] == []
        assert any("nothing to drop" in warning for warning in plan["warnings"])

    def test_plan_refuses_a_core_mode_destination(self, monkeypatch, dlt_home):
        """A dry run must refuse what execution would refuse, not print a plan."""

        @contextmanager
        def _core_mode(pipeline_name, destination, dataset_name):
            raise UnregisteredDestinationError("no adapter")
            yield  # pragma: no cover

        monkeypatch.setattr(cleanup_module, "open_destination_boundary", _core_mode)

        with pytest.raises(UnregisteredDestinationError):
            get_cleanup_plan(
                source=make_source(),
                resources=None,
                local=True,
                remote=True,
                dataset_name="ds",
                destination="filesystem",
            )

    def test_local_only_plan_never_opens_destination(self, no_boundary, local_pipeline_dir):
        plan = get_cleanup_plan(
            source=make_source(),
            resources=None,
            local=True,
            remote=False,
            dataset_name=None,
        )
        assert plan["local_exists"] is True
        assert plan["data_tables"] == []


# --- Safety: a core-mode refusal must land before anything is destroyed ---


class TestCoreModeRefusal:
    def test_clean_refuses_before_asking_dlt_to_drop(self, monkeypatch, dlt_drop, local_pipeline_dir):
        @contextmanager
        def _core_mode(pipeline_name, destination, dataset_name):
            raise UnregisteredDestinationError("no adapter")
            yield  # pragma: no cover

        monkeypatch.setattr(cleanup_module, "open_destination_boundary", _core_mode)

        with pytest.raises(UnregisteredDestinationError):
            clean_pipeline(
                source=make_source(),
                resources=None,
                local=True,
                remote=True,
                dataset_name="ds",
                destination="filesystem",
            )

        assert dlt_drop.constructed == []
        assert dlt_drop.executed == 0
        assert local_pipeline_dir.exists()  # local half never ran either


# --- Injection regression: hostile names are params, adapter-quoted, or escaped literals ---


class TestInjectionRegression:
    HOSTILE = 'x"; DROP TABLE users;--'

    def test_hostile_source_name_never_reaches_sql_text(self, fake_boundary, dlt_drop, dlt_home):
        source = make_source(name=self.HOSTILE, resources=("organizations",))
        fake_boundary.existing_tables = set(DLT_SYSTEM_TABLES)

        clean_pipeline(
            source=source,
            resources=None,
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        all_calls = fake_boundary.executed + fake_boundary.queried
        assert all(self.HOSTILE not in sql for sql, _ in all_calls)
        bound_params = [param for _, params in fake_boundary.executed for param in params]
        assert f"{self.HOSTILE}_pipeline" in bound_params  # _dlt_pipeline_state filter
        assert self.HOSTILE in bound_params  # _dlt_version/_dlt_loads schema filter

    def test_hostile_resource_name_bound_in_checkpoint_delete(self, fake_boundary, dlt_drop, dlt_home):
        source = make_source(resources=(self.HOSTILE,))
        fake_boundary.existing_tables = {"_dlt_custom_checkpoints"}

        clean_pipeline(
            source=source,
            resources=[self.HOSTILE],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        checkpoint_calls = [(sql, params) for sql, params in fake_boundary.executed if "checkpoints" in sql]
        assert checkpoint_calls == [
            (
                'DELETE FROM "test_dataset"."_dlt_custom_checkpoints" WHERE pipeline_name = ? AND resource_name = ?',
                ("test_source_pipeline", self.HOSTILE),
            )
        ]

    def test_hostile_dataset_name_refused_by_adapter_grammar(self, fake_boundary, dlt_drop, dlt_home):
        fake_boundary.existing_tables = set(DLT_SYSTEM_TABLES)
        source = make_source(resources=("organizations",))

        result = clean_pipeline(
            source=source,
            resources=None,
            local=False,
            remote=True,
            dataset_name=self.HOSTILE,
            destination="fake",
        )

        all_calls = fake_boundary.executed + fake_boundary.queried
        assert all(self.HOSTILE not in sql for sql, _ in all_calls)
        # Every dlt-ops bookkeeping DELETE degraded with a warning rather than
        # interpolating the name; none reported success.
        assert not any(item.endswith("(rows deleted)") for item in result["remote"])
        assert fake_boundary.executed == []

    def test_resource_names_reach_dlt_as_escaped_literals(self):
        """dlt compiles a bare selector to ^re.escape(name)$ — it cannot widen the selection.

        This is the property that lets cleanup forward resource names straight
        to `pipeline_drop` instead of pattern-matching them itself.
        """
        from dlt.common.schema.utils import compile_simple_regexes
        from dlt.common.schema.typing import TSimpleRegex

        pattern = compile_simple_regexes([TSimpleRegex(self.HOSTILE)])
        assert pattern.match(self.HOSTILE)
        assert not pattern.match("users")
        assert not pattern.match('x"; DROP TABLE orders;--')

        # A metacharacter in a resource name matches itself, nothing else.
        dotted = compile_simple_regexes([TSimpleRegex("or.s")])
        assert dotted.match("or.s")
        assert not dotted.match("orgs")


# --- End-to-end integration: DuckDB always, Postgres when POSTGRES_URL is set ---


def _cleanup_test_source():
    """Two incremental resources; ``orgs`` has a custom table name and nested data."""

    @dlt.source(name="cats")
    def cats():
        @dlt.resource(name="orgs", table_name="orgs_tbl", write_disposition="append", primary_key="id")
        def orgs(cursor=dlt.sources.incremental("id", initial_value=0)):
            yield [{"id": 1, "tags": [{"t": "a"}, {"t": "b"}]}, {"id": 2, "tags": [{"t": "c"}]}]

        @dlt.resource(name="depts", write_disposition="append", primary_key="id")
        def depts(cursor=dlt.sources.incremental("id", initial_value=0)):
            yield [{"id": 10}, {"id": 20}]

        return orgs, depts

    return cats


@pytest.fixture(
    params=[
        "duckdb",
        pytest.param(
            "postgres",
            marks=pytest.mark.skipif("POSTGRES_URL" not in environ, reason="POSTGRES_URL not set"),
        ),
    ]
)
def cleanup_destination(request, tmp_path, monkeypatch):
    """Destination factory + isolated dlt home/cwd for end-to-end cleanup runs."""
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-home"))
    monkeypatch.chdir(tmp_path)
    if request.param == "postgres":
        return dlt.destinations.postgres(environ["POSTGRES_URL"])
    return dlt.destinations.duckdb(str(tmp_path / "cleanup_e2e.duckdb"))


def _count_rows(destination, dataset, sql_from, tmp_path):
    """COUNT(*) via a throwaway probe pipeline; None when the relation is absent."""
    probe = dlt.pipeline(
        pipeline_name=f"probe_{uuid.uuid4().hex[:8]}",
        destination=destination,
        dataset_name=dataset,
        pipelines_dir=str(tmp_path / "probe-home"),
    )
    try:
        with probe.sql_client() as client:
            with client.execute_query(f"SELECT COUNT(*) FROM {sql_from}") as cursor:
                return cursor.fetchone()[0]
    except Exception:
        return None


@pytest.mark.integration
class TestCleanupEndToEnd:
    """Full + selective cleanup against a live destination (zero cloud creds)."""

    @pytest.fixture
    def e2e(self, cleanup_destination, tmp_path):
        source_fn = _cleanup_test_source()
        dataset = f"clean_ds_{uuid.uuid4().hex[:8]}"
        info = make_source(name="cats", resources=("orgs", "depts"), source_fn=source_fn)
        pipeline = dlt.pipeline(pipeline_name="cats_pipeline", destination=cleanup_destination, dataset_name=dataset)
        pipeline.run(source_fn())
        working_dir = tmp_path / "dlt-home" / "pipelines" / "cats_pipeline"
        assert working_dir.exists()
        return {
            "destination": cleanup_destination,
            "dataset": dataset,
            "info": info,
            "source_fn": source_fn,
            "working_dir": working_dir,
            "tmp_path": tmp_path,
        }

    def _count(self, e2e, table):
        return _count_rows(e2e["destination"], e2e["dataset"], f"{e2e['dataset']}.{table}", e2e["tmp_path"])

    def test_full_cleanup(self, e2e):
        assert self._count(e2e, "orgs_tbl") == 2
        assert self._count(e2e, "depts") == 2

        result = clean_pipeline(
            source=e2e["info"],
            resources=None,
            local=True,
            remote=True,
            dataset_name=e2e["dataset"],
            destination=e2e["destination"],
        )

        # Local working dir removed
        assert not e2e["working_dir"].exists()
        assert result["local"] == [str(e2e["working_dir"])]

        # Data tables dropped, nested child table included
        assert self._count(e2e, "orgs_tbl") is None
        assert self._count(e2e, "orgs_tbl__tags") is None
        assert self._count(e2e, "depts") is None

        # System tables survive (shared) but carry no rows for this pipeline/schema
        assert self._count(e2e, "_dlt_pipeline_state WHERE pipeline_name = 'cats_pipeline'") == 0
        assert self._count(e2e, "_dlt_loads WHERE schema_name = 'cats'") == 0
        assert self._count(e2e, "_dlt_version WHERE schema_name = 'cats'") == 0

        assert "table: orgs_tbl" in result["remote"]
        assert "table: depts" in result["remote"]
        assert "state: _dlt_pipeline_state (rows deleted)" in result["remote"]

    def test_selective_cleanup_drops_one_resource_and_resets_its_state(self, e2e):
        assert self._count(e2e, "orgs_tbl__tags") == 3

        result = clean_pipeline(
            source=e2e["info"],
            resources=["orgs"],
            local=True,
            remote=True,
            dataset_name=e2e["dataset"],
            destination=e2e["destination"],
        )

        # Target table gone WITH its nested child table; the sibling keeps its rows
        assert self._count(e2e, "orgs_tbl") is None
        assert self._count(e2e, "orgs_tbl__tags") is None
        assert self._count(e2e, "depts") == 2
        assert "table: orgs_tbl" in result["remote"]
        assert "state: reset orgs" in result["remote"]

        # Local surgery: state entry removed, schema file deleted, dir kept
        assert e2e["working_dir"].exists()
        local_state = json.loads((e2e["working_dir"] / "state.json").read_text())
        assert "orgs" not in local_state["sources"]["cats"]["resources"]
        assert "depts" in local_state["sources"]["cats"]["resources"]
        assert not (e2e["working_dir"] / "schemas" / "cats.schema.json").exists()

        # Re-run: no InStorageSchemaModified; the cleaned resource re-ingests
        # from scratch while the surviving resource's cursor holds (no dupes).
        rerun = dlt.pipeline(pipeline_name="cats_pipeline", destination=e2e["destination"], dataset_name=e2e["dataset"])
        rerun.run(e2e["source_fn"]())

        assert self._count(e2e, "orgs_tbl") == 2  # re-ingested from scratch
        assert self._count(e2e, "orgs_tbl__tags") == 3
        assert self._count(e2e, "depts") == 2  # incremental state preserved, no duplicates

    def test_full_cleanup_leaves_the_dataset_reusable(self, e2e):
        clean_pipeline(
            source=e2e["info"],
            resources=None,
            local=True,
            remote=True,
            dataset_name=e2e["dataset"],
            destination=e2e["destination"],
        )

        rerun = dlt.pipeline(pipeline_name="cats_pipeline", destination=e2e["destination"], dataset_name=e2e["dataset"])
        rerun.run(e2e["source_fn"]())

        assert self._count(e2e, "orgs_tbl") == 2
        assert self._count(e2e, "depts") == 2

    def test_remote_only_leaves_local_state_untouched(self, e2e):
        """dlt's drop is a pipeline run; it must not run through the user's working dir."""
        before = json.loads((e2e["working_dir"] / "state.json").read_text())

        clean_pipeline(
            source=e2e["info"],
            resources=["orgs"],
            local=False,
            remote=True,
            dataset_name=e2e["dataset"],
            destination=e2e["destination"],
        )

        assert self._count(e2e, "orgs_tbl") is None  # remote really was dropped
        after = json.loads((e2e["working_dir"] / "state.json").read_text())
        assert sorted(after["sources"]["cats"]["resources"]) == sorted(before["sources"]["cats"]["resources"])
        assert (e2e["working_dir"] / "schemas" / "cats.schema.json").exists()

    def test_dry_run_plan_changes_nothing(self, e2e):
        plan = get_cleanup_plan(
            source=e2e["info"],
            resources=["orgs"],
            local=True,
            remote=True,
            dataset_name=e2e["dataset"],
            destination=e2e["destination"],
        )

        assert sorted(plan["data_tables"]) == ["orgs_tbl", "orgs_tbl__tags"]
        assert plan["resource_states"] == ["orgs"]
        assert self._count(e2e, "orgs_tbl") == 2  # nothing executed
        assert self._count(e2e, "depts") == 2

    def test_clean_works_without_local_state(self, e2e):
        """The destination is the source of truth; local state is an optimization."""
        shutil.rmtree(e2e["working_dir"])
        # Phase-1-only record: no source_fn, so only the destination knows orgs -> orgs_tbl
        info = make_source(name="cats", resources=("orgs", "depts"))

        plan = get_cleanup_plan(
            source=info,
            resources=None,
            local=True,
            remote=True,
            dataset_name=e2e["dataset"],
            destination=e2e["destination"],
        )
        assert "orgs_tbl" in plan["data_tables"]
        assert plan["local_exists"] is False

        clean_pipeline(
            source=info,
            resources=None,
            local=True,
            remote=True,
            dataset_name=e2e["dataset"],
            destination=e2e["destination"],
        )

        assert self._count(e2e, "orgs_tbl") is None
        assert self._count(e2e, "depts") is None

    def test_clean_on_a_dataset_dlt_never_touched_is_a_no_op(self, e2e):
        """No state anywhere is nothing to do — not a PipelineNeverRan traceback."""
        ghost = make_source(name="ghost", resources=("nothing",))

        result = clean_pipeline(
            source=ghost,
            resources=None,
            local=True,
            remote=True,
            dataset_name=e2e["dataset"],
            destination=e2e["destination"],
        )

        assert result["local"] == []
        assert not any(item.startswith("table:") for item in result["remote"])
        # The real pipeline's data is untouched
        assert self._count(e2e, "orgs_tbl") == 2

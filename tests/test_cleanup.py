"""Tests for pipeline cleanup — adapter-routed, destination-agnostic.

Unit layers drive cleanup through a FakeAdapter at the DestinationAdapter
boundary; integration layers run real end-to-end cleanup against DuckDB
(credential-free) and, when POSTGRES_URL is set, against Postgres.
"""

import json
import logging
import re
import shutil
import uuid
import zlib
from contextlib import contextmanager
from os import environ
from pathlib import Path

import dlt
import pytest

from dlt_ops import SourceInfo, _compat
from dlt_ops.discovery import cleanup as cleanup_module
from dlt_ops.discovery.cleanup import (
    DLT_SYSTEM_TABLES,
    CleanupUnsupportedError,
    _clean_local_state_selective,
    _compress_dlt_state,
    _decode_dlt_schema,
    _decompress_dlt_state,
    _get_table_mapping_local,
    clean_pipeline,
    get_cleanup_plan,
    get_table_mapping,
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
    """In-memory DestinationAdapter double recording every boundary call.

    ``execute_query`` routes on the queried system table: ``version_rows``
    answer the tier-2 ``_dlt_version`` SELECT, ``state_rows`` the
    ``_dlt_pipeline_state`` join.
    """

    name = "fake"
    placeholder_style = "?"
    supports_if_exists = True
    supports_alter_add_column_if_not_exists = True
    supports_create_schema_if_not_exists = True
    timestamp_now_sql = "CURRENT_TIMESTAMP"
    _identifier_re = re.compile(r"[A-Za-z0-9_]+")

    def __init__(self, existing_tables=(), version_rows=None, state_rows=None):
        self.existing_tables = set(existing_tables)
        self.version_rows = version_rows or []
        self.state_rows = state_rows or []
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
        rows = self.version_rows if "_dlt_version" in sql else self.state_rows
        return _FakeCursor(rows)

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


# --- dlt-version compat guard ---


class TestCompatGuard:
    def test_supported_minors_match_ci_pin_file(self):
        """_compat's range and ci/dlt-versions.txt are one source of truth."""
        pin_file = REPO_ROOT / "ci" / "dlt-versions.txt"
        pinned = tuple(
            line.strip()
            for line in pin_file.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        )
        assert pinned == _compat.SUPPORTED_DLT_MINORS

    @pytest.mark.parametrize(
        ("version", "supported"),
        [("1.27.0", True), ("1.28.3", True), ("1.29.0", True), ("1.26.9", False), ("2.0.0", False), ("junk", False)],
    )
    def test_is_dlt_version_supported(self, version, supported):
        assert _compat.is_dlt_version_supported(version) is supported

    def test_unsupported_version_fails_clean(self, monkeypatch, dlt_home):
        monkeypatch.setattr(_compat, "installed_dlt_version", lambda: "1.99.0")
        with pytest.raises(CleanupUnsupportedError) as excinfo:
            clean_pipeline(make_source(), None, local=True, remote=False, dataset_name=None)
        message = str(excinfo.value)
        assert "1.99.0" in message
        assert _compat.supported_dlt_range() in message
        assert "pipeline.drop()" in message

    def test_unsupported_version_fails_plan(self, monkeypatch, dlt_home):
        monkeypatch.setattr(_compat, "installed_dlt_version", lambda: "0.5.0")
        with pytest.raises(CleanupUnsupportedError):
            get_cleanup_plan(make_source(), None, local=True, remote=False, dataset_name=None)

    def test_supported_version_passes_guard(self, monkeypatch, dlt_home):
        monkeypatch.setattr(_compat, "installed_dlt_version", lambda: f"{_compat.SUPPORTED_DLT_MINORS[0]}.5")
        result = clean_pipeline(make_source(), None, local=True, remote=False, dataset_name=None)
        assert result == {"local": [], "remote": []}


# --- Codec tests ---


class TestCodec:
    def test_state_roundtrip(self):
        state = {"key": "value", "nested": {"a": 1}}
        assert _decompress_dlt_state(_compress_dlt_state(state)) == state

    def test_state_decompress_raw_json_fallback(self):
        raw = json.dumps({"key": "value"})
        assert _decompress_dlt_state(raw) == {"key": "value"}

    def test_schema_decode_raw_json_is_primary(self):
        """_dlt_version.schema is stored as raw JSON on every verified destination."""
        schema = {"tables": {"orgs": {"resource": "organizations"}}}
        assert _decode_dlt_schema(json.dumps(schema)) == schema

    def test_schema_decode_accepts_compressed_fallback(self):
        schema = {"tables": {"orgs": {"resource": "organizations"}}}
        assert _decode_dlt_schema(_compress_dlt_state(schema)) == schema

    def test_state_compressed_blob_is_zlib_b64(self):
        import base64

        blob = _compress_dlt_state({"k": 1})
        assert json.loads(zlib.decompress(base64.b64decode(blob, validate=True))) == {"k": 1}


# --- Table mapping tests ---


class TestTableMapping:
    def test_tier1_local_schema(self, local_pipeline_dir):
        mapping = _get_table_mapping_local("test_source_pipeline", "test_source")
        assert mapping == {"organizations": "test_organizations", "lists": "test_lists"}

    def test_tier1_absent_returns_none(self, dlt_home):
        assert _get_table_mapping_local("missing_pipeline", "missing") is None

    def test_tier2_remote_schema_via_adapter(self, dlt_home):
        """Tier 2 decodes the raw-JSON schema from _dlt_version through the adapter."""
        schema = {"tables": {"orgs_tbl": {"resource": "organizations"}, "_dlt_loads": {}}}
        fake = FakeAdapter(existing_tables={"_dlt_version"}, version_rows=[(json.dumps(schema),)])

        mapping = get_table_mapping(make_source(), "test_source_pipeline", "test_source", "ds", fake, client=object())

        assert mapping == {"organizations": "orgs_tbl"}
        [(sql, params)] = fake.queried
        assert '"ds"."_dlt_version"' in sql
        assert params == ("test_source",)

    def test_tier2_skipped_when_version_table_absent(self, dlt_home):
        fake = FakeAdapter(existing_tables=set())
        source = make_source(resources=("organizations",))

        mapping = get_table_mapping(source, "test_source_pipeline", "test_source", "ds", fake, client=object())

        assert mapping == {"organizations": "organizations"}  # degraded to tier 3 convention
        assert fake.queried == []  # absence detected via table_exists, not a failed SELECT

    def test_tier3_source_instantiation(self, dlt_home):
        class _Res:
            table_name = "custom_table"

        class _Src:
            resources = {"organizations": _Res()}

        source = make_source(source_fn=lambda: _Src())
        mapping = get_table_mapping(source, "test_pipeline", "test", None)
        assert mapping["organizations"] == "custom_table"
        # "lists" not in the instantiated source, falls back to resource name
        assert mapping["lists"] == "lists"

    def test_tier3_not_importable_degrades_to_convention(self, dlt_home, caplog):
        """Phase-1-only record with nothing to import -> convention + warning."""
        source = make_source(resources=("a_res", "b_res"))
        with caplog.at_level(logging.WARNING, logger="dlt_ops.discovery.cleanup"):
            mapping = get_table_mapping(source, "test_source_pipeline", "test_source", None)
        assert mapping == {"a_res": "a_res", "b_res": "b_res"}
        assert any("resource_name == table_name" in record.message for record in caplog.records)

    def test_tier3_import_failure_degrades_to_convention(self, dlt_home, make_project, caplog):
        """A module that raises at import runs through Phase-2 introspect and degrades."""
        root = make_project(files={"broken/source/broken_api.py": "raise RuntimeError('boom at import')\n"})
        source = SourceInfo(
            name="broken_api",
            pipeline_name="broken",
            path=root / "broken",
            function_name="broken_api_source",
            resources=("a_res", "b_res"),
            module_stem="broken_api",
            module_path=root / "broken" / "source" / "broken_api.py",
        )

        with caplog.at_level(logging.WARNING, logger="dlt_ops.discovery.cleanup"):
            mapping = get_table_mapping(source, "broken_api_pipeline", "broken_api", None)

        assert mapping == {"a_res": "a_res", "b_res": "b_res"}
        assert any("resource_name == table_name" in record.message for record in caplog.records)

    def test_cascade_local_first(self, local_pipeline_dir):
        """Local schema wins; the source is never instantiated."""

        def _explode():
            raise AssertionError("source_fn must not be called when tier 1 resolves")

        source = make_source(source_fn=_explode)
        mapping = get_table_mapping(source, "test_source_pipeline", "test_source", "ds")
        assert mapping == {"organizations": "test_organizations", "lists": "test_lists"}


# --- Local state modification tests ---


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


# --- Full cleanup (adapter fakes) ---


class TestFullCleanup:
    def test_full_local_and_remote(self, fake_boundary, local_pipeline_dir):
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

        # Data tables dropped (mapping came from the local schema, tier 1)
        assert sorted(fake_boundary.dropped) == ["test_lists", "test_organizations"]

        # System tables: DELETE rows, never DROP
        assert not any(table in fake_boundary.dropped for table in DLT_SYSTEM_TABLES)
        deletes = [(sql, params) for sql, params in fake_boundary.executed if sql.startswith("DELETE")]
        delete_text = " | ".join(sql for sql, _ in deletes)
        assert '"test_dataset"."_dlt_pipeline_state"' in delete_text
        assert '"test_dataset"."_dlt_version"' in delete_text
        assert '"test_dataset"."_dlt_loads"' in delete_text
        assert ("test_source_pipeline",) in [params for _, params in deletes]  # pipeline-scoped filter
        assert ("test_source",) in [params for _, params in deletes]  # schema-scoped filter
        assert any("state: _dlt_pipeline_state (rows deleted)" == item for item in result["remote"])

    def test_full_deletes_checkpoint_rows_when_table_exists(self, fake_boundary, local_pipeline_dir):
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

    def test_absent_system_tables_are_skipped(self, fake_boundary, dlt_home):
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

        assert [item for item in result["remote"] if item.startswith("state:")] == []
        assert all(not sql.startswith("DELETE") for sql, _ in fake_boundary.executed)

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


# --- Selective cleanup (adapter fakes) ---


def _fake_state(resources: dict) -> dict:
    return {
        "_state_version": 5,
        "_state_engine_version": 4,
        "_version_hash": "abc",
        "destination_name": "fake",
        "destination_type": "dlt.destinations.fake",
        "dataset_name": "test_dataset",
        "sources": {"test_source": {"resources": resources}},
    }


class TestSelectiveCleanup:
    def test_selective_drops_only_target_tables(self, fake_boundary, local_pipeline_dir):
        result = clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert fake_boundary.dropped == ["test_organizations"]
        assert "table: test_organizations" in result["remote"]
        assert not any("test_lists" in item for item in result["remote"])

    def test_selective_state_surgery_roundtrips_blob(self, fake_boundary, local_pipeline_dir):
        """Surgery decodes the stored blob, removes the resource, re-encodes the SAME dict."""
        state = _fake_state(
            {
                "organizations": {"incremental": {"cursor": {"last_value": "v1"}}},
                "lists": {"incremental": {"cursor": {"last_value": "v2"}}},
            }
        )
        fake_boundary.existing_tables = set(DLT_SYSTEM_TABLES)
        fake_boundary.state_rows = [(_compress_dlt_state(state), 5, "abc")]

        result = clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        inserts = [(sql, params) for sql, params in fake_boundary.executed if sql.startswith("INSERT")]
        assert len(inserts) == 2
        loads_insert, state_insert = inserts
        assert "_dlt_loads" in loads_insert[0]
        load_id, schema_name, version_hash = loads_insert[1]
        assert load_id.isdigit()  # time_ns: sorts after every dlt epoch-seconds load_id
        assert schema_name == "test_source"
        assert version_hash == "abc"

        assert "_dlt_pipeline_state" in state_insert[0]
        version, engine_version, pipeline_name, blob, hash_param, load_id_param, _dlt_id = state_insert[1]
        assert (version, engine_version, pipeline_name) == (6, 4, "test_source_pipeline")
        assert (hash_param, load_id_param) == ("abc", load_id)

        rewritten = _decompress_dlt_state(blob)
        assert "organizations" not in rewritten["sources"]["test_source"]["resources"]
        assert rewritten["sources"]["test_source"]["resources"]["lists"] == {
            "incremental": {"cursor": {"last_value": "v2"}}
        }
        # Destination-naming values survive the roundtrip untouched
        assert rewritten["destination_name"] == "fake"
        assert rewritten["dataset_name"] == "test_dataset"
        assert rewritten["_state_version"] == 6
        assert "state: updated (removed 1 resource(s))" in result["remote"]

    def test_selective_without_remote_state_inserts_nothing(self, fake_boundary, local_pipeline_dir):
        fake_boundary.existing_tables = set(DLT_SYSTEM_TABLES)
        fake_boundary.state_rows = []

        result = clean_pipeline(
            source=make_source(),
            resources=["organizations"],
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        assert all(not sql.startswith("INSERT") for sql, _ in fake_boundary.executed)
        assert not any(item.startswith("state:") for item in result["remote"])

    def test_selective_deletes_checkpoints_per_resource(self, fake_boundary, local_pipeline_dir):
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


# --- Cleanup plan ---


class TestCleanupPlan:
    def test_full_plan(self, no_boundary, local_pipeline_dir):
        plan = get_cleanup_plan(
            source=make_source(),
            resources=None,
            local=True,
            remote=True,
            dataset_name="test_dataset",
        )

        assert plan["pipeline_name"] == "test_source_pipeline"
        assert plan["schema_name"] == "test_source"
        assert plan["is_full"] is True
        assert plan["local_exists"] is True
        assert sorted(plan["data_tables"]) == ["test_lists", "test_organizations"]
        assert plan["system_tables"] == list(DLT_SYSTEM_TABLES)

    def test_selective_plan(self, no_boundary, local_pipeline_dir):
        plan = get_cleanup_plan(
            source=make_source(),
            resources=["organizations"],
            local=True,
            remote=True,
            dataset_name="test_dataset",
        )

        assert plan["is_full"] is False
        assert plan["target_resources"] == ["organizations"]
        assert plan["data_tables"] == ["test_organizations"]
        assert plan["system_tables"] == []  # No system tables for selective

    def test_plan_degrades_when_destination_unreachable(self, monkeypatch, dlt_home, caplog):
        """A boundary failure downgrades the plan to local/source tiers instead of dying."""

        @contextmanager
        def _broken(pipeline_name, destination, dataset_name):
            raise RuntimeError("no credentials")
            yield  # pragma: no cover

        monkeypatch.setattr(cleanup_module, "open_destination_boundary", _broken)
        source = make_source(resources=("organizations",))

        with caplog.at_level(logging.WARNING, logger="dlt_ops.discovery.cleanup"):
            plan = get_cleanup_plan(source, None, local=False, remote=True, dataset_name="ds", destination="fake")

        assert plan["data_tables"] == ["organizations"]  # tier-3 convention
        assert any("Failed to open destination for cleanup plan" in record.message for record in caplog.records)


# --- Injection regression: hostile names are params or adapter-quoted, never text ---


class TestInjectionRegression:
    HOSTILE = 'x"; DROP TABLE users;--'

    def test_hostile_source_and_table_names_never_reach_sql_text(self, fake_boundary, dlt_home):
        class _Res:
            table_name = TestInjectionRegression.HOSTILE

        class _Src:
            resources = {"organizations": _Res()}

        source = make_source(name=self.HOSTILE, resources=("organizations",), source_fn=lambda: _Src())
        fake_boundary.existing_tables = set(DLT_SYSTEM_TABLES)

        result = clean_pipeline(
            source=source,
            resources=None,
            local=False,
            remote=True,
            dataset_name="test_dataset",
            destination="fake",
        )

        # The hostile table name was refused by the adapter grammar, not interpolated
        assert fake_boundary.dropped == []
        assert not any(item.startswith("table:") for item in result["remote"])

        # Hostile pipeline/schema names ride as bound params only
        all_calls = fake_boundary.executed + fake_boundary.queried
        assert all(self.HOSTILE not in sql for sql, _ in all_calls)
        bound_params = [param for _, params in fake_boundary.executed for param in params]
        assert f"{self.HOSTILE}_pipeline" in bound_params  # _dlt_pipeline_state filter
        assert self.HOSTILE in bound_params  # _dlt_version/_dlt_loads schema filter

    def test_hostile_resource_name_bound_in_checkpoint_delete(self, fake_boundary, dlt_home):
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

    def test_hostile_dataset_name_refused_by_adapter_grammar(self, fake_boundary, dlt_home):
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
        assert result["remote"] == []  # every per-table op degraded with a warning


# --- End-to-end integration: DuckDB always, Postgres when POSTGRES_URL is set ---


def _cleanup_test_source():
    """Two incremental resources; ``orgs`` maps to a custom table name."""

    @dlt.source(name="cats")
    def cats():
        @dlt.resource(name="orgs", table_name="orgs_tbl", write_disposition="append", primary_key="id")
        def orgs(cursor=dlt.sources.incremental("id", initial_value=0)):
            yield [{"id": 1}, {"id": 2}]

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

        # Data tables dropped
        assert self._count(e2e, "orgs_tbl") is None
        assert self._count(e2e, "depts") is None

        # System tables survive (shared) but carry no rows for this pipeline/schema
        assert self._count(e2e, "_dlt_pipeline_state WHERE pipeline_name = 'cats_pipeline'") == 0
        assert self._count(e2e, "_dlt_loads WHERE schema_name = 'cats'") == 0
        assert self._count(e2e, "_dlt_version WHERE schema_name = 'cats'") == 0

        assert "table: orgs_tbl" in result["remote"]
        assert "table: depts" in result["remote"]
        assert "state: _dlt_pipeline_state (rows deleted)" in result["remote"]

    def test_selective_cleanup_state_surgery_roundtrip(self, e2e):
        result = clean_pipeline(
            source=e2e["info"],
            resources=["orgs"],
            local=True,
            remote=True,
            dataset_name=e2e["dataset"],
            destination=e2e["destination"],
        )

        # Only the target table dropped; the sibling keeps its rows
        assert self._count(e2e, "orgs_tbl") is None
        assert self._count(e2e, "depts") == 2
        assert "table: orgs_tbl" in result["remote"]

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
        assert self._count(e2e, "depts") == 2  # incremental state preserved, no duplicates

    def test_plan_uses_remote_mapping_without_local_state(self, e2e):
        """Tier 2 live: with local state gone, the plan reads _dlt_version (raw JSON)."""
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
        assert plan["table_mapping"]["orgs"] == "orgs_tbl"

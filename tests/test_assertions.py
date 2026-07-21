"""Assertions: config parsing, built-ins, engine policy, quarantine
writer, the staged runner lifecycle on DuckDB, the three validate rules, and
Tier-2 preflight coverage.

Integration tests run real dlt pipelines against DuckDB in tmp_path
(credential-free lane); ledger and quarantine assertions read the destination
directly via duckdb — the test harness legitimately bypasses the adapter
boundary to verify destination state.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import tomllib
import types
from pathlib import Path
from textwrap import dedent
from typing import Any

import dlt
import duckdb
import pydantic
import pytest
from click.testing import CliRunner

import dlt_ops
import dlt_ops.discovery.runner as runner_mod
from dlt_ops.assertions import (
    AssertionConfigurationError,
    AssertionContext,
    AssertionFailedError,
    AssertionType,
)
from dlt_ops.assertions.builtin import MaxRowsPerLoad, MinRowsPerLoad, RequiredColumns, UniqueColumns
from dlt_ops.assertions.config import (
    check_specs,
    declared_columns_for_resource,
    parse_assertions,
    reserved_plugin_names,
    resolve_predicate,
    split_predicate,
)
from dlt_ops.assertions.engine import AssertionEngine
from dlt_ops.assertions.quarantine import REJECTED_COLUMNS, REJECTED_TABLE, QuarantineWriteError
from dlt_ops.cli.plugins import plugins as plugins_cli
from dlt_ops.discovery.models import ValidationContext
from dlt_ops.discovery.runner import run_pipeline
from dlt_ops.discovery.validators.assertions import (
    validate_assertion_columns,
    validate_assertion_config,
    validate_assertion_predicates,
)
from dlt_ops.plugins import registry as registry_mod
from dlt_ops.preflight import PluginNotRegisteredError, check_assertion_types
from dlt_ops.runs.writer import RUNS_COLUMNS, RUNS_TABLE, pipeline_name_for_source
from tests.test_runner import PROJECT_CONFIG, make_source_info

_WORKER_ENV_VARS = ("NORMALIZE__WORKERS", "LOAD__WORKERS", "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS")


@pytest.fixture(autouse=True)
def _isolate_run_env(tmp_path, monkeypatch):
    """Keep dlt state + DuckDB files in tmp_path; restore worker env vars."""
    saved = {var: os.environ.get(var) for var in _WORKER_ENV_VARS}
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
    monkeypatch.chdir(tmp_path)
    yield
    for var, value in saved.items():
        if value is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = value


@pytest.fixture(autouse=True)
def clean_registry():
    """Fresh plugin-registry scan per test — entry-point fakes must not leak."""
    registry_mod._reset_for_tests()
    yield
    registry_mod._reset_for_tests()


@pytest.fixture
def extra_entry_points(monkeypatch: pytest.MonkeyPatch):
    """Overlay fake entry points ON TOP of the real installed metadata."""
    real_entry_points = importlib.metadata.entry_points
    extras: list[importlib.metadata.EntryPoint] = []

    def fake_entry_points(*, group: str) -> tuple[importlib.metadata.EntryPoint, ...]:
        return tuple(real_entry_points(group=group)) + tuple(ep for ep in extras if ep.group == group)

    monkeypatch.setattr(registry_mod, "entry_points", fake_entry_points)

    def add(axis: str, name: str, value: str, dist: str) -> None:
        ep = importlib.metadata.EntryPoint(name=name, value=value, group=f"dlt_ops.{axis}")
        vars(ep).update(dist=types.SimpleNamespace(name=dist))
        extras.append(ep)

    return add


def _cfg(toml_text: str) -> dict[str, Any]:
    return tomllib.loads(dedent(toml_text))


def _db_file(source_name: str) -> Path:
    return Path.cwd() / f"{pipeline_name_for_source(source_name)}.duckdb"


def _query(source_name: str, sql: str) -> list[Any]:
    with duckdb.connect(str(_db_file(source_name))) as conn:
        return conn.execute(sql).fetchall()


def _runs_rows(source_name: str, dataset: str = "analytics") -> list[dict[str, Any]]:
    if not _db_file(source_name).exists():
        return []
    try:
        rows = _query(source_name, f"SELECT {', '.join(RUNS_COLUMNS)} FROM {dataset}.{RUNS_TABLE} ORDER BY started_at")
    except duckdb.CatalogException:
        return []
    return [dict(zip(RUNS_COLUMNS, row, strict=True)) for row in rows]


def _rejected_rows(source_name: str, dataset: str = "analytics") -> list[dict[str, Any]]:
    if not _db_file(source_name).exists():
        return []
    try:
        rows = _query(source_name, f"SELECT {', '.join(REJECTED_COLUMNS)} FROM {dataset}.{REJECTED_TABLE}")
    except duckdb.CatalogException:
        return []
    return [dict(zip(REJECTED_COLUMNS, row, strict=True)) for row in rows]


def _events_source(name: str, rows: list[dict[str, Any]] | list[list[dict[str, Any]]], paged: bool = False):
    """A one-resource source yielding `rows` (or, when paged, one list per page)."""

    @dlt.resource(name="events")
    def events():
        if paged:
            yield from rows
        else:
            yield rows

    return dlt.source(lambda: events, name=name)()


# --- Fixture third-party assertion type (registered via entry-point overlay) ---


class SuffixCheck:
    """Reference third-party assertion type: `path` must end with the value."""

    name = "acme_suffix_check"
    row_scoped = True

    def check_config(self, params, ctx):
        value = params.get("value")
        if not isinstance(value, str) or not value:
            return [f"acme_suffix_check requires a non-empty string value, got {value!r}"]
        return []

    def start(self, params):
        return None

    def observe(self, state, row, params):
        if str(row.get("path", "")).endswith(params["value"]):
            return None
        return f"path {row.get('path')!r} does not end with {params['value']!r}"

    def finalize(self, state, params):
        return None


# --- Predicates importable as tests.test_assertions:<name> ---

PREDICATE_CALLS: list[str] = []


def recording_pass_predicate(row):
    PREDICATE_CALLS.append(f"pass:{row.get('id')}")
    return True


def no_negative_ids(row):
    return row.get("id", 0) >= 0


NOT_CALLABLE = "just a string"


class TestParseAssertions:
    SECTION = "web_events"

    def test_absent_config_parses_empty(self):
        parsed = parse_assertions(_cfg("[dlt_ops]\n"), self.SECTION)
        assert parsed.resources == ()
        assert parsed.issues == ()
        assert not parsed.has_assertions

    def test_shorthand_normalizes_to_value_params(self):
        parsed = parse_assertions(
            _cfg(
                """
                [sources.web_events.dlt_ops.assertions.events]
                min_rows_per_load = 1
                required_columns = ["id", "path"]
                """
            ),
            self.SECTION,
        )
        (res,) = parsed.resources
        assert [(s.type_name, dict(s.params)) for s in res.specs] == [
            ("min_rows_per_load", {"value": 1}),
            ("required_columns", {"value": ["id", "path"]}),
        ]
        # Built-in default policy is fail (spec §2).
        assert {s.on_failure for s in res.specs} == {"fail"}

    def test_table_form_pops_on_failure_override(self):
        parsed = parse_assertions(
            _cfg(
                """
                [sources.web_events.dlt_ops.assertions.events]
                unique_columns = { value = ["id"], on_failure = "quarantine" }
                """
            ),
            self.SECTION,
        )
        (spec,) = parsed.resources[0].specs
        assert dict(spec.params) == {"value": ["id"]}  # on_failure never reaches the plugin
        assert spec.on_failure == "quarantine"

    def test_resource_level_default_applies_and_per_assertion_wins(self):
        parsed = parse_assertions(
            _cfg(
                """
                [sources.web_events.dlt_ops.assertions.events]
                on_failure = "warn"
                min_rows_per_load = 1
                unique_columns = { value = ["id"], on_failure = "quarantine" }
                """
            ),
            self.SECTION,
        )
        by_type = {s.type_name: s.on_failure for s in parsed.resources[0].specs}
        assert by_type == {"min_rows_per_load": "warn", "unique_columns": "quarantine"}

    def test_custom_entries_parse_and_come_last(self):
        parsed = parse_assertions(
            _cfg(
                """
                [sources.web_events.dlt_ops.assertions.events]
                required_columns = ["id"]

                [[sources.web_events.dlt_ops.assertions.events.custom]]
                predicate = "tests.test_assertions:no_negative_ids"
                on_failure = "warn"

                [[sources.web_events.dlt_ops.assertions.events.custom]]
                predicate = "tests.test_assertions.recording_pass_predicate"
                """
            ),
            self.SECTION,
        )
        specs = parsed.resources[0].specs
        assert [s.type_name for s in specs] == ["required_columns", "custom", "custom"]
        assert specs[1].predicate == "tests.test_assertions:no_negative_ids"
        assert specs[1].on_failure == "warn"
        assert specs[2].predicate == "tests.test_assertions.recording_pass_predicate"
        assert specs[2].on_failure == "fail"  # falls back to the built-in default
        assert parsed.issues == ()

    def test_custom_falls_back_to_resource_default(self):
        parsed = parse_assertions(
            _cfg(
                """
                [sources.web_events.dlt_ops.assertions.events]
                on_failure = "warn"

                [[sources.web_events.dlt_ops.assertions.events.custom]]
                predicate = "tests.test_assertions:no_negative_ids"
                """
            ),
            self.SECTION,
        )
        (spec,) = parsed.resources[0].specs
        assert spec.on_failure == "warn"

    def test_error_a_assertions_not_a_table(self):
        parsed = parse_assertions({"sources": {self.SECTION: {"dlt_ops": {"assertions": "nope"}}}}, self.SECTION)
        assert len(parsed.issues) == 1
        assert "must be a table" in parsed.issues[0].message

    def test_error_a_resource_entry_not_a_table(self):
        parsed = parse_assertions(
            {"sources": {self.SECTION: {"dlt_ops": {"assertions": {"events": 42}}}}}, self.SECTION
        )
        assert len(parsed.issues) == 1
        assert parsed.issues[0].resource_name == "events"
        assert "must be a table" in parsed.issues[0].message

    def test_error_d_on_failure_domain_at_every_level(self):
        parsed = parse_assertions(
            _cfg(
                """
                [sources.web_events.dlt_ops.assertions.events]
                on_failure = "explode"
                min_rows_per_load = { value = 1, on_failure = "retry" }

                [[sources.web_events.dlt_ops.assertions.events.custom]]
                predicate = "tests.test_assertions:no_negative_ids"
                on_failure = "shrug"
                """
            ),
            self.SECTION,
        )
        messages = [issue.message for issue in parsed.issues]
        assert len(messages) == 3
        assert all("invalid on_failure" in message for message in messages)

    def test_error_g_custom_shapes(self):
        parsed = parse_assertions(
            _cfg(
                """
                [sources.web_events.dlt_ops.assertions.events]
                custom = ["not-a-table"]

                [sources.web_events.dlt_ops.assertions.orders]
                custom = "not-a-list"

                [[sources.web_events.dlt_ops.assertions.clicks.custom]]
                on_failure = "warn"

                [[sources.web_events.dlt_ops.assertions.views.custom]]
                predicate = "not a module path"
                """
            ),
            self.SECTION,
        )
        messages = " | ".join(issue.message for issue in parsed.issues)
        assert len(parsed.issues) == 4
        assert "must be a table with a predicate key" in messages
        assert "must be an array of tables" in messages
        assert 'requires predicate = "module:attr"' in messages

    def test_referenced_types_filters_by_resource(self):
        parsed = parse_assertions(
            _cfg(
                """
                [sources.web_events.dlt_ops.assertions.events]
                min_rows_per_load = 1

                [sources.web_events.dlt_ops.assertions.orders]
                unique_columns = ["id"]

                [[sources.web_events.dlt_ops.assertions.orders.custom]]
                predicate = "tests.test_assertions:no_negative_ids"
                """
            ),
            self.SECTION,
        )
        assert parsed.referenced_types() == ("min_rows_per_load", "unique_columns")
        assert parsed.referenced_types(("events",)) == ("min_rows_per_load",)
        assert parsed.referenced_types(()) == ()


class TestCheckSpecs:
    SECTION = "web_events"

    def _ctx(self, resource_name: str, declared: tuple[str, ...] | None = None) -> AssertionContext:
        return AssertionContext(source_section=self.SECTION, resource_name=resource_name, declared_columns=declared)

    def _check(self, toml_text: str, known=("events",), declared=None):
        parsed = parse_assertions(_cfg(toml_text), self.SECTION)
        return check_specs(
            parsed,
            known_resources=set(known),
            context_for=lambda r: self._ctx(r, declared),
        )

    def test_error_b_unknown_resource(self):
        issues = self._check(
            """
            [sources.web_events.dlt_ops.assertions.ghosts]
            min_rows_per_load = 1
            """
        )
        assert len(issues) == 1
        assert "unknown resource 'ghosts'" in issues[0].message
        assert "events" in issues[0].message

    def test_error_c_unknown_type_lists_registered_names(self):
        issues = self._check(
            """
            [sources.web_events.dlt_ops.assertions.events]
            no_such_type = 1
            """
        )
        assert len(issues) == 1
        assert "unknown assertion type 'no_such_type'" in issues[0].message
        assert "min_rows_per_load" in issues[0].message  # registered names listed

    def test_error_e_quarantine_on_batch_scope(self):
        issues = self._check(
            """
            [sources.web_events.dlt_ops.assertions.events]
            min_rows_per_load = { value = 1, on_failure = "quarantine" }
            """
        )
        assert len(issues) == 1
        assert "batch-scoped" in issues[0].message
        assert "quarantine" in issues[0].message

    def test_error_f_params_rejected_by_check_config(self):
        issues = self._check(
            """
            [sources.web_events.dlt_ops.assertions.events]
            min_rows_per_load = -1
            max_rows_per_load = "many"
            required_columns = []
            unique_columns = { value = ["id"], typo = true }
            """
        )
        messages = " | ".join(issue.message for issue in issues)
        assert len(issues) == 4
        assert "min_rows_per_load requires an integer value >= 0" in messages
        assert "max_rows_per_load requires an integer value >= 1" in messages
        assert "required_columns requires a non-empty list" in messages
        assert "unknown parameter(s): typo" in messages

    def test_error_h_reserved_plugin_name(self):
        dlt_ops.register("assertion", "custom")(SuffixCheck)
        assert reserved_plugin_names() == ("custom",)
        errors = validate_assertion_config(ValidationContext(sources={}, config={}, project_root=Path(".")))
        assert len(errors) == 1
        assert "reserved name 'custom'" in errors[0].message

    def test_clean_config_has_no_issues(self):
        issues = self._check(
            """
            [sources.web_events.dlt_ops.assertions.events]
            min_rows_per_load = 1
            unique_columns = { value = ["id"], on_failure = "quarantine" }
            """
        )
        assert issues == []


class TestBuiltins:
    CTX = AssertionContext(source_section="s", resource_name="r", declared_columns=None)

    def test_min_rows_batch_semantics_across_pages(self):
        impl = MinRowsPerLoad()
        params = {"value": 3}
        state = impl.start(params)
        for row in ({"id": 1}, {"id": 2}):  # page 1
            assert impl.observe(state, row, params) is None
        verdict = impl.finalize(state, params)
        assert verdict == "row count 2 is below min_rows_per_load 3"
        assert impl.observe(state, {"id": 3}, params) is None  # page 2
        assert impl.finalize(state, params) is None

    def test_max_rows_batch_semantics(self):
        impl = MaxRowsPerLoad()
        params = {"value": 2}
        state = impl.start(params)
        for row in ({"id": 1}, {"id": 2}, {"id": 3}):
            assert impl.observe(state, row, params) is None  # never fails a row
        assert impl.finalize(state, params) == "row count 3 exceeds max_rows_per_load 2"

    def test_required_columns_checks_key_presence_not_nullness(self):
        impl = RequiredColumns()
        params = {"value": ["id", "path"]}
        state = impl.start(params)
        assert impl.observe(state, {"id": 1, "path": None}, params) is None
        assert impl.observe(state, {"id": 1}, params) == "missing required column(s): path"
        assert impl.finalize(state, params) is None

    def test_unique_columns_first_occurrence_passes_duplicates_fail(self):
        impl = UniqueColumns()
        params = {"value": ["id", "kind"]}
        state = impl.start(params)
        assert impl.observe(state, {"id": 1, "kind": "a"}, params) is None
        assert impl.observe(state, {"id": 1, "kind": "b"}, params) is None  # composite key differs
        verdict = impl.observe(state, {"id": 1, "kind": "a", "extra": 9}, params)
        assert verdict == "duplicate key id=1, kind='a'"
        # 16-byte hashes, not rows, accumulate (documented memory bound).
        assert all(isinstance(entry, bytes) and len(entry) == 16 for entry in state)

    def test_unique_columns_across_pages(self):
        impl = UniqueColumns()
        params = {"value": ["id"]}
        state = impl.start(params)
        assert impl.observe(state, {"id": 1}, params) is None  # page 1
        assert impl.observe(state, {"id": 1}, params) == "duplicate key id=1"  # page 2

    def test_int_check_config_rejects_bool_and_wrong_types(self):
        impl = MinRowsPerLoad()
        for bad in (True, "1", 1.5, None, -1):
            assert impl.check_config({"value": bad}, self.CTX), bad
        assert impl.check_config({"value": 0}, self.CTX) == []
        assert MaxRowsPerLoad().check_config({"value": 0}, self.CTX) != []

    def test_column_check_config_against_declared_columns(self):
        ctx = AssertionContext(source_section="s", resource_name="events", declared_columns=("id", "path"))
        assert RequiredColumns().check_config({"value": ["id", "path"]}, ctx) == []
        errors = UniqueColumns().check_config({"value": ["id", "ghost"]}, ctx)
        assert len(errors) == 1
        assert "'ghost'" in errors[0]
        assert "events" in errors[0]
        # None = model unresolvable: column-existence checking is skipped.
        assert UniqueColumns().check_config({"value": ["ghost"]}, self.CTX) == []

    def test_builtins_satisfy_the_protocol(self):
        for impl in (MinRowsPerLoad(), MaxRowsPerLoad(), RequiredColumns(), UniqueColumns()):
            assert isinstance(impl, AssertionType)

    def test_builtins_registered_through_entry_points_and_doctor(self):
        assert set(registry_mod.names("assertion")) == {
            "min_rows_per_load",
            "max_rows_per_load",
            "required_columns",
            "unique_columns",
        }
        assert registry_mod.source("assertion", "unique_columns").dist == "dlt-ops"
        result = CliRunner().invoke(plugins_cli, ["doctor"])
        assert result.exit_code == 0
        assert "min_rows_per_load" in result.output


class TestPredicateResolution:
    def test_split_both_forms(self):
        assert split_predicate("pkg.mod:attr") == ("pkg.mod", "attr")
        assert split_predicate("pkg.mod.attr") == ("pkg.mod", "attr")

    def test_resolve_installed_module(self):
        assert resolve_predicate("tests.test_assertions:no_negative_ids") is no_negative_ids
        assert resolve_predicate("tests.test_assertions.no_negative_ids") is no_negative_ids

    def test_resolve_project_local_module_via_project_root(self, tmp_path):
        (tmp_path / "quality_checks_probe_a.py").write_text("def always(row):\n    return True\n")
        fn = resolve_predicate("quality_checks_probe_a:always", tmp_path)
        assert fn({"id": 1}) is True

    def test_resolve_errors(self, tmp_path):
        with pytest.raises(ModuleNotFoundError):
            resolve_predicate("no_such_module_xyz:fn", tmp_path)
        with pytest.raises(AttributeError):
            resolve_predicate("tests.test_assertions:no_such_attr")
        with pytest.raises(TypeError, match="non-callable"):
            resolve_predicate("tests.test_assertions:NOT_CALLABLE")


class TestEngine:
    def _engine(self, config_toml: str, source_instance, **kwargs) -> AssertionEngine:
        return AssertionEngine.from_config(
            source_section="web_events",
            raw_config=_cfg(config_toml),
            source_instance=source_instance,
            **kwargs,
        )

    def test_inactive_without_assertions(self):
        engine = self._engine("[dlt_ops]\n", _events_source("web_events", [{"id": 1}]))
        assert not engine.active

    def test_construction_hard_fails_on_config_issue(self):
        with pytest.raises(AssertionConfigurationError, match="min_rows_per_load"):
            self._engine(
                """
                [sources.web_events.dlt_ops.assertions.events]
                min_rows_per_load = -1
                """,
                _events_source("web_events", [{"id": 1}]),
            )

    def test_construction_hard_fails_on_unresolvable_predicate(self):
        with pytest.raises(AssertionConfigurationError, match="not resolvable"):
            self._engine(
                """
                [[sources.web_events.dlt_ops.assertions.events.custom]]
                predicate = "no_such_module_xyz:fn"
                """,
                _events_source("web_events", [{"id": 1}]),
            )

    def test_unselected_resources_get_no_gate(self):
        source = _events_source("web_events", [{"id": 1}])
        source = source.with_resources()  # nothing selected
        engine = self._engine(
            """
            [sources.web_events.dlt_ops.assertions.events]
            min_rows_per_load = 1
            """,
            source,
        )
        assert not engine.active

    def test_gate_lands_after_the_load_timestamp_stamper(self):
        from dlt_ops.assertions.engine import _AssertionGate
        from dlt_ops.discovery.runner import _LoadTimestampStamper

        assert _AssertionGate.placement_affinity > _LoadTimestampStamper.placement_affinity


class TestRunnerLifecycle:
    """Full lifecycle on DuckDB, zero credentials (spec §3/§4/§8)."""

    ASSERTED_CONFIG = (
        PROJECT_CONFIG
        + """
        [sources.%(name)s.dlt_ops]
        schedule = "@daily"

        [sources.%(name)s.dlt_ops.assertions.events]
        %(assertions)s
        """
    )

    def _project(self, make_project, name: str, assertions: str) -> Path:
        return make_project(config=self.ASSERTED_CONFIG % {"name": name, "assertions": assertions})

    def test_pass_lifecycle_loads_and_completes(self, make_project):
        name = "assert_pass_rows"
        root = self._project(make_project, name, 'min_rows_per_load = 1\nrequired_columns = ["id"]')
        info = make_source_info(name, lambda: _events_source(name, [{"id": 1}, {"id": 2}]))
        run_pipeline(info, project_root=root)
        assert _query(name, "SELECT count(*) FROM analytics.events")[0][0] == 2
        (row,) = _runs_rows(name)
        assert row["status"] == "completed"
        assert _rejected_rows(name) == []  # no quarantine table without quarantined rows

    def test_row_scope_fail_aborts_before_load(self, make_project):
        name = "assert_rowfail_rows"
        root = self._project(make_project, name, 'required_columns = ["id", "path"]')
        info = make_source_info(name, lambda: _events_source(name, [{"id": 1, "path": "/"}, {"id": 2}]))
        with pytest.raises(AssertionFailedError, match="missing required column"):
            run_pipeline(info, project_root=root)
        assert _query(name, "SELECT 1 FROM information_schema.tables WHERE table_name = 'events'") == []
        (row,) = _runs_rows(name)
        assert row["status"] == "failed"
        assert "required_columns" in row["error_summary"]
        assert "AssertionFailedError" in row["error_summary"]

    def test_batch_scope_fail_drops_pending_package(self, make_project):
        """CRITICAL spec §3 hygiene: without drop_pending_packages the NEXT run
        auto-loads the rejected batch."""
        name = "assert_batchfail_rows"
        root = self._project(make_project, name, "min_rows_per_load = 2")
        good = [{"id": 1, "tag": "good"}, {"id": 2, "tag": "good"}]
        run_pipeline(make_source_info(name, lambda: _events_source(name, good)), project_root=root)

        bad = [{"id": 3, "tag": "rejected"}]
        with pytest.raises(AssertionFailedError, match="below min_rows_per_load"):
            run_pipeline(make_source_info(name, lambda: _events_source(name, bad)), project_root=root)

        probe = dlt.pipeline(
            pipeline_name=pipeline_name_for_source(name), destination="duckdb", dataset_name="analytics"
        )
        assert probe.list_extracted_load_packages() == []
        assert not probe.has_pending_data

        final = [{"id": 4, "tag": "good"}, {"id": 5, "tag": "good"}]
        run_pipeline(make_source_info(name, lambda: _events_source(name, final)), project_root=root)
        tags = {row[0] for row in _query(name, "SELECT DISTINCT tag FROM analytics.events")}
        assert tags == {"good"}  # the rejected batch never landed
        statuses = [row["status"] for row in _runs_rows(name)]
        assert statuses == ["completed", "failed", "completed"]

    def test_quarantine_writes_rejected_rows_and_loads_survivors(self, make_project):
        name = "assert_quarantine_rows"
        root = self._project(make_project, name, 'unique_columns = { value = ["id"], on_failure = "quarantine" }')
        rows = [{"id": 1, "path": "/"}, {"id": 2, "path": "/x"}, {"id": 1, "path": "/dup"}]
        info = make_source_info(name, lambda: _events_source(name, rows))
        run_pipeline(info, project_root=root)

        loaded = {row[0] for row in _query(name, "SELECT path FROM analytics.events")}
        assert loaded == {"/", "/x"}

        (rejected,) = _rejected_rows(name)
        assert rejected["pipeline_name"] == pipeline_name_for_source(name)
        assert rejected["source_section"] == name
        assert rejected["resource_name"] == "events"
        assert rejected["assertion_type"] == "unique_columns"
        assert json.loads(rejected["assertion_params"]) == {"value": ["id"]}
        assert rejected["violation"] == "duplicate key id=1"
        assert rejected["rejected_at"] is not None
        assert json.loads(rejected["row_json"]) == {"id": 1, "path": "/dup"}

        (run_row,) = _runs_rows(name)
        assert run_row["status"] == "completed"
        assert rejected["run_id"] == run_row["run_id"]  # joins _dlt_ops_runs

    def test_warn_logs_and_loads_everything(self, make_project, caplog):
        name = "assert_warn_rows"
        root = self._project(
            make_project, name, 'on_failure = "warn"\nrequired_columns = ["id", "path"]\nmin_rows_per_load = 10'
        )
        rows = [{"id": 1, "path": "/"}, {"id": 2}]
        info = make_source_info(name, lambda: _events_source(name, rows))
        with caplog.at_level("WARNING", logger="dlt_ops.assertions.engine"):
            run_pipeline(info, project_root=root)
        assert _query(name, "SELECT count(*) FROM analytics.events")[0][0] == 2  # row loads anyway
        (row,) = _runs_rows(name)
        assert row["status"] == "completed"
        messages = " | ".join(record.message for record in caplog.records)
        assert "'required_columns' warn" in messages
        assert "'min_rows_per_load' warn" in messages
        assert "warn summary" in messages
        assert "required_columns=1" in messages
        assert "min_rows_per_load=1" in messages

    def test_per_assertion_override_beats_resource_default(self, make_project):
        """Resource default warn; unique_columns overrides to quarantine — the
        duplicate is quarantined (not warned through), the missing column only warns."""
        name = "assert_precedence_rows"
        root = self._project(
            make_project,
            name,
            'on_failure = "warn"\n'
            'required_columns = ["id", "path"]\n'
            'unique_columns = { value = ["id"], on_failure = "quarantine" }',
        )
        rows = [{"id": 1, "path": "/"}, {"id": 2}, {"id": 1, "path": "/dup"}]
        info = make_source_info(name, lambda: _events_source(name, rows))
        run_pipeline(info, project_root=root)
        assert _query(name, "SELECT count(*) FROM analytics.events")[0][0] == 2  # dup dropped, warned row loads
        (rejected,) = _rejected_rows(name)
        assert rejected["assertion_type"] == "unique_columns"

    def test_multi_page_stream_batch_and_uniqueness_semantics(self, make_project):
        """Batch = resource × run, not per page: uniqueness spans pages and the
        row counter accumulates across pages."""
        name = "assert_paged_rows"
        root = self._project(
            make_project,
            name,
            'min_rows_per_load = 3\nunique_columns = { value = ["id"], on_failure = "quarantine" }',
        )
        pages = [[{"id": 1}, {"id": 2}], [{"id": 1}, {"id": 3}]]  # dup id=1 across pages
        info = make_source_info(name, lambda: _events_source(name, pages, paged=True))
        run_pipeline(info, project_root=root)
        loaded = sorted(row[0] for row in _query(name, "SELECT id FROM analytics.events"))
        assert loaded == [1, 2, 3]
        (rejected,) = _rejected_rows(name)
        assert rejected["violation"] == "duplicate key id=1"

    def test_custom_predicate_lifecycle_from_project_root(self, make_project):
        name = "assert_custom_rows"
        root = self._project(make_project, name, "")
        (root / "quality_checks_probe_b.py").write_text("def no_empty_path(row):\n    return bool(row.get('path'))\n")
        config = self.ASSERTED_CONFIG % {"name": name, "assertions": ""} + dedent(
            f"""
            [[sources.{name}.dlt_ops.assertions.events.custom]]
            predicate = "quality_checks_probe_b:no_empty_path"
            """
        )
        (root / ".dlt" / "config.toml").write_text(dedent(config))
        rows = [{"id": 1, "path": "/"}, {"id": 2, "path": ""}]
        info = make_source_info(name, lambda: _events_source(name, rows))
        with pytest.raises(AssertionFailedError, match="predicate quality_checks_probe_b:no_empty_path failed"):
            run_pipeline(info, project_root=root)
        (row,) = _runs_rows(name)
        assert row["status"] == "failed"
        assert "custom" in row["error_summary"]

    def test_third_party_type_executes_without_core_changes(self, make_project, extra_entry_points):
        extra_entry_points("assertion", "acme_suffix_check", "tests.test_assertions:SuffixCheck", "acme-assert")
        name = "assert_thirdparty_rows"
        root = self._project(make_project, name, 'acme_suffix_check = { value = ".html", on_failure = "quarantine" }')
        rows = [{"id": 1, "path": "a.html"}, {"id": 2, "path": "b.pdf"}]
        info = make_source_info(name, lambda: _events_source(name, rows))
        run_pipeline(info, project_root=root)
        loaded = [row[0] for row in _query(name, "SELECT path FROM analytics.events")]
        assert loaded == ["a.html"]
        (rejected,) = _rejected_rows(name)
        assert rejected["assertion_type"] == "acme_suffix_check"
        assert "does not end with" in rejected["violation"]

    def test_quarantine_write_failure_fails_the_run(self, make_project, monkeypatch):
        """Spec §4: write failure is run failure — the deliberate opposite of the
        best-effort runs writer. Fault injection at the adapter-resolution seam."""
        name = "assert_qfail_rows"
        root = self._project(make_project, name, 'unique_columns = { value = ["id"], on_failure = "quarantine" }')

        def _boom(pipeline):
            raise RuntimeError("injected adapter failure")

        monkeypatch.setattr("dlt_ops.assertions.quarantine.adapter_for_pipeline", _boom)
        rows = [{"id": 1}, {"id": 1}]
        info = make_source_info(name, lambda: _events_source(name, rows))
        with pytest.raises(QuarantineWriteError, match="injected adapter failure"):
            run_pipeline(info, project_root=root)

        # Nothing loaded, pending package dropped, ledger row failed.
        assert _query(name, "SELECT 1 FROM information_schema.tables WHERE table_name = 'events'") == []
        probe = dlt.pipeline(
            pipeline_name=pipeline_name_for_source(name), destination="duckdb", dataset_name="analytics"
        )
        assert probe.list_extracted_load_packages() == []
        (row,) = _runs_rows(name)
        assert row["status"] == "failed"
        assert "QuarantineWriteError" in row["error_summary"]

    def test_bad_config_hard_fails_before_pipeline_construction(self, make_project, monkeypatch):
        """Engine construction is pre-extract defense in depth (spec §7)."""
        name = "assert_badcfg_rows"
        root = self._project(make_project, name, 'min_rows_per_load = { value = 1, on_failure = "quarantine" }')

        def _fail(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("dlt.pipeline() must not be constructed")

        monkeypatch.setattr(runner_mod.dlt, "pipeline", _fail)
        info = make_source_info(name, lambda: _events_source(name, [{"id": 1}]))
        with pytest.raises(AssertionConfigurationError, match="batch-scoped"):
            run_pipeline(info, project_root=root)


def _make_validation_ctx(
    tmp_path: Path,
    assertions_toml: str,
    source_name: str = "web_events",
    resource_names: tuple[str, ...] = ("events",),
    source_fn=None,
) -> ValidationContext:
    info = make_source_info(
        source_name,
        source_fn if source_fn is not None else (lambda: _events_source(source_name, [{"id": 1}])),
    )
    info = __import__("attrs").evolve(info, resources=resource_names)
    return ValidationContext(
        sources={source_name: info},
        config=_cfg(assertions_toml),
        project_root=tmp_path,
    )


class TestValidateRules:
    def test_config_valid_rule_covers_structural_and_type_errors(self, tmp_path):
        ctx = _make_validation_ctx(
            tmp_path,
            """
            [sources.web_events.dlt_ops.assertions.events]
            on_failure = "explode"
            no_such_type = 1
            min_rows_per_load = { value = 1, on_failure = "quarantine" }
            required_columns = []

            [sources.web_events.dlt_ops.assertions.ghosts]
            min_rows_per_load = 1
            """,
        )
        messages = [error.message for error in validate_assertion_config(ctx)]
        joined = " | ".join(messages)
        assert len(messages) == 5
        assert "invalid on_failure 'explode'" in joined
        assert "unknown assertion type 'no_such_type'" in joined
        assert "batch-scoped" in joined
        assert "required_columns requires a non-empty list" in joined
        assert "unknown resource 'ghosts'" in joined

    def test_config_valid_rule_passes_clean_config(self, tmp_path):
        ctx = _make_validation_ctx(
            tmp_path,
            """
            [sources.web_events.dlt_ops.assertions.events]
            min_rows_per_load = 1
            unique_columns = { value = ["ghost"], on_failure = "quarantine" }
            """,
        )
        # Column existence is NOT this rule's job (separately exemptible).
        assert validate_assertion_config(ctx) == []

    def _typed_source(self, name: str = "web_events"):
        class Row(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(extra="forbid")

            id: int
            path: str

        @dlt.resource(name="events", columns=Row)
        def events():
            yield [{"id": 1, "path": "/"}]

        return dlt.source(lambda: events, name=name)()

    def test_columns_exist_rule_flags_unknown_column_on_pydantic_model(self, tmp_path):
        instance = self._typed_source()
        ctx = _make_validation_ctx(
            tmp_path,
            """
            [sources.web_events.dlt_ops.assertions.events]
            required_columns = ["id", "ghost"]
            unique_columns = ["path"]
            """,
            source_fn=lambda: instance,
        )
        errors = validate_assertion_columns(ctx)
        assert len(errors) == 1
        assert "'ghost'" in errors[0].message
        assert errors[0].field == "assertions.events.required_columns"

    def test_columns_exist_rule_skips_unresolvable_model(self, tmp_path):
        ctx = _make_validation_ctx(
            tmp_path,
            """
            [sources.web_events.dlt_ops.assertions.events]
            required_columns = ["ghost"]
            """,
        )
        # Untyped resource → declared model unresolvable → skipped, not failed.
        assert validate_assertion_columns(ctx) == []

    def test_declared_columns_helper(self):
        instance = self._typed_source("cols_probe")
        assert declared_columns_for_resource(instance.resources["events"]) == ("id", "path")
        untyped = _events_source("cols_probe2", [{"id": 1}])
        assert declared_columns_for_resource(untyped.resources["events"]) is None

    def test_predicate_rule_accepts_resolvable_project_local_predicate(self, tmp_path):
        (tmp_path / "quality_checks_probe_c.py").write_text("def ok(row):\n    return True\n")
        ctx = _make_validation_ctx(
            tmp_path,
            """
            [[sources.web_events.dlt_ops.assertions.events.custom]]
            predicate = "quality_checks_probe_c:ok"
            """,
        )
        assert validate_assertion_predicates(ctx) == []

    def test_predicate_rule_flags_unimportable_missing_and_noncallable(self, tmp_path):
        """Failure wording comes from the engine's own resolve_predicate — the
        probe fails exactly the way `run` would."""
        (tmp_path / "quality_checks_probe_d.py").write_text("NOT_CALLABLE = 42\n")
        ctx = _make_validation_ctx(
            tmp_path,
            """
            [[sources.web_events.dlt_ops.assertions.events.custom]]
            predicate = "no_such_module_xyz:fn"

            [[sources.web_events.dlt_ops.assertions.events.custom]]
            predicate = "quality_checks_probe_d:missing"

            [[sources.web_events.dlt_ops.assertions.events.custom]]
            predicate = "quality_checks_probe_d:NOT_CALLABLE"
            """,
        )
        messages = [error.message for error in validate_assertion_predicates(ctx)]
        joined = " | ".join(messages)
        assert len(messages) == 3
        assert "No module named 'no_such_module_xyz'" in joined
        assert "has no attribute 'missing'" in joined
        assert "non-callable" in joined

    def test_predicate_rule_reports_rule15_violations(self, tmp_path):
        """A resolvable predicate whose module misbehaves at import gets
        Rule-15 findings — the probe runs in the same audit sandbox as
        source-module import safety."""
        (tmp_path / "quality_checks_probe_e.py").write_text(
            dedent("""\
                import socket
                from pathlib import Path

                _probe = socket.socket()
                _probe.settimeout(0.05)
                try:
                    _probe.connect(("127.0.0.1", 9))  # attempt is the violation; port expected closed
                except OSError:
                    pass
                finally:
                    _probe.close()

                Path("predicate_canary.txt").write_text("side effect")

                def ok(row):
                    return True
                """)
        )
        ctx = _make_validation_ctx(
            tmp_path,
            """
            [[sources.web_events.dlt_ops.assertions.events.custom]]
            predicate = "quality_checks_probe_e:ok"
            """,
        )
        messages = [error.message for error in validate_assertion_predicates(ctx)]
        joined = " | ".join(messages)
        assert "not resolvable" not in joined  # the predicate itself resolves
        assert "Rule 15: network at import of predicate 'quality_checks_probe_e:ok'" in joined
        assert "Rule 15: disk-write at import of predicate 'quality_checks_probe_e:ok'" in joined
        assert "socket.connect" in joined

    def test_predicate_rule_probes_each_distinct_predicate_once(self, tmp_path, monkeypatch):
        (tmp_path / "quality_checks_probe_f.py").write_text("def ok(row):\n    return True\n")
        probed: list[str] = []

        def fake_probe(predicate: str, *, project_root: Path):
            probed.append(predicate)
            return __import__("dlt_ops.discovery.phase2", fromlist=["_SandboxVerdict"])._SandboxVerdict()

        import dlt_ops.discovery.validators.assertions as assertion_rules_mod

        monkeypatch.setattr(assertion_rules_mod, "run_predicate_sandbox_check", fake_probe)
        ctx = _make_validation_ctx(
            tmp_path,
            """
            [[sources.web_events.dlt_ops.assertions.events.custom]]
            predicate = "quality_checks_probe_f:ok"

            [[sources.web_events.dlt_ops.assertions.orders.custom]]
            predicate = "quality_checks_probe_f:ok"
            """,
            resource_names=("events", "orders"),
        )
        assert validate_assertion_predicates(ctx) == []
        assert probed == ["quality_checks_probe_f:ok"]

    def test_rules_ship_in_bare_validate_no_flag(self, make_project):
        """No --include-assertions flag: the rules are always-on core rules."""
        root = make_project(
            config="""
            [dlt_ops]
            default_destination = "duckdb"
            default_dataset = "analytics"

            [sources.web_events.dlt_ops]
            schedule = "@daily"

            [sources.web_events.dlt_ops.assertions.page_views]
            no_such_type = 1
            """,
            files={
                "web/source/web_events.py": """
                import dlt

                @dlt.resource(name="page_views")
                def page_views():
                    yield {"id": 1}

                @dlt.source(name="web_events")
                def web_events_source():
                    return page_views
                """
            },
        )
        errors = dlt_ops.validate_sources(root)
        assert any("unknown assertion type 'no_such_type'" in error.message for error in errors)


class TestPreflightAssertionAxis:
    def test_check_assertion_types_passes_for_builtins(self):
        raw = _cfg(
            """
            [sources.web_events.dlt_ops.assertions.events]
            min_rows_per_load = 1
            """
        )
        check_assertion_types("web_events", raw)

    def test_check_assertion_types_fails_on_unregistered_type(self):
        raw = _cfg(
            """
            [sources.web_events.dlt_ops.assertions.events]
            no_such_type = 1
            """
        )
        with pytest.raises(PluginNotRegisteredError, match="no_such_type"):
            check_assertion_types("web_events", raw)

    def test_selected_resources_scope_the_check(self):
        raw = _cfg(
            """
            [sources.web_events.dlt_ops.assertions.orders]
            no_such_type = 1
            """
        )
        check_assertion_types("web_events", raw, ("events",))  # orders not selected
        with pytest.raises(PluginNotRegisteredError):
            check_assertion_types("web_events", raw, ("orders",))

    def test_run_hard_fails_on_unregistered_type_with_validate_skipped(self, make_project, monkeypatch):
        """Tier-2 preflight (spec §7): validate never ran, the run still refuses
        to start — before any pipeline construction."""
        name = "assert_preflight_rows"
        root = make_project(
            config=PROJECT_CONFIG
            + f"""
            [sources.{name}.dlt_ops]
            schedule = "@daily"

            [sources.{name}.dlt_ops.assertions.events]
            no_such_type = 1
            """
        )

        def _fail(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("dlt.pipeline() must not be constructed")

        monkeypatch.setattr(runner_mod.dlt, "pipeline", _fail)
        info = make_source_info(name, lambda: _events_source(name, [{"id": 1}]))
        with pytest.raises(PluginNotRegisteredError, match="no_such_type"):
            run_pipeline(info, project_root=root)

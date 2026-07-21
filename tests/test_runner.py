"""Runner: config-chain resolution, Tier-2 preflight wiring, Rule 10/12 runtime
halves, load-timestamp stamping, capability tiers (core-mode filesystem run),
and the DuckDB end-to-end path.

Integration tests run real dlt pipelines against DuckDB and the local
filesystem destination in tmp_path (default credential-free lane); resolution
failure tests assert the typed error fires before any dlt.pipeline() is
constructed.
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from pathlib import Path
from typing import Any

import dlt
import pydantic
import pytest
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import PluggableRunContext
from dlt.common.runtime.run_context import switch_context

import dlt_ops.discovery.runner as runner_mod
from dlt_ops.config import UnresolvedDatasetError, UnresolvedDestinationError
from dlt_ops.destinations import ADAPTER_GATED_FEATURES
from dlt_ops.discovery.models import Schedule, SourceConfig, SourceInfo
from dlt_ops.discovery.runner import run_pipeline
from dlt_ops.discovery.scanner import discover_sources
from dlt_ops.preflight import MissingIncrementalCursorError, UnknownDestinationError
from dlt_ops.schema_contracts import CANONICAL_SCHEMA_CONTRACT, EVOLVE_SCHEMA_CONTRACT

PROJECT_CONFIG = """\
    [dlt_ops]
    default_destination = "duckdb"
    default_dataset = "analytics"
"""

_WORKER_ENV_VARS = ("NORMALIZE__WORKERS", "LOAD__WORKERS", "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS")


@pytest.fixture(autouse=True)
def _isolate_run_env(tmp_path, monkeypatch):
    """Keep dlt state in tmp_path and restore worker-tuning env vars.

    apply_dlt_overrides writes worker env vars; DLT_DATA_DIR/cwd keep pipeline
    working dirs and DuckDB files out of the real home directory.
    """
    saved = {var: os.environ.get(var) for var in _WORKER_ENV_VARS}
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt-data"))
    monkeypatch.chdir(tmp_path)
    yield
    for var, value in saved.items():
        if value is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = value


@pytest.fixture
def dlt_run_context():
    """Point dlt's own provider chain at a project root, restoring it afterwards.

    dlt reads ``.dlt/config.toml`` relative to its run context, which is fixed
    at whatever directory the process first resolved config in. Tests that
    assert on the config chain have to move it to the tmp project explicitly.
    """
    pluggable = Container()[PluggableRunContext]
    cookie = pluggable.push_context()
    try:
        yield lambda root: switch_context(str(root))
    finally:
        pluggable.pop_context(cookie)


def make_source_info(
    name: str,
    source_fn: Any,
    *,
    config: SourceConfig | None = None,
    path: Path = Path("."),
    uses_checkpoints: bool = False,
) -> SourceInfo:
    """Hand-built Phase-2-like SourceInfo around a live source callable."""
    return SourceInfo(
        name=name,
        pipeline_name=name,
        path=path,
        function_name=getattr(source_fn, "__name__", name),
        resources=(),
        module_stem=name,
        config=config,
        uses_checkpoints=uses_checkpoints,
        source_fn=source_fn,
    )


def _forbid_pipeline_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fail(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("dlt.pipeline() must not be constructed")

    monkeypatch.setattr(runner_mod.dlt, "pipeline", _fail)


def _query(pipeline: Any, sql: str) -> list[Any]:
    with pipeline.sql_client() as client:
        with client.execute_query(sql) as cursor:
            return cursor.fetchall()


def _table_columns(pipeline: Any, dataset: str, table: str) -> set[str]:
    rows = _query(
        pipeline,
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = '{dataset}' AND table_name = '{table}'",
    )
    return {row[0] for row in rows}


@dlt.source(name="simple_rows")
def simple_rows_source():
    @dlt.resource(name="events")
    def events():
        yield [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}, {"id": 3, "value": "c"}]

    return events


def incremental_source(name: str = "incremental_rows"):
    @dlt.resource(name="events")
    def events(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2023, 1, 1, tzinfo=dt.UTC))):
        yield [
            {"id": 1, "ts": dt.datetime(2024, 1, 5, tzinfo=dt.UTC)},
            {"id": 2, "ts": dt.datetime(2024, 2, 5, tzinfo=dt.UTC)},
            {"id": 3, "ts": dt.datetime(2024, 3, 5, tzinfo=dt.UTC)},
        ]

    return dlt.source(lambda: events, name=name)()


class StrictEvent(pydantic.BaseModel):
    """A `columns=` model that declares its contract: unknown fields fail the run."""

    model_config = pydantic.ConfigDict(extra="forbid")

    id: int


class LooseEvent(pydantic.BaseModel):
    """A `columns=` model leaving `extra` unset — Pydantic's silent-drop default."""

    id: int


class EvolvingEvent(pydantic.BaseModel):
    """A `columns=` model opting into column evolution."""

    model_config = pydantic.ConfigDict(extra="allow")

    id: int


def _batched_source(source_name: str, columns: Any, batches: list[list[dict]]):
    """Source factory serving one batch per instantiation, so a test can run twice.

    The runner calls ``source_fn()`` per run, so consecutive ``run_pipeline``
    calls see a fresh source over the next batch — the only way to assert what
    a *second* run does to an existing table.
    """

    def build() -> Any:
        @dlt.resource(name="events", columns=columns)
        def events():
            yield batches.pop(0)

        return dlt.source(lambda: events, name=source_name)()

    return build


class TestResolution:
    def test_unresolved_destination_fails_before_pipeline_construction(self, make_project, monkeypatch):
        root = make_project(config='[dlt_ops]\ndefault_dataset = "analytics"\n')
        _forbid_pipeline_construction(monkeypatch)
        info = make_source_info("simple_rows", simple_rows_source)
        with pytest.raises(UnresolvedDestinationError, match="default_destination"):
            run_pipeline(info, project_root=root)

    def test_unresolved_dataset_fails_before_pipeline_construction(self, make_project, monkeypatch):
        root = make_project(config='[dlt_ops]\ndefault_destination = "duckdb"\n')
        _forbid_pipeline_construction(monkeypatch)
        info = make_source_info("simple_rows", simple_rows_source)
        with pytest.raises(UnresolvedDatasetError, match="default_dataset"):
            run_pipeline(info, project_root=root)

    def test_per_source_override_beats_project_default(self, make_project):
        """[sources.<X>.dlt_ops] destination/dataset outrank [dlt_ops] defaults."""
        root = make_project(
            config="""\
            [dlt_ops]
            default_destination = "unregistered_warehouse"
            default_dataset = "analytics"
            """
        )
        config = SourceConfig(schedule=Schedule.DAILY, destination="duckdb", dataset="per_source_ds")
        info = make_source_info("override_rows", simple_rows_source, config=config)
        pipeline = run_pipeline(info, project_root=root)
        assert pipeline.dataset_name == "per_source_ds"
        assert _query(pipeline, "SELECT COUNT(*) FROM per_source_ds.events")[0][0] == 3

    def test_explicit_arguments_beat_the_config_chain(self, make_project):
        """CLI --dataset (and a caller-supplied destination) outrank both config layers."""
        root = make_project(config=PROJECT_CONFIG)
        config = SourceConfig(schedule=Schedule.DAILY, dataset="per_source_ds")
        info = make_source_info("explicit_rows", simple_rows_source, config=config)
        pipeline = run_pipeline(info, project_root=root, destination="duckdb", dataset_name="explicit_ds")
        assert pipeline.dataset_name == "explicit_ds"
        assert _query(pipeline, "SELECT COUNT(*) FROM explicit_ds.events")[0][0] == 3


class TestPreflightWiring:
    def test_unresolvable_destination_fails_before_pipeline_construction(self, make_project, monkeypatch):
        root = make_project(config='[dlt_ops]\ndefault_destination = "nope"\ndefault_dataset = "analytics"\n')
        _forbid_pipeline_construction(monkeypatch)
        info = make_source_info("simple_rows", simple_rows_source)
        with pytest.raises(UnknownDestinationError, match="'nope'"):
            run_pipeline(info, project_root=root)

    def test_bounds_without_incremental_cursor_fail_preflight(self, make_project, monkeypatch):
        root = make_project(config=PROJECT_CONFIG)
        _forbid_pipeline_construction(monkeypatch)
        info = make_source_info("simple_rows", simple_rows_source)
        bounds = (dt.datetime(2024, 2, 1, tzinfo=dt.UTC), dt.datetime(2024, 3, 1, tzinfo=dt.UTC))
        with pytest.raises(MissingIncrementalCursorError, match="events"):
            run_pipeline(info, project_root=root, bounds=bounds)


class TestCapabilityTiers:
    def test_core_mode_run_warns_once_and_succeeds(self, tmp_path, make_project, monkeypatch, caplog):
        """Core tier (filesystem, no adapter): the run loop completes, rows and
        the trace land, the ledger skips at INFO, and exactly one WARNING names
        the destination and every gated feature."""
        root = make_project(config=PROJECT_CONFIG)
        bucket = tmp_path / "bucket"
        monkeypatch.setenv("DESTINATION__FILESYSTEM__BUCKET_URL", str(bucket))
        info = make_source_info("fs_rows", simple_rows_source)

        with caplog.at_level(logging.INFO):
            run_pipeline(info, project_root=root, destination="filesystem")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "core mode" in r.getMessage()]
        assert len(warnings) == 1
        message = warnings[0].getMessage()
        assert "'filesystem'" in message
        for feature in ADAPTER_GATED_FEATURES:
            assert feature in message
        assert list(bucket.glob("analytics/events/*")), "rows must land on the core-tier destination"
        assert list(bucket.glob("analytics/_dlt_trace/*")), "trace persistence must run normally in core mode"
        assert "runs ledger skipped" in caplog.text
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR and r.name.startswith("dlt_ops")]

    @pytest.mark.parametrize("flag", [True, False])
    def test_preflight_receives_uses_checkpoints_from_source_info(self, make_project, monkeypatch, flag):
        """run_pipeline forwards SourceInfo's Phase-1 checkpoint detection to preflight."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("cp_rows", simple_rows_source, uses_checkpoints=flag)
        captured: dict[str, Any] = {}

        class _PreflightReached(Exception):
            pass

        def _capture(**kwargs: Any) -> None:
            captured.update(kwargs)
            raise _PreflightReached

        monkeypatch.setattr(runner_mod, "run_preflight", _capture)
        with pytest.raises(_PreflightReached):
            run_pipeline(info, project_root=root)
        assert captured["uses_checkpoints"] is flag
        assert captured["destination"] == "duckdb"

    def test_full_tier_run_emits_no_core_mode_notice(self, make_project, caplog):
        """DuckDB (registered adapter) behavior is unchanged: no core-mode
        warning, no ledger skip line."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("full_tier_rows", simple_rows_source)
        with caplog.at_level(logging.INFO):
            run_pipeline(info, project_root=root)
        assert "core mode" not in caplog.text
        assert "runs ledger skipped" not in caplog.text


class TestRule10SchemaContract:
    """The canonical contract reaches a resource by one of two routes, and both
    are asserted here on the loaded table rather than on the hint dict.

    A Pydantic `columns=` model — which this package's conventions make
    mandatory — gets its contract from dlt, derived from the model's `extra`
    setting at decoration time. Only a dict `columns=` or a model-less resource
    arrives with no contract at all, and those are the ones the runner supplies
    the literal to. The earlier version of this suite asserted the runtime
    auto-apply against a resource named "bare" that had no `columns=` at all,
    so it never touched the route every real source takes.
    """

    def test_model_forbidding_extra_runs_under_frozen_columns(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        build = _batched_source("strict_probe", StrictEvent, [[{"id": 1}]])
        instance = build()
        run_pipeline(make_source_info("strict_probe", lambda: instance), project_root=root)
        assert instance.selected_resources["events"].schema_contract == CANONICAL_SCHEMA_CONTRACT

    def test_unknown_column_is_rejected_not_silently_dropped(self, make_project):
        """The headline promise, end to end: a field the model does not declare
        fails the run, and never reaches the destination."""
        root = make_project(config=PROJECT_CONFIG)
        build = _batched_source(
            "strict_probe",
            StrictEvent,
            [[{"id": 1}, {"id": 2}], [{"id": 3, "surprise": "leaked"}]],
        )
        info = make_source_info("strict_probe", build)

        pipeline = run_pipeline(info, project_root=root)
        assert _query(pipeline, "SELECT COUNT(*) FROM analytics.events")[0][0] == 2

        with pytest.raises(Exception) as excinfo:
            run_pipeline(info, project_root=root)
        message = str(excinfo.value)
        assert "surprise" in message
        assert "freeze" in message

        # Rejected, not absorbed: the offending column never lands, and the
        # rows that carried it are not partially written either.
        assert "surprise" not in _table_columns(pipeline, "analytics", "events")
        assert _query(pipeline, "SELECT COUNT(*) FROM analytics.events")[0][0] == 2

    def test_unknown_column_fails_even_on_the_very_first_run(self, make_project):
        """A model's contract is enforced in the extract step, so it does not
        get the new-table free pass the normalize-time contract grants."""
        root = make_project(config=PROJECT_CONFIG)
        build = _batched_source("strict_first", StrictEvent, [[{"id": 1, "surprise": "leaked"}]])
        with pytest.raises(Exception) as excinfo:
            run_pipeline(make_source_info("strict_first", build), project_root=root)
        assert "surprise" in str(excinfo.value)

    def test_model_leaving_extra_unset_discards_silently(self, make_project):
        """Characterization of dlt's derivation, and the reason the
        `pydantic_model_forbids_extra` rule has to exist.

        Left alone, a model without `extra="forbid"` derives
        `columns: "discard_value"` and drops unknown fields without a word. The
        runner deliberately does NOT overwrite that at run time — doing so would
        also overrule an author's opted-in `extra="allow"` — so the gate is what
        keeps a project out of this state. If this test ever fails because the
        contract came out canonical, dlt changed its derivation and the rule's
        premise needs rechecking.
        """
        root = make_project(config=PROJECT_CONFIG)
        build = _batched_source("loose_probe", LooseEvent, [[{"id": 1, "surprise": "dropped"}]])
        instance = build()
        pipeline = run_pipeline(make_source_info("loose_probe", lambda: instance), project_root=root)

        contract = instance.selected_resources["events"].schema_contract
        assert contract["columns"] == "discard_value"
        assert contract != CANONICAL_SCHEMA_CONTRACT
        assert "surprise" not in _table_columns(pipeline, "analytics", "events")
        assert _query(pipeline, "SELECT COUNT(*) FROM analytics.events")[0][0] == 1

    def test_model_allowing_extra_keeps_its_evolve_contract(self, make_project):
        """The runner never overwrites a contract dlt derived, in either
        direction — an opted-in evolving model still evolves."""
        root = make_project(config=PROJECT_CONFIG)
        build = _batched_source("evolve_probe", EvolvingEvent, [[{"id": 1, "extra_col": "kept"}]])
        instance = build()
        pipeline = run_pipeline(make_source_info("evolve_probe", lambda: instance), project_root=root)
        assert instance.selected_resources["events"].schema_contract == EVOLVE_SCHEMA_CONTRACT
        assert "extra_col" in _table_columns(pipeline, "analytics", "events")

    def test_dict_columns_resource_gets_the_canonical_contract_applied(self, make_project):
        """The route where the runtime literal is genuinely novel: dlt derives
        nothing from a dict `columns=`, so the runner supplies the contract.

        Enforced at normalize time, where dlt forces `column_mode="evolve"`
        while the table does not yet exist — first run defines the schema, later
        unknown columns hard-fail.
        """
        root = make_project(config=PROJECT_CONFIG)
        build = _batched_source(
            "dict_probe",
            {"id": {"data_type": "bigint"}},
            [[{"id": 1}], [{"id": 2, "surprise": "late"}]],
        )
        info = make_source_info("dict_probe", build)

        pipeline = run_pipeline(info, project_root=root)
        assert _table_columns(pipeline, "analytics", "events") >= {"id"}

        with pytest.raises(Exception):
            run_pipeline(info, project_root=root)
        assert "surprise" not in _table_columns(pipeline, "analytics", "events")

    def test_resource_without_columns_gets_canonical_contract(self, make_project):
        root = make_project(config=PROJECT_CONFIG)

        @dlt.resource(name="bare")
        def bare():
            yield [{"id": 1}]

        instance = dlt.source(lambda: bare, name="contract_probe")()
        info = make_source_info("contract_probe", lambda: instance)
        run_pipeline(info, project_root=root)
        assert instance.selected_resources["bare"].schema_contract == CANONICAL_SCHEMA_CONTRACT

    def test_declared_contract_is_untouched(self, make_project):
        root = make_project(config=PROJECT_CONFIG)

        @dlt.resource(name="declared", schema_contract=dict(EVOLVE_SCHEMA_CONTRACT))
        def declared():
            yield [{"id": 1}]

        instance = dlt.source(lambda: declared, name="declared_probe")()
        info = make_source_info("declared_probe", lambda: instance)
        run_pipeline(info, project_root=root)
        assert instance.selected_resources["declared"].schema_contract == EVOLVE_SCHEMA_CONTRACT


class TestRule12TimeInterval:
    def test_bounds_honored_without_allow_external_schedulers_kwarg(self, make_project):
        """CR1-3: injected [from, to) bounds override the incremental window even
        though the resource never sets allow_external_schedulers."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("bounded_rows", lambda: incremental_source("bounded_rows"))
        bounds = (dt.datetime(2024, 2, 1, tzinfo=dt.UTC), dt.datetime(2024, 3, 1, tzinfo=dt.UTC))
        pipeline = run_pipeline(info, project_root=root, bounds=bounds)
        ids = [row[0] for row in _query(pipeline, "SELECT id FROM analytics.events ORDER BY id")]
        assert ids == [2]

    def test_plain_run_is_unbounded(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("unbounded_rows", lambda: incremental_source("unbounded_rows"))
        pipeline = run_pipeline(info, project_root=root)
        ids = [row[0] for row in _query(pipeline, "SELECT id FROM analytics.events ORDER BY id")]
        assert ids == [1, 2, 3]


class TestLoadTimestampStamping:
    def test_configured_column_lands_on_every_row(self, make_project):
        root = make_project(config=PROJECT_CONFIG + 'load_timestamp_column = "loaded_at"\n')
        info = make_source_info("stamped_rows", simple_rows_source)
        pipeline = run_pipeline(info, project_root=root)
        rows = _query(pipeline, "SELECT loaded_at FROM analytics.events")
        assert len(rows) == 3
        assert all(row[0] is not None for row in rows)
        # One timestamp per run: every row of the run carries the same value.
        assert len({row[0] for row in rows}) == 1

    def test_unset_column_is_absent(self, make_project):
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("unstamped_rows", simple_rows_source)
        pipeline = run_pipeline(info, project_root=root)
        assert "loaded_at" not in _table_columns(pipeline, "analytics", "events")

    def test_padded_config_value_stamps_the_stripped_column(self, make_project):
        """A padded TOML value stamps the column every other layer expects.

        The runner already treated blank as off by stripping in its guard, so
        a padded value stamped a column named with its padding intact — a name
        the `cursor_not_load_timestamp` rule and the reconciler's ignored-column
        set (both of which strip) would never match.
        """
        root = make_project(config=PROJECT_CONFIG + 'load_timestamp_column = "  loaded_at  "\n')
        info = make_source_info("padded_stamp_rows", simple_rows_source)
        pipeline = run_pipeline(info, project_root=root)
        assert "loaded_at" in _table_columns(pipeline, "analytics", "events")
        rows = _query(pipeline, "SELECT loaded_at FROM analytics.events")
        assert len(rows) == 3
        assert all(row[0] is not None for row in rows)

    def test_stamp_lands_after_pydantic_freeze_validation(self, make_project):
        """Stamping composes with Rule 14 + Rule 10: the stamp step must sit AFTER
        the resource's PydanticValidator in the pipe. The model's extra="forbid"
        derives columns=freeze, so a stamper placed before validation (a plain
        add_map step, placement_affinity 0 vs the validator's 0.9) makes every
        model-typed resource reject its own stamped rows.

        The model must forbid extra for this to test anything: left unset, the
        derived contract is discard_value and a mis-placed stamper would have
        its column quietly dropped instead of rejected.
        """
        root = make_project(config=PROJECT_CONFIG + 'load_timestamp_column = "loaded_at"\n')

        class Row(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(extra="forbid")

            id: int
            value: str

        @dlt.resource(name="events", columns=Row, primary_key="id")
        def typed_events():
            yield [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]

        instance = dlt.source(lambda: typed_events, name="typed_stamped_rows")()
        info = make_source_info("typed_stamped_rows", lambda: instance)
        pipeline = run_pipeline(info, project_root=root)
        rows = _query(pipeline, "SELECT loaded_at FROM analytics.events")
        assert len(rows) == 2
        assert all(row[0] is not None for row in rows)


class TestEndToEnd:
    def test_discovered_source_runs_with_config_resolved_destination(self, make_project):
        """Full path: tmp project tree -> discovery -> run_pipeline; destination
        and dataset come purely from .dlt/config.toml."""
        root = make_project(
            config="""\
            [dlt_ops]
            default_destination = "duckdb"
            default_dataset = "analytics"

            [sources.web_events.dlt_ops]
            schedule = "@daily"
            """,
            files={
                "web/source/web_events.py": """\
                import dlt

                @dlt.resource(name="page_views")
                def page_views():
                    yield [{"id": 1, "path": "/"}, {"id": 2, "path": "/pricing"}]

                @dlt.source(name="web_events")
                def web_events_source():
                    return page_views
                """
            },
        )
        sources = discover_sources(root)
        assert "web_events" in sources
        pipeline = run_pipeline(sources["web_events"], project_root=root)
        assert pipeline.destination.destination_type.endswith("duckdb")
        assert pipeline.dataset_name == "analytics"
        assert _query(pipeline, "SELECT COUNT(*) FROM analytics.page_views")[0][0] == 2


TUNED_PROJECT_CONFIG = """\
    [dlt_ops]
    default_destination = "duckdb"
    default_dataset = "analytics"

    [normalize]
    workers = 9

    [load]
    workers = 7
"""


class TestLocalWorkerDefaults:
    def test_duckdb_run_applies_local_defaults_even_with_explicit_dataset(self, make_project):
        """The old `is_local = dataset_name is None` conflation is gone: local
        worker tuning keys off the destination type, not dataset presence."""
        root = make_project(config=PROJECT_CONFIG)
        info = make_source_info("tuning_rows", simple_rows_source)
        run_pipeline(info, project_root=root, dataset_name="explicit_ds")
        assert os.environ.get("NORMALIZE__WORKERS") == "4"
        assert os.environ.get("LOAD__WORKERS") == "3"

    def test_configured_workers_survive_a_local_run(self, make_project, dlt_run_context):
        """Environment variables outrank every other dlt provider, so the
        dev-loop default is applied only where the project configured nothing.
        Writing it unconditionally would silently demote [normalize] workers."""
        root = make_project(config=TUNED_PROJECT_CONFIG)
        dlt_run_context(root)
        assert dlt.config.get("normalize.workers", int) == 9, "config.toml must be visible before the run"

        info = make_source_info("tuning_rows", simple_rows_source)
        run_pipeline(info, project_root=root)

        assert os.environ.get("NORMALIZE__WORKERS") is None
        assert os.environ.get("LOAD__WORKERS") is None
        assert dlt.config.get("normalize.workers", int) == 9
        assert dlt.config.get("load.workers", int) == 7

    def test_local_defaults_apply_when_nothing_is_configured(self, make_project, dlt_run_context):
        """The dev-loop convenience is intact: a project that says nothing about
        workers still gets the local numbers on a DuckDB run."""
        root = make_project(config=PROJECT_CONFIG)
        dlt_run_context(root)
        assert dlt.config.get("normalize.workers", int) is None

        info = make_source_info("tuning_rows", simple_rows_source)
        run_pipeline(info, project_root=root)

        assert dlt.config.get("normalize.workers", int) == 4
        assert dlt.config.get("load.workers", int) == 3
        # No local default is declared for file size — it stays dlt's business.
        assert os.environ.get("NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS") is None

    def test_explicit_flag_outranks_a_configured_value(self, make_project, dlt_run_context):
        """`-n` is an override, not a default: it wins over config.toml too."""
        root = make_project(config=TUNED_PROJECT_CONFIG)
        dlt_run_context(root)
        info = make_source_info("tuning_rows", simple_rows_source)
        run_pipeline(info, project_root=root, normalize_workers=2, file_max_items=500)

        assert os.environ.get("NORMALIZE__WORKERS") == "2"
        assert os.environ.get("NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS") == "500"
        assert dlt.config.get("normalize.workers", int) == 2
        assert dlt.config.get("load.workers", int) == 7

    def test_exported_env_var_outranks_the_local_default(self, make_project, dlt_run_context, monkeypatch):
        """A value the operator or orchestrator exported is a deliberate choice."""
        root = make_project(config=PROJECT_CONFIG)
        dlt_run_context(root)
        monkeypatch.setenv("LOAD__WORKERS", "6")
        info = make_source_info("tuning_rows", simple_rows_source)
        run_pipeline(info, project_root=root)

        assert os.environ.get("LOAD__WORKERS") == "6"
        assert os.environ.get("NORMALIZE__WORKERS") == "4"

    def test_local_default_keys_map_to_the_documented_env_vars(self):
        """The defaults are keyed by dlt config key; the env-var spelling comes
        from dlt's own EnvironProvider. Lock the pair the docs promise."""
        assert runner_mod._env_var_name("normalize.workers") == "NORMALIZE__WORKERS"
        assert runner_mod._env_var_name("load.workers") == "LOAD__WORKERS"
        assert (
            runner_mod._env_var_name("normalize.data_writer.file_max_items") == "NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS"
        )
        assert set(runner_mod._LOCAL_DEFAULTS) == {"normalize.workers", "load.workers"}


class TestNoShellOut:
    def test_runner_module_never_shells_out(self):
        """Credentials and destination access resolve in-process — a runner that
        shells out inherits whatever the local workstation happens to have."""
        source_text = Path(runner_mod.__file__).read_text()
        for token in ("gcloud", "subprocess"):
            assert token not in source_text, f"runner.py must not reference {token!r}"

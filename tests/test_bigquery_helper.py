"""BigQuery plugin tests: the opt-in adapter() helper + the plugin rule group."""

import importlib.metadata
from pathlib import Path
from types import SimpleNamespace

import pytest

import dlt_ops.bigquery
from dlt_ops import SourceInfo, ValidationContext, validate_sources
from dlt_ops.bigquery import adapter
from dlt_ops.bigquery.validators import (
    BIGQUERY_RULES,
    validate_bigquery_adapter_partitioning,
    validate_partition_hints,
)
from dlt_ops.config import ProjectConfig
from dlt_ops.discovery.validator import load_rule_specs, resolve_rules
from dlt_ops.discovery.validators import CORE_RULES
from dlt_ops.plugins import registry as registry_mod

BIGQUERY_RULE_IDS = {"bigquery_partitioning", "bigquery_partition_hints"}


@pytest.fixture(autouse=True)
def clean_registry():
    """Fresh plugin-registry scan per test — entry-point fakes must not leak."""
    registry_mod._reset_for_tests()
    yield
    registry_mod._reset_for_tests()


@pytest.fixture
def core_only_entry_points(monkeypatch: pytest.MonkeyPatch):
    """Simulate a core-only install: hide the first-party bigquery rule provider."""
    real_entry_points = importlib.metadata.entry_points

    def fake_entry_points(*, group: str) -> tuple[importlib.metadata.EntryPoint, ...]:
        eps = tuple(real_entry_points(group=group))
        if group == "dlt_ops.validators":
            return tuple(ep for ep in eps if ep.name != "bigquery")
        return eps

    monkeypatch.setattr(registry_mod, "entry_points", fake_entry_points)


def _make_ctx(tmp_path: Path, name: str = "my_api") -> ValidationContext:
    pipeline_dir = tmp_path / "my_pipe"
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    source = SourceInfo(
        name=name,
        pipeline_name="my_pipe",
        path=pipeline_dir,
        function_name=f"{name}_source",
        source_fn=lambda: None,
        resources=(),
        module_stem=name,
    )
    return ValidationContext(sources={name: source}, config={}, project_root=tmp_path)


# --- adapter() helper ---


class TestAdapterHelper:
    @pytest.fixture
    def fake_dlt_adapter(self, monkeypatch: pytest.MonkeyPatch) -> dict:
        """Mock dlt's bigquery_adapter; records the forwarded call."""
        import dlt.destinations.adapters as dlt_adapters

        recorded: dict = {}

        def fake_bigquery_adapter(data, partition=None, cluster=None, **kwargs):
            recorded.update(data=data, partition=partition, cluster=cluster, kwargs=kwargs)
            return "adapted-resource"

        monkeypatch.setattr(dlt_adapters, "bigquery_adapter", fake_bigquery_adapter)
        return recorded

    def test_forwards_partition_and_cluster(self, fake_dlt_adapter):
        result = adapter("res", partition="ingested_at", cluster=["tenant_id"])
        assert result == "adapted-resource"
        assert fake_dlt_adapter == {
            "data": "res",
            "partition": "ingested_at",
            "cluster": ["tenant_id"],
            "kwargs": {},
        }

    def test_extra_kwargs_pass_through(self, fake_dlt_adapter):
        adapter("res", partition="ingested_at", cluster="tenant_id", table_description="events")
        assert fake_dlt_adapter["kwargs"] == {"table_description": "events"}

    def test_exported_surface(self):
        assert dlt_ops.bigquery.__all__ == ["adapter"]
        assert dlt_ops.bigquery.adapter is adapter


# --- bigquery_partitioning (AST half) ---


class TestBigqueryAdapterPartitioningValidator:
    def _write_source(self, ctx: ValidationContext, body: str) -> None:
        pipeline_dir = next(iter(ctx.sources.values())).path
        (pipeline_dir / "aggregator.py").write_text(body, encoding="utf-8")

    def test_both_kwargs_pass(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        self._write_source(ctx, 'bigquery_adapter(res, partition="ingested_at", cluster=["id"])\n')
        assert validate_bigquery_adapter_partitioning(ctx) == []

    def test_missing_both_fails_twice(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        self._write_source(ctx, "bigquery_adapter(res, table_description='x')\n")
        assert len(validate_bigquery_adapter_partitioning(ctx)) == 2

    def test_escape_comments_pass(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        self._write_source(
            ctx,
            "# no-partition: tiny full-refresh table\n"
            "# no-cluster: tiny full-refresh table\n"
            "bigquery_adapter(res, table_description='x')\n",
        )
        assert validate_bigquery_adapter_partitioning(ctx) == []

    def test_missing_cluster_only_fails_once(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        self._write_source(ctx, 'bigquery_adapter(res, partition="ingested_at")\n')
        errors = validate_bigquery_adapter_partitioning(ctx)
        assert len(errors) == 1
        assert "no cluster=" in errors[0].message


# --- bigquery_partition_hints (runtime half) ---


class TestPartitionHintsValidator:
    def _ctx_with_resources(self, tmp_path, resources: dict, destination: str = "bigquery") -> ValidationContext:
        pipeline_dir = tmp_path / "my_pipe"
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        instance = SimpleNamespace(resources=resources)
        source = SourceInfo(
            name="my_api",
            pipeline_name="my_pipe",
            path=pipeline_dir,
            function_name="my_api_source",
            source_fn=lambda: instance,
            resources=tuple(resources),
            module_stem="my_api",
        )
        return ValidationContext(
            sources={"my_api": source},
            config={"dlt_ops": {"default_destination": destination}},
            project_root=tmp_path,
        )

    def _fake_resource(self, columns: dict):
        # Mirrors dlt's public `columns` property, which is what the validator
        # reads. dlt normalizes `columns=` into this dict at decoration time and
        # returns it unchanged — `x-`-prefixed hints included — so a stand-in
        # carrying the dict is faithful to a real DltResource.
        return SimpleNamespace(columns=columns)

    def test_partition_hint_passes(self, tmp_path):
        res = self._fake_resource({"ingested_at": {"data_type": "timestamp", "partition": True}})
        assert validate_partition_hints(self._ctx_with_resources(tmp_path, {"my_table": res})) == []

    def test_x_bigquery_partition_hint_passes(self, tmp_path):
        res = self._fake_resource({"creation_time": {"x-bigquery-partition": True}})
        assert validate_partition_hints(self._ctx_with_resources(tmp_path, {"my_table": res})) == []

    def test_no_partition_hint_fails(self, tmp_path):
        res = self._fake_resource({"id": {"data_type": "text"}})
        errors = validate_partition_hints(self._ctx_with_resources(tmp_path, {"my_table": res}))
        assert len(errors) == 1
        assert "no partition column hint" in errors[0].message

    def test_non_bigquery_destination_skipped(self, tmp_path):
        """Partition hints are BigQuery physics — a DuckDB-bound source is exempt."""
        res = self._fake_resource({"id": {"data_type": "text"}})
        assert validate_partition_hints(self._ctx_with_resources(tmp_path, {"my_table": res}, "duckdb")) == []

    def test_dlt_load_id_partition_fails(self, tmp_path):
        res = self._fake_resource({"_dlt_load_id": {"partition": True}})
        assert len(validate_partition_hints(self._ctx_with_resources(tmp_path, {"my_table": res}))) == 1

    def test_error_points_at_config_exemption_and_helper(self, tmp_path):
        """The justified-exception path is the config exemption; the fix path
        is the plugin's own adapter helper."""
        res = self._fake_resource({})
        errors = validate_partition_hints(self._ctx_with_resources(tmp_path, {"my_table": res}))
        assert len(errors) == 1
        assert "rule_exemptions" in errors[0].message
        assert "bigquery_partition_hints" in errors[0].message
        assert "dlt_ops.bigquery.adapter" in errors[0].message


# --- plugin rule group wiring (CR1-8) ---

NEUTRAL_SOURCE = """
    import dlt

    @dlt.resource(name="rows")
    def rows():
        yield {"id": 1}

    @dlt.source(name="events_api")
    def events_api_source():
        return rows
"""

# Parses (AST rule input) but never executes; not a source module, so
# discovery never imports it.
AGGREGATOR_FILE = """
    def apply(res, bigquery_adapter):
        return bigquery_adapter(res, table_description="events")
"""

PROJECT_FILES = {
    "events/source/events_api.py": NEUTRAL_SOURCE,
    "events/source/aggregator.py": AGGREGATOR_FILE,
}

# import_safety off skips the sandbox child — keeps these tests fast; the
# behavior under test is the bigquery rule group.
CONFIG_DEFAULT = """
    [dlt_ops]
    default_destination = "bigquery"

    [dlt_ops.rules]
    import_safety = false

    [sources.events_api.dlt_ops]
    schedule = "@daily"
"""

CONFIG_RULES_OFF = """
    [dlt_ops]
    default_destination = "bigquery"

    [dlt_ops.rules]
    import_safety = false
    bigquery_partitioning = false
    bigquery_partition_hints = false

    [sources.events_api.dlt_ops]
    schedule = "@daily"
"""


class TestBigqueryRuleGroup:
    def test_provider_registered_via_entry_point(self):
        """Dog-food: the bigquery rules arrive through the same entry-point
        group third-party plugins use."""
        eps = [ep for ep in importlib.metadata.entry_points(group="dlt_ops.validators") if ep.name == "bigquery"]
        assert eps, "the package must register its 'bigquery' rule provider"
        provider = eps[0].load()
        assert tuple(provider()) == BIGQUERY_RULES

    def test_rules_present_with_bigquery_origin_and_default_on(self):
        assembly = load_rule_specs()
        specs = {spec.rule_id: spec for spec in assembly.specs if spec.rule_id in BIGQUERY_RULE_IDS}
        assert set(specs) == BIGQUERY_RULE_IDS
        for spec in specs.values():
            assert spec.plugin == "bigquery"
            assert spec.default_on is True
        resolved = resolve_rules(ProjectConfig(), assembly)
        assert resolved["bigquery_partitioning"] is True
        assert resolved["bigquery_partition_hints"] is True

    def test_rules_not_in_core(self):
        assert not BIGQUERY_RULE_IDS & {spec.rule_id for spec in CORE_RULES}

    def test_absent_on_core_only_install(self, core_only_entry_points):
        """Without the bigquery provider the rules don't exist: unknown to
        assembly and to the knob's valid-ID set."""
        assembly = load_rule_specs()
        assert not BIGQUERY_RULE_IDS & set(assembly.known_ids)
        assert assembly.failures == ()

    def test_rules_fire_by_default_end_to_end(self, make_project):
        root = make_project(config=CONFIG_DEFAULT, files=PROJECT_FILES)
        errors = validate_sources(root)
        # AST half: the bigquery_adapter() call has no partition=/cluster=.
        assert any(e.field.startswith("bigquery_adapter.") for e in errors)
        # Runtime half: the instantiated resource resolves no partition hint.
        assert any(e.field == "resource.rows.partition" for e in errors)

    def test_knob_disables_the_plugin_rules(self, make_project):
        root = make_project(config=CONFIG_RULES_OFF, files=PROJECT_FILES)
        errors = validate_sources(root)
        assert not any(e.field.startswith("bigquery_adapter.") for e in errors)
        assert not any(e.field == "resource.rows.partition" for e in errors)
        # The plugin's rule IDs are known: disabling them is not an unknown-ID error.
        assert not any(e.field.startswith("rules.bigquery") for e in errors)

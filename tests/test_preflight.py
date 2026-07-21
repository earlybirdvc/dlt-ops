"""Tier-2 runtime preflight: the five locked conditions, each a typed error.

Plugin-facing checks run against a stubbed registry (the module-level default
is only a default — every check takes `registry=`); the rule-ID check rides
the real entry-point assembly so it can't drift from Tier-1 `validate`.
Destination capability (condition 2) is tabled per branch: dlt-resolvability,
the registered-adapter probe, and every core-mode engagement trigger.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import dlt
import pytest

from dlt_ops.config import ProjectConfig
from dlt_ops.destinations import ADAPTER_GATED_FEATURES
from dlt_ops.destinations.duckdb import DuckDBAdapter
from dlt_ops.destinations.protocol import DestinationAdapter
from dlt_ops.plugins.registry import FailedPlugin
from dlt_ops.preflight import (
    AdapterCapabilityError,
    DestinationCapabilityError,
    MissingIncrementalCursorError,
    PluginLoadFailedError,
    PluginNotRegisteredError,
    PreflightError,
    UnknownDestinationError,
    UnknownRuleIdError,
    check_destination_adapter,
    check_destination_capability,
    check_incremental_cursor,
    check_plugin_load_failures,
    check_plugin_registered,
    check_rule_ids,
    run_preflight,
)

BOUNDS = (
    dt.datetime(2024, 2, 1, tzinfo=dt.UTC),
    dt.datetime(2024, 3, 1, tzinfo=dt.UTC),
)


class StubRegistry:
    """Duck-typed stand-in for dlt_ops.plugins.registry."""

    def __init__(
        self,
        *,
        names_by_axis: dict[str, tuple[str, ...]] | None = None,
        failures: tuple[FailedPlugin, ...] = (),
        plugins: dict[tuple[str, str], Any] | None = None,
        load_errors: dict[tuple[str, str], Exception] | None = None,
    ) -> None:
        self._names = names_by_axis or {}
        self._failures = failures
        self._plugins = plugins or {}
        self._load_errors = load_errors or {}

    def names(self, axis: str) -> tuple[str, ...]:
        return tuple(self._names.get(axis, ()))

    def failures(self) -> tuple[FailedPlugin, ...]:
        return self._failures

    def get(self, axis: str, name: str) -> Any:
        key = (axis, name)
        if key in self._load_errors:
            raise self._load_errors[key]
        return self._plugins[key]


def healthy_registry() -> StubRegistry:
    return StubRegistry(
        names_by_axis={"destination": ("duckdb",)},
        plugins={("destination", "duckdb"): DuckDBAdapter},
    )


class IncompleteAdapter:
    """Adapter stub exposing only a name — every other Protocol member missing."""

    name = "duckdb"


@dlt.resource(name="plain_rows")
def plain_rows():
    yield [{"id": 1}]


@dlt.resource(name="incremental_rows")
def incremental_rows(ts=dlt.sources.incremental("ts", initial_value=dt.datetime(2023, 1, 1, tzinfo=dt.UTC))):
    yield [{"id": 1, "ts": dt.datetime(2024, 2, 15, tzinfo=dt.UTC)}]


@dlt.source(name="mixed")
def mixed_source():
    return [plain_rows, incremental_rows]


@dlt.source(name="incremental_only")
def incremental_only_source():
    return [incremental_rows]


def assertions_config(resource: str, on_failure: str, section: str = "mixed") -> dict[str, Any]:
    """Raw-config shape carrying one min_rows_per_load assertion for one resource."""
    return {
        "sources": {
            section: {
                "dlt_ops": {"assertions": {resource: {"min_rows_per_load": {"value": 1, "on_failure": on_failure}}}}
            }
        }
    }


class TestPluginRegistered:
    def test_registered_name_passes(self):
        check_plugin_registered("destination", "duckdb", registry=healthy_registry())

    def test_unregistered_name_raises(self):
        with pytest.raises(PluginNotRegisteredError, match="'nope'.*'destination'"):
            check_plugin_registered("destination", "nope", registry=healthy_registry())

    @pytest.mark.parametrize("axis", ["orchestrator", "secret_backend", "alert_sink", "assertion"])
    def test_helper_is_axis_generic(self, axis):
        """One check covers every plugin axis; the error names the axis."""
        with pytest.raises(PluginNotRegisteredError, match=axis):
            check_plugin_registered(axis, "missing", registry=StubRegistry())

    def test_error_hints_at_entry_point_group_and_doctor(self):
        with pytest.raises(PluginNotRegisteredError, match="dlt_ops.destination.*plugins doctor"):
            check_plugin_registered("destination", "nope", registry=StubRegistry())


class TestPluginLoadFailures:
    def test_clean_registry_passes(self):
        check_plugin_load_failures("destination", "duckdb", registry=healthy_registry())

    def test_recorded_soft_failure_raises(self):
        registry = StubRegistry(
            failures=(FailedPlugin(axis="destination", name="duckdb", dist="acme-plugin", error="ImportError: boom"),),
        )
        with pytest.raises(PluginLoadFailedError, match="ImportError: boom"):
            check_plugin_load_failures("destination", "duckdb", registry=registry)

    def test_failure_on_other_name_ignored(self):
        registry = StubRegistry(
            failures=(FailedPlugin(axis="destination", name="other", dist=None, error="boom"),),
        )
        check_plugin_load_failures("destination", "duckdb", registry=registry)

    def test_failure_on_other_axis_ignored(self):
        registry = StubRegistry(
            failures=(FailedPlugin(axis="alert_sink", name="duckdb", dist=None, error="boom"),),
        )
        check_plugin_load_failures("destination", "duckdb", registry=registry)


class TestDestinationAdapterCapability:
    def test_complete_adapter_passes(self):
        check_destination_adapter("duckdb", registry=healthy_registry())

    def test_already_instantiated_adapter_passes(self):
        registry = StubRegistry(plugins={("destination", "duckdb"): DuckDBAdapter()})
        check_destination_adapter("duckdb", registry=registry)

    def test_incomplete_adapter_raises_naming_missing_members(self):
        registry = StubRegistry(plugins={("destination", "duckdb"): IncompleteAdapter})
        with pytest.raises(AdapterCapabilityError, match="execute_sql"):
            check_destination_adapter("duckdb", registry=registry)

    def test_missing_capability_flag_detected(self):
        """Annotated Protocol attributes count as capability surface, not just methods."""
        complete = DuckDBAdapter()
        members = {
            member: getattr(complete, member)
            for member in dir(complete)
            if not member.startswith("_") and member != "supports_if_exists"
        }
        stub = type("FlaglessAdapter", (), members)()
        registry = StubRegistry(plugins={("destination", "duckdb"): stub})
        with pytest.raises(AdapterCapabilityError, match="supports_if_exists"):
            check_destination_adapter("duckdb", registry=registry)

    def test_load_error_wrapped_as_typed_error(self):
        registry = StubRegistry(load_errors={("destination", "duckdb"): ImportError("no module named quack")})
        with pytest.raises(PluginLoadFailedError, match="no module named quack"):
            check_destination_adapter("duckdb", registry=registry)

    def test_probe_covers_the_full_protocol(self):
        """The probe's contract is the Protocol itself — spot-check known members."""
        from dlt_ops.preflight import _protocol_members

        members = _protocol_members(DestinationAdapter)
        assert {"name", "placeholder_style", "supports_if_exists", "execute_sql", "fetch_columns"} <= members


class TestDestinationCapability:
    """Condition 2: resolvability, the registered-adapter probe, core-mode engagement."""

    def test_core_tier_destination_proceeds(self):
        """No adapter + nothing engaged = core mode; the run is allowed through."""
        check_destination_capability("filesystem", registry=StubRegistry())

    def test_registered_dlt_destination_without_adapter_proceeds(self):
        """Adapter absence alone is no longer a refusal — even for engines that ship adapters."""
        check_destination_capability("duckdb", registry=StubRegistry())

    def test_unresolvable_destination_raises_with_dlt_wording(self):
        with pytest.raises(UnknownDestinationError, match="'snowflaek' is not a dlt destination") as excinfo:
            check_destination_capability("snowflaek", registry=StubRegistry())
        # dlt owns destination names; its own resolution message rides along.
        assert str(excinfo.value.__cause__) in str(excinfo.value)

    def test_registered_adapter_gets_the_full_probe(self):
        check_destination_capability("duckdb", registry=healthy_registry())

    def test_registered_but_load_failing_adapter_still_fails(self):
        registry = StubRegistry(
            names_by_axis={"destination": ("duckdb",)},
            load_errors={("destination", "duckdb"): ImportError("no module named quack")},
        )
        with pytest.raises(PluginLoadFailedError, match="no module named quack"):
            check_destination_capability("duckdb", registry=registry)

    def test_registered_but_soft_failed_adapter_still_fails(self):
        registry = StubRegistry(
            names_by_axis={"destination": ("duckdb",)},
            failures=(FailedPlugin(axis="destination", name="duckdb", dist=None, error="boom"),),
            plugins={("destination", "duckdb"): DuckDBAdapter},
        )
        with pytest.raises(PluginLoadFailedError, match="boom"):
            check_destination_capability("duckdb", registry=registry)

    def test_capability_incomplete_adapter_still_fails(self):
        registry = StubRegistry(
            plugins={("destination", "duckdb"): IncompleteAdapter}, names_by_axis={"destination": ("duckdb",)}
        )
        with pytest.raises(AdapterCapabilityError, match="execute_sql"):
            check_destination_capability("duckdb", registry=registry)

    def test_module_path_ref_normalizes_to_the_registered_engine(self):
        """A config ref like 'dlt.destinations.duckdb' lands on the 'duckdb' adapter."""
        check_destination_capability("dlt.destinations.duckdb", registry=healthy_registry())

    @pytest.mark.parametrize(
        ("kwargs", "named"),
        [
            pytest.param({"require_adapter": True}, "require_destination_adapter", id="strict-knob"),
            pytest.param({"uses_checkpoints": True}, "checkpoints", id="checkpoints"),
            pytest.param(
                {"adapter_required_for": "backfill (chunk state in _dlt_backfills)"},
                "backfill \\(chunk state in _dlt_backfills\\)",
                id="caller-declared",
            ),
            pytest.param(
                {"quarantine_resources": ("rows",)},
                'assertion quarantine \\(on_failure = "quarantine" on resource\\(s\\): rows\\)',
                id="quarantine",
            ),
        ],
    )
    def test_each_engagement_trigger_names_itself(self, kwargs, named):
        with pytest.raises(DestinationCapabilityError, match=named):
            check_destination_capability("filesystem", registry=StubRegistry(), **kwargs)

    def test_error_lists_features_adapters_and_remedies(self):
        registry = StubRegistry(names_by_axis={"destination": ("duckdb", "bigquery")})
        with pytest.raises(DestinationCapabilityError) as excinfo:
            check_destination_capability("snowflake", uses_checkpoints=True, registry=registry)
        message = str(excinfo.value)
        assert "'snowflake'" in message
        for feature in ADAPTER_GATED_FEATURES:
            assert feature in message
        assert "'duckdb', 'bigquery'" in message
        assert "dlt_ops.destination" in message
        assert "docs/reference/destinations.md" in message


class TestRuleIds:
    def test_empty_rules_pass(self):
        check_rule_ids(ProjectConfig())

    def test_known_rule_id_passes(self):
        from dlt_ops.discovery.validator import load_rule_specs

        known = load_rule_specs().known_ids
        assert known, "core rules must be registered via entry points in the test env"
        check_rule_ids(ProjectConfig(rules={known[0]: False}))

    def test_unknown_rule_id_raises(self):
        with pytest.raises(UnknownRuleIdError, match="definitely_not_a_rule"):
            check_rule_ids(ProjectConfig(rules={"definitely_not_a_rule": True}))


class TestIncrementalCursor:
    def test_no_bounds_is_noop(self):
        check_incremental_cursor(mixed_source(), None)

    def test_bounds_with_cursorless_resource_raises(self):
        with pytest.raises(MissingIncrementalCursorError, match="plain_rows"):
            check_incremental_cursor(mixed_source(), BOUNDS)

    def test_bounds_with_cursor_everywhere_passes(self):
        check_incremental_cursor(incremental_only_source(), BOUNDS)

    def test_deselecting_cursorless_resource_passes(self):
        check_incremental_cursor(mixed_source().with_resources("incremental_rows"), BOUNDS)


class TestRunPreflight:
    def test_happy_path(self):
        run_preflight(
            destination="duckdb",
            project_config=ProjectConfig(),
            source=incremental_only_source(),
            bounds=BOUNDS,
            registry=healthy_registry(),
        )

    def test_core_tier_destination_proceeds(self):
        """The one deliberate pass-through: adapter-less destination, nothing engaged."""
        run_preflight(
            destination="filesystem",
            project_config=ProjectConfig(),
            source=incremental_only_source(),
            registry=StubRegistry(),
        )

    @pytest.mark.parametrize(
        ("destination", "registry_kwargs", "project_config", "source_factory", "bounds", "expected"),
        [
            pytest.param(
                "duckdb",
                {"names_by_axis": {"destination": ("duckdb",)}, "plugins": {("destination", "duckdb"): DuckDBAdapter}},
                ProjectConfig(alert_sinks=("ghost",)),
                None,
                None,
                PluginNotRegisteredError,
                id="1-plugin-not-registered",
            ),
            pytest.param(
                "not_a_dlt_destination",
                {"names_by_axis": {}},
                ProjectConfig(),
                None,
                None,
                UnknownDestinationError,
                id="2a-destination-unresolvable",
            ),
            pytest.param(
                "duckdb",
                {
                    "names_by_axis": {"destination": ("duckdb",)},
                    "plugins": {("destination", "duckdb"): IncompleteAdapter},
                },
                ProjectConfig(),
                None,
                None,
                AdapterCapabilityError,
                id="2b-adapter-missing-capability",
            ),
            pytest.param(
                "filesystem",
                {"names_by_axis": {}},
                ProjectConfig(require_destination_adapter=True),
                None,
                None,
                DestinationCapabilityError,
                id="2c-adapter-required-but-absent",
            ),
            pytest.param(
                "duckdb",
                {
                    "names_by_axis": {"destination": ("duckdb",)},
                    "failures": (FailedPlugin(axis="destination", name="duckdb", dist=None, error="boom"),),
                    "plugins": {("destination", "duckdb"): DuckDBAdapter},
                },
                ProjectConfig(),
                None,
                None,
                PluginLoadFailedError,
                id="3-plugin-soft-failed",
            ),
            pytest.param(
                "duckdb",
                {
                    "names_by_axis": {"destination": ("duckdb",)},
                    "plugins": {("destination", "duckdb"): DuckDBAdapter},
                },
                ProjectConfig(rules={"typo_rule": True}),
                None,
                None,
                UnknownRuleIdError,
                id="4-unknown-rule-id",
            ),
            pytest.param(
                "duckdb",
                {
                    "names_by_axis": {"destination": ("duckdb",)},
                    "plugins": {("destination", "duckdb"): DuckDBAdapter},
                },
                ProjectConfig(),
                mixed_source,
                BOUNDS,
                MissingIncrementalCursorError,
                id="5-cursor-missing-for-bounds",
            ),
        ],
    )
    def test_each_locked_condition_raises_its_typed_error(
        self, destination, registry_kwargs, project_config, source_factory, bounds, expected
    ):
        with pytest.raises(expected):
            run_preflight(
                destination=destination,
                project_config=project_config,
                source=source_factory() if source_factory else None,
                bounds=bounds,
                registry=StubRegistry(**registry_kwargs),
            )

    def test_uses_checkpoints_engages_the_capability_check(self):
        with pytest.raises(DestinationCapabilityError, match="checkpoints"):
            run_preflight(
                destination="filesystem",
                project_config=ProjectConfig(),
                uses_checkpoints=True,
                registry=StubRegistry(),
            )

    def test_adapter_required_for_engages_the_capability_check(self):
        with pytest.raises(DestinationCapabilityError, match="backfill \\(chunk state"):
            run_preflight(
                destination="filesystem",
                project_config=ProjectConfig(),
                adapter_required_for="backfill (chunk state in _dlt_backfills)",
                registry=StubRegistry(),
            )

    def test_quarantine_on_selected_resource_engages_the_capability_check(self):
        with pytest.raises(DestinationCapabilityError, match="assertion quarantine.*plain_rows"):
            run_preflight(
                destination="filesystem",
                project_config=ProjectConfig(),
                source=mixed_source(),
                raw_config=assertions_config("plain_rows", "quarantine"),
                source_section="mixed",
                registry=StubRegistry(),
            )

    def test_quarantine_on_unselected_resource_stays_core_mode(self):
        run_preflight(
            destination="filesystem",
            project_config=ProjectConfig(),
            source=mixed_source().with_resources("incremental_rows"),
            raw_config=assertions_config("plain_rows", "quarantine"),
            source_section="mixed",
            registry=StubRegistry(),
        )

    def test_non_quarantine_assertions_stay_core_mode(self):
        """`fail`/`warn` assertions never need the adapter — no engagement."""
        run_preflight(
            destination="filesystem",
            project_config=ProjectConfig(),
            source=mixed_source(),
            raw_config=assertions_config("plain_rows", "warn"),
            source_section="mixed",
            registry=StubRegistry(names_by_axis={"assertion": ("min_rows_per_load",)}),
        )

    def test_quarantine_on_full_tier_destination_is_no_engagement(self):
        """With an adapter registered, quarantine is simply available — no capability error."""
        registry = StubRegistry(
            names_by_axis={"destination": ("duckdb",), "assertion": ("min_rows_per_load",)},
            plugins={("destination", "duckdb"): DuckDBAdapter},
        )
        run_preflight(
            destination="duckdb",
            project_config=ProjectConfig(),
            source=mixed_source(),
            raw_config=assertions_config("plain_rows", "quarantine"),
            source_section="mixed",
            registry=registry,
        )

    def test_every_condition_is_a_preflight_error(self):
        for error in (
            PluginNotRegisteredError,
            PluginLoadFailedError,
            UnknownDestinationError,
            AdapterCapabilityError,
            DestinationCapabilityError,
            UnknownRuleIdError,
            MissingIncrementalCursorError,
        ):
            assert issubclass(error, PreflightError)

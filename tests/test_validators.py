"""Validator-framework tests: rule registry, resolution, exemptions, plugin groups, CLI."""

import importlib.metadata
import tomllib
import types
from pathlib import Path
from textwrap import dedent
from typing import Any, Optional

import attrs
import pydantic
import pytest
from click.testing import CliRunner

from dlt_ops import (
    RuleSpec,
    Schedule,
    SourceConfig,
    SourceInfo,
    ValidationContext,
    ValidationError,
    Validator,
    validate_sources,
)
from dlt_ops.cli.cli import cli
from dlt_ops.config import ProjectConfig
from dlt_ops.discovery import validator as validator_mod
from dlt_ops.discovery.validator import (
    RuleAssembly,
    RuleProviderFailure,
    check_unknown_rule_ids,
    load_rule_exemptions,
    load_rule_specs,
    resolve_rules,
    rule_provider_errors,
    rules_config_errors,
)
from dlt_ops.discovery.validators import CORE_RULES
from dlt_ops.discovery.validators.import_safety import validate_import_errors
from dlt_ops.plugins import registry as registry_mod

# The locked KEEP list: core is destination- and orchestrator-agnostic.
# airflow_var_required lives in the Airflow plugin group; bigquery_* rules in
# the BigQuery plugin group (tests/test_bigquery_helper.py).
EXPECTED_CORE_IDS = {
    "import_safety",
    "config_section_required",
    "schedule_required",
    "explicit_source_name",
    "module_name_matches_section",
    "orphan_config_sections",
    "no_resource_overlap",
    "json_hints_for_dict_fields",
    "pydantic_columns_required",
    "pydantic_model_forbids_extra",
    "schema_contract_declared",
    "explicit_resource_name_multi_source",
    "cursor_not_load_timestamp",
    "secret_backend_registered",
    "alert_sink_registered",
    "destination_capability",
    "stale_sources",
    "assertion_config_valid",
    "assertion_columns_exist",
    "assertion_predicate_resolvable",
    "incremental_cursor_required",
}

# Core rules that ship OFF. A missing incremental cursor is a policy, not a
# provable defect — a full refresh is legitimate and nothing the package sees
# separates it from an oversight — so adopting the rule is a decision rather
# than an upgrade surprise. Locked here so a rule can never quietly switch
# default in either direction.
EXPECTED_OPT_IN_CORE_IDS = {"incremental_cursor_required"}


@pytest.fixture(autouse=True)
def clean_registry():
    """Fresh plugin-registry scan per test — entry-point fakes must not leak."""
    registry_mod._reset_for_tests()
    yield
    registry_mod._reset_for_tests()


@pytest.fixture
def extra_entry_points(monkeypatch: pytest.MonkeyPatch):
    """Overlay fake entry points ON TOP of the real installed metadata.

    Returns an `add(axis, name, value, dist)` hook; the real entry points
    (including the package's own `core` rule provider) stay visible.
    """
    real_entry_points = importlib.metadata.entry_points
    extras: list[importlib.metadata.EntryPoint] = []

    def fake_entry_points(*, group: str) -> tuple[importlib.metadata.EntryPoint, ...]:
        return tuple(real_entry_points(group=group)) + tuple(ep for ep in extras if ep.group == group)

    monkeypatch.setattr(registry_mod, "entry_points", fake_entry_points)

    def add(axis: str, name: str, value: str, dist: str) -> None:
        ep = importlib.metadata.EntryPoint(name=name, value=value, group=f"dlt_ops.{axis}")
        # Mirrors EntryPoint._for(dist): real scans attach the owning Distribution.
        vars(ep).update(dist=types.SimpleNamespace(name=dist))
        extras.append(ep)

    return add


# --- Dummy plugin rule provider (loaded via entry point in the tests below) ---


def _dummy_rule_validator(ctx: ValidationContext) -> list[ValidationError]:
    """Dummy plugin rule: always emits one finding."""
    return [ValidationError(source_name="plugin_probe", field="dummy", message="dummy rule fired")]


def dummy_rules() -> tuple[RuleSpec, ...]:
    """Entry-point provider used by the plugin-group tests."""
    return (RuleSpec(rule_id="dummy_rule", validator=_dummy_rule_validator, plugin="acme_rules"),)


def exploding_rules() -> tuple[RuleSpec, ...]:
    """Entry-point provider that raises on enumeration (loads fine, then blows up)."""
    raise RuntimeError("provider exploded while enumerating rules")


# --- Neutral project fixtures ---

NEUTRAL_SOURCE = """
    import dlt

    @dlt.resource(name="{resource}")
    def {resource}():
        yield {{"id": 1}}

    @dlt.source(name="{name}")
    def {name}_source():
        return {resource}
"""


def _source_body(name: str, resource: str = "rows") -> str:
    return NEUTRAL_SOURCE.format(name=name, resource=resource)


def _make_ctx(
    tmp_path: Path,
    pipeline: str = "my_pipe",
    name: str = "my_api",
    *,
    resources: tuple[str, ...] = (),
    schema_contract_evolve_reason: str | None = None,
    attach_config: bool = False,
    config: dict | None = None,
) -> ValidationContext:
    """Build a minimal ValidationContext pointing at tmp_path/<pipeline>.

    When `schema_contract_evolve_reason` is passed, or `attach_config=True`,
    a SourceConfig is attached to the SourceInfo (Schedule.HOURLY as a
    filler). Otherwise `SourceInfo.config` stays None — mirrors the orphan /
    misconfigured-source case. `config` is the raw config.toml dict exposed
    as `ctx.config` (empty when omitted).
    """
    pipeline_dir = tmp_path / pipeline
    pipeline_dir.mkdir(parents=True, exist_ok=True)
    source_config: SourceConfig | None = None
    if schema_contract_evolve_reason is not None or attach_config:
        source_config = SourceConfig(
            schedule=Schedule.HOURLY,
            schema_contract_evolve_reason=schema_contract_evolve_reason,
        )
    source = SourceInfo(
        name=name,
        pipeline_name=pipeline,
        path=pipeline_dir,
        function_name=f"{name}_source",
        source_fn=lambda: None,
        resources=resources,
        module_stem=name,
        config=source_config,
    )
    return ValidationContext(sources={name: source}, config=config or {}, project_root=tmp_path)


# --- Framework: RuleSpec + registry ---


class TestRuleSpec:
    def test_description_is_first_docstring_line(self):
        spec = next(s for s in CORE_RULES if s.rule_id == "schedule_required")
        assert spec.description == "Check schedule field exists and is one of the values in the Schedule enum."

    def test_immutable(self):
        spec = CORE_RULES[0]
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            spec.rule_id = "renamed"


class TestCoreRuleRegistry:
    def test_core_rule_ids_exactly_the_disposition_set(self):
        assert {spec.rule_id for spec in CORE_RULES} == EXPECTED_CORE_IDS
        assert len(CORE_RULES) == len(EXPECTED_CORE_IDS)

    def test_every_core_rule_is_core_owned_and_callable(self):
        for spec in CORE_RULES:
            assert spec.plugin == "core"
            assert callable(spec.validator)

    def test_default_state_matches_the_locked_opt_in_list(self):
        for spec in CORE_RULES:
            expected = spec.rule_id not in EXPECTED_OPT_IN_CORE_IDS
            assert spec.default_on is expected, f"{spec.rule_id} changed its shipped default"

    def test_import_error_surfacing_is_not_a_rule(self):
        """A module that cannot import cannot run — always-on infrastructure,
        deliberately outside the knob/exemption machinery."""
        assert validate_import_errors not in {spec.validator for spec in CORE_RULES}

    def test_core_provider_registered_via_own_entry_point(self):
        """Dog-food: core rules arrive through the same entry-point group plugins use."""
        eps = [ep for ep in importlib.metadata.entry_points(group="dlt_ops.validators") if ep.name == "core"]
        assert eps, "the package must register its own 'core' rule provider"
        provider = eps[0].load()
        assert tuple(provider()) == CORE_RULES

    def test_load_rule_specs_assembles_core_without_failures(self):
        assembly = load_rule_specs()
        assert set(assembly.known_ids) >= EXPECTED_CORE_IDS
        assert assembly.failures == ()

    def test_resolved_core_rule_set_is_exactly_the_keep_list(self):
        """The entry-point-assembled core rule set == the locked KEEP list —
        moved/deleted rules must not resurface under the core origin."""
        assembly = load_rule_specs()
        core_ids = {spec.rule_id for spec in assembly.specs if spec.plugin == "core"}
        assert core_ids == EXPECTED_CORE_IDS


class TestResolveRules:
    def _assembly(self) -> RuleAssembly:
        return RuleAssembly(specs=CORE_RULES, failures=())

    def test_defaults_follow_each_rule_s_registered_default(self):
        resolved = resolve_rules(ProjectConfig(), self._assembly())
        assert set(resolved) == EXPECTED_CORE_IDS
        assert {rule_id for rule_id, on in resolved.items() if not on} == EXPECTED_OPT_IN_CORE_IDS

    def test_explicit_true_enables_an_opt_in_rule(self):
        """The opt-in half of the knob: default-off rules turn on from config."""
        config = ProjectConfig(rules={"incremental_cursor_required": True})
        resolved = resolve_rules(config, self._assembly())
        assert resolved["incremental_cursor_required"] is True

    def test_explicit_false_disables_exactly_that_rule(self):
        config = ProjectConfig(rules={"schedule_required": False})
        resolved = resolve_rules(config, self._assembly())
        assert resolved["schedule_required"] is False
        off = {rule_id for rule_id, on in resolved.items() if not on}
        assert off == EXPECTED_OPT_IN_CORE_IDS | {"schedule_required"}

    def test_explicit_true_is_a_no_op(self):
        resolved = resolve_rules(ProjectConfig(rules={"schedule_required": True}), self._assembly())
        assert resolved["schedule_required"] is True

    def test_unknown_id_not_injected_into_resolution(self):
        resolved = resolve_rules(ProjectConfig(rules={"no_such_rule": False}), self._assembly())
        assert "no_such_rule" not in resolved

    def test_non_bool_value_keeps_registry_default(self):
        resolved = resolve_rules(ProjectConfig(rules={"schedule_required": "off"}), self._assembly())
        assert resolved["schedule_required"] is True

    def test_check_unknown_rule_ids_sorted(self):
        unknown = check_unknown_rule_ids(["zzz_rule", "schedule_required", "aaa_rule"], EXPECTED_CORE_IDS)
        assert unknown == ("aaa_rule", "zzz_rule")
        assert check_unknown_rule_ids(["schedule_required"], EXPECTED_CORE_IDS) == ()

    def test_rules_config_errors_name_valid_ids(self):
        errors = rules_config_errors(ProjectConfig(rules={"schedle_required": False}), EXPECTED_CORE_IDS)
        assert len(errors) == 1
        assert errors[0].field == "rules.schedle_required"
        assert "unknown rule id" in errors[0].message
        assert "schedule_required" in errors[0].message  # valid-ID hint

    def test_rules_config_errors_flag_non_bool_values(self):
        errors = rules_config_errors(ProjectConfig(rules={"schedule_required": "off"}), EXPECTED_CORE_IDS)
        assert len(errors) == 1
        assert "must be true or false" in errors[0].message


class TestExemptionParsing:
    def test_valid_exemption_parsed(self):
        raw = {"sources": {"events_api": {"dlt_ops": {"rule_exemptions": {"schedule_required": "no schedule"}}}}}
        exemptions, errors = load_rule_exemptions(raw, EXPECTED_CORE_IDS)
        assert exemptions == {"events_api": {"schedule_required": "no schedule"}}
        assert errors == []

    def test_empty_reason_is_config_error(self):
        raw = {"sources": {"events_api": {"dlt_ops": {"rule_exemptions": {"schedule_required": ""}}}}}
        exemptions, errors = load_rule_exemptions(raw, EXPECTED_CORE_IDS)
        assert exemptions == {}
        assert len(errors) == 1
        assert errors[0].field == "rule_exemptions.schedule_required"
        assert "non-empty" in errors[0].message

    def test_whitespace_reason_is_config_error(self):
        raw = {"sources": {"events_api": {"dlt_ops": {"rule_exemptions": {"schedule_required": "   "}}}}}
        exemptions, errors = load_rule_exemptions(raw, EXPECTED_CORE_IDS)
        assert exemptions == {}
        assert len(errors) == 1

    def test_non_string_reason_is_config_error(self):
        raw = {"sources": {"events_api": {"dlt_ops": {"rule_exemptions": {"schedule_required": True}}}}}
        exemptions, errors = load_rule_exemptions(raw, EXPECTED_CORE_IDS)
        assert exemptions == {}
        assert len(errors) == 1

    def test_unknown_rule_id_is_config_error_naming_valid_ids(self):
        raw = {"sources": {"events_api": {"dlt_ops": {"rule_exemptions": {"no_such_rule": "why"}}}}}
        exemptions, errors = load_rule_exemptions(raw, EXPECTED_CORE_IDS)
        assert exemptions == {}
        assert len(errors) == 1
        assert errors[0].field == "rule_exemptions.no_such_rule"
        assert "unknown rule id" in errors[0].message
        assert "schedule_required" in errors[0].message

    def test_non_table_exemptions_is_config_error(self):
        raw = {"sources": {"events_api": {"dlt_ops": {"rule_exemptions": "schedule_required"}}}}
        exemptions, errors = load_rule_exemptions(raw, EXPECTED_CORE_IDS)
        assert exemptions == {}
        assert len(errors) == 1
        assert errors[0].field == "rule_exemptions"

    def test_no_sources_table(self):
        assert load_rule_exemptions({}, EXPECTED_CORE_IDS) == ({}, [])


# --- Framework end-to-end through validate_sources ---


class TestRulesKnobEndToEnd:
    """[dlt_ops.rules] <id> = false disables exactly that rule."""

    # import_safety off skips the sandbox child — keeps these tests fast; the
    # knob resolution under test is schedule_required.
    CONFIG_DEFAULT = """
        [dlt_ops]

        [dlt_ops.rules]
        import_safety = false

        [sources.events_api.dlt_ops]
    """
    FILES = {"events/source/events_api.py": _source_body("events_api")}

    def test_rule_on_by_default(self, make_project):
        root = make_project(config=self.CONFIG_DEFAULT, files=self.FILES)
        errors = validate_sources(root)
        assert any(e.field == "schedule" for e in errors)

    def test_knob_disables_exactly_that_rule(self, make_project):
        root = make_project(config=self.CONFIG_DEFAULT, files=self.FILES)
        # Same project, schedule_required flipped off.
        off_root = make_project(
            config="""
                [dlt_ops]

                [dlt_ops.rules]
                import_safety = false
                schedule_required = false

                [sources.events_api.dlt_ops]
            """,
            files=self.FILES,
            name="project_off",
        )
        on_errors = validate_sources(root)
        off_errors = validate_sources(off_root)
        assert any(e.field == "schedule" for e in on_errors)
        assert not any(e.field == "schedule" for e in off_errors)
        # Other rules keep firing: the resource declares no columns= model.
        assert any("columns=" in e.message for e in off_errors)

    def test_unknown_rule_id_fails_validate_naming_valid_ids(self, make_project):
        root = make_project(
            config="""
                [dlt_ops]

                [dlt_ops.rules]
                schedle_required = false
            """
        )
        errors = validate_sources(root)
        unknown = [e for e in errors if e.field == "rules.schedle_required"]
        assert len(unknown) == 1
        assert not unknown[0].is_warning
        assert "unknown rule id" in unknown[0].message
        assert "schedule_required" in unknown[0].message

    def test_non_bool_knob_value_fails_validate(self, make_project):
        root = make_project(
            config="""
                [dlt_ops]

                [dlt_ops.rules]
                schedule_required = "off"
            """
        )
        errors = validate_sources(root)
        assert any("must be true or false" in e.message for e in errors)


class TestExemptionsEndToEnd:
    """[sources.<X>.dlt_ops.rule_exemptions] suppresses per (source, rule)."""

    def test_exemption_suppresses_findings_for_that_source_only(self, make_project):
        root = make_project(
            config="""
                [dlt_ops]

                [dlt_ops.rules]
                import_safety = false

                [sources.events_api.dlt_ops.rule_exemptions]
                schedule_required = "provider has no schedule concept"

                [sources.orders_api.dlt_ops]
            """,
            files={
                "events/source/events_api.py": _source_body("events_api"),
                "orders/source/orders_api.py": _source_body("orders_api", resource="orders"),
            },
        )
        errors = validate_sources(root)
        schedule_errors = {e.source_name for e in errors if e.field == "schedule"}
        assert schedule_errors == {"orders_api"}
        # The exemption is rule-scoped, not source-wide: events_api still has
        # other findings (its resource declares no columns= model).
        assert any(e.source_name == "events_api" and "columns=" in e.message for e in errors)

    def test_empty_reason_is_validate_error(self, make_project):
        root = make_project(
            config="""
                [dlt_ops]

                [sources.events_api.dlt_ops.rule_exemptions]
                schedule_required = ""
            """
        )
        errors = validate_sources(root)
        reason_errors = [e for e in errors if e.field == "rule_exemptions.schedule_required"]
        assert len(reason_errors) == 1
        assert not reason_errors[0].is_warning
        assert "non-empty" in reason_errors[0].message

    def test_unknown_exemption_rule_id_is_validate_error(self, make_project):
        root = make_project(
            config="""
                [dlt_ops]

                [sources.events_api.dlt_ops.rule_exemptions]
                no_such_rule = "documented reason"
            """
        )
        errors = validate_sources(root)
        unknown = [e for e in errors if e.field == "rule_exemptions.no_such_rule"]
        assert len(unknown) == 1
        assert "unknown rule id" in unknown[0].message
        assert "schedule_required" in unknown[0].message


class TestPluginValidatorGroup:
    """A plugin's rule group auto-activates on load; the knob applies uniformly."""

    PROVIDER_VALUE = "tests.test_validators:dummy_rules"

    def test_plugin_rules_auto_active(self, extra_entry_points, make_project):
        extra_entry_points("validators", "acme_rules", self.PROVIDER_VALUE, "acme-rules-dist")
        root = make_project()
        errors = validate_sources(root)
        assert any(e.message == "dummy rule fired" for e in errors)

    def test_plugin_rule_listed_with_plugin_origin(self, extra_entry_points):
        extra_entry_points("validators", "acme_rules", self.PROVIDER_VALUE, "acme-rules-dist")
        assembly = load_rule_specs()
        dummy = next(spec for spec in assembly.specs if spec.rule_id == "dummy_rule")
        assert dummy.plugin == "acme_rules"
        assert set(assembly.known_ids) >= EXPECTED_CORE_IDS | {"dummy_rule"}

    def test_plugin_rule_disableable_via_knob_like_core_rules(self, extra_entry_points, make_project):
        extra_entry_points("validators", "acme_rules", self.PROVIDER_VALUE, "acme-rules-dist")
        root = make_project(
            config="""
                [dlt_ops]

                [dlt_ops.rules]
                dummy_rule = false
            """
        )
        errors = validate_sources(root)
        assert not any(e.message == "dummy rule fired" for e in errors)
        # The plugin's rule ID is known: disabling it is not an unknown-ID error.
        assert not any(e.field == "rules.dummy_rule" for e in errors)

    def test_soft_failed_provider_recorded_and_core_rules_survive(self, extra_entry_points, make_project):
        extra_entry_points("validators", "broken_rules", "dltx_missing_rules_mod:provider", "acme-broken")
        assembly = load_rule_specs()
        assert set(assembly.known_ids) >= EXPECTED_CORE_IDS
        assert len(assembly.failures) == 1
        assert assembly.failures[0].provider == "broken_rules"
        assert "ModuleNotFoundError" in assembly.failures[0].error
        # validate still runs on the surviving rules.
        root = make_project(
            config="""
                [dlt_ops]

                [sources.ghost_api]
                url = "https://example.test"
            """
        )
        errors = validate_sources(root, strict=True)
        assert any(e.field == "config_section" and e.is_warning for e in errors)


class TestRuleProviderFailuresSurface:
    """A provider that contributes no rules must be reported by every `validate`
    run. It used to be recorded only into `RuleAssembly.failures`, which nothing
    but `--show-resolved-rules` read — and that path returns before validation
    ever runs, so a normal run checked less than it claimed and said nothing."""

    EXPLODING = "tests.test_validators:exploding_rules"
    MISSING = "dltx_missing_rules_mod:provider"

    def _provider_errors(self, errors: list[ValidationError]) -> list[ValidationError]:
        return [e for e in errors if e.source_name == "dlt_ops.validators"]

    def test_raising_provider_surfaces_in_a_normal_validate_run(self, extra_entry_points, make_project):
        extra_entry_points("validators", "acme_rules", self.EXPLODING, "acme-rules-dist")
        errors = validate_sources(make_project())

        reported = self._provider_errors(errors)
        assert len(reported) == 1, errors
        assert reported[0].field == "validators.acme_rules"
        assert "provider exploded" in reported[0].message

    def test_provider_failure_is_an_error_not_a_filtered_warning(self, extra_entry_points, make_project):
        """Warnings are dropped from every non-`--strict` run, so a warning here
        would be invisible in exactly the run this must not pass silently."""
        extra_entry_points("validators", "acme_rules", self.EXPLODING, "acme-rules-dist")
        reported = self._provider_errors(validate_sources(make_project(), strict=False))

        assert reported and all(not e.is_warning for e in reported)

    def test_unloadable_provider_surfaces_too(self, extra_entry_points, make_project):
        """The load-failure twin of the raising provider: same soft-fail record,
        same reporting path."""
        extra_entry_points("validators", "broken_rules", self.MISSING, "acme-broken")
        reported = self._provider_errors(validate_sources(make_project()))

        assert [e.field for e in reported] == ["validators.broken_rules"]
        assert "ModuleNotFoundError" in reported[0].message

    def test_validate_cli_fails_when_a_provider_contributed_nothing(self, extra_entry_points, make_project):
        extra_entry_points("validators", "acme_rules", self.EXPLODING, "acme-rules-dist")
        root = make_project()
        result = CliRunner().invoke(cli, ["--root", str(root), "pipeline", "validate"])

        assert result.exit_code == 1, result.output
        assert "All sources validated successfully" not in result.output
        assert "acme_rules" in result.output

    def test_core_provider_failure_is_fatal(self, make_project, monkeypatch):
        """Core owns the baseline rule set; a run that lost it proves nothing."""
        assembly = RuleAssembly(
            specs=(),
            failures=(RuleProviderFailure(provider="core", error="ImportError: core is broken"),),
        )
        monkeypatch.setattr(validator_mod, "load_rule_specs", lambda: assembly)
        errors = validate_sources(make_project())

        fatal = [e for e in self._provider_errors(errors) if not e.is_warning]
        assert [e.field for e in fatal] == ["validators.core"]
        assert "core is broken" in fatal[0].message

    def test_healthy_environment_reports_no_provider_errors(self, make_project):
        """Guard against a false positive: an optional extra that is merely
        absent returns no rules and is not a failure."""
        assert self._provider_errors(validate_sources(make_project(), strict=True)) == []

    def test_rule_provider_errors_is_a_pure_projection_of_failures(self):
        assembly = RuleAssembly(
            specs=(),
            failures=(
                RuleProviderFailure(provider="acme", error="RuntimeError: nope"),
                RuleProviderFailure(provider="other", error="duplicate rule id 'x'; skipped"),
            ),
        )
        errors = rule_provider_errors(assembly)

        assert [e.field for e in errors] == ["validators.acme", "validators.other"]
        assert all(e.source_name == "dlt_ops.validators" for e in errors)
        assert rule_provider_errors(RuleAssembly(specs=(), failures=())) == []


class TestShowResolvedRules:
    """pipeline validate --show-resolved-rules: every rule, on/off, origin."""

    def _invoke(self, root: Path):
        return CliRunner().invoke(cli, ["--root", str(root), "pipeline", "validate", "--show-resolved-rules"])

    def test_lists_every_core_rule_with_state_and_origin(self, make_project):
        root = make_project(
            config="""
                [dlt_ops]

                [dlt_ops.rules]
                schedule_required = false
            """
        )
        result = self._invoke(root)
        assert result.exit_code == 0, result.output
        for rule_id in EXPECTED_CORE_IDS:
            assert rule_id in result.output
        assert "core" in result.output
        schedule_line = next(line for line in result.output.splitlines() if "schedule_required" in line)
        assert "off" in schedule_line
        contract_line = next(line for line in result.output.splitlines() if "schema_contract_declared" in line)
        assert "on" in contract_line

    def test_unknown_configured_rule_id_exits_1(self, make_project):
        root = make_project(
            config="""
                [dlt_ops]

                [dlt_ops.rules]
                bogus_rule = false
            """
        )
        result = self._invoke(root)
        assert result.exit_code == 1
        assert "bogus_rule" in result.output
        assert "valid rule ids" in result.output

    def test_soft_failed_provider_listed_as_unavailable_with_reason(self, extra_entry_points, make_project):
        extra_entry_points("validators", "broken_rules", "dltx_missing_rules_mod:provider", "acme-broken")
        result = self._invoke(make_project())
        assert result.exit_code == 0, result.output
        assert "Unavailable rule providers" in result.output
        assert "broken_rules" in result.output
        assert "ModuleNotFoundError" in result.output

    def test_never_imports_source_modules(self, make_project):
        """Rule listing is config-only — no discovery, no sandbox, no imports."""
        canary_source = """
            from pathlib import Path

            import dlt

            Path(__file__).with_name("canary.txt").write_text("side effect")

            @dlt.source(name="probe_api")
            def probe_api_source():
                return []
        """
        root = make_project(files={"probe/source/probe_api.py": canary_source})
        result = self._invoke(root)
        assert result.exit_code == 0, result.output
        assert not (root / "probe" / "source" / "canary.txt").exists()


class TestValidateCliStrict:
    """--strict must fail on warnings — the import-failure bypass closes via the orphan warning."""

    # Config section with no matching source -> orphan warning. This is what a
    # source whose module fails to import looks like: discovery drops it, only
    # the orphan section remains.
    CONFIG = """
        [dlt_ops]

        [sources.ghost_api]
        base_url = "https://example.test"
    """

    def test_strict_keeps_warnings(self, make_project):
        errors = validate_sources(make_project(config=self.CONFIG), strict=True)
        assert any(e.is_warning and "Orphan" in e.message for e in errors)

    def test_non_strict_filters_warnings(self, make_project):
        assert validate_sources(make_project(config=self.CONFIG), strict=False) == []

    def test_cli_strict_exits_nonzero_on_warning(self, make_project):
        root = make_project(config=self.CONFIG)
        result = CliRunner().invoke(cli, ["--root", str(root), "pipeline", "validate", "--strict"])
        assert result.exit_code == 1
        assert "warnings treated as errors" in result.output

    def test_cli_non_strict_exits_zero_on_warning(self, make_project):
        root = make_project(config=self.CONFIG)
        result = CliRunner().invoke(cli, ["--root", str(root), "pipeline", "validate"])
        assert result.exit_code == 0


class TestValidatorProtocol:
    """The Validator protocol and the custom-validators escape hatch."""

    def test_custom_validator_signature(self):
        def custom_validator(ctx: ValidationContext) -> list[ValidationError]:
            return [ValidationError(source_name="test", field="custom", message="Custom validation message")]

        validator: Validator = custom_validator
        ctx = ValidationContext(sources={}, config={}, project_root=Path("/test"))
        errors = validator(ctx)
        assert len(errors) == 1
        assert errors[0].field == "custom"

    def test_validate_sources_uses_custom_validators(self, make_project):
        """Custom lists bypass rule resolution: exactly these callables run."""
        custom_called = []

        def tracking_validator(ctx: ValidationContext) -> list[ValidationError]:
            custom_called.append(True)
            return []

        validate_sources(make_project(), validators=[tracking_validator])
        assert len(custom_called) == 1

    def test_validate_sources_defaults_to_resolved_rules(self, make_project):
        errors = validate_sources(make_project())
        assert isinstance(errors, list)


class TestValidationContext:
    def test_creation(self):
        ctx = ValidationContext(sources={}, config={"sources": {}}, project_root=Path("/test"))
        assert ctx.sources == {}
        assert ctx.config == {"sources": {}}
        assert ctx.project_root == Path("/test")
        assert ctx.resolved_rules == {}
        assert ctx.exemptions == {}

    def test_immutable(self):
        ctx = ValidationContext(sources={}, config={}, project_root=Path("/test"))
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            ctx.sources = {"new": "value"}

    def test_rule_enabled_defaults_on(self):
        ctx = ValidationContext(sources={}, config={}, project_root=Path("/test"))
        assert ctx.rule_enabled("schedule_required") is True
        ctx = ValidationContext(
            sources={},
            config={},
            project_root=Path("/test"),
            resolved_rules={"schedule_required": False},
        )
        assert ctx.rule_enabled("schedule_required") is False

    def test_is_exempt(self):
        ctx = ValidationContext(
            sources={},
            config={},
            project_root=Path("/test"),
            exemptions={"events_api": {"schedule_required": "why"}},
        )
        assert ctx.is_exempt("events_api", "schedule_required") is True
        assert ctx.is_exempt("events_api", "config_section_required") is False
        assert ctx.is_exempt("orders_api", "schedule_required") is False


# --- Per-validator unit tests (config rules) ---


class _SecretsValue:
    """Sentinel whose repr mimics a dlt.secrets.value default parameter."""

    def __repr__(self) -> str:
        return "dlt.secrets.value"


def _secrets_source(api_key: Any = _SecretsValue()) -> None:
    return None


def _config_ctx(
    config: dict,
    *,
    name: str = "events_api",
    module_stem: str | None = None,
    decorator_name: str | None = "events_api",
    source_fn: Any = None,
) -> ValidationContext:
    source = SourceInfo(
        name=name,
        pipeline_name="events",
        path=Path("/proj/events"),
        function_name=f"{name}_source",
        source_fn=source_fn or (lambda: None),
        resources=("rows",),
        module_stem=module_stem or name,
        decorator_name=decorator_name,
    )
    return ValidationContext(sources={name: source}, config=config, project_root=Path("/proj"))


class TestConfigValidators:
    def test_config_section_missing_fails(self):
        from dlt_ops.discovery.validators.config import validate_config_sections

        errors = validate_config_sections(_config_ctx({"sources": {}}))
        assert len(errors) == 1
        assert "Missing config section" in errors[0].message

    def test_config_section_present_passes(self):
        from dlt_ops.discovery.validators.config import validate_config_sections

        assert validate_config_sections(_config_ctx({"sources": {"events_api": {}}})) == []

    def test_schedule_missing_fails(self):
        from dlt_ops.discovery.validators.config import validate_schedules

        errors = validate_schedules(_config_ctx({"sources": {"events_api": {"dlt_ops": {}}}}))
        assert len(errors) == 1
        assert "Missing 'schedule'" in errors[0].message

    def test_schedule_invalid_fails(self):
        from dlt_ops.discovery.validators.config import validate_schedules

        config = {"sources": {"events_api": {"dlt_ops": {"schedule": "@fortnightly"}}}}
        errors = validate_schedules(_config_ctx(config))
        assert len(errors) == 1
        assert "Invalid schedule" in errors[0].message

    def test_schedule_valid_passes(self):
        from dlt_ops.discovery.validators.config import validate_schedules

        config = {"sources": {"events_api": {"dlt_ops": {"schedule": "@daily"}}}}
        assert validate_schedules(_config_ctx(config)) == []

    def test_missing_decorator_name_fails(self):
        from dlt_ops.discovery.validators.config import validate_decorator_names

        errors = validate_decorator_names(_config_ctx({}, decorator_name=None))
        assert len(errors) == 1
        assert "@dlt.source(name=" in errors[0].message

    def test_explicit_decorator_name_passes(self):
        from dlt_ops.discovery.validators.config import validate_decorator_names

        assert validate_decorator_names(_config_ctx({})) == []

    def test_module_name_mismatch_fails(self):
        from dlt_ops.discovery.validators.config import validate_module_names

        errors = validate_module_names(_config_ctx({}, module_stem="wrong_module"))
        assert len(errors) == 1
        assert "Module filename mismatch" in errors[0].message

    def test_module_name_match_passes(self):
        from dlt_ops.discovery.validators.config import validate_module_names

        assert validate_module_names(_config_ctx({})) == []

    def test_orphan_section_warns(self):
        from dlt_ops.discovery.validators.config import validate_orphan_sections

        errors = validate_orphan_sections(_config_ctx({"sources": {"events_api": {}, "ghost_api": {}}}))
        assert len(errors) == 1
        assert errors[0].is_warning
        assert "Orphan" in errors[0].message

    def test_known_non_source_sections_not_orphans(self):
        from dlt_ops.discovery.validators.config import validate_orphan_sections

        config = {"sources": {"events_api": {}, "data_writer": {}, "extract": {}}}
        assert validate_orphan_sections(_config_ctx(config)) == []


class TestAirflowVarRequiredBody:
    """The `airflow_var_required` rule body, which lives in the Airflow plugin.

    Tested here because the ValidationContext helpers live here; the rule's
    registration and end-to-end behaviour are in tests/test_airflow_runtime.py.
    Ungated on purpose — the plugin's validator module is importable without
    Airflow, and that property is what keeps a bare install's `plugins doctor`
    green.
    """

    def test_airflow_var_missing_for_secrets_source_fails(self):
        from dlt_ops.airflow.validators import validate_airflow_vars

        config = {"sources": {"events_api": {"dlt_ops": {}}}}
        errors = validate_airflow_vars(_config_ctx(config, source_fn=_secrets_source))
        assert len(errors) == 1
        assert "airflow_var" in errors[0].message

    def test_airflow_var_present_passes(self):
        from dlt_ops.airflow.validators import validate_airflow_vars

        config = {"sources": {"events_api": {"dlt_ops": {"airflow_var": "EVENTS_API_SECRETS"}}}}
        assert validate_airflow_vars(_config_ctx(config, source_fn=_secrets_source)) == []

    def test_airflow_var_not_required_without_secrets(self):
        from dlt_ops.airflow.validators import validate_airflow_vars

        assert validate_airflow_vars(_config_ctx({"sources": {"events_api": {"dlt_ops": {}}}})) == []

    def test_core_validators_do_not_re_export_the_rule(self):
        """Core owns no orchestrator rule — the boundary CORE_RULES claims."""
        import dlt_ops.discovery.validators as core_validators
        import dlt_ops.discovery.validators.config as core_config

        assert not hasattr(core_validators, "validate_airflow_vars")
        assert not hasattr(core_config, "validate_airflow_vars")


# --- destination_capability ---


class _IncompleteFilesystemAdapter:
    """Registered destination adapter missing the DestinationAdapter capability surface."""

    name = "filesystem"


class TestDestinationCapabilityValidator:
    """Per-source branches: config chain, typo guard, adapter probe, core-mode warning + upgrades."""

    def _ctx(
        self,
        tmp_path: Path,
        config_toml: str = "[dlt_ops]\n",
        *,
        source_config: SourceConfig | None = None,
        uses_checkpoints: bool = False,
    ) -> ValidationContext:
        """ctx whose project_root carries a real marker — the rule loads ProjectConfig from disk."""
        (tmp_path / ".dlt").mkdir(parents=True, exist_ok=True)
        (tmp_path / ".dlt" / "config.toml").write_text(dedent(config_toml))
        source = SourceInfo(
            name="events_api",
            pipeline_name="events",
            path=tmp_path / "events",
            function_name="events_api_source",
            source_fn=lambda: None,
            resources=("rows",),
            module_stem="events_api",
            config=source_config,
            uses_checkpoints=uses_checkpoints,
        )
        return ValidationContext(
            sources={"events_api": source},
            config=tomllib.loads(dedent(config_toml)),
            project_root=tmp_path,
        )

    def test_unresolved_destination_is_error(self, tmp_path):
        from dlt_ops.discovery.validators.config import validate_destination_capability

        errors = validate_destination_capability(self._ctx(tmp_path))
        assert len(errors) == 1
        assert errors[0].source_name == "events_api"
        assert errors[0].field == "destination"
        assert not errors[0].is_warning
        assert "No destination configured" in errors[0].message

    def test_typo_destination_is_error_with_dlt_wording(self, tmp_path):
        from dlt_ops.discovery.validators.config import validate_destination_capability

        ctx = self._ctx(tmp_path, '[dlt_ops]\ndefault_destination = "snowflaek"\n')
        errors = validate_destination_capability(ctx)
        assert len(errors) == 1
        assert not errors[0].is_warning
        assert "'snowflaek' is not a dlt destination" in errors[0].message

    def test_core_tier_destination_warns_naming_dark_features(self, tmp_path):
        from dlt_ops.destinations import ADAPTER_GATED_FEATURES
        from dlt_ops.discovery.validators.config import validate_destination_capability

        ctx = self._ctx(tmp_path, '[dlt_ops]\ndefault_destination = "filesystem"\n')
        errors = validate_destination_capability(ctx)
        assert len(errors) == 1
        assert errors[0].field == "destination"
        assert errors[0].is_warning
        assert "core mode" in errors[0].message
        for feature in ADAPTER_GATED_FEATURES:
            assert feature in errors[0].message

    def test_quarantine_engagement_upgrades_to_error(self, tmp_path):
        from dlt_ops.discovery.validators.config import validate_destination_capability

        config = """
            [dlt_ops]
            default_destination = "filesystem"

            [sources.events_api.dlt_ops.assertions.rows]
            min_rows_per_load = { value = 1, on_failure = "quarantine" }
        """
        errors = validate_destination_capability(self._ctx(tmp_path, config))
        assert len(errors) == 1
        assert not errors[0].is_warning
        assert "assertion quarantine" in errors[0].message
        assert "rows" in errors[0].message

    def test_quarantine_on_other_source_does_not_upgrade(self, tmp_path):
        """Engagement is per source: a sibling's quarantine config leaves this source a warning."""
        from dlt_ops.discovery.validators.config import validate_destination_capability

        config = """
            [dlt_ops]
            default_destination = "filesystem"

            [sources.orders_api.dlt_ops.assertions.orders]
            min_rows_per_load = { value = 1, on_failure = "quarantine" }
        """
        errors = validate_destination_capability(self._ctx(tmp_path, config))
        assert len(errors) == 1
        assert errors[0].is_warning

    def test_checkpoints_engagement_upgrades_to_error(self, tmp_path):
        from dlt_ops.discovery.validators.config import validate_destination_capability

        ctx = self._ctx(tmp_path, '[dlt_ops]\ndefault_destination = "filesystem"\n', uses_checkpoints=True)
        errors = validate_destination_capability(ctx)
        assert len(errors) == 1
        assert not errors[0].is_warning
        assert "checkpoints" in errors[0].message

    def test_require_destination_adapter_knob_upgrades_to_error(self, tmp_path):
        from dlt_ops.discovery.validators.config import validate_destination_capability

        config = """
            [dlt_ops]
            default_destination = "filesystem"
            require_destination_adapter = true
        """
        errors = validate_destination_capability(self._ctx(tmp_path, config))
        assert len(errors) == 1
        assert not errors[0].is_warning
        assert "require_destination_adapter" in errors[0].message

    def test_full_tier_destination_is_silent(self, tmp_path):
        from dlt_ops.discovery.validators.config import validate_destination_capability

        ctx = self._ctx(tmp_path, '[dlt_ops]\ndefault_destination = "duckdb"\n')
        assert validate_destination_capability(ctx) == []

    def test_per_source_override_resolves_without_project_default(self, tmp_path):
        from dlt_ops.discovery.validators.config import validate_destination_capability

        ctx = self._ctx(tmp_path, source_config=SourceConfig(schedule=Schedule.HOURLY, destination="duckdb"))
        assert validate_destination_capability(ctx) == []

    def test_registered_but_load_failing_adapter_is_error(self, tmp_path, extra_entry_points):
        from dlt_ops.discovery.validators.config import validate_destination_capability

        extra_entry_points("destination", "filesystem", "dltx_missing_adapter_mod:Adapter", "acme-fs")
        ctx = self._ctx(tmp_path, '[dlt_ops]\ndefault_destination = "filesystem"\n')
        errors = validate_destination_capability(ctx)
        assert len(errors) == 1
        assert not errors[0].is_warning
        assert "failed to load" in errors[0].message

    def test_capability_incomplete_adapter_is_error(self, tmp_path):
        from dlt_ops.discovery.validators.config import validate_destination_capability
        from dlt_ops.plugins import register

        register("destination", "filesystem")(_IncompleteFilesystemAdapter)
        ctx = self._ctx(tmp_path, '[dlt_ops]\ndefault_destination = "filesystem"\n')
        errors = validate_destination_capability(ctx)
        assert len(errors) == 1
        assert not errors[0].is_warning
        assert "missing required capability member" in errors[0].message


class TestDestinationCapabilityEndToEnd:
    """The rule through validate_sources: warning surfacing, the knob, per-source exemptions."""

    FILES = {"events/source/events_api.py": _source_body("events_api")}
    # import_safety off skips the sandbox child — keeps these tests fast; the
    # behavior under test is destination_capability.
    CORE_TIER_CONFIG = """
        [dlt_ops]
        default_destination = "filesystem"

        [dlt_ops.rules]
        import_safety = false

        [sources.events_api.dlt_ops]
        schedule = "@daily"
    """

    def test_core_tier_warning_kept_in_strict_filtered_otherwise(self, make_project):
        root = make_project(config=self.CORE_TIER_CONFIG, files=self.FILES)
        strict = [e for e in validate_sources(root, strict=True) if e.field == "destination"]
        assert len(strict) == 1
        assert strict[0].is_warning
        assert "core mode" in strict[0].message
        assert [e for e in validate_sources(root) if e.field == "destination"] == []

    def test_knob_disables_exactly_this_rule(self, make_project):
        # No destination anywhere in config: the rule errors when enabled.
        no_destination = """
            [dlt_ops]

            [dlt_ops.rules]
            import_safety = false
            {knob}

            [sources.events_api.dlt_ops]
            schedule = "@daily"
        """
        on_root = make_project(config=no_destination.format(knob=""), files=self.FILES, name="rule_on")
        off_root = make_project(
            config=no_destination.format(knob="destination_capability = false"), files=self.FILES, name="rule_off"
        )
        assert [e for e in validate_sources(on_root) if e.field == "destination"]
        assert [e for e in validate_sources(off_root) if e.field == "destination"] == []

    def test_exemption_suppresses_per_source(self, make_project):
        root = make_project(
            config="""
                [dlt_ops]
                default_destination = "filesystem"

                [dlt_ops.rules]
                import_safety = false

                [sources.events_api.dlt_ops]
                schedule = "@daily"

                [sources.events_api.dlt_ops.rule_exemptions]
                destination_capability = "core tier accepted: no ledger consumers for this source"
            """,
            files=self.FILES,
        )
        assert [e for e in validate_sources(root, strict=True) if e.field == "destination"] == []


class TestResourceOverlapValidator:
    def _source(self, name: str, pipeline: str, resources: tuple[str, ...]) -> SourceInfo:
        return SourceInfo(
            name=name,
            pipeline_name=pipeline,
            path=Path(f"/proj/{pipeline}"),
            function_name=f"{name}_source",
            source_fn=lambda: None,
            resources=resources,
            module_stem=name,
        )

    def test_overlap_in_same_pipeline_fails(self):
        from dlt_ops.discovery.validators.resources import validate_no_resource_overlap

        ctx = ValidationContext(
            sources={
                "a_api": self._source("a_api", "shared", ("rows",)),
                "b_api": self._source("b_api", "shared", ("rows",)),
            },
            config={},
            project_root=Path("/proj"),
        )
        errors = validate_no_resource_overlap(ctx)
        assert len(errors) == 1
        assert "already defined" in errors[0].message

    def test_same_resource_across_pipelines_passes(self):
        from dlt_ops.discovery.validators.resources import validate_no_resource_overlap

        ctx = ValidationContext(
            sources={
                "a_api": self._source("a_api", "alpha", ("rows",)),
                "b_api": self._source("b_api", "beta", ("rows",)),
            },
            config={},
            project_root=Path("/proj"),
        )
        assert validate_no_resource_overlap(ctx) == []


# --- Pydantic dict-field detection (json_hints_for_dict_fields internals) ---


class TestPydanticDictFieldDetection:
    def test_is_dict_type_plain_dict(self):
        from dlt_ops.discovery.validators.schema import _is_dict_type

        assert _is_dict_type(dict) is True

    def test_is_dict_type_generic_dict(self):
        from dlt_ops.discovery.validators.schema import _is_dict_type

        assert _is_dict_type(dict[str, Any]) is True

    def test_is_dict_type_list_of_dict(self):
        from dlt_ops.discovery.validators.schema import _is_dict_type

        assert _is_dict_type(list[dict]) is True

    def test_is_dict_type_list_of_generic_dict(self):
        from dlt_ops.discovery.validators.schema import _is_dict_type

        assert _is_dict_type(list[dict[str, Any]]) is True

    def test_is_dict_type_plain_list(self):
        from dlt_ops.discovery.validators.schema import _is_dict_type

        assert _is_dict_type(list) is False

    def test_is_dict_type_list_of_str(self):
        from dlt_ops.discovery.validators.schema import _is_dict_type

        assert _is_dict_type(list[str]) is False

    def test_is_dict_type_str(self):
        from dlt_ops.discovery.validators.schema import _is_dict_type

        assert _is_dict_type(str) is False

    def test_get_pydantic_dict_fields_simple(self):
        from dlt_ops.discovery.validators.schema import _get_pydantic_dict_fields

        class Model(pydantic.BaseModel):
            name: str
            data: dict

        assert _get_pydantic_dict_fields(Model) == ["data"]

    def test_get_pydantic_dict_fields_list_of_dict(self):
        from dlt_ops.discovery.validators.schema import _get_pydantic_dict_fields

        class Model(pydantic.BaseModel):
            name: str
            items: list[dict]

        assert _get_pydantic_dict_fields(Model) == ["items"]

    def test_get_pydantic_dict_fields_optional_dict(self):
        from dlt_ops.discovery.validators.schema import _get_pydantic_dict_fields

        class Model(pydantic.BaseModel):
            name: str
            config: Optional[dict] = None

        assert _get_pydantic_dict_fields(Model) == ["config"]

    def test_get_pydantic_dict_fields_multiple(self):
        from dlt_ops.discovery.validators.schema import _get_pydantic_dict_fields

        class Model(pydantic.BaseModel):
            name: str
            query: dict
            filters: list[dict]
            settings: Optional[dict] = None

        assert set(_get_pydantic_dict_fields(Model)) == {"query", "filters", "settings"}

    def test_get_pydantic_dict_fields_no_dicts(self):
        from dlt_ops.discovery.validators.schema import _get_pydantic_dict_fields

        class Model(pydantic.BaseModel):
            name: str
            count: int
            tags: list[str]

        assert _get_pydantic_dict_fields(Model) == []


# --- pydantic_columns_required internals ---


class TestResourceColumnsHintValidator:
    def test_has_columns_kwarg_present(self, tmp_path):
        from dlt_ops.discovery.validators.schema import _has_columns_kwarg

        source_file = tmp_path / "resource.py"
        source_file.write_text(
            "import dlt\n"
            "import pydantic\n\n"
            "class MyModel(pydantic.BaseModel):\n"
            "    id: int\n\n"
            "@dlt.resource(columns=MyModel, write_disposition='replace')\n"
            "def my_resource():\n"
            "    yield []\n"
        )
        assert _has_columns_kwarg(source_file, "my_resource") is True

    def test_has_columns_kwarg_missing(self, tmp_path):
        from dlt_ops.discovery.validators.schema import _has_columns_kwarg

        source_file = tmp_path / "resource.py"
        source_file.write_text(
            "import dlt\n\n@dlt.resource(write_disposition='replace')\ndef my_resource():\n    yield []\n"
        )
        assert _has_columns_kwarg(source_file, "my_resource") is False

    def test_has_columns_kwarg_dict_columns(self, tmp_path):
        from dlt_ops.discovery.validators.schema import _has_columns_kwarg

        source_file = tmp_path / "resource.py"
        source_file.write_text(
            'import dlt\n\n@dlt.resource(columns={"id": {"data_type": "bigint"}})\ndef my_resource():\n    yield []\n'
        )
        assert _has_columns_kwarg(source_file, "my_resource") is True

    def test_has_columns_kwarg_no_dlt_resource_decorator(self, tmp_path):
        from dlt_ops.discovery.validators.schema import _has_columns_kwarg

        source_file = tmp_path / "resource.py"
        source_file.write_text(
            "def some_decorator(fn):\n    return fn\n\n@some_decorator\ndef my_resource():\n    yield []\n"
        )
        assert _has_columns_kwarg(source_file, "my_resource") is False

    def test_has_columns_kwarg_wrong_function_name(self, tmp_path):
        from dlt_ops.discovery.validators.schema import _has_columns_kwarg

        source_file = tmp_path / "resource.py"
        source_file.write_text(
            "import dlt\n"
            "import pydantic\n\n"
            "class MyModel(pydantic.BaseModel):\n"
            "    id: int\n\n"
            "@dlt.resource(columns=MyModel)\n"
            "def other_resource():\n"
            "    yield []\n"
        )
        assert _has_columns_kwarg(source_file, "my_resource") is False


class TestColumnsPydanticCheck:
    def test_attribute_form_accepted(self, tmp_path):
        import ast

        from dlt_ops.discovery.validators.schema import _check_columns_is_pydantic

        node = ast.parse("cfg.model", mode="eval").body
        assert _check_columns_is_pydantic("src", "res", tmp_path / "f.py", "_resource", "mod", node) is None

    def test_dict_form_rejected(self, tmp_path):
        import ast

        from dlt_ops.discovery.validators.schema import _check_columns_is_pydantic

        node = ast.parse('{"id": {"data_type": "bigint"}}', mode="eval").body
        error = _check_columns_is_pydantic("src", "res", tmp_path / "f.py", "_resource", "mod", node)
        assert error is not None
        assert "Pydantic model" in error.message


# --- pydantic_model_forbids_extra ---


class TestPydanticModelForbidsExtra:
    """The rule reads the contract dlt derived from the model rather than
    re-deriving it, so these cases run real `@dlt.resource` decorations and
    assert against dlt 1.29's actual mapping of `extra` -> column mode."""

    def _ctx(self, tmp_path, resource) -> ValidationContext:
        instance = types.SimpleNamespace(resources={"events": resource})
        source = SourceInfo(
            name="my_api",
            pipeline_name="my_pipe",
            path=tmp_path,
            function_name="my_api_source",
            source_fn=lambda: instance,
            resources=("events",),
            module_stem="my_api",
        )
        return ValidationContext(sources={"my_api": source}, config={}, project_root=tmp_path)

    def _resource(self, tmp_path, **kwargs):
        import dlt

        @dlt.resource(name="events", **kwargs)
        def events():
            yield [{"id": 1}]

        return events

    def _errors(self, tmp_path, **kwargs):
        from dlt_ops.discovery.validators.schema import validate_pydantic_model_forbids_extra

        return validate_pydantic_model_forbids_extra(self._ctx(tmp_path, self._resource(tmp_path, **kwargs)))

    def test_model_leaving_extra_unset_is_an_error(self, tmp_path):
        class Loose(pydantic.BaseModel):
            id: int

        errors = self._errors(tmp_path, columns=Loose)
        assert len(errors) == 1
        assert errors[0].field == "resource.events"

    def test_error_names_the_model_and_the_exact_line_to_add(self, tmp_path):
        class Loose(pydantic.BaseModel):
            id: int

        message = self._errors(tmp_path, columns=Loose)[0].message
        assert "Loose" in message
        assert 'model_config = pydantic.ConfigDict(extra="forbid")' in message
        assert "discard_value" in message

    def test_model_forbidding_extra_passes(self, tmp_path):
        class Strict(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(extra="forbid")
            id: int

        assert self._errors(tmp_path, columns=Strict) == []

    def test_model_allowing_extra_passes(self, tmp_path):
        """`extra="allow"` derives the evolve contract — the opt-in route, not
        silent loss. Whether the source may opt in is `schema_contract_declared`'s
        business, not this rule's."""

        class Evolving(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(extra="allow")
            id: int

        assert self._errors(tmp_path, columns=Evolving) == []

    def test_explicit_extra_ignore_is_an_error(self, tmp_path):
        """Spelling the silent drop out loud is still a silent drop."""

        class Ignoring(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(extra="ignore")
            id: int

        assert len(self._errors(tmp_path, columns=Ignoring)) == 1

    def test_inherited_model_config_is_honored(self, tmp_path):
        """Pydantic merges `model_config` up the MRO and dlt reads the merged
        value, so a strict base is enough."""

        class StrictBase(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(extra="forbid")

        class Child(StrictBase):
            id: int

        assert self._errors(tmp_path, columns=Child) == []

    def test_explicit_canonical_contract_rescues_a_loose_model(self, tmp_path):
        """dlt lets an explicit `schema_contract=` override the model's `extra`,
        so this resource does freeze at run time and must not be flagged."""

        class Loose(pydantic.BaseModel):
            id: int

        from dlt_ops.schema_contracts import CANONICAL_SCHEMA_CONTRACT

        errors = self._errors(tmp_path, columns=Loose, schema_contract=dict(CANONICAL_SCHEMA_CONTRACT))
        assert errors == []

    def test_dict_columns_resource_is_not_this_rules_business(self, tmp_path):
        """dlt derives no contract from a dict `columns=`; the runner supplies
        the canonical literal for those at run time."""
        assert self._errors(tmp_path, columns={"id": {"data_type": "bigint"}}) == []

    def test_resource_without_columns_is_not_flagged(self, tmp_path):
        assert self._errors(tmp_path) == []

    def test_uninstantiable_source_is_skipped_not_failed(self, tmp_path):
        from dlt_ops.discovery.validators.schema import validate_pydantic_model_forbids_extra

        def boom():
            raise RuntimeError("cannot build")

        source = SourceInfo(
            name="broken",
            pipeline_name="my_pipe",
            path=tmp_path,
            function_name="broken_source",
            source_fn=boom,
            resources=(),
            module_stem="broken",
        )
        ctx = ValidationContext(sources={"broken": source}, config={}, project_root=tmp_path)
        assert validate_pydantic_model_forbids_extra(ctx) == []


# --- schema_contract_declared ---


class TestSchemaContractValidator:
    CANONICAL = 'schema_contract={"tables": "evolve", "columns": "freeze", "data_type": "freeze"}'
    EVOLVE_LITERAL = 'schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "freeze"}'

    def _write_resource(self, ctx: ValidationContext, body: str) -> None:
        pipeline_dir = next(iter(ctx.sources.values())).path
        (pipeline_dir / "resource.py").write_text(body)

    def test_canonical_contract_passes(self, tmp_path):
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        ctx = _make_ctx(tmp_path)
        self._write_resource(ctx, f"import dlt\n\n@dlt.resource({self.CANONICAL})\ndef my_resource():\n    yield []\n")
        assert validate_schema_contract(ctx) == []

    def test_missing_contract_passes(self, tmp_path):
        """Absent contract is fine — the runtime auto-applies the canonical literal."""
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        ctx = _make_ctx(tmp_path)
        self._write_resource(
            ctx, "import dlt\n\n@dlt.resource(write_disposition='append')\ndef my_resource():\n    yield []\n"
        )
        assert validate_schema_contract(ctx) == []

    def test_non_canonical_contract_fails(self, tmp_path):
        """Truly non-canonical literal (neither CANONICAL nor EVOLVE) fails.

        The fixture pins config to have no `schema_contract_evolve_reason`, so
        the failure mode is the "not one of the two accepted shapes" branch —
        not the evolve-opt-in branch tested elsewhere.
        """
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        ctx = _make_ctx(tmp_path)  # config=None -> not opted in for evolve
        self._write_resource(
            ctx,
            'import dlt\n\n@dlt.resource(schema_contract={"tables": "evolve", "columns": "discard_row", "data_type": "freeze"})\ndef my_resource():\n    yield []\n',
        )
        errors = validate_schema_contract(ctx)
        assert len(errors) == 1
        assert "non-canonical" in errors[0].message

    def test_non_literal_contract_fails(self, tmp_path):
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        ctx = _make_ctx(tmp_path)
        self._write_resource(
            ctx,
            "import dlt\n\nCONTRACT = {}\n\n@dlt.resource(schema_contract=CONTRACT)\ndef my_resource():\n    yield []\n",
        )
        assert len(validate_schema_contract(ctx)) == 1

    def test_tests_dir_excluded(self, tmp_path):
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        ctx = _make_ctx(tmp_path)
        pipeline_dir = next(iter(ctx.sources.values())).path
        tests_dir = pipeline_dir / "tests"
        tests_dir.mkdir()
        (tests_dir / "test_x.py").write_text(
            'import dlt\n\n@dlt.resource(schema_contract={"tables": "discard_row"})\ndef fixture_resource():\n    yield []\n'
        )
        assert validate_schema_contract(ctx) == []

    # --- evolve contract opt-in via schema_contract_evolve_reason ---

    def test_evolve_contract_passes_when_config_reason_set(self, tmp_path):
        """Evolve literal accepted when the source's config carries a non-empty reason."""
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        ctx = _make_ctx(tmp_path, schema_contract_evolve_reason="Provider ships nullable additive fields.")
        self._write_resource(
            ctx,
            f"import dlt\n\n@dlt.resource({self.EVOLVE_LITERAL})\ndef my_resource():\n    yield []\n",
        )
        assert validate_schema_contract(ctx) == []

    def test_evolve_contract_fails_when_config_reason_missing(self, tmp_path):
        """Evolve literal rejected when the source has no config block at all."""
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        ctx = _make_ctx(tmp_path)  # source.config is None (orphan/misconfigured)
        self._write_resource(
            ctx,
            f"import dlt\n\n@dlt.resource({self.EVOLVE_LITERAL})\ndef my_resource():\n    yield []\n",
        )
        errors = validate_schema_contract(ctx)
        assert len(errors) == 1
        assert "schema_contract_evolve_reason" in errors[0].message

    def test_evolve_contract_fails_when_config_reason_empty_string(self, tmp_path):
        """Empty string is treated as absence — opt-in requires justification."""
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        ctx = _make_ctx(tmp_path, schema_contract_evolve_reason="   ")
        self._write_resource(
            ctx,
            f"import dlt\n\n@dlt.resource({self.EVOLVE_LITERAL})\ndef my_resource():\n    yield []\n",
        )
        errors = validate_schema_contract(ctx)
        assert len(errors) == 1
        assert "schema_contract_evolve_reason" in errors[0].message

    def test_canonical_still_passes_regardless_of_config(self, tmp_path):
        """Freeze literal is always accepted, with or without the reason set."""
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        for reason in (None, "opted in"):
            ctx = _make_ctx(tmp_path / f"case_{reason}", schema_contract_evolve_reason=reason)
            self._write_resource(
                ctx,
                f"import dlt\n\n@dlt.resource({self.CANONICAL})\ndef my_resource():\n    yield []\n",
            )
            assert validate_schema_contract(ctx) == []

    def test_evolve_contract_multi_source_dir_only_configured_source(self, tmp_path):
        """Two sources in one pipeline dir. Evolve literal only accepted for the
        source with `schema_contract_evolve_reason` set.

        The AST walk finds two `@dlt.resource` calls in the shared dir;
        the ownership map routes each to its own source, and the exemption
        decision keys off `owning_source.config`.
        """
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        pipeline_dir = tmp_path / "webshop"
        pipeline_dir.mkdir()
        # Two @dlt.resource calls with distinct name= kwargs, both evolve literal.
        (pipeline_dir / "res_a.py").write_text(
            f'import dlt\n\n@dlt.resource(name="res_a", {self.EVOLVE_LITERAL})\ndef _res_a():\n    yield []\n'
        )
        (pipeline_dir / "res_b.py").write_text(
            f'import dlt\n\n@dlt.resource(name="res_b", {self.EVOLVE_LITERAL})\ndef _res_b():\n    yield []\n'
        )
        cfg_with_reason = SourceConfig(schedule=Schedule.HOURLY, schema_contract_evolve_reason="justified")
        cfg_without = SourceConfig(schedule=Schedule.HOURLY)
        source_a = SourceInfo(
            name="src_a",
            pipeline_name="webshop",
            path=pipeline_dir,
            function_name="src_a_source",
            source_fn=lambda: None,
            resources=("res_a",),
            module_stem="src_a",
            config=cfg_with_reason,
        )
        source_b = SourceInfo(
            name="src_b",
            pipeline_name="webshop",
            path=pipeline_dir,
            function_name="src_b_source",
            source_fn=lambda: None,
            resources=("res_b",),
            module_stem="src_b",
            config=cfg_without,
        )
        ctx = ValidationContext(sources={"src_a": source_a, "src_b": source_b}, config={}, project_root=tmp_path)
        errors = validate_schema_contract(ctx)
        assert len(errors) == 1
        assert errors[0].source_name == "src_b"
        assert "schema_contract_evolve_reason" in errors[0].message

    def test_missing_name_kwarg_in_multi_source_dir_fails(self, tmp_path):
        """Companion validator: @dlt.resource without name= in a multi-source dir errors."""
        from dlt_ops.discovery.validators.platform_rules import (
            validate_resource_name_explicit_in_multi_source_dir,
        )

        pipeline_dir = tmp_path / "webshop"
        pipeline_dir.mkdir()
        (pipeline_dir / "res_a.py").write_text(
            f"import dlt\n\n@dlt.resource({self.CANONICAL})\ndef _res_a():\n    yield []\n"
        )
        cfg = SourceConfig(schedule=Schedule.HOURLY)
        source_a = SourceInfo(
            name="src_a",
            pipeline_name="webshop",
            path=pipeline_dir,
            function_name="src_a_source",
            source_fn=lambda: None,
            resources=(),
            module_stem="src_a",
            config=cfg,
        )
        source_b = SourceInfo(
            name="src_b",
            pipeline_name="webshop",
            path=pipeline_dir,
            function_name="src_b_source",
            source_fn=lambda: None,
            resources=(),
            module_stem="src_b",
            config=cfg,
        )
        ctx = ValidationContext(sources={"src_a": source_a, "src_b": source_b}, config={}, project_root=tmp_path)
        errors = validate_resource_name_explicit_in_multi_source_dir(ctx)
        assert len(errors) == 1
        assert "missing an explicit name= kwarg" in errors[0].message
        assert errors[0].source_name == "webshop"

    def test_missing_name_kwarg_in_single_source_dir_passes(self, tmp_path):
        """Companion validator does not fire in single-source dirs."""
        from dlt_ops.discovery.validators.platform_rules import (
            validate_resource_name_explicit_in_multi_source_dir,
        )

        ctx = _make_ctx(tmp_path, attach_config=True)
        self._write_resource(
            ctx,
            f"import dlt\n\n@dlt.resource({self.CANONICAL})\ndef my_resource():\n    yield []\n",
        )
        assert validate_resource_name_explicit_in_multi_source_dir(ctx) == []

    def _unresolvable_owner_ctx(self, tmp_path, body: str) -> ValidationContext:
        """Multi-source dir + a resource whose owner cannot be resolved."""
        pipeline_dir = tmp_path / "multi"
        pipeline_dir.mkdir()
        (pipeline_dir / "res_x.py").write_text(body)
        cfg = SourceConfig(schedule=Schedule.HOURLY)
        sources = {
            name: SourceInfo(
                name=name,
                pipeline_name="multi",
                path=pipeline_dir,
                function_name=f"{name}_source",
                source_fn=lambda: None,
                resources=(),
                module_stem=name,
                config=cfg,
            )
            for name in ("src_a", "src_b")
        }
        return ValidationContext(sources=sources, config={}, project_root=tmp_path)

    def test_unresolvable_owner_missing_contract_passes(self, tmp_path):
        """Absent contract passes even when ownership cannot be resolved —
        the runtime auto-apply covers every resource uniformly."""
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        # Non-literal name= (attribute access) so ownership cannot resolve
        # to a specific source; no schema_contract kwarg at all.
        ctx = self._unresolvable_owner_ctx(
            tmp_path,
            "import dlt\n\nclass Cfg:\n    target_table = 'res_x'\ncfg = Cfg()\n"
            "@dlt.resource(name=cfg.target_table)\ndef _res_x():\n    yield []\n",
        )
        assert validate_schema_contract(ctx) == []

    def test_unresolvable_owner_evolve_literal_fails(self, tmp_path):
        """Evolve declared where no owner resolves: the opt-in cannot be
        attributed to any source, so the literal is rejected."""
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        ctx = self._unresolvable_owner_ctx(
            tmp_path,
            "import dlt\n\nclass Cfg:\n    target_table = 'res_x'\ncfg = Cfg()\n"
            f"@dlt.resource(name=cfg.target_table, {self.EVOLVE_LITERAL})\ndef _res_x():\n    yield []\n",
        )
        errors = validate_schema_contract(ctx)
        assert len(errors) == 1
        assert "no owning source can be resolved" in errors[0].message
        assert errors[0].source_name == "multi"

    def test_unresolvable_owner_non_canonical_literal_fails(self, tmp_path):
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        ctx = self._unresolvable_owner_ctx(
            tmp_path,
            "import dlt\n\nclass Cfg:\n    target_table = 'res_x'\ncfg = Cfg()\n"
            '@dlt.resource(name=cfg.target_table, schema_contract={"tables": "discard_row"})\n'
            "def _res_x():\n    yield []\n",
        )
        errors = validate_schema_contract(ctx)
        assert len(errors) == 1
        assert "non-canonical" in errors[0].message
        assert errors[0].source_name == "multi"

    def test_ownership_map_flags_duplicate_resource_names(self, tmp_path):
        """Belt-and-braces vs validate_no_resource_overlap: if two sources
        declare the same resource name, the rule emits its own error rather
        than silently picking a winner for the exemption check."""
        from dlt_ops.discovery.validators.platform_rules import validate_schema_contract

        pipeline_dir = tmp_path / "shared_pipe"
        pipeline_dir.mkdir()
        cfg = SourceConfig(schedule=Schedule.HOURLY)
        source_a = SourceInfo(
            name="src_a",
            pipeline_name="shared_pipe",
            path=pipeline_dir,
            function_name="src_a_source",
            source_fn=lambda: None,
            resources=("shared_res",),
            module_stem="src_a",
            config=cfg,
        )
        source_b = SourceInfo(
            name="src_b",
            pipeline_name="shared_pipe",
            path=pipeline_dir,
            function_name="src_b_source",
            source_fn=lambda: None,
            resources=("shared_res",),
            module_stem="src_b",
            config=cfg,
        )
        ctx = ValidationContext(sources={"src_a": source_a, "src_b": source_b}, config={}, project_root=tmp_path)
        errors = validate_schema_contract(ctx)
        duplicate_errors = [e for e in errors if "declared by multiple sources" in e.message]
        assert len(duplicate_errors) == 1
        assert duplicate_errors[0].source_name == "src_a|src_b"


# --- incremental_cursor_required ---


class TestIncrementalCursorRequiredValidator:
    """The gap its sibling leaves: a resource with NO cursor at all.

    `cursor_not_load_timestamp` flags a wrong cursor, and only when
    `load_timestamp_column` is set — so a resource that full-refreshes on every
    scheduled run passed validate silently. Opt-in, because a full refresh is a
    legitimate choice the package cannot tell apart from an oversight.
    """

    @staticmethod
    def _source(*, cursor: bool):
        import datetime as dt

        import dlt

        def build():
            if cursor:

                @dlt.resource(name="events")
                def events(ts=dlt.sources.incremental("updated_at", initial_value=dt.datetime(2024, 1, 1))):
                    yield {"id": 1, "updated_at": dt.datetime(2024, 6, 1)}
            else:

                @dlt.resource(name="events")
                def events():
                    yield {"id": 1}

            return dlt.source(lambda: events, name="cursor_probe")()

        return build

    def _ctx(self, tmp_path: Path, *, cursor: bool, schedule: Schedule = Schedule.DAILY) -> ValidationContext:
        info = SourceInfo(
            name="cursor_probe",
            pipeline_name="probe",
            path=tmp_path / "probe",
            function_name="cursor_probe_source",
            resources=("events",),
            module_stem="cursor_probe",
            config=SourceConfig(schedule=schedule),
            source_fn=self._source(cursor=cursor),
        )
        return ValidationContext(sources={"cursor_probe": info}, config={}, project_root=tmp_path)

    def _validate(self, ctx: ValidationContext):
        from dlt_ops.discovery.validators.platform_rules import validate_incremental_cursor_required

        return validate_incremental_cursor_required(ctx)

    def test_missing_cursor_on_a_scheduled_source_is_reported(self, tmp_path):
        errors = self._validate(self._ctx(tmp_path, cursor=False))

        assert len(errors) == 1
        assert errors[0].source_name == "cursor_probe"
        assert errors[0].field == "incremental.events"
        assert "no incremental cursor" in errors[0].message
        assert "re-extracts it in full" in errors[0].message

    def test_it_is_an_error_not_a_warning(self, tmp_path):
        """validate_sources drops warnings outside --strict, so a warning here
        would be invisible in the run that matters."""
        assert all(not e.is_warning for e in self._validate(self._ctx(tmp_path, cursor=False)))

    def test_declared_cursor_passes(self, tmp_path):
        assert self._validate(self._ctx(tmp_path, cursor=True)) == []

    def test_manual_schedule_is_out_of_scope(self, tmp_path):
        """The harm is a full refresh repeating on a cadence; an on-demand
        source re-reads everything when someone asks it to."""
        assert self._validate(self._ctx(tmp_path, cursor=False, schedule=Schedule.MANUAL)) == []

    def test_source_without_parsed_config_is_skipped(self, tmp_path):
        info = SourceInfo(
            name="no_config",
            pipeline_name="probe",
            path=tmp_path / "probe",
            function_name="no_config_source",
            resources=("events",),
            module_stem="no_config",
            source_fn=self._source(cursor=False),
        )
        ctx = ValidationContext(sources={"no_config": info}, config={}, project_root=tmp_path)
        assert self._validate(ctx) == []

    def test_uninstantiable_source_is_skipped_not_flagged(self, tmp_path):
        """Instantiation failure is another rule's finding; guessing "no cursor"
        from it would be a false positive."""

        def boom():
            raise RuntimeError("cannot instantiate")

        info = SourceInfo(
            name="boom_api",
            pipeline_name="probe",
            path=tmp_path / "probe",
            function_name="boom_source",
            resources=("events",),
            module_stem="boom_api",
            config=SourceConfig(schedule=Schedule.DAILY),
            source_fn=boom,
        )
        ctx = ValidationContext(sources={"boom_api": info}, config={}, project_root=tmp_path)
        assert self._validate(ctx) == []

    def test_rule_ships_off_and_turns_on_from_config(self, tmp_path, make_project):
        """End-to-end through validate_sources: silent by default, loud on opt-in."""
        source = dedent("""
            import dlt
            import pydantic

            class Row(pydantic.BaseModel):
                model_config = pydantic.ConfigDict(extra="forbid")
                id: int

            @dlt.resource(name="dim_rows", columns=Row)
            def dim_rows():
                yield {"id": 1}

            @dlt.source(name="dim_api")
            def dim_api_source():
                return dim_rows
        """)
        base = '[dlt_ops]\ndefault_destination = "duckdb"\ndefault_dataset = "raw"\n\n[sources.dim_api.dlt_ops]\nschedule = "@daily"\n'
        files = {"dim/source/dim_api.py": source}

        off = validate_sources(make_project(config=base, files=files, name="off"))
        assert [e for e in off if e.field.startswith("incremental.")] == []

        on_config = base + "\n[dlt_ops.rules]\nincremental_cursor_required = true\n"
        on = validate_sources(make_project(config=on_config, files=files, name="on"))
        assert [e.field for e in on if e.field.startswith("incremental.")] == ["incremental.dim_rows"]

    def test_exemption_records_an_intended_full_refresh(self, tmp_path, make_project):
        """The escape hatch the message names, with its mandatory reason."""
        source = dedent("""
            import dlt
            import pydantic

            class Row(pydantic.BaseModel):
                model_config = pydantic.ConfigDict(extra="forbid")
                id: int

            @dlt.resource(name="dim_rows", columns=Row)
            def dim_rows():
                yield {"id": 1}

            @dlt.source(name="dim_api")
            def dim_api_source():
                return dim_rows
        """)
        config = (
            '[dlt_ops]\ndefault_destination = "duckdb"\ndefault_dataset = "raw"\n\n'
            "[dlt_ops.rules]\nincremental_cursor_required = true\n\n"
            '[sources.dim_api.dlt_ops]\nschedule = "@daily"\n\n'
            "[sources.dim_api.dlt_ops.rule_exemptions]\n"
            'incremental_cursor_required = "small dimension table; a full refresh is intended"\n'
        )
        errors = validate_sources(make_project(config=config, files={"dim/source/dim_api.py": source}, name="exempt"))

        assert [e for e in errors if e.field.startswith("incremental.")] == []


# --- cursor_not_load_timestamp ---


class TestCursorNotLoadTimestampValidator:
    """Fires only when [dlt_ops] load_timestamp_column is configured."""

    CONFIG = {"dlt_ops": {"load_timestamp_column": "ingested_at"}}

    def _write(self, ctx: ValidationContext, body: str) -> None:
        pipeline_dir = next(iter(ctx.sources.values())).path
        (pipeline_dir / "resource.py").write_text(body)

    def test_business_cursor_passes_when_configured(self, tmp_path):
        from dlt_ops.discovery.validators.platform_rules import validate_cursor_not_load_timestamp

        ctx = _make_ctx(tmp_path, config=self.CONFIG)
        self._write(ctx, 'import dlt\n\ncur = dlt.sources.incremental("updated_at")\n')
        assert validate_cursor_not_load_timestamp(ctx) == []

    def test_configured_column_cursor_fails(self, tmp_path):
        from dlt_ops.discovery.validators.platform_rules import validate_cursor_not_load_timestamp

        ctx = _make_ctx(tmp_path, config=self.CONFIG)
        self._write(ctx, 'import dlt\n\ncur = dlt.sources.incremental("ingested_at")\n')
        errors = validate_cursor_not_load_timestamp(ctx)
        assert len(errors) == 1
        assert "load_timestamp_column" in errors[0].message
        assert "ingested_at" in errors[0].message

    def test_subscripted_form_checked(self, tmp_path):
        from dlt_ops.discovery.validators.platform_rules import validate_cursor_not_load_timestamp

        ctx = _make_ctx(tmp_path, config=self.CONFIG)
        self._write(
            ctx,
            'import dlt\nfrom datetime import datetime\n\ncur = dlt.sources.incremental[datetime]("ingested_at")\n',
        )
        assert len(validate_cursor_not_load_timestamp(ctx)) == 1

    def test_inert_when_unset(self, tmp_path):
        """No configured stamp column -> nothing to guard, any cursor passes."""
        from dlt_ops.discovery.validators.platform_rules import validate_cursor_not_load_timestamp

        ctx = _make_ctx(tmp_path)
        self._write(ctx, 'import dlt\n\ncur = dlt.sources.incremental("ingested_at")\n')
        assert validate_cursor_not_load_timestamp(ctx) == []

    def test_inert_when_blank(self, tmp_path):
        from dlt_ops.discovery.validators.platform_rules import validate_cursor_not_load_timestamp

        ctx = _make_ctx(tmp_path, config={"dlt_ops": {"load_timestamp_column": "   "}})
        self._write(ctx, 'import dlt\n\ncur = dlt.sources.incremental("ingested_at")\n')
        assert validate_cursor_not_load_timestamp(ctx) == []

    def test_other_column_names_not_flagged(self, tmp_path):
        """The guard is exactly the configured column, not any timestamp-ish name."""
        from dlt_ops.discovery.validators.platform_rules import validate_cursor_not_load_timestamp

        ctx = _make_ctx(tmp_path, config=self.CONFIG)
        self._write(ctx, 'import dlt\n\ncur = dlt.sources.incremental("loaded_at")\n')
        assert validate_cursor_not_load_timestamp(ctx) == []

    def test_padded_configured_column_still_fires(self, tmp_path):
        """A padded TOML value must not disarm the rule.

        `load_timestamp_column = " ingested_at "` is the column `ingested_at`
        everywhere else — the runner strips it before stamping and the
        reconciler strips it before ignoring it. Comparing the raw padded
        string here matches no source literal, so the rule would silently pass
        a pipeline that cursors on the stamp column.
        """
        from dlt_ops.discovery.validators.platform_rules import validate_cursor_not_load_timestamp

        ctx = _make_ctx(tmp_path, config={"dlt_ops": {"load_timestamp_column": "  ingested_at  "}})
        self._write(ctx, 'import dlt\n\ncur = dlt.sources.incremental("ingested_at")\n')
        errors = validate_cursor_not_load_timestamp(ctx)
        assert len(errors) == 1
        assert "'ingested_at'" in errors[0].message

    def test_non_string_configured_value_is_inert(self, tmp_path):
        """A hand-authored non-string value reads as off, never as a column name."""
        from dlt_ops.discovery.validators.platform_rules import validate_cursor_not_load_timestamp

        ctx = _make_ctx(tmp_path, config={"dlt_ops": {"load_timestamp_column": 42}})
        self._write(ctx, 'import dlt\n\ncur = dlt.sources.incremental("ingested_at")\n')
        assert validate_cursor_not_load_timestamp(ctx) == []


class TestLoadTimestampColumnReader:
    """One key, one reading — the reader every consumer layer shares.

    The rule, the runner's row stamper, and the reconciler's ignored-column
    set all resolve `[dlt_ops] load_timestamp_column` through this function.
    Divergence between them is silent: a stamped column the rule doesn't
    recognise, or an ignored column that doesn't match what landed.
    """

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("loaded_at", "loaded_at"),
            ("  loaded_at  ", "loaded_at"),
            ("\tloaded_at\n", "loaded_at"),
            ("", None),
            ("   ", None),
            (None, None),
            (42, None),
            (True, None),
            (["loaded_at"], None),
        ],
        ids=["plain", "padded", "tabbed", "empty", "blank", "absent", "int", "bool", "list"],
    )
    def test_normalizes_every_shape(self, raw, expected):
        from dlt_ops.discovery.models import resolve_load_timestamp_column

        assert resolve_load_timestamp_column(raw) == expected

    def test_reconciler_and_validator_agree_on_a_padded_value(self):
        """The two config shapes (ProjectConfig vs raw dict) read identically."""
        from dlt_ops.discovery.validators.platform_rules import _configured_load_timestamp_column
        from dlt_ops.reconciler.common import configured_load_timestamp_column

        raw = {"dlt_ops": {"load_timestamp_column": "  loaded_at  "}}
        project_config = ProjectConfig(raw=raw["dlt_ops"])
        ctx = ValidationContext(sources={}, config=raw, project_root=Path("/tmp/nowhere"))

        assert _configured_load_timestamp_column(ctx) == "loaded_at"
        assert configured_load_timestamp_column(project_config) == "loaded_at"

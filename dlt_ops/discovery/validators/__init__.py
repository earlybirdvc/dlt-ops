from dlt_ops.discovery.models import RuleSpec
from dlt_ops.discovery.validators.assertions import (
    validate_assertion_columns,
    validate_assertion_config,
    validate_assertion_predicates,
)
from dlt_ops.discovery.validators.config import (
    validate_alert_sinks,
    validate_config_sections,
    validate_decorator_names,
    validate_destination_capability,
    validate_module_names,
    validate_orphan_sections,
    validate_schedules,
    validate_secret_backends,
)
from dlt_ops.discovery.validators.import_safety import (
    validate_import_errors,
    validate_import_safety,
)
from dlt_ops.discovery.validators.platform_rules import (
    INCREMENTAL_CURSOR_RULE_ID,
    validate_cursor_not_load_timestamp,
    validate_incremental_cursor_required,
    validate_resource_name_explicit_in_multi_source_dir,
    validate_schema_contract,
)
from dlt_ops.discovery.validators.resources import validate_no_resource_overlap
from dlt_ops.discovery.validators.schema import (
    validate_json_column_hints,
    validate_pydantic_model_forbids_extra,
    validate_resource_columns_hint,
)
from dlt_ops.discovery.validators.staleness import validate_stale_sources

# The package's own rules. Registered through the same `dlt_ops.validators`
# entry-point group third-party plugins use (provider: `core_rules` below) —
# one lookup path for core and plugin rules alike. `validate_import_errors` is
# deliberately absent: import-error surfacing is always-on infrastructure, not
# a rule (no ID, no knob).
#
# Destination- and orchestrator-specific rules live in their plugin's own
# provider, body included — `dlt_ops.bigquery.validators:bigquery_rules` and
# `dlt_ops.airflow.validators:airflow_rules` (`airflow_var_required`). Core
# stays destination- and orchestrator-agnostic.
#
# Rule IDs are stable within a major version; the knob
# ([dlt_ops.rules]) and exemptions (rule_exemptions) key on them.
CORE_RULES: tuple[RuleSpec, ...] = (
    # Import safety first: a module with import-time side effects is the most
    # actionable finding — everything below assumes modules load cleanly.
    RuleSpec(rule_id="import_safety", validator=validate_import_safety, plugin="core"),
    RuleSpec(rule_id="config_section_required", validator=validate_config_sections, plugin="core"),
    RuleSpec(rule_id="schedule_required", validator=validate_schedules, plugin="core"),
    RuleSpec(rule_id="explicit_source_name", validator=validate_decorator_names, plugin="core"),
    RuleSpec(rule_id="module_name_matches_section", validator=validate_module_names, plugin="core"),
    RuleSpec(rule_id="orphan_config_sections", validator=validate_orphan_sections, plugin="core"),
    RuleSpec(rule_id="no_resource_overlap", validator=validate_no_resource_overlap, plugin="core"),
    RuleSpec(rule_id="json_hints_for_dict_fields", validator=validate_json_column_hints, plugin="core"),
    RuleSpec(rule_id="pydantic_columns_required", validator=validate_resource_columns_hint, plugin="core"),
    # Runs right after pydantic_columns_required: that rule mandates the model,
    # this one makes the contract dlt derives from it fail loudly rather than
    # discard silently.
    RuleSpec(
        rule_id="pydantic_model_forbids_extra",
        validator=validate_pydantic_model_forbids_extra,
        plugin="core",
    ),
    RuleSpec(rule_id="schema_contract_declared", validator=validate_schema_contract, plugin="core"),
    RuleSpec(
        rule_id="explicit_resource_name_multi_source",
        validator=validate_resource_name_explicit_in_multi_source_dir,
        plugin="core",
    ),
    RuleSpec(rule_id="cursor_not_load_timestamp", validator=validate_cursor_not_load_timestamp, plugin="core"),
    # The only opt-in core rule. Its sibling above catches a WRONG cursor; this
    # one catches a MISSING one, which is a policy rather than a defect — a full
    # refresh is legitimate, and nothing the package can see separates "chose to"
    # from "forgot to". Shipped off so adopting it is a decision, not an upgrade
    # surprise; `validate --show-resolved-rules` is where it is discovered.
    RuleSpec(
        rule_id=INCREMENTAL_CURSOR_RULE_ID,
        validator=validate_incremental_cursor_required,
        plugin="core",
        default_on=False,
    ),
    RuleSpec(rule_id="secret_backend_registered", validator=validate_secret_backends, plugin="core"),
    RuleSpec(rule_id="alert_sink_registered", validator=validate_alert_sinks, plugin="core"),
    RuleSpec(rule_id="destination_capability", validator=validate_destination_capability, plugin="core"),
    RuleSpec(rule_id="stale_sources", validator=validate_stale_sources, plugin="core"),
    # Assertion rules are three IDs, not one, so a project can exempt column
    # checking without disabling structural config validation (spec §6).
    RuleSpec(rule_id="assertion_config_valid", validator=validate_assertion_config, plugin="core"),
    RuleSpec(rule_id="assertion_columns_exist", validator=validate_assertion_columns, plugin="core"),
    RuleSpec(rule_id="assertion_predicate_resolvable", validator=validate_assertion_predicates, plugin="core"),
)


def core_rules() -> tuple[RuleSpec, ...]:
    """Rule provider for the package's own entry point in ``dlt_ops.validators``.

    Provider contract (what any plugin's entry point must satisfy): a
    zero-argument callable returning an iterable of :class:`RuleSpec`.
    """
    return CORE_RULES


__all__ = [
    "CORE_RULES",
    "INCREMENTAL_CURSOR_RULE_ID",
    "core_rules",
    "validate_alert_sinks",
    "validate_assertion_columns",
    "validate_assertion_config",
    "validate_assertion_predicates",
    "validate_config_sections",
    "validate_cursor_not_load_timestamp",
    "validate_incremental_cursor_required",
    "validate_decorator_names",
    "validate_destination_capability",
    "validate_import_errors",
    "validate_import_safety",
    "validate_json_column_hints",
    "validate_module_names",
    "validate_no_resource_overlap",
    "validate_orphan_sections",
    "validate_pydantic_model_forbids_extra",
    "validate_resource_columns_hint",
    "validate_resource_name_explicit_in_multi_source_dir",
    "validate_schema_contract",
    "validate_schedules",
    "validate_secret_backends",
    "validate_stale_sources",
]

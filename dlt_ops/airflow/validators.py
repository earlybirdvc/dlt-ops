"""Airflow plugin rules (validator provider ``airflow``).

``airflow_var_required``: a source whose signature uses ``dlt.secrets.value``
must configure ``airflow_var`` — meaningful only for projects orchestrated by
Airflow, so both the rule and its body live here rather than in core. It is
auto-active exactly when the extra is installed: the provider itself always
loads (a bare install's ``plugins doctor`` stays green) but contributes no
rules unless ``airflow`` is importable. Per-project opt-out:
``[dlt_ops.rules] airflow_var_required = false``; per-source exemptions go
through ``[sources.<X>.dlt_ops.rule_exemptions]``.

Importable without Airflow, like the rest of the plugin surface — the module
touches only config data, never an Airflow import.
"""

from __future__ import annotations

import importlib.util

from dlt_ops.discovery.models import RuleSpec, ValidationContext, ValidationError
from dlt_ops.discovery.validators.config import _source_uses_secrets

__all__ = ["AIRFLOW_RULES", "airflow_rules", "validate_airflow_vars"]


def validate_airflow_vars(ctx: ValidationContext) -> list[ValidationError]:
    """Check if source uses secrets, airflow_var must be set.

    Reads the raw ``[sources.<X>.dlt_ops]`` table rather than a parsed core
    model: ``airflow_var`` is this plugin's trigger key, and core does not
    own it.
    """
    errors: list[ValidationError] = []
    sources_config = ctx.config.get("sources", {})

    for name, source in ctx.sources.items():
        section = sources_config.get(source.config_section, {})
        ext = section.get("dlt_ops", {})

        if _source_uses_secrets(source.source_fn):
            airflow_var = ext.get("airflow_var")
            if not airflow_var:
                errors.append(
                    ValidationError(
                        source_name=name,
                        field="airflow_var",
                        message=f"Source uses secrets but 'airflow_var' not configured in [sources.{source.config_section}.dlt_ops]",
                    )
                )
    return errors


AIRFLOW_RULES: tuple[RuleSpec, ...] = (
    RuleSpec(rule_id="airflow_var_required", validator=validate_airflow_vars, plugin="airflow"),
)


def airflow_rules() -> tuple[RuleSpec, ...]:
    """Rule provider for the ``airflow`` entry point in ``dlt_ops.validators``."""
    if importlib.util.find_spec("airflow") is None:
        return ()
    return AIRFLOW_RULES

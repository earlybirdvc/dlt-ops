"""Airflow plugin rules (validator provider ``airflow``).

``airflow_var_required`` wraps core's ``validate_airflow_vars``: a source
whose signature uses ``dlt.secrets.value`` must configure ``airflow_var`` —
meaningful only for projects orchestrated by Airflow, so the rule ships here
rather than in core defaults. It is auto-active exactly when the extra is
installed: the provider itself always loads (a bare install's ``plugins
doctor`` stays green) but contributes no rules unless ``airflow`` is
importable. Per-project opt-out: ``[dlt_ops.rules]
airflow_var_required = false``; per-source exemptions go through
``[sources.<X>.dlt_ops.rule_exemptions]``.
"""

from __future__ import annotations

import importlib.util

from dlt_ops.discovery.models import RuleSpec
from dlt_ops.discovery.validators.config import validate_airflow_vars

__all__ = ["AIRFLOW_RULES", "airflow_rules"]

AIRFLOW_RULES: tuple[RuleSpec, ...] = (
    RuleSpec(rule_id="airflow_var_required", validator=validate_airflow_vars, plugin="airflow"),
)


def airflow_rules() -> tuple[RuleSpec, ...]:
    """Rule provider for the ``airflow`` entry point in ``dlt_ops.validators``."""
    if importlib.util.find_spec("airflow") is None:
        return ()
    return AIRFLOW_RULES

"""Airflow adapter for dlt-ops — install with ``pip install 'dlt-ops[airflow]'``.

Implements the core orchestrator interface (``dlt_ops.orchestration``)
on Airflow primitives. Two surfaces with different import requirements:

- Plugin surface — importable WITHOUT Airflow, because the plugin registry
  loads every registered plugin (a bare install's ``validate`` / preflight /
  ``plugins doctor`` must stay healthy): :class:`AirflowVariableBackend`
  (secret-backend axis; imports Airflow lazily at fetch time) and
  :func:`airflow_rules` (validator provider; contributes no rules unless
  Airflow is installed).
- Adapter surface — hard Airflow imports, module-level guard with the install
  hint: :func:`build_schedule_dags` / ``SCHEDULE_CRON_MAP`` (``factory``) and
  :func:`cleanup_old_dlt_files` (``tasks``). Without the extra, importing
  those modules (or touching these names on the package) raises a clear
  ``ImportError``.

Per-environment deployments (what an ``ENVIRONMENT`` → dataset mapping would
do) are config, not code: point each environment at its own project root with
its own ``.dlt/`` directory — prod and staging each carry a ``config.toml``
whose ``[dlt_ops] default_dataset`` / per-source ``dataset`` name the
right target. Dataset resolution is the core config chain; the adapter adds
no environment policy.
"""

import importlib
from typing import Any

_INSTALL_HINT = (
    "Apache Airflow is not installed; the dlt-ops Airflow adapter "
    "requires it. Install the extra: pip install 'dlt-ops[airflow]'"
)

from dlt_ops.airflow.secrets import AirflowVariableBackend  # noqa: E402
from dlt_ops.airflow.validators import airflow_rules  # noqa: E402

__all__ = [
    "SCHEDULE_CRON_MAP",
    "AirflowVariableBackend",
    "airflow_rules",
    "build_schedule_dags",
    "cleanup_old_dlt_files",
]

# Adapter surface, resolved lazily (PEP 562) so the plugin surface above stays
# importable on Airflow-less installs; the target modules carry the guard.
_ADAPTER_EXPORTS = {
    "SCHEDULE_CRON_MAP": "dlt_ops.airflow.factory",
    "build_schedule_dags": "dlt_ops.airflow.factory",
    "cleanup_old_dlt_files": "dlt_ops.airflow.tasks",
}


def __getattr__(name: str) -> Any:
    module = _ADAPTER_EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(importlib.import_module(module), name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_ADAPTER_EXPORTS))

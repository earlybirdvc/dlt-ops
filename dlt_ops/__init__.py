"""Reusable extensions for dlt (Data Load Tool) pipelines — public API.

Public-vs-internal convention:

- Public = names exported here (listed in ``__all__``) or from an
  explicitly-public subpackage. Everything else is internal — importable,
  but with no stability promise.
- New internal modules are underscore-prefixed (``_sandbox_child.py`` is the
  pattern); existing non-underscore internals are grandfathered but never
  exported.
- Every ``__init__.py`` that exports anything declares ``__all__``.

Reconciler names are re-exported lazily (PEP 562 ``__getattr__``) so that
``import dlt_ops`` never pulls the reconciler's backend dependencies
at import time.
"""

import importlib
from typing import Any

from dlt_ops.assertions import AssertionContext, AssertionFailedError, AssertionType
from dlt_ops.checkpoints import cleanup_checkpoints, list_checkpoints, with_checkpoints
from dlt_ops.destinations import DestinationAdapter
from dlt_ops.discovery import (
    RuleSpec,
    Schedule,
    SourceConfig,
    SourceInfo,
    ValidationContext,
    ValidationError,
    Validator,
    discover_sources,
    validate_sources,
)
from dlt_ops.plugins import register
from dlt_ops.pydantic_fields import drop_unknown_nulls, extract_model_column_names
from dlt_ops.secrets import SecretBackend

# Single source of truth for the package version; pyproject.toml reads it
# via hatchling's [tool.hatch.version], and python-semantic-release stamps
# it at release time (0.0.0 = no release cut yet; must stay plain semver —
# a PEP440 suffix like .dev0 would survive the stamp regex).
__version__ = "0.0.0"

_LAZY_RECONCILER_EXPORTS = frozenset(
    {"AlertSink", "DriftFinding", "ReconcileResult", "detect_removal", "reconcile_all", "reconcile_source"}
)

__all__ = [
    "AlertSink",
    "AssertionContext",
    "AssertionFailedError",
    "AssertionType",
    "DestinationAdapter",
    "DriftFinding",
    "ReconcileResult",
    "RuleSpec",
    "Schedule",
    "SecretBackend",
    "SourceConfig",
    "SourceInfo",
    "ValidationContext",
    "ValidationError",
    "Validator",
    "cleanup_checkpoints",
    "detect_removal",
    "discover_sources",
    "drop_unknown_nulls",
    "extract_model_column_names",
    "list_checkpoints",
    "reconcile_all",
    "reconcile_source",
    "register",
    "validate_sources",
    "with_checkpoints",
]


def __getattr__(name: str) -> Any:
    if name in _LAZY_RECONCILER_EXPORTS:
        return getattr(importlib.import_module("dlt_ops.reconciler"), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | _LAZY_RECONCILER_EXPORTS)

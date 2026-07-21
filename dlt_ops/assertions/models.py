"""Assertion contracts: the plugin Protocol, static context, and typed errors.

The assertion axis follows the accumulator model (assertions spec §5):
``check_config`` is the static half — consumed by ``pipeline validate`` and
re-run cheaply at engine build for defense in depth; ``start`` / ``observe`` /
``finalize`` are the load-time half — consumed by the engine between extract
and load. ``row_scoped`` is the single scope discriminator.

Verdict handling is policy, and policy is engine-owned: plugins never see
``on_failure``, never write quarantine rows, never touch destination clients.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, Protocol, runtime_checkable

import attrs

ASSERTION_AXIS = "assertion"
"""Plugin-registry axis; entry-point group ``dlt_ops.assertion`` (frozen)."""

OnFailure = Literal["fail", "quarantine", "warn"]

ON_FAILURE_VALUES: tuple[str, ...] = ("fail", "quarantine", "warn")
"""Valid ``on_failure`` values at every level (resource default, per-assertion)."""

DEFAULT_ON_FAILURE = "fail"
"""Built-in default: bad data does not load, with zero side effects not asked for."""


class AssertionFailedError(Exception):
    """A configured assertion failed with ``on_failure = "fail"``.

    Raised by the engine (row verdicts inside the extract gate, batch verdicts
    at ``finalize``); the run aborts, nothing loads, and the runner drops the
    pending extracted package so the rejected batch cannot auto-load on the
    next run.
    """

    def __init__(self, *, source_section: str, resource_name: str, assertion_type: str, message: str) -> None:
        self.source_section = source_section
        self.resource_name = resource_name
        self.assertion_type = assertion_type
        self.assertion_message = message
        super().__init__(f"assertion {assertion_type!r} failed on {source_section}.{resource_name}: {message}")


class AssertionConfigurationError(Exception):
    """Assertion config failed the engine's pre-extract re-checks.

    Tier-2 defense in depth (spec §7): engine construction re-runs the cheap
    structural checks (``on_failure`` domain, quarantine-on-batch-scope,
    ``check_config``) and imports custom predicates; any failure is a hard
    fail before extract, regardless of whether ``validate`` ever ran.
    """


@attrs.frozen
class AssertionContext:
    """Static facts ``check_config`` may validate against. No data, no clients."""

    source_section: str
    resource_name: str
    declared_columns: tuple[str, ...] | None
    """Column names from the resource's ``columns=`` Pydantic model; None when
    the model is unresolvable (e.g. ``pydantic_columns_required`` exempted).
    Types MUST skip column-existence checks when this is None."""


@runtime_checkable
class AssertionType(Protocol):
    """One assertion type: a named accumulator with a static config check.

    Implementations register under the ``dlt_ops.assertion`` entry-point
    group (or the runtime twin ``dlt_ops.register("assertion", name)``);
    entry points conventionally register the class, instantiated with no
    arguments.
    """

    name: str
    """Registry name == TOML key. Frozen once released (config compatibility)."""

    row_scoped: bool
    """True → ``observe`` emits per-row verdicts (quarantine-compatible).
    False → batch-scope only; ``on_failure = "quarantine"`` is a config error."""

    def check_config(self, params: Mapping[str, Any], ctx: AssertionContext) -> list[str]:
        """Statically checkable config errors (Tier 1). Empty list = valid.

        This is the enforced-opinions declaration mechanism: everything a type
        CAN check without data (param types/domains, column references against
        ``ctx.declared_columns``) it MUST check here. Never touches data,
        network, or clients.
        """
        ...

    def start(self, params: Mapping[str, Any]) -> Any:
        """Fresh accumulator state for one (resource, run) batch."""
        ...

    def observe(self, state: Any, row: Mapping[str, Any], params: Mapping[str, Any]) -> str | None:
        """Per-row verdict: None = pass; message = THIS ROW fails.

        Batch-scope types accumulate and return None.
        """
        ...

    def finalize(self, state: Any, params: Mapping[str, Any]) -> str | None:
        """Batch verdict after the last row: None = pass; message = batch fails."""
        ...

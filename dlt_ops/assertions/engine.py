"""Assertion engine — gate wiring, accumulators, verdict policy (assertions spec §3/§5).

The engine owns everything uniform across assertion types: shorthand→params
normalization (via ``assertions.config``), ``on_failure`` resolution,
quarantine buffering, warn counting, and locking. Plugins never see
``on_failure`` — verdict handling is policy, and policy is engine-owned.

One :class:`_AssertionGate` (a dlt ``FilterItem`` pipe step) is attached per
resource that has any assertion or custom predicate configured. Its
``placement_affinity`` pins it after the load-timestamp stamper (1.2) — and
therefore after the resource's PydanticValidator (0.9) and incremental filter (1) — so
assertions observe the final row shape, and only rows that would actually
load. The batch for every batch-scoped verdict is all rows one resource
yields during one ``pipeline.extract()`` invocation of one run.

Concurrency: dlt runs transforms of ``parallelized=True`` resources in worker
threads; accumulator updates go through one ``threading.Lock`` per resource
gate. Coarse, correct, cheap at v0.1 scale.

Quarantined rows buffer in memory until ``flush_quarantine`` — a pathological
all-rows-quarantined run holds the batch in RAM (documented; no cap in v0.1).
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import attrs
from dlt.extract.items_transform import FilterItem

from dlt_ops.assertions.config import (
    CUSTOM_TYPE_NAME,
    AssertionSpec,
    check_specs,
    declared_columns_for_resource,
    load_assertion_type,
    parse_assertions,
    resolve_predicate,
)
from dlt_ops.assertions.models import (
    AssertionConfigurationError,
    AssertionContext,
    AssertionFailedError,
    AssertionType,
)
from dlt_ops.assertions.quarantine import QuarantinedRow, QuarantineWriter
from dlt_ops.plugins import registry as _default_registry

logger = logging.getLogger(__name__)

_WARN_LOG_LIMIT = 5
"""Per (resource, type): individual warn verdicts logged up to this many rows;
the rest are counted silently and reported in the finalize summary line."""


class _AssertionGate(FilterItem):
    """Assertion gate pinned after the load-timestamp stamper (placement_affinity 1.2).

    dlt places pipe steps by ``placement_affinity``; 1.3 lands the gate after
    the PydanticValidator (0.9), the incremental filter (1), any limit (1.1),
    and the stamp step (1.2), so assertions see the final row shape and only
    rows that survive to load.
    """

    placement_affinity = 1.3


class _PredicateRunner:
    """Engine-internal predicate-runner assertion wrapping one ``custom`` entry.

    Custom predicates are NOT plugins (spec §5): row-scope only, boolean
    verdict, auto-generated failure message from the qualname. Third parties
    who outgrow predicates ship a real ``dlt_ops.assertion`` entry point.
    """

    name = CUSTOM_TYPE_NAME
    row_scoped = True

    def __init__(self, qualname: str, predicate: Callable[[Mapping[str, Any]], bool]) -> None:
        self._qualname = qualname
        self._predicate = predicate

    def check_config(self, params: Mapping[str, Any], ctx: AssertionContext) -> list[str]:
        return []

    def start(self, params: Mapping[str, Any]) -> Any:
        return None

    def observe(self, state: Any, row: Mapping[str, Any], params: Mapping[str, Any]) -> str | None:
        if self._predicate(row):
            return None
        return f"predicate {self._qualname} failed"

    def finalize(self, state: Any, params: Mapping[str, Any]) -> str | None:
        return None


@attrs.define
class _BoundAssertion:
    """One spec bound to its type implementation and per-run accumulator."""

    spec: AssertionSpec
    impl: AssertionType
    state: Any
    warn_count: int = 0


@attrs.define
class _ResourceRuntime:
    """Per-resource run state: bound assertions, lock, quarantine buffer."""

    resource_name: str
    entries: list[_BoundAssertion]
    lock: threading.Lock = attrs.field(factory=threading.Lock)
    quarantined: list[tuple[dict[str, Any], AssertionSpec, str]] = attrs.field(factory=list)


class AssertionEngine:
    """Per-run assertion execution: streaming observation + batch gate.

    Built once per ``run_pipeline`` invocation via :meth:`from_config`;
    construction re-runs the cheap static checks and imports custom predicates
    (Tier-2 defense in depth, spec §7) so a bad config hard-fails before
    extract even when ``validate`` never ran.
    """

    def __init__(self, source_section: str, runtimes: dict[str, _ResourceRuntime]) -> None:
        self._source_section = source_section
        self._runtimes = runtimes

    @classmethod
    def from_config(
        cls,
        *,
        source_section: str,
        raw_config: Mapping[str, Any],
        source_instance: Any,
        project_root: Path | None = None,
        registry: Any = _default_registry,
    ) -> AssertionEngine:
        """Build the engine for one run from the parsed project config.

        Raises:
            AssertionConfigurationError: any structural or ``check_config``
                issue, or a custom predicate that fails to import — hard fail
                before extract.
        """
        parsed = parse_assertions(raw_config, source_section)
        issues = list(parsed.issues)

        def _context_for(resource_name: str) -> AssertionContext:
            resource = source_instance.resources.get(resource_name)
            declared = declared_columns_for_resource(resource) if resource is not None else None
            return AssertionContext(
                source_section=source_section, resource_name=resource_name, declared_columns=declared
            )

        issues.extend(
            check_specs(
                parsed,
                known_resources=set(source_instance.resources.keys()),
                context_for=_context_for,
                registry=registry,
            )
        )
        if issues:
            listed = "; ".join(f"[{issue.field}] {issue.message}" for issue in issues)
            raise AssertionConfigurationError(f"invalid assertion config for source {source_section!r}: {listed}")

        selected = set(source_instance.selected_resources.keys())
        runtimes: dict[str, _ResourceRuntime] = {}
        for res in parsed.resources:
            if res.resource_name not in selected or not res.specs:
                continue
            entries: list[_BoundAssertion] = []
            for spec in res.specs:
                impl: AssertionType
                if spec.is_custom:
                    qualname = spec.predicate or ""
                    try:
                        predicate = resolve_predicate(qualname, project_root)
                    except Exception as exc:
                        raise AssertionConfigurationError(
                            f"custom predicate {qualname!r} on {source_section}.{res.resource_name} "
                            f"is not resolvable: {type(exc).__name__}: {exc}"
                        ) from exc
                    impl = _PredicateRunner(qualname, predicate)
                else:
                    impl = load_assertion_type(spec.type_name, registry=registry)
                entries.append(_BoundAssertion(spec=spec, impl=impl, state=impl.start(spec.params)))
            runtimes[res.resource_name] = _ResourceRuntime(resource_name=res.resource_name, entries=entries)
        return cls(source_section, runtimes)

    @property
    def active(self) -> bool:
        """True when at least one selected resource has assertions configured."""
        return bool(self._runtimes)

    @property
    def has_quarantined(self) -> bool:
        return any(runtime.quarantined for runtime in self._runtimes.values())

    def attach(self, source_instance: Any) -> None:
        """Append one assertion gate per asserted selected resource."""
        for name, runtime in self._runtimes.items():
            source_instance.selected_resources[name].add_step(_AssertionGate(self._make_gate(runtime)))
            logger.info(
                f"Assertion gate attached to resource {name!r} ({len(runtime.entries)} assertion(s), declaration order)"
            )

    def _make_gate(self, runtime: _ResourceRuntime) -> Callable[[Any], bool]:
        def _gate(row: Any) -> bool:
            return self._observe_row(runtime, row)

        return _gate

    def _observe_row(self, runtime: _ResourceRuntime, row: Any) -> bool:
        """Feed one row to every accumulator in declaration order (customs last).

        Verdict policy per spec §3: ``fail`` raises immediately; ``quarantine``
        buffers the row, drops it from the stream, and skips the remaining
        assertions for this row; ``warn`` logs + counts and continues.
        """
        with runtime.lock:
            for entry in runtime.entries:
                verdict = entry.impl.observe(entry.state, row, entry.spec.params)
                if verdict is None:
                    continue
                if entry.spec.on_failure == "fail":
                    raise AssertionFailedError(
                        source_section=self._source_section,
                        resource_name=runtime.resource_name,
                        assertion_type=entry.spec.type_name,
                        message=verdict,
                    )
                if entry.spec.on_failure == "quarantine":
                    runtime.quarantined.append((dict(row), entry.spec, verdict))
                    return False
                entry.warn_count += 1
                if entry.warn_count <= _WARN_LOG_LIMIT:
                    logger.warning(
                        f"assertion {entry.spec.type_name!r} warn on "
                        f"{self._source_section}.{runtime.resource_name}: {verdict}"
                    )
        return True

    def finalize(self) -> None:
        """Batch verdicts after the last row + the per-resource warn summary.

        Raises:
            AssertionFailedError: a batch-scoped assertion with
                ``on_failure = "fail"`` failed — the caller must drop the
                pending package and abort before normalize/load.
        """
        for runtime in self._runtimes.values():
            for entry in runtime.entries:
                verdict = entry.impl.finalize(entry.state, entry.spec.params)
                if verdict is None:
                    continue
                if entry.spec.on_failure == "fail":
                    raise AssertionFailedError(
                        source_section=self._source_section,
                        resource_name=runtime.resource_name,
                        assertion_type=entry.spec.type_name,
                        message=verdict,
                    )
                # Quarantine on batch scope is a config error caught at build;
                # the only remaining policy here is warn.
                entry.warn_count += 1
                logger.warning(
                    f"assertion {entry.spec.type_name!r} warn on "
                    f"{self._source_section}.{runtime.resource_name}: {verdict}"
                )
            warned = [(entry.spec.type_name, entry.warn_count) for entry in runtime.entries if entry.warn_count]
            if warned:
                summary = ", ".join(f"{type_name}={count}" for type_name, count in warned)
                logger.warning(f"assertion warn summary for {self._source_section}.{runtime.resource_name}: {summary}")

    def flush_quarantine(self, writer: QuarantineWriter) -> int:
        """Drain quarantine buffers into ``_dlt_rejected`` via the writer.

        Returns the number of rows written. Propagates
        :class:`~dlt_ops.assertions.quarantine.QuarantineWriteError` —
        write failure is run failure (spec §4).
        """
        rows: list[QuarantinedRow] = []
        for runtime in self._runtimes.values():
            for row, spec, verdict in runtime.quarantined:
                rows.append(
                    QuarantinedRow(
                        resource_name=runtime.resource_name,
                        assertion_type=spec.type_name,
                        assertion_params=json.dumps(dict(spec.params), default=str, sort_keys=True),
                        violation=verdict,
                        row_json=json.dumps(row, default=str),
                    )
                )
        if rows:
            writer.write(rows)
        for runtime in self._runtimes.values():
            runtime.quarantined.clear()
        return len(rows)

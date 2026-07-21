"""Assertion config parsing — one parser, three consumers (engine, validators, preflight).

Parses ``[sources.<X>.dlt_ops.assertions.<resource>]`` tables into
normalized :class:`AssertionSpec` lists (assertions spec §1):

- **Shorthand** values (scalar / array) normalize to ``{"value": <shorthand>}``.
- **Table form** values are inline tables; the parser pops ``on_failure`` (the
  per-assertion override) and hands the remainder to the plugin as params.
- Reserved keys per resource table: ``on_failure`` (resource-level default)
  and ``custom`` (array of ``predicate = "module:attr"`` tables, row-scoped,
  evaluated after the declared types).

``on_failure`` resolution precedence (lowest → highest): built-in ``"fail"``
→ resource-level key → per-assertion key. Parsing is engine-owned, never
plugin-owned: plugins receive normalized params and never see ``on_failure``.
"""

from __future__ import annotations

import importlib
import re
import sys
from collections.abc import Callable, Collection, Mapping
from pathlib import Path
from typing import Any, cast

import attrs
import pydantic

from dlt_ops.assertions.models import (
    ASSERTION_AXIS,
    DEFAULT_ON_FAILURE,
    ON_FAILURE_VALUES,
    AssertionContext,
    AssertionType,
)
from dlt_ops.plugins import registry as _default_registry
from dlt_ops.pydantic_fields import extract_model_column_names

RESERVED_ASSERTION_KEYS: tuple[str, ...] = ("on_failure", "custom")
"""Keys inside a resource's assertions table that never name an assertion type.

A plugin registering one of these names is a ``validate`` error — the config
key would be unreachable.
"""

CUSTOM_TYPE_NAME = "custom"
"""``assertion_type`` value custom predicates carry (specs and ``_dlt_rejected`` rows)."""

_IDENT = r"[A-Za-z_][A-Za-z0-9_]*"
_MODULE = rf"{_IDENT}(?:\.{_IDENT})*"
# `module:attr` (entry-point value syntax) or dotted `module.attr` (the
# disambiguation-mapping leniency mirrored from plugins/registry.py).
_PREDICATE_RE = re.compile(rf"^(?:{_MODULE}:{_IDENT}|{_MODULE}\.{_IDENT})$")

_ON_FAILURE_HINT = f"valid values: {', '.join(repr(v) for v in ON_FAILURE_VALUES)}"


@attrs.frozen
class AssertionSpec:
    """One normalized assertion declaration for one resource."""

    type_name: str
    """Registry name of the assertion type; :data:`CUSTOM_TYPE_NAME` for predicates."""

    params: Mapping[str, Any]
    """Normalized params mapping (shorthand already folded into ``{"value": ...}``;
    custom entries carry the predicate qualname under ``"predicate"``)."""

    on_failure: str
    """Fully resolved policy for this assertion (precedence applied)."""

    is_custom: bool = False

    @property
    def predicate(self) -> str | None:
        """The ``module:attr`` / dotted predicate path for custom specs; None otherwise."""
        value = self.params.get("predicate") if self.is_custom else None
        return value if isinstance(value, str) else None


@attrs.frozen
class ResourceAssertions:
    """All assertions declared for one resource, in evaluation order.

    Declared assertion-type keys keep TOML declaration order (tomllib
    preserves key order); custom predicates come last.
    """

    resource_name: str
    specs: tuple[AssertionSpec, ...]


@attrs.frozen
class AssertionIssue:
    """One config error found by the parser or the type-aware checks."""

    resource_name: str | None
    field: str
    message: str


@attrs.frozen
class ParsedAssertions:
    """Parse result for one source section: well-formed specs + config issues."""

    source_section: str
    resources: tuple[ResourceAssertions, ...]
    issues: tuple[AssertionIssue, ...]

    @property
    def has_assertions(self) -> bool:
        return any(res.specs for res in self.resources)

    def referenced_types(self, resource_names: Collection[str] | None = None) -> tuple[str, ...]:
        """Sorted unique assertion-type names referenced (customs excluded).

        ``resource_names`` restricts to a resource subset (the preflight passes
        the run's selected resources).
        """
        names = {
            spec.type_name
            for res in self.resources
            if resource_names is None or res.resource_name in resource_names
            for spec in res.specs
            if not spec.is_custom
        }
        return tuple(sorted(names))

    def quarantine_resources(self, resource_names: Collection[str] | None = None) -> tuple[str, ...]:
        """Resource names carrying at least one ``on_failure = "quarantine"`` spec, in declaration order.

        ``resource_names`` restricts to a resource subset (the preflight passes
        the run's selected resources). The one derivation of quarantine
        engagement — Tier-1 ``validate`` and the Tier-2 preflight both consume
        it, so the tiers can't drift on when quarantine gates a destination.
        """
        return tuple(
            res.resource_name
            for res in self.resources
            if (resource_names is None or res.resource_name in resource_names)
            and any(spec.on_failure == "quarantine" for spec in res.specs)
        )


def _resolve_on_failure(raw: Any, default: str, resource_name: str, field: str, issues: list[AssertionIssue]) -> str:
    """Per-level ``on_failure`` resolution; domain violations recorded, default kept."""
    if raw is None:
        return default
    if raw not in ON_FAILURE_VALUES:
        issues.append(
            AssertionIssue(
                resource_name=resource_name,
                field=field,
                message=f"invalid on_failure {raw!r}; {_ON_FAILURE_HINT}",
            )
        )
        return default
    return str(raw)


def _parse_custom_entries(
    raw_custom: Any, resource_name: str, resource_default: str, issues: list[AssertionIssue]
) -> list[AssertionSpec]:
    field = f"assertions.{resource_name}.custom"
    if not isinstance(raw_custom, list):
        issues.append(
            AssertionIssue(
                resource_name=resource_name,
                field=field,
                message="custom must be an array of tables ([[...assertions.<resource>.custom]] entries)",
            )
        )
        return []
    specs: list[AssertionSpec] = []
    for index, entry in enumerate(raw_custom):
        entry_field = f"{field}[{index}]"
        if not isinstance(entry, dict):
            issues.append(
                AssertionIssue(
                    resource_name=resource_name,
                    field=entry_field,
                    message=f"custom entry must be a table with a predicate key, got {entry!r}",
                )
            )
            continue
        predicate = entry.get("predicate")
        if not isinstance(predicate, str) or not _PREDICATE_RE.match(predicate):
            issues.append(
                AssertionIssue(
                    resource_name=resource_name,
                    field=entry_field,
                    message=f'custom entry requires predicate = "module:attr" (dotted module.attr also '
                    f"accepted), got {predicate!r}",
                )
            )
            continue
        on_failure = _resolve_on_failure(entry.get("on_failure"), resource_default, resource_name, entry_field, issues)
        params = {key: value for key, value in entry.items() if key != "on_failure"}
        specs.append(AssertionSpec(type_name=CUSTOM_TYPE_NAME, params=params, on_failure=on_failure, is_custom=True))
    return specs


def parse_assertions(raw_config: Mapping[str, Any], source_section: str) -> ParsedAssertions:
    """Parse one source's assertions config (structural half; no registry access).

    Returns every well-formed spec plus an issue per structural violation:
    non-table assertions/resource values, ``on_failure`` outside the domain,
    malformed ``custom`` entries. Type-aware checks (unknown types, scope,
    ``check_config``) live in :func:`check_specs`.
    """
    sources = raw_config.get("sources")
    section = sources.get(source_section) if isinstance(sources, dict) else None
    ext = section.get("dlt_ops") if isinstance(section, dict) else None
    raw = ext.get("assertions") if isinstance(ext, dict) else None
    if raw is None:
        return ParsedAssertions(source_section=source_section, resources=(), issues=())
    if not isinstance(raw, dict):
        issue = AssertionIssue(
            resource_name=None,
            field="assertions",
            message=f"[sources.{source_section}.dlt_ops] assertions must be a table of per-resource "
            f"tables, got {raw!r}",
        )
        return ParsedAssertions(source_section=source_section, resources=(), issues=(issue,))

    issues: list[AssertionIssue] = []
    resources: list[ResourceAssertions] = []
    for resource_name, entry in raw.items():
        if not isinstance(entry, dict):
            issues.append(
                AssertionIssue(
                    resource_name=resource_name,
                    field=f"assertions.{resource_name}",
                    message=f"[sources.{source_section}.dlt_ops.assertions.{resource_name}] must be "
                    f"a table of assertion declarations, got {entry!r}",
                )
            )
            continue
        resource_default = _resolve_on_failure(
            entry.get("on_failure"),
            DEFAULT_ON_FAILURE,
            resource_name,
            f"assertions.{resource_name}.on_failure",
            issues,
        )
        specs: list[AssertionSpec] = []
        for key, value in entry.items():
            if key in RESERVED_ASSERTION_KEYS:
                continue
            if isinstance(value, dict):
                table = dict(value)
                on_failure = _resolve_on_failure(
                    table.pop("on_failure", None),
                    resource_default,
                    resource_name,
                    f"assertions.{resource_name}.{key}.on_failure",
                    issues,
                )
                params: dict[str, Any] = table
            else:
                on_failure = resource_default
                params = {"value": value}
            specs.append(AssertionSpec(type_name=key, params=params, on_failure=on_failure))
        raw_custom = entry.get("custom")
        if raw_custom is not None:
            specs.extend(_parse_custom_entries(raw_custom, resource_name, resource_default, issues))
        resources.append(ResourceAssertions(resource_name=resource_name, specs=tuple(specs)))
    return ParsedAssertions(source_section=source_section, resources=tuple(resources), issues=tuple(issues))


def load_assertion_type(name: str, *, registry: Any = _default_registry) -> AssertionType:
    """Resolve assertion plugin ``name`` from the registry and instantiate it.

    Entry points conventionally register the type class; already-constructed
    instances (e.g. runtime registrations) pass through as-is. Mirrors
    ``destinations.get_adapter``.
    """
    plugin: Any = registry.get(ASSERTION_AXIS, name)
    return plugin() if isinstance(plugin, type) else plugin


def reserved_plugin_names(*, registry: Any = _default_registry) -> tuple[str, ...]:
    """Registered assertion plugin names that collide with reserved config keys."""
    return tuple(name for name in registry.names(ASSERTION_AXIS) if name in RESERVED_ASSERTION_KEYS)


def check_specs(
    parsed: ParsedAssertions,
    *,
    known_resources: Collection[str] | None,
    context_for: Callable[[str], AssertionContext],
    registry: Any = _default_registry,
) -> list[AssertionIssue]:
    """Type-aware config checks over parsed specs (needs the plugin registry).

    Emits: unknown resource names (``known_resources`` is the authoritative
    live list; None skips the check), unknown assertion types (message lists
    registered names), ``on_failure = "quarantine"`` on a batch-scoped type,
    and params rejected by the type's ``check_config`` against the
    :class:`AssertionContext` that ``context_for`` builds per resource.

    A registered type whose load raises is skipped here — soft-failed plugins
    are surfaced by the plugin-health wiring (`plugins doctor`) and hard-fail
    the Tier-2 preflight.
    """
    issues: list[AssertionIssue] = []
    for res in parsed.resources:
        if known_resources is not None and res.resource_name not in known_resources:
            issues.append(
                AssertionIssue(
                    resource_name=res.resource_name,
                    field=f"assertions.{res.resource_name}",
                    message=f"assertions configured for unknown resource {res.resource_name!r}; "
                    f"source resources: {', '.join(sorted(known_resources)) or '(none)'}",
                )
            )
            continue
        for spec in res.specs:
            if spec.is_custom:
                continue
            field = f"assertions.{res.resource_name}.{spec.type_name}"
            if spec.type_name not in registry.names(ASSERTION_AXIS):
                registered = ", ".join(repr(n) for n in registry.names(ASSERTION_AXIS)) or "(none)"
                issues.append(
                    AssertionIssue(
                        resource_name=res.resource_name,
                        field=field,
                        message=f"unknown assertion type {spec.type_name!r}; registered assertion types: "
                        f"{registered}. Install one under the 'dlt_ops.assertion' entry-point group, "
                        f"then inspect the registry with `dlt-ops plugins doctor`.",
                    )
                )
                continue
            try:
                impl = load_assertion_type(spec.type_name, registry=registry)
            except Exception:
                continue
            if spec.on_failure == "quarantine" and not impl.row_scoped:
                issues.append(
                    AssertionIssue(
                        resource_name=res.resource_name,
                        field=field,
                        message=f"assertion type {spec.type_name!r} is batch-scoped; "
                        f'on_failure = "quarantine" is invalid — there are no specific rows to quarantine '
                        f"when a batch verdict fails",
                    )
                )
            ctx = context_for(res.resource_name)
            issues.extend(
                AssertionIssue(resource_name=res.resource_name, field=field, message=message)
                for message in impl.check_config(spec.params, ctx)
            )
    return issues


def split_predicate(predicate: str) -> tuple[str, str]:
    """``(module, attr)`` from a ``module:attr`` or dotted ``module.attr`` path."""
    if ":" in predicate:
        module, _, attr = predicate.partition(":")
        return module, attr
    module, _, attr = predicate.rpartition(".")
    return module, attr


def resolve_predicate(predicate: str, project_root: Path | None = None) -> Callable[[Mapping[str, Any]], bool]:
    """Import a custom predicate and return the callable.

    Standard import machinery first; a ``ModuleNotFoundError`` retries with
    the project root temporarily on ``sys.path`` so project-local predicate
    modules resolve the same way they do in the ``validate`` subprocess probe
    (which runs with the project root as cwd).

    Raises:
        ModuleNotFoundError: the predicate module is not importable.
        AttributeError: the module has no such attribute.
        TypeError: the attribute is not callable.
    """
    module_name, attr = split_predicate(predicate)
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        if project_root is None:
            raise
        root = str(project_root)
        sys.path.insert(0, root)
        try:
            module = importlib.import_module(module_name)
        finally:
            if root in sys.path:
                sys.path.remove(root)
    obj: Any = getattr(module, attr)
    if not callable(obj):
        raise TypeError(f"predicate {predicate!r} resolved to a non-callable {type(obj).__name__}")
    return cast("Callable[[Mapping[str, Any]], bool]", obj)


def declared_columns_for_resource(resource: Any) -> tuple[str, ...] | None:
    """Column names from a live resource's ``columns=`` Pydantic model; None when unresolvable.

    dlt keeps the raw model class in ``_hints['columns']`` before instantiation
    and moves it onto the ``PydanticValidator`` step (``validator.model`` /
    ``original_model``) once the source is instantiated — both shapes resolve.
    """
    hints = getattr(resource, "_hints", {})
    validator = getattr(resource, "validator", None)
    candidates = (
        hints.get("columns"),
        hints.get("original_columns"),
        getattr(validator, "original_model", None),
        getattr(validator, "model", None),
    )
    for candidate in candidates:
        if isinstance(candidate, type) and issubclass(candidate, pydantic.BaseModel):
            return tuple(sorted(extract_model_column_names(candidate)))
    return None

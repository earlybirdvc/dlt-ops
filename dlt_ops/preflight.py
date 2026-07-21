"""Tier-2 runtime preflight — hard-fail fast on the five locked conditions.

The runtime does NOT trust that Tier-1
``validate`` ran (a scheduler-triggered run skips CLI steps), so every
``run`` / ``backfill`` entry re-checks a narrow list of critical
preconditions before any pipeline work:

1. Referenced plugin not registered on its axis — secret backends, alert
   sinks, assertion types. The destination axis is governed by condition 2.
2. Destination capability, three sub-checks in order:
   a. the resolved destination name must resolve as a dlt destination
      (``Destination.from_reference`` — the typo guard at the layer that
      owns destination names);
   b. if a ``DestinationAdapter`` is registered for its engine name, the
      adapter must load and expose the full capability surface (the
      :class:`~dlt_ops.destinations.protocol.DestinationAdapter`
      Protocol members are the contract — a present-but-broken adapter is
      a hard fail);
   c. if no adapter is registered, the run proceeds in core mode unless it
      engages an adapter-gated gate feature — checkpoints, assertion
      quarantine on a selected resource, a caller-declared requirement
      (backfill) — or ``[dlt_ops] require_destination_adapter``
      demands one.
3. Plugin soft-failed at load (the runtime would otherwise silently lose
   the feature) — same axes as 1, plus the destination adapter when one is
   registered.
4. ``[dlt_ops.rules]`` references an unknown rule ID (typo guard).
5. Source's incremental cursor missing when backfill bounds were supplied.

Conditions 1 and 3 cover every plugin axis; ``check_secret_backends``
applies them to the secret-backend engaged per source (implicit v0.1
selection, see ``dlt_ops.secrets``) when ``sources`` flow in,
``check_alert_sinks`` to every explicitly configured
``[dlt_ops] alert_sinks`` name, and ``check_assertion_types`` to every
assertion type referenced by the running source's (selected resources')
assertions config — a coverage extension of condition 1, not a new condition
(assertions spec §7).

Intentionally redundant with Tier 1 — the redundancy is the point. Keep it
narrow: the five conditions, nothing else.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any

from dlt_ops.assertions.config import parse_assertions
from dlt_ops.assertions.models import ASSERTION_AXIS
from dlt_ops.config import ProjectConfig
from dlt_ops.destinations import ADAPTER_GATED_FEATURES, engine_name, has_adapter
from dlt_ops.destinations.protocol import DestinationAdapter
from dlt_ops.discovery.validator import check_unknown_rule_ids, load_rule_specs
from dlt_ops.discovery.validators.config import _source_uses_secrets
from dlt_ops.plugins import registry as _default_registry
from dlt_ops.secrets.setup import SECRET_BACKEND_AXIS, resolve_backend

__all__ = [
    "AdapterCapabilityError",
    "DestinationCapabilityError",
    "MissingIncrementalCursorError",
    "PluginLoadFailedError",
    "PluginNotRegisteredError",
    "PreflightError",
    "UnknownDestinationError",
    "UnknownRuleIdError",
    "check_alert_sinks",
    "check_assertion_types",
    "check_destination_adapter",
    "check_destination_capability",
    "check_incremental_cursor",
    "check_plugin_load_failures",
    "check_plugin_registered",
    "check_rule_ids",
    "check_secret_backends",
    "run_preflight",
]


class PreflightError(Exception):
    """Base for Tier-2 preflight failures; raised before any pipeline work starts."""


class PluginNotRegisteredError(PreflightError):
    """A referenced plugin name has no registration on its axis."""


class PluginLoadFailedError(PreflightError):
    """A referenced plugin is registered but its load raised (soft-fail record)."""


class UnknownDestinationError(PreflightError):
    """The resolved destination name is not a destination dlt can resolve (typo guard)."""


class AdapterCapabilityError(PreflightError):
    """A destination adapter is missing part of the required capability surface."""


class DestinationCapabilityError(PreflightError):
    """The run engages an adapter-gated feature on a destination with no registered adapter."""


class UnknownRuleIdError(PreflightError):
    """``[dlt_ops.rules]`` references a rule ID no provider registered."""


class MissingIncrementalCursorError(PreflightError):
    """Backfill bounds were supplied but a selected resource declares no incremental cursor."""


def _protocol_members(protocol: type) -> frozenset[str]:
    """Public members (attributes + methods) a Protocol requires.

    Hand-rolled because ``typing.get_protocol_members`` needs Python 3.12 and
    the package targets 3.11. Annotated attributes and defined callables on
    every non-``typing`` base count; underscore names don't.
    """
    members: set[str] = set()
    for base in protocol.__mro__:
        if base is object or base.__module__ == "typing":
            continue
        members.update(name for name in getattr(base, "__annotations__", {}) if not name.startswith("_"))
        members.update(name for name, value in vars(base).items() if not name.startswith("_") and callable(value))
    return frozenset(members)


def check_plugin_registered(axis: str, name: str, *, registry: Any = _default_registry) -> None:
    """Condition 1 — the referenced plugin must be registered on its axis.

    Axis-generic on purpose: the same check covers secret backend, alert
    sink, assertion, and orchestrator references as those call sites land.
    The destination axis is the exception — adapter absence there is a
    capability question, answered by :func:`check_destination_capability`.

    Raises:
        PluginNotRegisteredError: no registration for ``<axis>/<name>``.
    """
    registered = registry.names(axis)
    if name not in registered:
        known = ", ".join(repr(known) for known in registered) or "(none)"
        raise PluginNotRegisteredError(
            f"no plugin {name!r} registered for axis {axis!r}; registered: {known}. "
            f"Install one under the 'dlt_ops.{axis}' entry-point group, "
            f"then inspect the registry with `dlt-ops plugins doctor`."
        )


def check_plugin_load_failures(axis: str, name: str, *, registry: Any = _default_registry) -> None:
    """Condition 3 — the referenced plugin must not have soft-failed at load.

    The registry records load failures instead of crashing; a run that kept
    going would silently lose the feature.

    Raises:
        PluginLoadFailedError: a recorded load failure exists for ``<axis>/<name>``.
    """
    for failure in registry.failures():
        if failure.axis == axis and failure.name == name:
            raise PluginLoadFailedError(f"plugin {axis}/{name} failed to load: {failure.error}")


def check_destination_adapter(name: str, *, registry: Any = _default_registry) -> None:
    """Condition 2b — a registered destination adapter must expose the full capability surface.

    Loads the adapter (instantiating a registered class, matching
    ``destinations.get_adapter``) and probes every
    :class:`~dlt_ops.destinations.protocol.DestinationAdapter` member.
    Applies only when an adapter is registered for the engine name — a
    present-but-broken adapter silently losing installed features stays a
    hard fail.

    Raises:
        PluginLoadFailedError: loading or instantiating the adapter raised.
        AdapterCapabilityError: one or more Protocol members are missing.
    """
    try:
        plugin = registry.get("destination", name)
        adapter = plugin() if isinstance(plugin, type) else plugin
    except Exception as exc:
        raise PluginLoadFailedError(f"plugin destination/{name} failed to load: {exc}") from exc
    missing = sorted(member for member in _protocol_members(DestinationAdapter) if not hasattr(adapter, member))
    if missing:
        raise AdapterCapabilityError(
            f"destination adapter {name!r} is missing required capability member(s): {', '.join(missing)}. "
            f"The DestinationAdapter Protocol is the contract; the plugin is incomplete or outdated."
        )


def check_destination_capability(
    destination: str,
    *,
    require_adapter: bool = False,
    uses_checkpoints: bool = False,
    adapter_required_for: str | None = None,
    quarantine_resources: Collection[str] = (),
    registry: Any = _default_registry,
) -> None:
    """Condition 2 — the destination must support everything this run engages.

    Three sub-checks, in order (ordering is message quality: a typo reports
    "unknown destination", never "no adapter"):

    a. ``destination`` must resolve as a dlt destination.
    b. If a ``DestinationAdapter`` is registered for its engine name, the
       adapter must not have soft-failed at load and must expose the full
       capability surface (conditions 3 and 2b on the destination axis).
    c. If no adapter is registered, the run proceeds in core mode unless an
       adapter-gated feature is engaged: ``require_adapter`` (the
       ``[dlt_ops] require_destination_adapter`` knob),
       ``uses_checkpoints``, a caller-declared requirement
       (``adapter_required_for``, e.g. backfill's chunk state), or assertion
       quarantine on a selected resource (``quarantine_resources``).

    Raises:
        UnknownDestinationError: dlt cannot resolve ``destination`` (dlt's
            resolution message is included).
        PluginLoadFailedError: the registered adapter soft-failed at load.
        AdapterCapabilityError: the registered adapter is capability-incomplete.
        DestinationCapabilityError: no adapter is registered and at least one
            adapter-gated feature is engaged.
    """
    # dlt import stays function-local so importing this module keeps the CLI
    # import budget flat.
    from dlt.common.destination import Destination
    from dlt.common.destination.exceptions import UnknownDestinationModule

    try:
        resolved = Destination.from_reference(destination)
    except UnknownDestinationModule as exc:
        raise UnknownDestinationError(f"destination {destination!r} is not a dlt destination: {exc}") from exc

    if has_adapter(destination, registry=registry):
        name = engine_name(resolved)
        check_plugin_load_failures("destination", name, registry=registry)
        check_destination_adapter(name, registry=registry)
        return

    engaged: list[str] = []
    if require_adapter:
        engaged.append("require_destination_adapter = true ([dlt_ops])")
    if uses_checkpoints:
        engaged.append("checkpoints")
    if adapter_required_for is not None:
        engaged.append(adapter_required_for)
    if quarantine_resources:
        engaged.append(
            f'assertion quarantine (on_failure = "quarantine" on resource(s): '
            f"{', '.join(sorted(quarantine_resources))})"
        )
    if not engaged:
        return
    registered = ", ".join(repr(known) for known in registry.names("destination")) or "(none)"
    raise DestinationCapabilityError(
        f"destination {destination!r} has no registered DestinationAdapter, but this run engages "
        f"adapter-gated feature(s): {'; '.join(engaged)}. Features gated on an adapter: "
        f"{', '.join(ADAPTER_GATED_FEATURES)}. Registered adapters: {registered}. Install a DestinationAdapter "
        f"under the 'dlt_ops.destination' entry-point group, switch to a destination that has one, or "
        f"remove the feature from the run; see docs/reference/destinations.md."
    )


def _quarantine_resources(
    source_section: str | None,
    raw_config: Mapping[str, Any] | None,
    selected_resources: Collection[str] | None,
) -> tuple[str, ...]:
    """(Selected) resource names whose parsed assertions set ``on_failure = "quarantine"``.

    Thin guard over
    :meth:`~dlt_ops.assertions.config.ParsedAssertions.quarantine_resources`
    (the derivation shared with Tier-1 ``validate``): no source section means
    no assertions config to consult. ``None`` selection mirrors the scope
    :func:`check_assertion_types` applies — every configured resource counts.
    """
    if source_section is None:
        return ()
    return parse_assertions(raw_config or {}, source_section).quarantine_resources(selected_resources)


def _needs_secret_path(source: Any) -> bool:
    """The "needs a resolvable secret path" trigger: dlt.secrets in the source signature.

    Shares :func:`~dlt_ops.discovery.validators.config._source_uses_secrets`
    with Tier-1 ``validate``. A source that Phase 2 never introspected can't be
    inspected — assume it might use secrets (the heuristic's own policy).
    """
    if not getattr(source, "is_introspected", False):
        return True
    return _source_uses_secrets(source.source_fn)


def check_secret_backends(
    sources: Mapping[str, Any],
    raw_config: Mapping[str, Any] | None = None,
    *,
    registry: Any = _default_registry,
) -> None:
    """Conditions 1 & 3 on the secret-backend axis, per discovered source.

    Resolution mirrors ``setup_secrets``: every registered backend may claim
    the source's ``[sources.<X>.dlt_ops]`` table; no claim = the
    ``secrets_toml`` default. The engaged backend (default included) must be
    registered and must not have soft-failed at load. Sources that neither
    engage a backend nor need a resolvable secret path are skipped.

    Raises:
        PluginLoadFailedError: resolution itself failed — an installed
            backend raised at load or claim time, or two backends claim one
            source; the run would otherwise silently lose its secrets.
        PluginNotRegisteredError: the engaged backend has no registration.
    """
    config = raw_config or {}
    for name, source in sources.items():
        try:
            engagement = resolve_backend(source.config_section, config, registry=registry)
        except Exception as exc:
            raise PluginLoadFailedError(f"secret-backend resolution failed for source {name!r}: {exc}") from exc
        if not engagement.requests and not _needs_secret_path(source):
            continue
        check_plugin_registered(SECRET_BACKEND_AXIS, engagement.name, registry=registry)
        check_plugin_load_failures(SECRET_BACKEND_AXIS, engagement.name, registry=registry)


def check_alert_sinks(project_config: ProjectConfig, *, registry: Any = _default_registry) -> None:
    """Conditions 1 & 3 on the alert-sink axis, per explicitly configured sink.

    Only names the project actually wrote into ``[dlt_ops]
    alert_sinks`` are checked (``None`` = key unset = the core logging
    default, which ships with the package). Each configured name must be
    registered, load, AND construct with its
    ``[dlt_ops.alert_sink.<name>]`` options (the instantiation step
    matches ``check_destination_adapter``) — an extra-gated sink (e.g.
    ``sentry`` without ``dlt-ops[sentry]``) loads but raises at
    construction, and a run that kept going would silently lose its alerts.

    Raises:
        PluginNotRegisteredError: no registration for ``alert_sink/<name>``.
        PluginLoadFailedError: the sink's load or construction raised.
    """
    for name in project_config.alert_sinks or ():
        check_plugin_registered("alert_sink", name, registry=registry)
        try:
            plugin = registry.get("alert_sink", name)
            if isinstance(plugin, type):
                plugin(**project_config.alert_sink_options.get(name, {}))
        except Exception as exc:
            raise PluginLoadFailedError(f"plugin alert_sink/{name} failed to load: {exc}") from exc


def check_assertion_types(
    source_section: str,
    raw_config: Mapping[str, Any] | None,
    selected_resources: Collection[str] | None = None,
    *,
    registry: Any = _default_registry,
) -> None:
    """Conditions 1 & 3 on the assertion axis, per referenced assertion type.

    Coverage extension of condition 1 (assertions spec §7): every assertion
    type name referenced by the (selected) resources' assertions config must
    be registered and must not have soft-failed at load — a run that kept
    going would silently skip the data-quality gate. Custom predicates are
    not plugins and are covered by engine construction instead.

    Raises:
        PluginNotRegisteredError: a referenced type has no registration.
        PluginLoadFailedError: a referenced type soft-failed at load.
    """
    parsed = parse_assertions(raw_config or {}, source_section)
    for name in parsed.referenced_types(selected_resources):
        check_plugin_registered(ASSERTION_AXIS, name, registry=registry)
        check_plugin_load_failures(ASSERTION_AXIS, name, registry=registry)


def check_rule_ids(project_config: ProjectConfig) -> None:
    """Condition 4 — every ``[dlt_ops.rules]`` entry must name a known rule.

    Shares :func:`~dlt_ops.discovery.validator.check_unknown_rule_ids`
    with Tier-1 ``validate`` so the two tiers can't drift.

    Raises:
        UnknownRuleIdError: at least one configured rule ID is unknown.
    """
    assembly = load_rule_specs()
    unknown = check_unknown_rule_ids(project_config.rules, assembly.known_ids)
    if unknown:
        listed = ", ".join(sorted(assembly.known_ids)) or "(none)"
        raise UnknownRuleIdError(
            f"unknown rule id(s) in [dlt_ops.rules]: {', '.join(unknown)}; valid rule ids: {listed}"
        )


def check_incremental_cursor(source: Any, bounds: tuple[Any, Any] | None) -> None:
    """Condition 5 — backfill bounds require an incremental cursor on every selected resource.

    ``source`` is a live dlt source instance. Without a cursor the injected
    interval is silently ignored and each chunk re-extracts everything.
    No bounds = nothing to check.

    Raises:
        MissingIncrementalCursorError: bounds supplied and at least one
            selected resource has no incremental cursor.
    """
    if bounds is None:
        return
    missing = sorted(
        name for name, resource in source.selected_resources.items() if getattr(resource, "incremental", None) is None
    )
    if missing:
        raise MissingIncrementalCursorError(
            f"backfill bounds were supplied but resource(s) without an incremental cursor are selected: "
            f"{', '.join(missing)}. Declare a dlt.sources.incremental cursor or deselect them."
        )


def run_preflight(
    *,
    destination: str,
    project_config: ProjectConfig,
    source: Any | None = None,
    bounds: tuple[Any, Any] | None = None,
    sources: Mapping[str, Any] | None = None,
    raw_config: Mapping[str, Any] | None = None,
    source_section: str | None = None,
    uses_checkpoints: bool = False,
    adapter_required_for: str | None = None,
    registry: Any = _default_registry,
) -> None:
    """Run all five Tier-2 checks; the first violated condition raises its typed error.

    Called at the top of every ``run`` / ``backfill`` entry. ``source`` is the
    live dlt source instance (consulted when ``bounds`` are supplied, and for
    the selected-resource scope of the assertion and quarantine checks);
    ``sources`` is the discovered-source mapping and ``raw_config`` the parsed
    ``.dlt/config.toml`` — supplied together, they extend conditions 1 & 3 to
    each source's engaged secret backend. Explicitly configured alert sinks
    get the same two conditions via ``check_alert_sinks``; ``source_section``
    plus ``raw_config`` extend them to every assertion type the running
    source references via ``check_assertion_types``.

    The destination capability check (condition 2) additionally consumes
    ``uses_checkpoints`` (the source's checkpoint-decorator detection flag)
    and ``adapter_required_for`` (a verb's own adapter-gated requirement,
    e.g. backfill's chunk state); assertion-quarantine engagement is derived
    here from the same assertions config the assertion check parses.
    """
    selected = tuple(source.selected_resources.keys()) if source is not None else None
    check_destination_capability(
        destination,
        require_adapter=project_config.require_destination_adapter,
        uses_checkpoints=uses_checkpoints,
        adapter_required_for=adapter_required_for,
        quarantine_resources=_quarantine_resources(source_section, raw_config, selected),
        registry=registry,
    )
    if sources is not None:
        check_secret_backends(sources, raw_config, registry=registry)
    check_alert_sinks(project_config, registry=registry)
    if source_section is not None:
        check_assertion_types(source_section, raw_config, selected, registry=registry)
    check_rule_ids(project_config)
    if source is not None:
        check_incremental_cursor(source, bounds)

import inspect
from typing import Any

from dlt_ops.assertions.config import parse_assertions
from dlt_ops.config import (
    ProjectConfig,
    ProjectConfigError,
    UnresolvedDestinationError,
    load_project_config,
    resolve_destination,
)
from dlt_ops.destinations import core_mode_notice, has_adapter
from dlt_ops.discovery.models import (
    Schedule,
    ValidationContext,
    ValidationError,
)
from dlt_ops.plugins import registry as plugins
from dlt_ops.secrets.setup import SECRET_BACKEND_AXIS, resolve_backend


def _source_uses_secrets(source_fn: Any) -> bool:
    """Check if source function uses dlt.secrets.value in its signature."""
    try:
        sig = inspect.signature(source_fn)
        for param in sig.parameters.values():
            if param.default is not inspect.Parameter.empty:
                if "dlt.secrets" in str(param.default):
                    return True
        return False
    except Exception:
        # If we can't inspect, assume it might use secrets
        return True


def validate_config_sections(ctx: ValidationContext) -> list[ValidationError]:
    """Check every discovered source has [sources.{config_section}] in config.toml."""
    errors: list[ValidationError] = []
    sources_config = ctx.config.get("sources", {})
    config_sections = set(sources_config.keys())

    for name, source in ctx.sources.items():
        if source.config_section not in config_sections:
            errors.append(
                ValidationError(
                    source_name=name,
                    field="config_section",
                    message=f"Missing config section [sources.{source.config_section}]",
                )
            )
    return errors


def validate_schedules(ctx: ValidationContext) -> list[ValidationError]:
    """Check schedule field exists and is one of the values in the Schedule enum."""
    errors: list[ValidationError] = []
    sources_config = ctx.config.get("sources", {})

    for name, source in ctx.sources.items():
        section = sources_config.get(source.config_section, {})
        ext = section.get("dlt_ops", {})

        schedule_str = ext.get("schedule")
        if not schedule_str:
            errors.append(
                ValidationError(
                    source_name=name,
                    field="schedule",
                    message=f"Missing 'schedule' field in [sources.{source.config_section}.dlt_ops]",
                )
            )
        else:
            try:
                Schedule.from_string(schedule_str)
            except ValueError as e:
                errors.append(
                    ValidationError(
                        source_name=name,
                        field="schedule",
                        message=str(e),
                    )
                )
    return errors


def validate_secret_backends(ctx: ValidationContext) -> list[ValidationError]:
    """Check each source's engaged secret backend is registered and healthy.

    Tier-1 twin of preflight's ``check_secret_backends`` — both consume
    ``dlt_ops.secrets.setup.resolve_backend`` so the tiers can't drift.
    A source that neither engages a backend nor uses ``dlt.secrets`` in its
    signature needs no resolvable secret path and is skipped.
    """
    errors: list[ValidationError] = []
    for name, source in ctx.sources.items():
        try:
            engagement = resolve_backend(source.config_section, ctx.config)
        except Exception as e:
            errors.append(
                ValidationError(
                    source_name=name,
                    field="secret_backend",
                    message=f"secret-backend resolution failed: {type(e).__name__}: {e}",
                )
            )
            continue
        if not engagement.requests and not _source_uses_secrets(source.source_fn):
            continue
        if engagement.name not in plugins.names(SECRET_BACKEND_AXIS):
            errors.append(
                ValidationError(
                    source_name=name,
                    field="secret_backend",
                    message=f"Source needs secret backend '{engagement.name}' but no such plugin is registered "
                    f"under the 'dlt_ops.secret_backend' entry-point group; "
                    f"inspect with `dlt-ops plugins doctor`",
                )
            )
            continue
        errors.extend(
            ValidationError(
                source_name=name,
                field="secret_backend",
                message=f"secret backend '{engagement.name}' failed to load: {failure.error}",
            )
            for failure in plugins.failures()
            if failure.axis == SECRET_BACKEND_AXIS and failure.name == engagement.name
        )
    return errors


def validate_alert_sinks(ctx: ValidationContext) -> list[ValidationError]:
    """Check every configured alert sink is a registered, loadable plugin.

    Tier-1 twin of preflight's ``check_alert_sinks``: only names explicitly
    written into ``[dlt_ops] alert_sinks`` are enforced (the key unset
    means the core logging default, which ships with the package). Each name
    must be registered on the ``alert_sink`` axis, load, and construct with
    its ``[dlt_ops.alert_sink.<name>]`` options — an extra-gated sink
    (e.g. ``sentry`` without ``dlt-ops[sentry]``) loads but raises at
    construction, and a typo'd option key raises there too.
    """
    table = ctx.config.get("dlt_ops")
    raw = table.get("alert_sinks") if isinstance(table, dict) else None
    if raw is None:
        return []
    if not isinstance(raw, list) or any(not isinstance(name, str) for name in raw):
        return [
            ValidationError(
                source_name="dlt_ops.alert_sinks",
                field="alert_sinks",
                message='[dlt_ops] alert_sinks must be a list of sink-name strings, e.g. alert_sinks = ["logging"]',
            )
        ]
    options_table = table.get("alert_sink") if isinstance(table, dict) else None
    options_by_sink = options_table if isinstance(options_table, dict) else {}
    errors: list[ValidationError] = []
    for name in raw:
        if name not in plugins.names("alert_sink"):
            errors.append(
                ValidationError(
                    source_name="dlt_ops.alert_sinks",
                    field="alert_sinks",
                    message=f"alert_sinks references '{name}' but no such plugin is registered under the "
                    f"'dlt_ops.alert_sink' entry-point group; inspect with `dlt-ops plugins doctor`",
                )
            )
            continue
        options = options_by_sink.get(name)
        try:
            plugin = plugins.get("alert_sink", name)
            if isinstance(plugin, type):
                plugin(**(options if isinstance(options, dict) else {}))
        except Exception as e:
            errors.append(
                ValidationError(
                    source_name="dlt_ops.alert_sinks",
                    field="alert_sinks",
                    message=f"alert sink '{name}' failed to load: {type(e).__name__}: {e}",
                )
            )
    return errors


def validate_destination_capability(ctx: ValidationContext) -> list[ValidationError]:
    """Check each source's destination supports every adapter-gated feature its config engages.

    Tier-1 twin of preflight's ``check_destination_capability`` — the rule
    consumes the same check, so the tiers can't drift on what a destination
    must support. Per source, an error when the destination is unresolvable
    through the config chain (per-source override, then project default),
    is not a destination dlt can resolve (typo guard), or has a registered
    adapter that fails to load or is capability-incomplete. A destination
    with no registered adapter runs in core mode: a warning naming the dark
    features, upgraded to an error when the source engages one — assertion
    quarantine, ``@with_checkpoints`` (AST-detected; an aliased import
    escapes) — or ``[dlt_ops] require_destination_adapter`` demands
    the full tier.
    """
    # Function-local import: preflight imports this module (it shares
    # _source_uses_secrets), so a top-level import would be circular.
    from dlt_ops.preflight import PreflightError, check_destination_capability

    try:
        project_config = load_project_config(ctx.project_root)
    except ProjectConfigError:
        project_config = ProjectConfig()

    errors: list[ValidationError] = []
    for name, source in ctx.sources.items():
        try:
            destination = resolve_destination(source.config, project_config)
        except UnresolvedDestinationError as e:
            errors.append(ValidationError(source_name=name, field="destination", message=str(e)))
            continue
        quarantine = parse_assertions(ctx.config, source.config_section).quarantine_resources()
        try:
            check_destination_capability(
                destination,
                require_adapter=project_config.require_destination_adapter,
                uses_checkpoints=source.uses_checkpoints,
                quarantine_resources=quarantine,
            )
        except PreflightError as e:
            errors.append(ValidationError(source_name=name, field="destination", message=str(e)))
            continue
        if not has_adapter(destination):
            errors.append(
                ValidationError(
                    source_name=name,
                    field="destination",
                    message=core_mode_notice(destination),
                    is_warning=True,
                )
            )
    return errors


def validate_decorator_names(ctx: ValidationContext) -> list[ValidationError]:
    """Check explicit name parameter required in @dlt.source decorator."""
    errors: list[ValidationError] = []

    for name, source in ctx.sources.items():
        if not source.decorator_name:
            errors.append(
                ValidationError(
                    source_name=name,
                    field="decorator_name",
                    message=f"Source function '{source.function_name}' must have explicit @dlt.source(name='...') parameter. "
                    f'Add @dlt.source(name="{source.config_section}") to the decorator.',
                )
            )
    return errors


def validate_module_names(ctx: ValidationContext) -> list[ValidationError]:
    """Check module filename equals config section (for dlt.secrets.value resolution)."""
    errors: list[ValidationError] = []

    for name, source in ctx.sources.items():
        if source.module_stem != source.config_section:
            errors.append(
                ValidationError(
                    source_name=name,
                    field="module_stem",
                    message=f"Module filename mismatch: '{source.module_stem}.py' but config section is "
                    f"'{source.config_section}'. Rename module to '{source.config_section}.py' "
                    f"so dlt.secrets.value resolves correctly in resources.",
                )
            )
    return errors


def validate_orphan_sections(ctx: ValidationContext) -> list[ValidationError]:
    """Warn about orphan config sections (no matching source)."""
    errors: list[ValidationError] = []
    sources_config = ctx.config.get("sources", {})
    config_sections = set(sources_config.keys())

    used_sections = {source.config_section for source in ctx.sources.values()}

    orphan_sections = config_sections - used_sections
    # Exclude known non-source sections
    known_non_source_sections = {"data_writer", "normalize", "load", "extract"}
    orphan_sections = orphan_sections - known_non_source_sections

    for section in orphan_sections:
        errors.append(
            ValidationError(
                source_name=section,
                field="config_section",
                message=f"Orphan config section [sources.{section}] has no matching source",
                is_warning=True,
            )
        )
    return errors

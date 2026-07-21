"""The three always-on assertion rules (assertions spec §6).

Three rule IDs, not one, so a project can exempt column checking (e.g. a
source with intentionally dynamic columns) without also disabling structural
config validation:

- ``assertion_config_valid`` — structural + type-aware config checks.
- ``assertion_columns_exist`` — column references vs the resource's declared
  Pydantic model; skipped (not failed) when the model is unresolvable.
- ``assertion_predicate_resolvable`` — custom predicates import and are
  callable, probed in the same Rule-15 audit-hook sandbox child that checks
  source modules: predicate import side effects never run in the validate
  process, and import-time network I/O / disk writes / pipeline runs /
  process spawns in predicate modules are reported alongside resolvability.

No ``--include-assertions`` flag and no dry-run: config checks are always-on
in bare ``validate``; facts that require extracting data are ``run``'s job.
"""

from __future__ import annotations

from dlt_ops.assertions.config import (
    ParsedAssertions,
    check_specs,
    declared_columns_for_resource,
    load_assertion_type,
    parse_assertions,
    reserved_plugin_names,
)
from dlt_ops.assertions.models import AssertionContext
from dlt_ops.discovery.models import SourceInfo, ValidationContext, ValidationError
from dlt_ops.discovery.phase2 import _SandboxVerdict, run_predicate_sandbox_check
from dlt_ops.plugins import registry as plugins


def _parsed_for(source: SourceInfo, ctx: ValidationContext) -> ParsedAssertions:
    return parse_assertions(ctx.config, source.config_section)


def _issue_errors(source_name: str, parsed: ParsedAssertions) -> list[ValidationError]:
    return [
        ValidationError(source_name=source_name, field=issue.field, message=issue.message) for issue in parsed.issues
    ]


def validate_assertion_config(ctx: ValidationContext) -> list[ValidationError]:
    """Assertion config must be structurally valid and name registered types.

    Emits (spec §6, rule ``assertion_config_valid``): non-table assertions or
    resource entries; resource keys naming no resource of the source (Phase-2
    authoritative list); unknown assertion type keys; ``on_failure`` outside
    {fail, quarantine, warn} at any level; ``quarantine`` on a batch-scoped
    type; params rejected by the type's ``check_config`` (shape/domain — the
    column-existence half lives in ``assertion_columns_exist``); malformed
    ``custom`` entries; a plugin registering a reserved name.
    """
    errors: list[ValidationError] = []
    for name in reserved_plugin_names(registry=plugins):
        errors.append(
            ValidationError(
                source_name="dlt_ops.assertions",
                field="assertions",
                message=f"assertion plugin registers the reserved name {name!r} — reserved keys "
                f"(on_failure, custom) can never be referenced from an assertions table",
            )
        )
    for name, source in ctx.sources.items():
        parsed = _parsed_for(source, ctx)
        errors.extend(_issue_errors(name, parsed))

        def _structural_context(resource_name: str, section: str = source.config_section) -> AssertionContext:
            # declared_columns=None on purpose: the column-existence half is
            # assertion_columns_exist's job (separately exemptible).
            return AssertionContext(source_section=section, resource_name=resource_name, declared_columns=None)

        errors.extend(
            ValidationError(source_name=name, field=issue.field, message=issue.message)
            for issue in check_specs(
                parsed,
                known_resources=set(source.resources),
                context_for=_structural_context,
                registry=plugins,
            )
        )
    return errors


def validate_assertion_columns(ctx: ValidationContext) -> list[ValidationError]:
    """Columns referenced by assertion params must exist on the declared model.

    Runs each type's ``check_config`` twice — once without and once with the
    resource's declared Pydantic columns — and reports only the additional
    errors, i.e. exactly the column-existence findings. Skipped (not failed)
    for a resource whose model is unresolvable — ``pydantic_columns_required``
    already polices that separately.
    """
    errors: list[ValidationError] = []
    for name, source in ctx.sources.items():
        parsed = _parsed_for(source, ctx)
        if not parsed.has_assertions:
            continue
        try:
            source_instance = source.source_fn()
        except Exception:
            continue
        for res in parsed.resources:
            resource = source_instance.resources.get(res.resource_name)
            if resource is None:
                continue  # assertion_config_valid flags unknown resources
            declared = declared_columns_for_resource(resource)
            if declared is None:
                continue
            for spec in res.specs:
                if spec.is_custom:
                    continue
                try:
                    impl = load_assertion_type(spec.type_name, registry=plugins)
                except Exception:
                    continue
                base_ctx = AssertionContext(
                    source_section=source.config_section, resource_name=res.resource_name, declared_columns=None
                )
                full_ctx = AssertionContext(
                    source_section=source.config_section,
                    resource_name=res.resource_name,
                    declared_columns=declared,
                )
                structural = set(impl.check_config(spec.params, base_ctx))
                errors.extend(
                    ValidationError(
                        source_name=name,
                        field=f"assertions.{res.resource_name}.{spec.type_name}",
                        message=message,
                    )
                    for message in impl.check_config(spec.params, full_ctx)
                    if message not in structural
                )
    return errors


def validate_assertion_predicates(ctx: ValidationContext) -> list[ValidationError]:
    """Custom predicates must import cleanly and resolve to a callable.

    Emits (rule ``assertion_predicate_resolvable``): module unimportable,
    attribute missing, attribute not callable — worded by the engine's own
    ``resolve_predicate``, so validate fails exactly the way ``run`` would —
    plus one Rule-15 finding per import-time violation (network I/O, disk
    write, pipeline run, process spawn) recorded by the audit-hook sandbox
    the predicate module is imported in. Each distinct predicate path is
    probed once per validate pass.
    """
    errors: list[ValidationError] = []
    verdicts: dict[str, _SandboxVerdict] = {}
    for name, source in ctx.sources.items():
        parsed = _parsed_for(source, ctx)
        for res in parsed.resources:
            for index, spec in enumerate(spec for spec in res.specs if spec.is_custom):
                predicate = spec.predicate
                if predicate is None:
                    continue  # malformed entries are assertion_config_valid findings
                if predicate not in verdicts:
                    verdicts[predicate] = run_predicate_sandbox_check(predicate, project_root=ctx.project_root)
                verdict = verdicts[predicate]
                field = f"assertions.{res.resource_name}.custom[{index}]"
                if verdict.sandbox_error is not None:
                    errors.append(
                        ValidationError(
                            source_name=name,
                            field=field,
                            message=f"custom predicate {predicate!r} probe failed: {verdict.sandbox_error}",
                        )
                    )
                elif verdict.import_error is not None:
                    errors.append(
                        ValidationError(
                            source_name=name,
                            field=field,
                            message=f"custom predicate {predicate!r} is not resolvable: {verdict.import_error}",
                        )
                    )
                errors.extend(
                    ValidationError(
                        source_name=name,
                        field=field,
                        message=f"Rule 15: {violation.kind} at import of predicate {predicate!r} — "
                        f"{violation.event}({violation.target})",
                    )
                    for violation in verdict.violations
                )
    return errors

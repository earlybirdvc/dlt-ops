"""Core platform rules over pipeline source trees.

Mostly AST checks: they scan every .py file in a pipeline directory (tests
excluded) and never import project code. The exception is
:func:`validate_incremental_cursor_required`, which reads the live resource
rather than the source text — whether a resource carries a cursor is settled by
``apply_hints`` and factory code an AST cannot follow, and Phase 2 has already
imported the module. The rule IDs these validators register under live in
``discovery.validators.CORE_RULES``.
"""

import ast
import logging
from collections import defaultdict
from pathlib import Path

from dlt_ops.discovery.models import (
    Schedule,
    SourceInfo,
    ValidationContext,
    ValidationError,
    resolve_load_timestamp_column,
)
from dlt_ops.discovery.validators._common import (
    find_calls,
    get_keyword,
    iter_py_files,
    parse_file,
    rel,
    unique_pipeline_dirs,
)
from dlt_ops.schema_contracts import CANONICAL_SCHEMA_CONTRACT, EVOLVE_SCHEMA_CONTRACT

logger = logging.getLogger(__name__)

INCREMENTAL_CURSOR_RULE_ID = "incremental_cursor_required"
"""Rule ID of the opt-in missing-cursor rule; the exemption message quotes it."""

# Schedules that make a full refresh repeat on a cadence. @manual is excluded:
# a source only run on demand re-reads everything when someone asks it to,
# which is the case the rule has no opinion about.
_RECURRING_SCHEDULES = frozenset(Schedule) - {Schedule.MANUAL}


# The remedy for a declared-but-non-canonical contract, worded once because
# two call sites emit it. "Omit it" is the advice in both cases, but the reason
# splits on the columns= hint: dlt derives a contract from a Pydantic model at
# decoration time and never leaves those resources for the runtime to fill in,
# so citing the runtime auto-apply alone would be wrong for exactly the
# resources `pydantic_columns_required` mandates.
_NON_CANONICAL_REMEDY = (
    "Omit it — with a Pydantic columns= model dlt derives the contract from the model's extra "
    "(keep that canonical via extra='forbid'), and without one the runtime applies the canonical "
    f"literal — or declare it inline: schema_contract={CANONICAL_SCHEMA_CONTRACT}"
)


def _is_dlt_resource_func(func: ast.expr) -> bool:
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "resource"
        and isinstance(func.value, ast.Name)
        and func.value.id == "dlt"
    )


def _is_incremental_func(func: ast.expr) -> bool:
    """Match dlt.sources.incremental and dlt.sources.incremental[T]."""
    if isinstance(func, ast.Subscript):
        func = func.value
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "incremental"
        and isinstance(func.value, ast.Attribute)
        and func.value.attr == "sources"
        and isinstance(func.value.value, ast.Name)
        and func.value.value.id == "dlt"
    )


def _resource_name_literal(call: ast.Call) -> str | None:
    """Extract the `name="..."` string literal from a @dlt.resource call.

    Returns None if name= is missing or non-literal (attribute, f-string, call).
    Callers use this to look up the owning source in the ownership map; a None
    result triggers fallback logic.
    """
    kw = get_keyword(call, "name")
    if kw is None:
        return None
    if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
        return kw.value.value
    return None


def _resource_has_name_kwarg(call: ast.Call) -> bool:
    """True iff a @dlt.resource call has a name= kwarg in any expression form.

    The companion validator only requires PRESENCE — non-literal name= (factory
    patterns like `name=cfg.target_table`, `name=f"..."`) is fine. Ownership
    resolution falls back to strict-check semantics when it can't resolve
    statically.
    """
    return get_keyword(call, "name") is not None


def _sources_by_pipeline_dir(ctx: ValidationContext) -> dict[Path, list[SourceInfo]]:
    """Group sources by their pipeline directory (a dir may host >1 source)."""
    grouped: dict[Path, list[SourceInfo]] = defaultdict(list)
    for source in ctx.sources.values():
        grouped[source.path].append(source)
    return grouped


def _build_resource_ownership_map(
    ctx: ValidationContext,
) -> tuple[dict[str, SourceInfo | None], list[ValidationError]]:
    """Invert SourceInfo.resources into resource_name -> owning SourceInfo.

    Duplicate guard: if two sources claim the same resource name, emit an
    error naming both owners and mark the resource as unmappable (None) so
    downstream lookups skip it. `validate_no_resource_overlap` also flags
    this, but the contract check must be self-consistent even in a
    broken-tree case.
    """
    errors: list[ValidationError] = []
    mapping: dict[str, SourceInfo | None] = {}
    seen: dict[str, SourceInfo] = {}
    for source in ctx.sources.values():
        for resource_name in source.resources:
            if resource_name in seen:
                prior = seen[resource_name]
                errors.append(
                    ValidationError(
                        source_name=f"{prior.name}|{source.name}",
                        field=f"schema_contract.{resource_name}",
                        message=f"resource '{resource_name}' declared by multiple sources — "
                        f"cannot resolve owning source for schema_contract check",
                    )
                )
                mapping[resource_name] = None
            else:
                seen[resource_name] = source
                mapping[resource_name] = source
    return mapping, errors


def _resolve_owning_source(
    call: ast.Call,
    sources_in_dir: list[SourceInfo],
    ownership_map: dict[str, SourceInfo | None],
) -> SourceInfo | None:
    """Resolve the source that owns a @dlt.resource call site.

    Priority:
      1. If name="X" is a string literal and X is in the ownership map: use
         that owner. `None` marker in the map (duplicate) means unmappable —
         return None.
      2. Fall back to the sole source in the pipeline dir when there is
         exactly one (covers non-literal name= AND missing name= in
         single-source dirs).

    Multi-source dir + non-literal / missing name= → None (unresolvable).
    Callers apply a strict tightening for that case: CANONICAL literal is
    always accepted, EVOLVE literal is rejected with a "cannot resolve owning
    source" message, other literals get the standard non-canonical error.
    """
    resource_name = _resource_name_literal(call)
    if resource_name is not None:
        if resource_name in ownership_map:
            return ownership_map[resource_name]
        # name= present but resource unknown to discovery (import failed etc.).
        if len(sources_in_dir) == 1:
            return sources_in_dir[0]
        return None
    if len(sources_in_dir) == 1:
        return sources_in_dir[0]
    return None


def validate_schema_contract(ctx: ValidationContext) -> list[ValidationError]:
    """A declared schema_contract must be valid; an absent one is fine.

    A @dlt.resource with no schema_contract passes, because the contract comes
    from elsewhere either way — and which "elsewhere" depends on the columns=
    hint. With a dict/list hint or none at all dlt derives no contract and the
    runtime applies the canonical literal. With a Pydantic model dlt derives one
    from the model's `extra` at decoration time, so the runtime never fills that
    resource in; `pydantic_model_forbids_extra` is the rule that keeps the
    derivation canonical there.

    A declared contract must be either the canonical literal, or the evolve
    literal on a source that opted in via a non-empty
    `schema_contract_evolve_reason` under [sources.<X>.dlt_ops] in
    .dlt/config.toml.

    Ownership is resolved via a per-resource-name map built from
    `SourceInfo.resources`, then a fallback to the sole source in
    single-source pipeline dirs. When ownership CANNOT be resolved in a
    multi-source dir (non-literal or missing name=), the check tightens:
    CANONICAL is accepted, EVOLVE is rejected with a "cannot resolve owner"
    message (the opt-in cannot be attributed to any source), and other
    declared values get the standard non-canonical error.
    """
    ownership_map, errors = _build_resource_ownership_map(ctx)
    sources_by_dir = _sources_by_pipeline_dir(ctx)

    for pipeline_dir in sorted(sources_by_dir):
        sources_in_dir = sources_by_dir[pipeline_dir]
        for py_file in iter_py_files(pipeline_dir):
            tree = parse_file(py_file)
            if tree is None:
                continue
            for call in find_calls(tree, _is_dlt_resource_func):
                kw = get_keyword(call, "schema_contract")
                if kw is None:
                    continue

                owning_source = _resolve_owning_source(call, sources_in_dir, ownership_map)
                location = f"{rel(py_file, ctx)}:{call.lineno}"
                try:
                    value = ast.literal_eval(kw.value)
                except (ValueError, SyntaxError):
                    value = None

                if value == CANONICAL_SCHEMA_CONTRACT:
                    continue

                if owning_source is None:
                    # Unresolvable owner (multi-source dir + non-literal /
                    # missing name=): EVOLVE cannot be attributed to any
                    # source's opt-in, so it is rejected outright.
                    if value == EVOLVE_SCHEMA_CONTRACT:
                        errors.append(
                            ValidationError(
                                source_name=pipeline_dir.name,
                                field=f"schema_contract.{location}",
                                message=f"@dlt.resource at {location} declares the evolve "
                                f"schema_contract literal but no owning source can be resolved "
                                f"(multi-source pipeline dir '{pipeline_dir.name}' with non-literal "
                                f'or missing name=). Use an explicit string-literal name="..." '
                                f"so the opt-in can be attributed to a specific source.",
                            )
                        )
                        continue
                    errors.append(
                        ValidationError(
                            source_name=pipeline_dir.name,
                            field=f"schema_contract.{location}",
                            message=f"@dlt.resource at {location} has non-canonical schema_contract. "
                            f"{_NON_CANONICAL_REMEDY}",
                        )
                    )
                    continue

                source_name = owning_source.name
                if value == EVOLVE_SCHEMA_CONTRACT:
                    if owning_source.config is not None and owning_source.config.is_schema_contract_evolve:
                        continue
                    errors.append(
                        ValidationError(
                            source_name=source_name,
                            field=f"schema_contract.{location}",
                            message=f"@dlt.resource at {location} declares the evolve schema_contract "
                            f"literal but source '{source_name}' has not opted in. Set a non-empty "
                            f"'schema_contract_evolve_reason' under [sources.{source_name}.dlt_ops] "
                            f"in .dlt/config.toml.",
                        )
                    )
                    continue

                errors.append(
                    ValidationError(
                        source_name=source_name,
                        field=f"schema_contract.{location}",
                        message=f"@dlt.resource at {location} has non-canonical schema_contract. "
                        f"{_NON_CANONICAL_REMEDY}",
                    )
                )

    return errors


def validate_resource_name_explicit_in_multi_source_dir(ctx: ValidationContext) -> list[ValidationError]:
    """Companion to schema_contract_declared: in a pipeline dir with >1
    source, every @dlt.resource must set an explicit `name=` kwarg so
    ownership can be attributed for the schema_contract check.

    The kwarg must be PRESENT — the expression form is not constrained. Factory
    patterns like `name=cfg.target_table` or `name=f"..."` pass; a bare
    `@dlt.resource(...)` decorator (dlt's function-name fallback) fails.
    Single-source dirs are unaffected — the sole source owns any resource in
    the tree.
    """
    errors: list[ValidationError] = []

    for pipeline_dir, sources_in_dir in _sources_by_pipeline_dir(ctx).items():
        if len(sources_in_dir) < 2:
            continue
        for py_file in iter_py_files(pipeline_dir):
            tree = parse_file(py_file)
            if tree is None:
                continue
            for call in find_calls(tree, _is_dlt_resource_func):
                if not _resource_has_name_kwarg(call):
                    location = f"{rel(py_file, ctx)}:{call.lineno}"
                    errors.append(
                        ValidationError(
                            source_name=pipeline_dir.name,
                            field=f"resource.name.{location}",
                            message=f"@dlt.resource at {location} in multi-source pipeline dir "
                            f"'{pipeline_dir.name}' is missing an explicit name= kwarg. "
                            f"Explicit name= is required so resource-to-source ownership "
                            f"can be attributed for the schema_contract check.",
                        )
                    )

    return errors


def _configured_load_timestamp_column(ctx: ValidationContext) -> str | None:
    """Non-empty [dlt_ops] load_timestamp_column, else None.

    The raw-config-shaped view of the one reader
    (``discovery.models.resolve_load_timestamp_column``), so the name this rule
    compares against is byte-for-byte the one the runner stamps.
    """
    table = ctx.config.get("dlt_ops")
    if not isinstance(table, dict):
        return None
    return resolve_load_timestamp_column(table.get("load_timestamp_column"))


def validate_incremental_cursor_required(ctx: ValidationContext) -> list[ValidationError]:
    """Resources of a recurring-schedule source declare an incremental cursor.

    The gap its sibling leaves: ``cursor_not_load_timestamp`` catches a WRONG
    cursor and only when ``load_timestamp_column`` is configured, so a resource
    with no cursor at all re-extracts everything on every run while validate
    exits 0.

    **Opt-in** (``default_on=False``). A full refresh is a legitimate choice —
    small dimension tables are the obvious case — and nothing visible to this
    package distinguishes "chose to full-refresh" from "forgot the cursor". So
    the rule states a policy a project adopts, rather than a defect the package
    can prove. Turn it on with ``[dlt_ops.rules] incremental_cursor_required =
    true``.

    Error, not warning, when it is on: a warning renders but never fails a run
    outside ``--strict``, so a project that deliberately switched the rule on
    would buy visibility and no gate. Opting in is the decision; enforcing it
    is the point.

    Scoped to sources whose config declares a recurring schedule, because the
    harm is a full refresh repeating on a cadence; ``@manual`` sources and
    sources with no parsed config are out of scope. Reads the live resource
    rather than the AST — ``apply_hints(incremental=...)`` and factory-built
    cursors are invisible to a source-text scan, and a false "no cursor" would
    be worse than the gap.

    Granularity note: exemptions are per source, so a source mixing incremental
    and deliberately-full-refresh resources exempts all of them together.
    """
    errors: list[ValidationError] = []

    for name in sorted(ctx.sources):
        source = ctx.sources[name]
        config = source.config
        if config is None or config.schedule not in _RECURRING_SCHEDULES:
            continue
        try:
            instance = source.source_fn()
        except Exception as exc:
            logger.debug(f"Could not instantiate source {name} for cursor check: {exc}")
            continue
        for resource_name in sorted(instance.resources):
            if getattr(instance.resources[resource_name], "incremental", None) is not None:
                continue
            errors.append(
                ValidationError(
                    source_name=name,
                    field=f"incremental.{resource_name}",
                    message=(
                        f"resource '{resource_name}' declares no incremental cursor, so every "
                        f"{config.schedule.value} run of '{name}' re-extracts it in full. Add a "
                        f"dlt.sources.incremental cursor on the provider's business timestamp "
                        f"(e.g. updated_at), or record the intent: "
                        f"[sources.{name}.dlt_ops.rule_exemptions] "
                        f'{INCREMENTAL_CURSOR_RULE_ID} = "<why a full refresh is intended>"'
                    ),
                )
            )

    return errors


def validate_cursor_not_load_timestamp(ctx: ValidationContext) -> list[ValidationError]:
    """The load-timestamp column is the extraction timestamp, never the incremental cursor.

    Reads the project-level `[dlt_ops] load_timestamp_column` key; inert
    when it is unset (no configured stamp column means nothing to guard).
    Catches a wrong cursor only — a MISSING one is
    :func:`validate_incremental_cursor_required`'s question, and opt-in.
    """
    column = _configured_load_timestamp_column(ctx)
    if column is None:
        return []

    errors: list[ValidationError] = []

    for pipeline_name, pipeline_dir in sorted(unique_pipeline_dirs(ctx).items()):
        for py_file in iter_py_files(pipeline_dir):
            tree = parse_file(py_file)
            if tree is None:
                continue
            for call in find_calls(tree, _is_incremental_func):
                if not call.args:
                    continue
                first = call.args[0]
                if isinstance(first, ast.Constant) and first.value == column:
                    location = f"{rel(py_file, ctx)}:{call.lineno}"
                    errors.append(
                        ValidationError(
                            source_name=pipeline_name,
                            field=f"incremental.{location}",
                            message=f"dlt.sources.incremental('{column}', ...) at {location}: "
                            f"'{column}' is the configured load_timestamp_column — it advances on "
                            f"every run, so cursoring on it silently skips in-window source "
                            f"updates. Use the provider's business timestamp (e.g. updated_at) "
                            f"as the cursor.",
                        )
                    )

    return errors

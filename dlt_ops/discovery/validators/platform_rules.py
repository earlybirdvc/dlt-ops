"""Core platform rules over pipeline source trees.

AST checks scan every .py file in a pipeline directory (tests excluded) and
never import project code; the rule IDs these validators register under live
in ``discovery.validators.CORE_RULES``.
"""

import ast
from collections import defaultdict
from pathlib import Path

from dlt_ops.discovery.models import SourceInfo, ValidationContext, ValidationError
from dlt_ops.discovery.validators._common import (
    find_calls,
    get_keyword,
    iter_py_files,
    parse_file,
    rel,
    unique_pipeline_dirs,
)
from dlt_ops.schema_contracts import CANONICAL_SCHEMA_CONTRACT, EVOLVE_SCHEMA_CONTRACT


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

    A @dlt.resource with no schema_contract passes — the runtime auto-applies
    the canonical literal. A declared contract must be either the canonical
    literal, or the evolve literal on a source that opted in via a non-empty
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
                            f"Omit it (the runtime applies the canonical contract) or use the inline "
                            f"literal: schema_contract={CANONICAL_SCHEMA_CONTRACT}",
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
                        f"Omit it (the runtime applies the canonical contract) or use the inline "
                        f"literal: schema_contract={CANONICAL_SCHEMA_CONTRACT}",
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
    """Non-empty [dlt_ops] load_timestamp_column, else None."""
    table = ctx.config.get("dlt_ops")
    if not isinstance(table, dict):
        return None
    column = table.get("load_timestamp_column")
    if isinstance(column, str) and column.strip():
        return column
    return None


def validate_cursor_not_load_timestamp(ctx: ValidationContext) -> list[ValidationError]:
    """The load-timestamp column is the extraction timestamp, never the incremental cursor.

    Reads the project-level `[dlt_ops] load_timestamp_column` key; inert
    when it is unset (no configured stamp column means nothing to guard).
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

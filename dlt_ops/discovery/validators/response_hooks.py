"""The ``response_hook_raises_http_errors`` core rule.

An AST check over every .py file in a pipeline directory (tests excluded); it
never imports project code. The rule matches the hook by function name rather
than by import path, so a project can adopt :func:`dlt_ops.http.raise_for_status`
before, or instead of, switching every import — and any hook of that name
satisfies the rule.
"""

import ast
from collections.abc import Iterator

from dlt_ops.discovery.models import ValidationContext, ValidationError
from dlt_ops.discovery.validators._common import (
    get_keyword,
    iter_py_files,
    parse_file,
    rel,
    unique_pipeline_dirs,
)
from dlt_ops.http import raise_for_status

RAISE_FOR_STATUS_HOOK = raise_for_status.__name__
"""The hook name the rule requires first in every custom ``response`` list."""

Scope = ast.Module | ast.FunctionDef | ast.AsyncFunctionDef


def _arg_defaults(scope: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.expr]:
    return [d for d in (*scope.args.defaults, *scope.args.kw_defaults) if d is not None]


def _split_scope(scope: Scope) -> tuple[list[ast.AST], list[ast.FunctionDef | ast.AsyncFunctionDef]]:
    """Split a scope into the nodes it executes and the nested scopes it defines."""
    local: list[ast.AST] = []
    children: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    stack: list[ast.AST] = list(scope.body)

    while stack:
        node = stack.pop()
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            children.append(node)
            # A nested function's decorators and argument defaults are evaluated
            # by the enclosing scope, not inside the nested one.
            outer = [*node.decorator_list, *_arg_defaults(node)]
            local.extend(outer)
            stack.extend(outer)
            continue
        local.append(node)
        stack.extend(ast.iter_child_nodes(node))

    return local, children


def _hook_calls_by_scope(
    scope: Scope,
    inherited: dict[str, list[ast.expr]],
) -> Iterator[tuple[ast.Call, dict[str, list[ast.expr]]]]:
    """Yield every ``hooks=``-bearing call paired with the names visible to it.

    Name resolution follows two rules, and the check is correct only while both hold:

    *Across* scopes, a name binds per function rather than per file — two
    resources in one module that each build a local ``hooks`` list would
    otherwise collide on name alone, one silently answering for the other. A
    scope's own bindings shadow the enclosing ones, as Python's do.

    *Within* a scope, a name keeps **every** expression bound to it, and the
    caller requires all of them to raise. Picking one winner means picking a
    traversal order, and any order is wrong for some input — a re-bound
    ``hooks`` would be judged on the binding that isn't in effect at the call.
    Requiring all of them is order-independent and errs toward reporting.
    """
    local_nodes, child_scopes = _split_scope(scope)

    local_binds: dict[str, list[ast.expr]] = {}
    for node in local_nodes:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    local_binds.setdefault(target.id, []).append(node.value)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None:
            local_binds.setdefault(node.target.id, []).append(node.value)

    assigns = {**inherited, **local_binds}

    for node in local_nodes:
        if isinstance(node, ast.Call) and get_keyword(node, "hooks") is not None:
            yield node, assigns

    for child in child_scopes:
        yield from _hook_calls_by_scope(child, assigns)


def _resolve(expr: ast.expr, scope_assigns: dict[str, list[ast.expr]]) -> list[ast.expr]:
    """One-hop name resolution. A name with no visible binding resolves to itself.

    Returning the bare ``Name`` is what makes an unfollowable reference fail the
    caller's check rather than slip past it.
    """
    if isinstance(expr, ast.Name):
        return scope_assigns.get(expr.id, [expr])
    return [expr]


def _response_hook_exprs(call: ast.Call, scope_assigns: dict[str, list[ast.expr]]) -> list[ast.expr] | None:
    """Every expression that could be this call's ``response`` hook list.

    ``None`` means the rule has nothing to enforce: no ``hooks=`` at all, or a
    ``hooks=`` value that doesn't read as a dict carrying a ``response`` key —
    in which case dlt installs its own raising handler.

    Anything else is returned for checking, including shapes this resolver
    cannot follow (a helper call, a name bound outside the visible scopes). A
    hook list the validator cannot read is one it cannot prove raises, so it
    fails closed, matching ``schema_contract_declared`` on a variable-valued
    ``schema_contract``.

    Follows the inline literal (``hooks={"response": [...]}``) and the one-hop
    indirection (``hooks={"response": my_hooks}`` / ``hooks=my_hooks_dict``)
    used when the list is built conditionally.
    """
    kw = get_keyword(call, "hooks")
    if kw is None:
        return None

    hooks: list[ast.expr] = []
    for candidate in _resolve(kw.value, scope_assigns):
        if not isinstance(candidate, ast.Dict):
            continue
        for key, item in zip(candidate.keys, candidate.values, strict=False):
            if isinstance(key, ast.Constant) and key.value == "response":
                hooks.extend(_resolve(item, scope_assigns))
    return hooks or None


def _starts_with_raise(expr: ast.expr) -> bool:
    return (
        isinstance(expr, ast.List)
        and bool(expr.elts)
        and isinstance(expr.elts[0], ast.Name)
        and expr.elts[0].id == RAISE_FOR_STATUS_HOOK
    )


def validate_response_hooks_raise(ctx: ValidationContext) -> list[ValidationError]:
    """A custom `response` hook list must start with `raise_for_status`.

    Reads the hook list from the source text only, and reports a list it cannot
    follow.
    """
    errors: list[ValidationError] = []

    for pipeline_name, pipeline_dir in sorted(unique_pipeline_dirs(ctx).items()):
        for py_file in iter_py_files(pipeline_dir):
            tree = parse_file(py_file)
            if tree is None:
                continue

            for call, scope_assigns in _hook_calls_by_scope(tree, {}):
                hook_exprs = _response_hook_exprs(call, scope_assigns)
                if hook_exprs is None:
                    continue
                if all(_starts_with_raise(expr) for expr in hook_exprs):
                    continue

                location = f"{rel(py_file, ctx)}:{call.lineno}"
                errors.append(
                    ValidationError(
                        source_name=pipeline_name,
                        field=f"response_hooks.{location}",
                        message=f"Response hooks at {location} must resolve to a list whose first "
                        f"element is `{RAISE_FOR_STATUS_HOOK}` (from dlt_ops) — either an inline "
                        f"literal or a variable the validator can follow. Passing any response hook "
                        f"replaces dlt's error handler, and its session runs with "
                        f"raise_for_status=False — an HTTP error then parses as an ordinary body, "
                        f"yields zero records, and the run succeeds silently.",
                    )
                )

    return errors

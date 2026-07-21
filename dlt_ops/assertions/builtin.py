"""First-party assertion types (assertions spec §9).

All four register as ``dlt_ops.assertion`` entry points in this
package's own ``pyproject.toml`` — the same path a third-party type uses.
Batch scope means one ``pipeline.extract()`` invocation of one resource in one
run: not per-API-page, not per-dlt load-package. Cross-run scope is explicitly
out — ``unique_columns`` asserts uniqueness within the load batch only; cross-run
dedupe is dlt merge/primary-key or warehouse territory.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from dlt_ops.assertions.models import AssertionContext


def _unknown_param_issues(params: Mapping[str, Any], type_name: str) -> list[str]:
    unknown = sorted(set(params) - {"value"})
    if unknown:
        return [f"{type_name} got unknown parameter(s): {', '.join(unknown)} (allowed: value)"]
    return []


def _int_value_issues(params: Mapping[str, Any], type_name: str, minimum: int) -> list[str]:
    issues = _unknown_param_issues(params, type_name)
    value = params.get("value")
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        issues.append(f"{type_name} requires an integer value >= {minimum}, got {value!r}")
    return issues


def _column_list_issues(params: Mapping[str, Any], type_name: str, ctx: AssertionContext) -> list[str]:
    issues = _unknown_param_issues(params, type_name)
    value = params.get("value")
    if not isinstance(value, list) or not value or any(not isinstance(c, str) or not c for c in value):
        issues.append(f"{type_name} requires a non-empty list of column-name strings, got {value!r}")
        return issues
    if ctx.declared_columns is not None:
        issues.extend(
            f"{type_name} references column {column!r}, which is absent from the declared Pydantic model "
            f"of resource {ctx.resource_name!r}"
            for column in value
            if column not in ctx.declared_columns
        )
    return issues


class MinRowsPerLoad:
    """Batch fails when the load batch carries fewer rows than ``value`` (int >= 0).

    Guards silent empty loads: an upstream outage or a broken cursor yielding
    zero rows fails the run instead of quietly loading nothing.
    """

    name = "min_rows_per_load"
    row_scoped = False

    def check_config(self, params: Mapping[str, Any], ctx: AssertionContext) -> list[str]:
        return _int_value_issues(params, self.name, 0)

    def start(self, params: Mapping[str, Any]) -> Any:
        return {"rows": 0}

    def observe(self, state: Any, row: Mapping[str, Any], params: Mapping[str, Any]) -> str | None:
        state["rows"] += 1
        return None

    def finalize(self, state: Any, params: Mapping[str, Any]) -> str | None:
        if state["rows"] < params["value"]:
            return f"row count {state['rows']} is below min_rows_per_load {params['value']}"
        return None


class MaxRowsPerLoad:
    """Batch fails when the load batch carries more rows than ``value`` (int > 0).

    Guards the failure mode ``min_rows_per_load`` cannot: runaway extraction
    (pagination loop, fan-out bug, upstream dump) flooding the destination.
    """

    name = "max_rows_per_load"
    row_scoped = False

    def check_config(self, params: Mapping[str, Any], ctx: AssertionContext) -> list[str]:
        return _int_value_issues(params, self.name, 1)

    def start(self, params: Mapping[str, Any]) -> Any:
        return {"rows": 0}

    def observe(self, state: Any, row: Mapping[str, Any], params: Mapping[str, Any]) -> str | None:
        state["rows"] += 1
        return None

    def finalize(self, state: Any, params: Mapping[str, Any]) -> str | None:
        if state["rows"] > params["value"]:
            return f"row count {state['rows']} exceeds max_rows_per_load {params['value']}"
        return None


class RequiredColumns:
    """Row fails when any configured column key is absent from the row.

    Key **presence**, not non-nullness: a key carrying None passes. Non-null
    enforcement belongs to the resource's Pydantic ``columns=`` model.
    """

    name = "required_columns"
    row_scoped = True

    def check_config(self, params: Mapping[str, Any], ctx: AssertionContext) -> list[str]:
        return _column_list_issues(params, self.name, ctx)

    def start(self, params: Mapping[str, Any]) -> Any:
        return None

    def observe(self, state: Any, row: Mapping[str, Any], params: Mapping[str, Any]) -> str | None:
        missing = [column for column in params["value"] if column not in row]
        if missing:
            return f"missing required column(s): {', '.join(missing)}"
        return None

    def finalize(self, state: Any, params: Mapping[str, Any]) -> str | None:
        return None


class UniqueColumns:
    """Duplicate-key rows fail; the first occurrence of each key passes.

    Uniqueness is asserted **within the load batch only** (one resource, one
    run). Cross-run dedupe is dlt merge/primary-key or warehouse territory.

    Memory bound (stated, not hidden): the accumulator stores a 16-byte
    ``sha256(canonical key tuple)`` prefix per distinct key in a Python set —
    order 100 bytes/row real overhead, so 10M distinct keys is roughly 1 GB.
    Acceptable for the runnable-script identity; point large backfills at dlt
    merge keys instead.
    """

    name = "unique_columns"
    row_scoped = True

    def check_config(self, params: Mapping[str, Any], ctx: AssertionContext) -> list[str]:
        return _column_list_issues(params, self.name, ctx)

    def start(self, params: Mapping[str, Any]) -> Any:
        return set()

    def observe(self, state: Any, row: Mapping[str, Any], params: Mapping[str, Any]) -> str | None:
        columns = params["value"]
        key = tuple((column, row.get(column)) for column in columns)
        digest = hashlib.sha256(repr(key).encode()).digest()[:16]
        if digest in state:
            return "duplicate key " + ", ".join(f"{column}={row.get(column)!r}" for column in columns)
        state.add(digest)
        return None

    def finalize(self, state: Any, params: Mapping[str, Any]) -> str | None:
        return None

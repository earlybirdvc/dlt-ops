"""BigQuery plugin rules: partition/cluster hygiene for BigQuery-bound resources.

Registered through the ``dlt_ops.validators`` entry-point group
(provider ``bigquery``) and auto-active once loadable, like any plugin rule
group. Both rules are default-on and can be flipped off per project via
``[dlt_ops.rules]``; per-source exemptions go through
``[sources.<X>.dlt_ops.rule_exemptions]``.
"""

import ast
import logging
from typing import Any

from dlt_ops.discovery.models import RuleSpec, ValidationContext, ValidationError
from dlt_ops.discovery.validators._common import (
    find_calls,
    get_keyword,
    iter_py_files,
    parse_file,
    rel,
    unique_pipeline_dirs,
)

logger = logging.getLogger(__name__)

_NO_PARTITION_ESCAPE = "# no-partition:"
_NO_CLUSTER_ESCAPE = "# no-cluster:"


def _is_bigquery_adapter_func(func: ast.expr) -> bool:
    if isinstance(func, ast.Name):
        return func.id == "bigquery_adapter"
    return isinstance(func, ast.Attribute) and func.attr == "bigquery_adapter"


def _has_escape_comment(lines: list[str], call_lineno: int, marker: str) -> bool:
    """Check the two physical lines above the call for an escape comment."""
    for offset in (2, 3):
        idx = call_lineno - offset
        if 0 <= idx < len(lines) and marker in lines[idx]:
            return True
    return False


def validate_bigquery_adapter_partitioning(ctx: ValidationContext) -> list[ValidationError]:
    """Every bigquery_adapter() call passes partition= and cluster= (AST half).

    Escape hatch: a `# no-partition: <reason>` / `# no-cluster: <reason>`
    comment on the line directly above the call.
    """
    errors: list[ValidationError] = []

    for pipeline_name, pipeline_dir in sorted(unique_pipeline_dirs(ctx).items()):
        for py_file in iter_py_files(pipeline_dir):
            tree = parse_file(py_file)
            if tree is None:
                continue
            calls = find_calls(tree, _is_bigquery_adapter_func)
            if not calls:
                continue
            lines = py_file.read_text(encoding="utf-8").splitlines()
            for call in calls:
                location = f"{rel(py_file, ctx)}:{call.lineno}"
                if get_keyword(call, "partition") is None and not _has_escape_comment(
                    lines, call.lineno, _NO_PARTITION_ESCAPE
                ):
                    errors.append(
                        ValidationError(
                            source_name=pipeline_name,
                            field=f"bigquery_adapter.{location}",
                            message=f"bigquery_adapter() at {location} has no partition=. "
                            f"Add partition= or a '{_NO_PARTITION_ESCAPE} <reason>' comment above the call.",
                        )
                    )
                if get_keyword(call, "cluster") is None and not _has_escape_comment(
                    lines, call.lineno, _NO_CLUSTER_ESCAPE
                ):
                    errors.append(
                        ValidationError(
                            source_name=pipeline_name,
                            field=f"bigquery_adapter.{location}",
                            message=f"bigquery_adapter() at {location} has no cluster=. "
                            f"Add cluster= or a '{_NO_CLUSTER_ESCAPE} <reason>' comment above the call.",
                        )
                    )

    return errors


def _resource_partition_columns(resource: Any) -> list[str]:
    hints = getattr(resource, "_hints", {})
    columns = hints.get("columns", {})
    if not isinstance(columns, dict):
        return []
    return [
        name
        for name, hint in columns.items()
        if isinstance(hint, dict) and (hint.get("partition") or hint.get("x-bigquery-partition"))
    ]


def _source_targets_bigquery(source: Any, ctx: ValidationContext) -> bool:
    """Resolve the source's destination via the config chain; unresolved ≠ BigQuery.

    Partition/cluster hints are BigQuery physics — a source landing in DuckDB or
    Postgres must not be held to them. Unresolved destinations are another
    rule's finding, not this one's.
    """
    override = source.config.destination if source.config is not None else None
    resolved = override or ctx.config.get("dlt_ops", {}).get("default_destination")
    return resolved == "bigquery"


def validate_partition_hints(ctx: ValidationContext) -> list[ValidationError]:
    """Every BigQuery-bound resource resolves a real partition hint (runtime half).

    Catches resources yielded without any bigquery_adapter() call — invisible
    to the AST check. `_dlt_load_id` does not count: it is STRING and BigQuery
    silently ignores STRING partition keys. Sources whose resolved destination
    is not BigQuery are skipped.

    Per-source opt-out: [sources.<X>.dlt_ops.rule_exemptions]
    bigquery_partition_hints = "<reason>" (the framework filters findings
    for exempted sources).
    """
    errors: list[ValidationError] = []

    for source in ctx.sources.values():
        if not _source_targets_bigquery(source, ctx):
            continue
        try:
            source_instance = source.source_fn()
        except Exception as e:
            logger.debug(f"Could not instantiate source {source.name}: {e}")
            continue

        for resource_name, resource in source_instance.resources.items():
            partition_cols = [c for c in _resource_partition_columns(resource) if c != "_dlt_load_id"]
            if not partition_cols:
                errors.append(
                    ValidationError(
                        source_name=source.name,
                        field=f"resource.{resource_name}.partition",
                        message=f"Resource '{resource_name}' has no partition column hint at runtime. "
                        f"Apply partition/cluster hints via dlt_ops.bigquery.adapter(...) "
                        f"(or apply_hints with a partitioned timestamp column). For a justified "
                        f"exception, set [sources.{source.name}.dlt_ops.rule_exemptions] "
                        f'bigquery_partition_hints = "<reason>".',
                    )
                )

    return errors


BIGQUERY_RULES: tuple[RuleSpec, ...] = (
    RuleSpec(rule_id="bigquery_partitioning", validator=validate_bigquery_adapter_partitioning, plugin="bigquery"),
    RuleSpec(rule_id="bigquery_partition_hints", validator=validate_partition_hints, plugin="bigquery"),
)


def bigquery_rules() -> tuple[RuleSpec, ...]:
    """Rule provider for the ``bigquery`` entry point in ``dlt_ops.validators``."""
    return BIGQUERY_RULES

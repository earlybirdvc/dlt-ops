"""Import-health validators: Rule 15 (import safety) + importability.

Rule 15: source modules MUST be import-safe — no network I/O
AND no disk writes at module load (disk reads permitted). Findings come from
the Phase-2 sandbox (``discovery.phase2``); this validator only surfaces them.

Registered as rule ``import_safety``: the framework applies the
``[dlt_ops.rules]`` knob (and ``discovery.phase2`` reads the same knob
to skip the sandbox child entirely when off). Sandbox-crash guarding is not
affected by the knob: ``introspect`` isolates per-module errors regardless,
so a broken module never crashes sibling discovery.
"""

from dlt_ops.discovery.models import SourceInfo, ValidationContext, ValidationError


def _all_sources(ctx: ValidationContext) -> dict[str, SourceInfo]:
    """Full Phase-2 output when available; hand-built contexts fall back to sources."""
    return ctx.introspected or ctx.sources


def validate_import_safety(ctx: ValidationContext) -> list[ValidationError]:
    """Rule 15: flag import-time network I/O / disk writes / pipeline runs."""
    errors: list[ValidationError] = []
    for name in sorted(_all_sources(ctx)):
        source = _all_sources(ctx)[name]
        for violation in source.import_violations:
            errors.append(
                ValidationError(
                    source_name=name,
                    field="import_safety",
                    message=(
                        f"Rule 15: {violation.kind} at import of {source.module_stem}.py — "
                        f"{violation.event}({violation.target})"
                    ),
                )
            )
    return errors


def validate_import_errors(ctx: ValidationContext) -> list[ValidationError]:
    """A source whose module cannot import cannot run — always on, no knob."""
    errors: list[ValidationError] = []
    for name in sorted(_all_sources(ctx)):
        source = _all_sources(ctx)[name]
        if source.import_error:
            errors.append(
                ValidationError(
                    source_name=name,
                    field="import",
                    message=f"source module {source.module_stem}.py: {source.import_error}",
                )
            )
    return errors

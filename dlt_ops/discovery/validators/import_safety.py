"""Import-health validators: Rule 15 (import safety) + importability.

Rule 15: source modules MUST be import-safe — no network I/O
AND no disk writes at module load (disk reads permitted). Findings come from
the Phase-2 sandbox (``discovery.phase2``); this validator only surfaces them.
The sandbox attributes each event to the module body that caused it, so a
library initialising itself under the project's ``import requests`` is not the
project's violation — see ``discovery._sandbox_child``.

Excluding a source from Phase 2 also excludes it from every rule that iterates
``ctx.sources``, so :func:`validate_import_errors` states that lost coverage
per source. Silence there is the failure mode that matters most: it renders
"clean" and "unchecked" identical in ``validate`` output.

Registered as rule ``import_safety``: the framework applies the
``[dlt_ops.rules]`` knob (and ``discovery.phase2`` reads the same knob
to skip the sandbox child entirely when off). Sandbox-crash guarding is not
affected by the knob: ``introspect`` isolates per-module errors regardless,
so a broken module never crashes sibling discovery.

``import_safety`` is the one rule with no per-source exemption, and
:func:`validate_import_errors` rejects one as a config error. Every other
exemption filters findings out of a rule that only ever reported; this one also
decides whether the module is imported into the calling process at all. That
decision cannot be expressed per source: it is made per *module* (one module
may declare several sources, so exempting one would import for all of them),
and it is made in a discovery pass every consumer runs — the DAG factory and
``run`` included — not just ``validate``. The project-wide
``[dlt_ops.rules] import_safety = false`` switch is the only opt-out, because
"execute project code in this process" is a property of the process, not of
the source being loaded into it.
"""

from dlt_ops.discovery.models import SourceInfo, ValidationContext, ValidationError

RULE_ID = "import_safety"
"""The rule's registered ID — the knob keys on it, and exemptions may not."""

_EXEMPTION_REFUSED = (
    f"rule '{RULE_ID}' cannot be exempted per source: the rule is the containment boundary that keeps a "
    f"module violating Rule 15 out of this process, and suppressing the finding does not hand the module "
    f"back to the importer. Remove the exemption; [dlt_ops.rules] {RULE_ID} = false is the only opt-out."
)

COVERAGE_FIELD = "validation_coverage"
"""Field of the reduced-coverage finding — the one marker that names lost rule coverage."""


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


def _refused_exemptions(ctx: ValidationContext) -> list[ValidationError]:
    """Sources that try to exempt ``import_safety`` — a config error, not an exemption."""
    return [
        ValidationError(source_name=name, field=f"rule_exemptions.{RULE_ID}", message=_EXEMPTION_REFUSED)
        for name in sorted(ctx.exemptions)
        if RULE_ID in ctx.exemptions[name]
    ]


def _reduced_coverage(ctx: ValidationContext) -> list[ValidationError]:
    """Sources Phase 2 could not introspect — every source-inspecting rule skipped them.

    ``validate_sources`` hands rules the import-OK subset (``ctx.sources``), so
    an excluded source is not merely unimported: it is invisible to the rules
    that iterate sources, and validation returns nothing about it. That gap is
    the single worst thing this pass can do quietly, because "no findings" and
    "never checked" are indistinguishable in the output — so it is stated per
    excluded source, always on, at error level.

    Error rather than warning for the reason
    :func:`~dlt_ops.discovery.validator.rule_provider_errors` gives: a warning
    is filtered out of every non-``--strict`` run, and the default run is
    exactly the one that must not imply it checked more than it did.

    Keyed on ``ctx.introspected`` alone: the coverage question only has meaning
    against full Phase-2 output, and a hand-built context carrying just
    ``sources`` has no exclusion to report.
    """
    return [
        ValidationError(
            source_name=name,
            field=COVERAGE_FIELD,
            message=(
                f"reduced rule coverage: source {name!r} failed Phase-2 introspection, so it is absent "
                f"from the introspected source set every source-inspecting rule iterates — those rules "
                f"did not run for it. Its config, schema, resource and assertion findings are unknown, "
                f"not clean. Fix the 'import' finding reported for this source to restore full coverage."
            ),
        )
        for name in sorted(ctx.introspected)
        if not ctx.introspected[name].is_introspected
    ]


def validate_import_errors(ctx: ValidationContext) -> list[ValidationError]:
    """Import health that no knob or exemption can silence.

    Three always-on findings: a source whose module cannot import cannot run,
    the rule coverage that source's exclusion cost this run
    (:func:`_reduced_coverage`), and a source that tries to exempt
    ``import_safety`` per source. All ride this path rather than the rule
    framework deliberately — a rule's findings are filtered by that source's
    exemptions, so routing the refusal through a rule would let the very
    exemption being refused suppress the refusal, and routing the coverage
    finding through one would let a rule the source never reached silence the
    notice that it never reached it.
    """
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
    errors.extend(_reduced_coverage(ctx))
    errors.extend(_refused_exemptions(ctx))
    return errors

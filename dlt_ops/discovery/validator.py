"""Rule framework: assembly from the plugin registry, resolution, exemptions, execution.

Rules arrive as :class:`RuleSpec` groups from providers registered in the
``dlt_ops.validators`` entry-point group (the package's own rules ship
through the same mechanism — see ``discovery.validators.core_rules``).
Resolution overlays ``[dlt_ops.rules]`` on the registry defaults;
``[sources.<X>.dlt_ops.rule_exemptions]`` suppresses a rule's findings
for one source, each exemption carrying a mandatory non-empty reason.
Everything is resolved once per ``validate_sources`` run and handed to
validators via :class:`ValidationContext`.
"""

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import attrs

from dlt_ops.config import ProjectConfig, ProjectRootNotFoundError, load_project_config, load_raw_config
from dlt_ops.discovery.models import RuleSpec, ValidationContext, ValidationError, Validator
from dlt_ops.discovery.phase1 import discover
from dlt_ops.discovery.phase2 import introspect
from dlt_ops.discovery.validators.import_safety import validate_import_errors
from dlt_ops.plugins import registry as plugins

# The plugin axis rule providers register under (entry-point group
# `dlt_ops.validators`). Provider contract: an entry point in the group
# resolves to a zero-argument callable returning an iterable of RuleSpec.
# Installing a distribution with such an entry point auto-activates its rules;
# per-rule opt-out goes through [dlt_ops.rules].
VALIDATORS_AXIS = "validators"


@attrs.frozen
class RuleProviderFailure:
    """A rule provider that could not be loaded or enumerated (soft-fail record).

    Its rules are unavailable this run — surfaced by every ``validate`` run
    via :func:`rule_provider_errors`, listed by ``validate
    --show-resolved-rules``, and the load failure itself by ``plugins doctor``.
    """

    provider: str
    error: str


@attrs.frozen
class RuleAssembly:
    """Every known rule this run, plus providers whose rules are unavailable."""

    specs: tuple[RuleSpec, ...]
    failures: tuple[RuleProviderFailure, ...]

    @property
    def known_ids(self) -> tuple[str, ...]:
        return tuple(spec.rule_id for spec in self.specs)


def load_rule_specs() -> RuleAssembly:
    """Assemble RuleSpecs from every provider in the ``dlt_ops.validators`` group.

    A provider that raises on load or enumeration is recorded as a
    :class:`RuleProviderFailure` instead of crashing validation (the plugin
    registry's soft-fail policy). A rule ID already claimed by an earlier
    provider is skipped and recorded — rule IDs are globally unique.
    """
    specs: list[RuleSpec] = []
    claimed: dict[str, str] = {}
    failures: list[RuleProviderFailure] = []
    for provider_name in plugins.names(VALIDATORS_AXIS):
        try:
            provider = plugins.get(VALIDATORS_AXIS, provider_name)
            provided = tuple(provider())
        except Exception as exc:
            failures.append(RuleProviderFailure(provider=provider_name, error=f"{type(exc).__name__}: {exc}"))
            continue
        for spec in provided:
            if spec.rule_id in claimed:
                failures.append(
                    RuleProviderFailure(
                        provider=provider_name,
                        error=f"duplicate rule id {spec.rule_id!r} (already registered by "
                        f"{claimed[spec.rule_id]!r}); skipped",
                    )
                )
                continue
            claimed[spec.rule_id] = provider_name
            specs.append(spec)
    return RuleAssembly(specs=tuple(specs), failures=tuple(failures))


def rule_provider_errors(assembly: RuleAssembly) -> list[ValidationError]:
    """Providers whose rules did not make it into this run — hard errors.

    A provider that raised contributes zero rules, so ``validate`` would
    otherwise check less than it claims and still report success. These are
    errors rather than warnings for two reasons: a warning renders without
    failing the run outside ``--strict``, and this is precisely the run that
    must not exit 0; and the Tier-2 runtime preflight already hard-fails on a
    plugin that soft-failed at load, so a passing Tier 1 followed by a
    refused ``run`` would be the worse contradiction.

    The provider a project cannot lose is ``core`` — it owns the baseline
    rule set — but a broken third-party provider is a real defect too (an
    optional dependency that is merely absent returns no rules and does not
    land here), so severity does not split on the owner.
    """
    return [
        ValidationError(
            source_name="dlt_ops.validators",
            field=f"validators.{failure.provider}",
            message=f"rules unavailable this run: {failure.error}",
        )
        for failure in assembly.failures
    ]


def check_unknown_rule_ids(configured: Iterable[str], known_ids: Iterable[str]) -> tuple[str, ...]:
    """Rule IDs referenced in config but registered by no provider (sorted).

    The typo guard shared by Tier 1 (``validate``) and the Tier-2 runtime
    preflight: both fail on an unknown rule ID rather than silently ignoring
    the entry.
    """
    return tuple(sorted(set(configured) - set(known_ids)))


def resolve_rules(project_config: ProjectConfig, assembly: RuleAssembly | None = None) -> dict[str, bool]:
    """Resolved on/off per known rule: registry defaults overlaid by ``[dlt_ops.rules]``.

    Missing entry = the rule's registered default (on for every core rule);
    explicit ``false`` disables. Unknown or non-bool overlay entries are
    ignored here — callers surface them as errors via
    :func:`check_unknown_rule_ids` / :func:`rules_config_errors`.
    """
    if assembly is None:
        assembly = load_rule_specs()
    resolved = {spec.rule_id: spec.default_on for spec in assembly.specs}
    for rule_id, value in project_config.rules.items():
        if rule_id in resolved and isinstance(value, bool):
            resolved[rule_id] = value
    return resolved


def _known_ids_hint(known_ids: Iterable[str]) -> str:
    listed = ", ".join(sorted(known_ids))
    return f"valid rule ids: {listed}" if listed else "no rules are registered"


def rules_config_errors(project_config: ProjectConfig, known_ids: Iterable[str]) -> list[ValidationError]:
    """Config errors in the ``[dlt_ops.rules]`` table: unknown IDs, non-bool values."""
    known = tuple(known_ids)
    errors = [
        ValidationError(
            source_name="dlt_ops.rules",
            field=f"rules.{rule_id}",
            message=f"unknown rule id '{rule_id}' in [dlt_ops.rules]; {_known_ids_hint(known)}",
        )
        for rule_id in check_unknown_rule_ids(project_config.rules, known)
    ]
    for rule_id, value in project_config.rules.items():
        if rule_id in known and not isinstance(value, bool):
            errors.append(
                ValidationError(
                    source_name="dlt_ops.rules",
                    field=f"rules.{rule_id}",
                    message=f"[dlt_ops.rules] {rule_id} must be true or false, got {value!r}",
                )
            )
    return errors


def load_rule_exemptions(
    raw_config: Mapping[str, Any], known_ids: Iterable[str]
) -> tuple[dict[str, dict[str, str]], list[ValidationError]]:
    """Parse ``[sources.<X>.dlt_ops.rule_exemptions]`` tables.

    Returns ``({source: {rule_id: reason}}, config errors)``. Every exemption
    must name a known rule ID and carry a non-empty string reason — an
    unjustified or misspelled exemption is a config error, never a silently
    weaker exemption.
    """
    known = set(known_ids)
    exemptions: dict[str, dict[str, str]] = {}
    errors: list[ValidationError] = []
    sources = raw_config.get("sources")
    if not isinstance(sources, dict):
        return {}, []
    for source_name, section in sources.items():
        if not isinstance(section, dict):
            continue
        ext = section.get("dlt_ops")
        if not isinstance(ext, dict):
            continue
        raw = ext.get("rule_exemptions")
        if raw is None:
            continue
        if not isinstance(raw, dict):
            errors.append(
                ValidationError(
                    source_name=source_name,
                    field="rule_exemptions",
                    message=f"[sources.{source_name}.dlt_ops.rule_exemptions] must be a table of "
                    f'rule_id = "<reason>" entries',
                )
            )
            continue
        for rule_id, reason in raw.items():
            if rule_id not in known:
                errors.append(
                    ValidationError(
                        source_name=source_name,
                        field=f"rule_exemptions.{rule_id}",
                        message=f"unknown rule id '{rule_id}' in "
                        f"[sources.{source_name}.dlt_ops.rule_exemptions]; {_known_ids_hint(known)}",
                    )
                )
                continue
            if not isinstance(reason, str) or not reason.strip():
                errors.append(
                    ValidationError(
                        source_name=source_name,
                        field=f"rule_exemptions.{rule_id}",
                        message=f"exemption for rule '{rule_id}' in "
                        f"[sources.{source_name}.dlt_ops.rule_exemptions] requires a non-empty "
                        f'reason string: {rule_id} = "<why this source is exempt>"',
                    )
                )
                continue
            exemptions.setdefault(source_name, {})[rule_id] = reason
    return exemptions, errors


def _load_project_config(project_root: Path) -> ProjectConfig:
    """ProjectConfig for the root; empty config when the marker table is absent."""
    try:
        return load_project_config(project_root)
    except ProjectRootNotFoundError:
        return ProjectConfig()


def _apply_strict(findings: list[ValidationError], strict: bool) -> list[ValidationError]:
    """Re-tag every warning as an error under ``strict``; pass through otherwise.

    The single site of the strict policy, so ``is_warning`` alone answers "does
    this finding fail the run?" for every caller in both modes and no caller has
    to re-implement the promotion. Findings are never dropped: a warning is
    rendered by an ordinary run, it just does not fail it.
    """
    if not strict:
        return findings
    return [attrs.evolve(finding, is_warning=False) if finding.is_warning else finding for finding in findings]


def validate_sources(
    project_root: Path,
    *,
    validators: list[Validator] | None = None,
    strict: bool = False,
) -> list[ValidationError]:
    """Run the resolved rule set against discovered sources.

    Discovery runs both phases: Phase 1 (AST) lists everything — including a
    placeholder per source module that does not parse — and Phase 2
    (sandboxed import) attaches callables and records import failures /
    Rule 15 findings. Validators that instantiate sources see only the
    import-OK subset (``ctx.sources``); the import-health validators see the
    full Phase-2 output (``ctx.introspected``).

    Rule assembly + resolution happen once per run: registry defaults
    overlaid by ``[dlt_ops.rules]`` decide which rules execute, and
    ``rule_exemptions`` findings are filtered per (source, rule) pair.
    Config problems — unknown rule IDs, non-bool knob values, unjustified
    exemptions — surface as errors in the returned list, as do rule
    providers that contributed no rules (:func:`rule_provider_errors`): a run
    that quietly checks less than it claims must not report success.
    Import-error surfacing (a module that cannot import cannot run — it fails
    to parse, raises, or is withheld for violating Rule 15) is
    infrastructure, not a rule: always on, no knob.

    Every finding is returned in both modes — warnings included, so callers
    can render them — and ``is_warning`` carries the finding's severity *for
    this run*: ``strict`` promotes warnings to errors before returning, so a
    caller decides pass/fail with one question in either mode, "is any finding
    not a warning?".

    Args:
        project_root: Path to the project root
        validators: Escape hatch — run exactly these callables instead of the
            resolved rule set. Rule knobs and exemptions key on rule IDs and
            therefore do not apply to a custom list.
        strict: If True, promote warnings to errors (``is_warning=False``) so
            they fail the run.

    Returns:
        Every finding, errors and warnings alike.
    """
    introspected = introspect(project_root, discover(project_root, include_unloadable=True))
    sources = {name: info for name, info in introspected.items() if info.is_introspected}
    config = load_raw_config(project_root)

    if validators is not None:
        ctx = ValidationContext(
            sources=sources,
            config=config,
            project_root=project_root,
            introspected=introspected,
        )
        return _apply_strict([error for validator in validators for error in validator(ctx)], strict)

    assembly = load_rule_specs()
    project_config = _load_project_config(project_root)
    errors = rule_provider_errors(assembly)
    errors.extend(rules_config_errors(project_config, assembly.known_ids))
    exemptions, exemption_errors = load_rule_exemptions(config, assembly.known_ids)
    errors.extend(exemption_errors)
    resolved = resolve_rules(project_config, assembly)

    ctx = ValidationContext(
        sources=sources,
        config=config,
        project_root=project_root,
        introspected=introspected,
        resolved_rules=resolved,
        exemptions=exemptions,
    )

    errors.extend(validate_import_errors(ctx))
    for spec in assembly.specs:
        if not resolved.get(spec.rule_id, spec.default_on):
            continue
        errors.extend(
            finding for finding in spec.validator(ctx) if not ctx.is_exempt(finding.source_name, spec.rule_id)
        )

    return _apply_strict(errors, strict)

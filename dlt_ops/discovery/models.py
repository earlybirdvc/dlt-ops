from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

import attrs


def resolve_load_timestamp_column(raw: Any) -> str | None:
    """Normalize a raw ``[dlt_ops] load_timestamp_column`` value; None = feature off.

    The single reading of the key, because three layers act on it and any
    disagreement between them is a silent bug: the runner stamps the column on
    every row, the ``cursor_not_load_timestamp`` rule compares source code
    against it, and the reconciler both windows on it and auto-registers it as
    an ignored column. Surrounding whitespace is stripped so all three see the
    same column name the destination actually holds; unset, empty, blank, or
    non-string values all read as off.

    Lives here rather than in ``dlt_ops.config`` so the validator, runner, and
    reconciler layers can each reach it without importing one another.
    """
    return raw.strip() if isinstance(raw, str) and raw.strip() else None


class Schedule(str, Enum):
    """Valid schedule tags for dlt pipelines."""

    HOURLY = "@hourly"
    TWO_HOURLY = "@2hourly"
    DAILY = "@daily"
    WEEKLY = "@weekly"
    MONTHLY = "@monthly"
    MANUAL = "@manual"

    @classmethod
    def from_string(cls, value: str) -> "Schedule":
        """Parse schedule from string, with helpful error message."""
        for schedule in cls:
            if schedule.value == value:
                return schedule
        valid = [s.value for s in cls]
        raise ValueError(f"Invalid schedule '{value}'. Valid: {valid}")


@attrs.frozen
class SourceConfig:
    """Config from config.toml for a source.

    All custom keys are under [sources.X.dlt_ops]:
    - schedule: Schedule enum value
    - destination: per-source destination override; falls back to
      [dlt_ops].default_destination (see dlt_ops.config)
    - dataset: per-source dataset override; falls back to
      [dlt_ops].default_dataset
    - airflow_var: Airflow Variable name, surfaced by `pipeline list`
      / `pipeline resources`. Parsed for display only — the Airflow secret
      backend reads its own trigger keys straight off the raw
      [sources.X.dlt_ops] table, so core never acts on this value.
    - schema_contract_evolve_reason: opt-in for the evolve schema
      contract literal. Non-empty string = the source may declare
      `{"tables": "evolve", "columns": "evolve", "data_type": "freeze"}`
      on its @dlt.resource calls. Absence / empty string = default freeze.
    - injected_columns: infrastructure keys stamped per-row inside a
      resource rather than coming from the upstream payload. Consumed by
      the reconciler so it doesn't flag them as unknown drift.
      `loaded_at` is always ignored — no need to list it.
    """

    schedule: Schedule
    destination: str | None = None  # Destination override (project default if None)
    dataset: str | None = None  # Dataset override (project default if None)
    airflow_var: str | None = None  # Variable name for secrets (display only)
    schema_contract_evolve_reason: str | None = None
    injected_columns: tuple[str, ...] = ()

    @property
    def is_schema_contract_evolve(self) -> bool:
        """True iff a non-empty justification is present.

        Empty string is treated as absence so opt-in requires actual
        justification, not just presence of the key. Non-string TOML values
        (int/bool/list) are treated as absence rather than raising — the
        scanner passes the value through untouched so a hand-authored
        `schema_contract_evolve_reason = 42` reaches here as an int.
        """
        reason = self.schema_contract_evolve_reason
        return isinstance(reason, str) and bool(reason.strip())


@attrs.frozen
class ImportViolation:
    """One Rule 15 finding from the Phase-2 import-safety sandbox.

    Rule 15: source modules must be import-safe — no network I/O and no disk
    writes at module load (disk reads permitted).
    """

    kind: str  # "network" | "disk-write" | "pipeline-run" | "process-spawn"
    event: str  # CPython audit event name ("socket.connect", "open", ...) or "dlt.pipeline"
    target: str  # offending call target: address, path, executable, ...


@attrs.frozen
class SourceInfo:
    """Discovered dlt source with metadata.

    Two-phase population:

    - Phase 1 (``discovery.phase1.discover``) fills the static fields from a
      pure AST scan and never imports project code. ``resources`` is then a
      static approximation: ``@dlt.resource`` declarations in the source's own
      module plus the pipeline's ``resource/*.py`` siblings (dynamic resource
      factories only resolve in Phase 2).
    - Phase 2 (``discovery.phase2.introspect``) enriches: attaches the imported
      ``source_fn``, replaces ``resources`` with the authoritative instantiated
      list, and records import failures / import-safety findings.
    """

    name: str  # source name (config_section): "github_api"
    pipeline_name: str  # directory name: "github"
    path: Path  # full path to dir
    function_name: str  # "github_api_source"
    resources: tuple[str, ...]  # ("issues", "pulls", ...) — static in Phase 1, live in Phase 2
    module_stem: str  # source file stem: "github_api" (from github_api.py)
    config: SourceConfig | None = None  # parsed from config.toml (None if missing/invalid)
    decorator_name: str | None = None  # explicit name from @dlt.source(name=...) decorator
    module_path: Path | None = attrs.field(default=None, kw_only=True)  # source file: .../source/github_api.py
    # Static checkpoint detection (Phase 1): True when the source's own module
    # or any resource/*.py sibling applies a decorator whose terminal name is
    # `with_checkpoints` (bare or attribute form, called or not). Deliberately
    # pipeline-dir coarse — resource/*.py is shared across the dir's sources,
    # so a decorated shared resource marks every source that may select it.
    # Aliased imports escape the name-based match.
    uses_checkpoints: bool = attrs.field(default=False, kw_only=True)
    # Phase-2 enrichment. Private + property so consumers keep the non-optional
    # `source.source_fn()` call surface; Phase-1-only records raise on access.
    _source_fn: Callable[..., Any] | None = attrs.field(default=None, kw_only=True, alias="source_fn")
    import_error: str | None = attrs.field(default=None, kw_only=True)  # Phase-2 import/sandbox failure
    import_violations: tuple[ImportViolation, ...] = attrs.field(default=(), kw_only=True)  # Rule 15 findings

    @property
    def source_fn(self) -> Callable[..., Any]:
        """The imported @dlt.source callable (Phase-2 enrichment).

        Raises:
            RuntimeError: the record is Phase-1-only (not introspected, or its
                module failed to import — see ``import_error``).
        """
        if self._source_fn is None:
            raise RuntimeError(
                f"Source '{self.name}' has no source_fn: Phase-1 static record "
                f"(import error: {self.import_error or 'not introspected'}). "
                "Run discovery.phase2.introspect() to attach callables."
            )
        return self._source_fn

    @property
    def is_introspected(self) -> bool:
        """True iff Phase 2 attached the imported source callable."""
        return self._source_fn is not None

    @property
    def config_section(self) -> str:
        """Config section name (same as name)."""
        return self.name


@attrs.frozen
class ValidationError:
    """Validation error for a source."""

    source_name: str
    field: str
    message: str
    is_warning: bool = False


@attrs.frozen
class ValidationContext:
    """Context passed to all validators.

    ``sources`` holds only import-OK sources (``source_fn`` attached) so rules
    that instantiate sources keep working. ``introspected`` is the full Phase-2
    output — including sources whose module failed to import or violated
    Rule 15 — for the import-health validators.

    ``resolved_rules`` is the per-run rule resolution (registry defaults
    overlaid by ``[dlt_ops.rules]``) and ``exemptions`` the parsed
    ``[sources.<X>.dlt_ops.rule_exemptions]`` view
    (``{source: {rule_id: reason}}``). Both are populated once per
    ``validate_sources`` run so validators consult a single resolution
    instead of re-reading raw config.
    """

    sources: dict[str, SourceInfo]
    config: dict[str, Any]  # raw config.toml
    project_root: Path
    introspected: dict[str, SourceInfo] = attrs.field(factory=dict, kw_only=True)
    resolved_rules: dict[str, bool] = attrs.field(factory=dict, kw_only=True)
    exemptions: dict[str, dict[str, str]] = attrs.field(factory=dict, kw_only=True)

    def rule_enabled(self, rule_id: str) -> bool:
        """Resolved on/off state for a rule; unresolved (hand-built context) = on."""
        return self.resolved_rules.get(rule_id, True)

    def is_exempt(self, source_name: str, rule_id: str) -> bool:
        """True iff the source carries a justified exemption for the rule."""
        return rule_id in self.exemptions.get(source_name, {})


class Validator(Protocol):
    """Protocol for validator functions."""

    def __call__(self, ctx: ValidationContext) -> list[ValidationError]: ...


@attrs.frozen
class RuleSpec:
    """One registered validation rule: stable identity + validator + provenance.

    ``rule_id`` is the rule's public identity: the ``[dlt_ops.rules]``
    knob and ``rule_exemptions`` tables key on it, so it never renames within
    a major version. ``plugin`` names the provider that registered the rule
    ("core" for the package's own rules, the plugin's entry-point name
    otherwise) — shown as the rule's origin in ``validate
    --show-resolved-rules``. ``default_on=False`` ships a rule opt-in.
    """

    rule_id: str
    validator: Validator
    plugin: str
    default_on: bool = True

    @property
    def description(self) -> str:
        """One-line rule summary: the validator docstring's first line."""
        doc = (getattr(self.validator, "__doc__", None) or "").strip()
        return doc.splitlines()[0] if doc else ""

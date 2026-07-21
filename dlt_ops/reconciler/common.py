"""Shared plumbing for the additive + removal detectors.

Both detectors need the same handful of primitives: the source's ignored
(injected) column set, a lookup from ``SourceInfo`` + resource name to the
Pydantic model dlt attached via ``columns=<Model>``, canonical-dialect
identifier quoting, and the ``reproduce_sql`` alert sinks attach to every
drift event.

:func:`run_detection` is the shared driver both public entry points run on:
project bootstrap, source lookup, dataset resolution, destination-boundary
acquisition, error mapping into ``ReconcileResult.error``, and emission. Each
detector supplies only what actually differs — its traversal, an optional
precheck, and whether a source-level failure is its own to report.

All SQL fragments produced here are CANONICAL (DuckDB dialect, double-quoted
identifiers) per the DestinationAdapter boundary contract — the adapter owns
transpilation to the destination-native dialect.
"""

from __future__ import annotations

import logging
import time
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, Protocol

import attrs
from dlt.common.normalizers.naming.snake_case import NamingConvention

from dlt_ops.config import (
    ProjectConfig,
    ProjectConfigError,
    find_project_root,
    load_project_config,
    resolve_dataset,
    resolve_destination,
)
from dlt_ops.destinations.protocol import render_canonical_identifier, render_canonical_table_ref
from dlt_ops.discovery.models import SourceInfo, resolve_load_timestamp_column
from dlt_ops.discovery.scanner import discover_sources
from dlt_ops.pydantic_fields import extract_model_column_names
from dlt_ops.reconciler._emission import emit_findings, resolve_sink
from dlt_ops.reconciler.models import ReconcileResult

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    import pydantic

    from dlt_ops.reconciler.models import DriftFinding
    from dlt_ops.reconciler.protocols import AlertSink, QueryRunner, SchemaFetcher


logger = logging.getLogger(__name__)


class _Normalizer(Protocol):
    """Structural view of the identifier normalizer contract.

    Parameter name mirrors ``dlt.common.normalizers.naming.NamingConvention``
    (``identifier``, not ``name``) so a real dlt NamingConvention structurally
    satisfies this Protocol under strict type checkers that enforce
    positional-or-keyword parameter-name compatibility.
    """

    def normalize_identifier(self, identifier: str) -> str: ...


# Fallback when a source instance cannot be built (test fakes, orphan sources,
# a source_fn that raises before the schema is attached). dlt's default is
# snake_case, so this fallback matches the real write path for any pipeline
# without a custom convention — but the primary path derives the exact
# NamingConvention from the source's own dlt Schema so a custom convention
# cannot silently diverge from what the destination persists.
_DEFAULT_NAMING: _Normalizer = NamingConvention()


def resolve_source_naming(source: SourceInfo) -> _Normalizer:
    """Return the NamingConvention the source's dlt Schema uses on writes.

    A ``DltSource`` carries its own ``dlt.common.schema.Schema`` whose
    ``.naming`` reflects the destination-side normalizer dlt will apply to
    every column on the write. Reading it here means the reconciler stays
    correct even if a source ever opts into a custom convention
    (``[schema] naming = "..."`` or a schema built with an override in
    Python) — a hardcoded constant in a generic package is a smell.

    Falls back to the default snake_case convention when the source cannot
    be instantiated or exposes no schema (test fakes, orphan sources). The
    fallback matches the write path of any pipeline without a naming
    override; it would only diverge in the exact scenario the guard is
    designed for — a custom naming convention — and even then it surfaces
    as loud false-positive drift (the same class this module already
    emits), not silent data corruption.
    """
    try:
        src_instance: Any = source.source_fn()
    except Exception:
        logger.debug("source_fn() failed while resolving naming for source=%s; using default", source.name)
        return _DEFAULT_NAMING
    schema = getattr(src_instance, "schema", None)
    naming = getattr(schema, "naming", None)
    if naming is None:
        return _DEFAULT_NAMING
    return naming


def destination_column_names(
    model: "type[pydantic.BaseModel]",
    naming: "_Normalizer | None" = None,
) -> set[str]:
    """Return the set of destination column names a model's fields will produce.

    A Pydantic attribute `startTime` lands at the destination as `start_time`;
    a `Field(alias="FROM")` alias lands as `from`. Callers (the additive and
    removal detectors) diff against live destination columns, which are
    already destination-side — so we mirror the write path here.

    Every candidate raw-payload key (attribute name + alias +
    string-valued validation_alias, via ``extract_model_column_names``) is run
    through the same normalizer dlt uses on the write. Set semantics dedupe
    collisions (e.g. a snake_case attribute plus an uppercased alias that
    normalizes to the same name).

    ``naming`` is injected by the caller from ``resolve_source_naming(source)``
    so every source uses its own dlt Schema's convention, not a hardcoded one.
    When omitted (fast-path callers, tests), falls back to the default
    snake_case convention.

    Reconciler-scoped on purpose. ``pydantic_fields.extract_model_column_names``
    stays the source-side view because ``drop_unknown_nulls`` runs at ingest
    time, where the raw payload speaks source-side keys.
    """
    normalizer = naming if naming is not None else _DEFAULT_NAMING
    return {normalizer.normalize_identifier(name) for name in extract_model_column_names(model)}


def configured_load_timestamp_column(project_config: ProjectConfig) -> str | None:
    """Non-empty project-level ``[dlt_ops] load_timestamp_column``, else None.

    When set, the runner stamps it on every row, the removal detector windows
    on it, additive sampling orders by it, and the ignored set auto-registers
    it. The ``ProjectConfig``-shaped view of the one reader
    (``discovery.models.resolve_load_timestamp_column``).
    """
    return resolve_load_timestamp_column(project_config.raw.get("load_timestamp_column"))


def ignored_columns_for(
    source: SourceInfo,
    naming: "_Normalizer",
    project_config: "ProjectConfig",
) -> frozenset[str]:
    """The live destination columns the additive diff must NOT treat as drift.

    Union of:

    - project-level ``[dlt_ops] injected_columns`` — keys the project
      stamps on every row of every source;
    - per-source ``[sources.<X>.dlt_ops] injected_columns`` — keys one
      source's aggregator or resource stamps on its rows;
    - the configured ``load_timestamp_column`` — auto-registered because the
      runner stamps it (never part of the Pydantic model by design).

    ``source.config`` may be ``None`` for orphan/misconfigured sources; the
    union then degenerates to the project-level pieces alone, and drift
    detection still runs.

    Each key is run through the source's own destination-side ``naming``
    convention so it matches the persisted column shape — a camelCase
    injected key ``sessionId`` declared in TOML would otherwise mismatch the
    ``session_id`` column dlt writes and surface as false-positive drift.
    """
    raw_project = project_config.raw.get("injected_columns")
    project_level = tuple(c for c in raw_project if isinstance(c, str)) if isinstance(raw_project, list) else ()
    per_source: tuple[str, ...] = source.config.injected_columns if source.config else ()
    load_ts = configured_load_timestamp_column(project_config)
    raw = frozenset(project_level) | frozenset(per_source) | (frozenset({load_ts}) if load_ts else frozenset())
    return frozenset(naming.normalize_identifier(name) for name in raw)


canonical_ident = render_canonical_identifier
"""Validate a name against the canonical identifier grammar and quote it.

The detectors' alias for the boundary's own rule (see
``destinations.protocol.render_canonical_identifier``) — one implementation,
so a tightened grammar reaches the reconciler's SQL too. A rejected name
raises ``ValueError`` and lands in the caller's per-resource error isolation
instead of reaching SQL text.

Detectors build canonical SQL as text and hand it to ``QueryRunner.query``,
which is where the default grammar (rather than the resolved adapter's,
possibly tighter, one) comes from: the port passes SQL, not identifiers, so
there is no adapter to ask at these call sites.
"""

canonical_table_ref = render_canonical_table_ref
"""``dataset.table`` reference in canonical form; both parts validated."""


def resource_pydantic_model(source: SourceInfo, resource_name: str) -> "type[pydantic.BaseModel] | None":
    """Return the Pydantic model attached to ``columns=`` on a dlt resource.

    dlt wraps the user's model in a ``dlt.common.libs.pydantic.<Name>ExtraAllow``
    subclass (extra="allow") for its own validation. That subclass inherits
    ``model_fields`` from the original, so ``extract_model_column_names``
    still works against it — we return whatever dlt exposes.

    Returns ``None`` when the resource has no Pydantic model attached
    (e.g. ``columns=`` given as a dict, or omitted entirely). Both detectors
    treat ``None`` as "nothing to diff against" and skip the resource.
    """
    try:
        src_instance = source.source_fn()
    except Exception:
        logger.exception("source_fn() failed for source=%s", source.name)
        raise

    resource = src_instance.resources.get(resource_name)
    if resource is None:
        return None

    validator = getattr(resource, "validator", None)
    if validator is None:
        return None
    return getattr(validator, "model", None)


def build_reproduce_sql(
    dataset: str,
    table: str,
    columns: tuple[str, ...],
    *,
    first_seen_at: "datetime",
    load_timestamp_column: str | None,
) -> str:
    """Build the copy-pasteable canonical-dialect SELECT attached to a finding.

    Owned by the reconciler (not the emitter) so alert sinks stay pure
    serializers with no SQL knowledge — the result rides on
    ``DriftFinding.reproduce_sql``. When no load-timestamp column is
    configured there is no time axis to anchor on, so the time predicate is
    dropped and the SELECT degrades to a plain ``LIMIT 5`` preview.
    """
    preview_columns = list(columns[:5])
    projection = ", ".join(canonical_ident(col) for col in preview_columns) if preview_columns else "*"
    time_predicate = ""
    if load_timestamp_column:
        time_predicate = f"WHERE {canonical_ident(load_timestamp_column)} >= TIMESTAMP '{first_seen_at.isoformat()}' "
    return f"SELECT {projection} FROM {canonical_table_ref(dataset, table)} {time_predicate}LIMIT 5"


@attrs.frozen
class DetectionContext:
    """Everything a detector traversal needs, once :func:`run_detection` resolved it.

    ``fetcher`` is optional because a detector that never reads live schemas
    (removal windows on coverage instead) is driven with a runner alone, and
    must not be forced to open a destination boundary it has no use for.
    """

    source: SourceInfo
    runner: "QueryRunner"
    dataset: str
    project_config: ProjectConfig
    sink: "AlertSink"
    fetcher: "SchemaFetcher | None" = None


def run_detection(
    source_name: str,
    *,
    detect: "Callable[[DetectionContext], list[DriftFinding]]",
    dry_run: bool,
    sink: "AlertSink",
    runner: "QueryRunner | None",
    fetcher: "SchemaFetcher | None" = None,
    needs_fetcher: bool = False,
    dataset: str | None = None,
    sources: dict[str, SourceInfo] | None = None,
    project_root: Any | None = None,
    project_config: ProjectConfig | None = None,
    precheck: "Callable[[ProjectConfig], str | None] | None" = None,
    source_error_context: str | None = None,
) -> ReconcileResult:
    """Drive one source through a detector traversal WITHOUT flushing the sink.

    The caller owns the flush (see :func:`with_resolved_sink`): a single-source
    entry point flushes per call, while a full sweep batches N calls and pays
    the drain cost once.

    Every failure mode between "a source name" and "a list of findings" is
    handled here, identically for every detector: project bootstrap, unknown
    source, unresolvable dataset or destination, and boundary acquisition each
    map to a ``ReconcileResult`` carrying ``error``, never to a raise — an
    orchestrated sweep must survive one bad source.

    Args:
        detect: The traversal. Receives a fully-resolved
            :class:`DetectionContext` and returns findings; it owns per-resource
            error isolation, this driver owns everything around it.
        needs_fetcher: True when ``detect`` reads ``ctx.fetcher``, which makes
            an injected runner alone insufficient to skip opening the
            destination boundary.
        precheck: Consulted after the source is known. A returned string is a
            non-fatal degradation: detection is skipped and the result carries
            it in ``warnings``.
        source_error_context: Emitted as the sink error context when ``detect``
            raises. None for a traversal that already reported the failure
            through the sink itself — re-emitting produces two events per bug.
    """
    started = time.monotonic()

    def _elapsed_ms() -> int:
        return int((time.monotonic() - started) * 1000)

    def _failed(message: str) -> ReconcileResult:
        return ReconcileResult(source_name=source_name, findings=(), duration_ms=_elapsed_ms(), error=message)

    try:
        if sources is None or project_config is None:
            root = project_root if project_root is not None else find_project_root()
            if sources is None:
                sources = discover_sources(root)
            if project_config is None:
                project_config = load_project_config(root)
    except Exception as exc:
        sink.emit_error(exc, source_name=source_name, context="discover_sources")
        logger.exception("project discovery failed for source=%s", source_name)
        return _failed(f"discover_sources failed: {exc}")

    source = sources.get(source_name)
    if source is None:
        return _failed(f"source {source_name!r} not found in discovered sources")

    if precheck is not None:
        warning = precheck(project_config)
        if warning is not None:
            return ReconcileResult(
                source_name=source_name,
                findings=(),
                duration_ms=_elapsed_ms(),
                error=None,
                warnings=(warning,),
            )

    try:
        resolved_dataset = dataset if dataset is not None else resolve_dataset(source.config, project_config)
    except ProjectConfigError as exc:
        return _failed(str(exc))

    with ExitStack() as stack:
        if runner is not None and (fetcher is not None or not needs_fetcher):
            resolved_fetcher, resolved_runner = fetcher, runner
        else:
            # Default path: open the source's own destination boundary
            # (config-chain resolution; DestinationAdapter + live client).
            try:
                destination = resolve_destination(source.config, project_config)
            except ProjectConfigError as exc:
                return _failed(str(exc))
            # Imported here so the injected-fakes path (tests) never touches
            # pipeline construction.
            from dlt_ops.reconciler._adapters import destination_defaults

            try:
                default_fetcher, default_runner = stack.enter_context(
                    destination_defaults(source.name, destination, resolved_dataset)
                )
            except Exception as exc:
                sink.emit_error(exc, source_name=source_name, context="open_destination")
                logger.exception("failed to open destination for source=%s", source_name)
                return _failed(f"failed to open destination {destination!r}: {exc}")
            resolved_fetcher = fetcher if fetcher is not None else default_fetcher
            resolved_runner = runner if runner is not None else default_runner

        try:
            findings = detect(
                DetectionContext(
                    source=source,
                    runner=resolved_runner,
                    dataset=resolved_dataset,
                    project_config=project_config,
                    sink=sink,
                    fetcher=resolved_fetcher,
                )
            )
        except Exception as exc:
            if source_error_context is not None:
                sink.emit_error(exc, source_name=source_name, context=source_error_context)
            return _failed(f"source-level failure: {exc}")

    if not dry_run:
        emit_findings(sink, findings)

    return ReconcileResult(
        source_name=source_name,
        findings=tuple(findings),
        duration_ms=_elapsed_ms(),
        error=None,
    )


def with_resolved_sink(
    sink: "AlertSink | None",
    *,
    dry_run: bool,
    project_config: ProjectConfig | None,
    project_root: Any | None,
    run: "Callable[[AlertSink], ReconcileResult]",
) -> ReconcileResult:
    """Resolve the sink for one public invocation, run ``run``, always flush.

    The flush is the reason this exists: a sink with a background transport
    queue must drain before a short-lived CLI or orchestrator task exits, so it
    happens on every exit path including a raise.
    """
    resolved = resolve_sink(sink, dry_run=dry_run, project_config=project_config, project_root=project_root)
    try:
        return run(resolved)
    finally:
        resolved.flush()

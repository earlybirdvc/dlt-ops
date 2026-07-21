"""Additive-drift detection: the destination has columns the Pydantic model doesn't know.

Per-source, per-resource. Compares each resource's live destination schema
against its ``columns=<PydanticModel>`` declaration, subtracts injected
infrastructure keys and dlt system columns (``_dlt_*``), and emits one alert
event per drifted RESOURCE (not one per column) with the full drifted set +
sample values + inferred types.

Runs against every source regardless of contract mode:

- **evolve sources** — primary drift signal: freeze can't reject the new
  column at ingest, so the additive detector is the only signal that
  something upstream shipped a field the model doesn't know.
- **freeze sources** — catches the patched-schema failure mode: an engineer
  hand-patches the destination schema to unblock ingest but forgets or
  defers the Pydantic model PR. Freeze is satisfied at ingest, but
  ``live_columns > model_columns`` — the reconciler fires.

**Injected-column awareness.** Every infrastructure column (project-level
``[dlt_ops] injected_columns``, per-source
``[sources.X.dlt_ops] injected_columns``, and the configured
``load_timestamp_column`` the runner stamps) is filtered out before the diff
— see ``common.ignored_columns_for``. Hardcoding any such key in this file
would leak per-project knowledge into generic reconciler code; adding a new
injected column is a one-line TOML edit instead.

**Per-source destination.** Each source reconciles against its own resolved
destination + dataset from the config chain — state lives where the data
lands, so multi-destination projects need no cross-destination auth.

**Detection ⟂ emission.** The traversal (``_detect_source_drift``) is a
pure function returning ``list[DriftFinding]``. Alert emission happens in
the public wrapper ``reconcile_source`` after the traversal, gated by
``dry_run``. Tests can exercise the detector without any sink at all.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from dlt_ops.config import ProjectConfig, find_project_root, load_project_config
from dlt_ops.discovery.models import SourceInfo
from dlt_ops.discovery.scanner import discover_sources
from dlt_ops.reconciler._emission import resolve_sink
from dlt_ops.reconciler.common import (
    DetectionContext,
    build_reproduce_sql,
    canonical_ident,
    canonical_table_ref,
    configured_load_timestamp_column,
    destination_column_names,
    ignored_columns_for,
    resolve_source_naming,
    resource_pydantic_model,
    run_detection,
    with_resolved_sink,
)
from dlt_ops.reconciler.models import DriftFinding, DriftKind, ReconcileResult
from dlt_ops.reconciler.protocols import TableRef

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dlt_ops.destinations import ColumnInfo
    from dlt_ops.reconciler.protocols import AlertSink, QueryRunner, SchemaFetcher


logger = logging.getLogger(__name__)


# Sample lookback: a hard lower bound on the load-timestamp column so
# time-partitioned destinations prune to the trailing week instead of
# full-scanning to resolve the ORDER BY. Matches removal.py's coverage bound.
_SAMPLE_WINDOW_DAYS = 7

_SAMPLE_LIMIT = 5


def is_dlt_system_column(name: str) -> bool:
    """dlt's own metadata columns.

    Prefix match (not a hardcoded set) so any future ``_dlt_*`` column dlt
    introduces stays transparent without a code change.
    """
    return name.startswith("_dlt_")


def _fetch_sample_values(
    runner: "QueryRunner",
    dataset: str,
    resource_name: str,
    columns: tuple[str, ...],
    *,
    load_timestamp_column: str | None,
) -> "Mapping[str, list[Any]]":
    """Fetch up to 5 recent non-null values per drifted column.

    One query per resource (not per column) — the SELECT list carries every
    drifted column. Canonical dialect, executed through the injected runner
    (the DestinationAdapter boundary in prod). When a load-timestamp column
    is configured the sample is the most recent shape (windowed + ordered on
    it, bounds parameter-bound); without one the query degrades to an
    unordered ``LIMIT 5``.

    Empty return on any error (query failure, permission gap, resource not
    yet materialised) — the alert still fires without the samples;
    ``sample_values`` is a nice-to-have context field, not a correctness
    gate.
    """
    if not columns:
        return {}

    projection = ", ".join(canonical_ident(col) for col in columns)
    table_ref = canonical_table_ref(dataset, resource_name)
    if load_timestamp_column:
        ts = canonical_ident(load_timestamp_column)
        sql = f"SELECT {projection} FROM {table_ref} WHERE {ts} >= ? ORDER BY {ts} DESC LIMIT {_SAMPLE_LIMIT}"
        params: tuple[Any, ...] = (datetime.now(tz=UTC) - timedelta(days=_SAMPLE_WINDOW_DAYS),)
    else:
        sql = f"SELECT {projection} FROM {table_ref} LIMIT {_SAMPLE_LIMIT}"
        params = ()

    try:
        rows = runner.query(sql, params)
    except Exception:
        logger.warning("sample-values query failed for %s.%s — continuing without samples", dataset, resource_name)
        return {}

    samples: dict[str, list[Any]] = {col: [] for col in columns}
    for row in rows:
        for position, col in enumerate(columns):
            try:
                value = row[position]
            except (KeyError, IndexError):
                value = None
            # Trim to a JSON-serialisable primitive representation — nested
            # structs / lists get str-repr'd rather than crashing a sink's
            # serializer.
            if value is not None and not isinstance(value, (str, int, float, bool)):
                value = repr(value)
            samples[col].append(value)
    return samples


def _detect_resource_drift(
    source: SourceInfo,
    resource_name: str,
    live_columns: "tuple[ColumnInfo, ...]",
    *,
    runner: "QueryRunner",
    dataset: str,
    ignored_columns: frozenset[str],
    naming: "Any",
    load_timestamp_column: str | None,
) -> DriftFinding | None:
    """Return a DriftFinding when the live destination has additive drift vs the model.

    Runs entirely in-memory + one small SELECT for samples. All external I/O
    happens via ``runner.query()`` and ``source.source_fn()``; both raise on
    failure and the caller wraps this function in a try/except so a single
    broken resource doesn't sink the whole source's reconciliation.

    ``naming`` is the destination-side NamingConvention resolved once at the
    source level via ``resolve_source_naming`` — threaded down so each
    resource's model-column set is normalized with the same convention dlt
    actually uses on the write path.
    """
    model = resource_pydantic_model(source, resource_name)
    if model is None:
        logger.debug(
            "resource %s.%s has no Pydantic columns= — skipping additive detection", source.name, resource_name
        )
        return None

    live_names = {col.name for col in live_columns}
    live_types = {col.name: col.data_type for col in live_columns}
    # Both sides must speak destination-side (post-dlt-normalize) names.
    # `destination_column_names` runs the model's attribute names + aliases
    # through the same NamingConvention dlt uses on the write — otherwise a
    # Pydantic `startTime` (raw payload key) reads as drift against the
    # persisted `start_time` column.
    model_columns = destination_column_names(model, naming)

    drift: set[str] = set()
    for col_name in live_names:
        if col_name in model_columns:
            continue
        if col_name in ignored_columns:
            continue
        if is_dlt_system_column(col_name):
            continue
        drift.add(col_name)

    if not drift:
        return None

    drifted = tuple(sorted(drift))
    inferred_types = tuple(live_types.get(col, "") for col in drifted)
    samples = _fetch_sample_values(runner, dataset, resource_name, drifted, load_timestamp_column=load_timestamp_column)
    first_seen_at = datetime.now(tz=UTC)

    return DriftFinding(
        kind=DriftKind.ADDITIVE,
        pipeline_name=source.pipeline_name,
        source_name=source.name,
        resource_name=resource_name,
        columns=drifted,
        inferred_types=inferred_types,
        sample_values=samples,
        first_seen_at=first_seen_at,
        reproduce_sql=build_reproduce_sql(
            dataset,
            resource_name,
            drifted,
            first_seen_at=first_seen_at,
            load_timestamp_column=load_timestamp_column,
        ),
    )


def _detect_source_drift(
    source: SourceInfo,
    *,
    fetcher: "SchemaFetcher",
    runner: "QueryRunner",
    dataset: str,
    project_config: ProjectConfig,
    sink: "AlertSink",
) -> list[DriftFinding]:
    """Traverse a source's resources and return every additive drift finding.

    Pure detection — never emits findings. Per-resource errors are trapped
    and reported through the sink's error path; source-level failures
    (fetcher raises) propagate to the caller so the public
    ``reconcile_source`` wrapper surfaces them via ``result.error``.
    """
    refs = [TableRef(dataset=dataset, table=resource_name) for resource_name in source.resources]
    try:
        schemas = fetcher.fetch(refs)
    except Exception as exc:
        sink.emit_error(exc, source_name=source.name, context="fetch_schemas")
        logger.exception("schema fetch failed for source=%s", source.name)
        raise

    # Resolve the source's own NamingConvention once (source_fn()-backed) so
    # every resource in the loop normalizes model names with the exact
    # convention dlt uses on the write path — not a hardcoded default.
    naming = resolve_source_naming(source)
    ignored_columns = ignored_columns_for(source, naming, project_config)
    load_timestamp_column = configured_load_timestamp_column(project_config)
    findings: list[DriftFinding] = []
    for resource_name in source.resources:
        # No matching destination table — the resource has never landed OR
        # the dataset is stale. Both are "no additive drift possible"; log
        # and skip so the sweep continues.
        columns = schemas.get(TableRef(dataset=dataset, table=resource_name))
        if columns is None:
            logger.debug(
                "resource %s.%s not present in %s — skipping additive detection",
                source.name,
                resource_name,
                dataset,
            )
            continue

        try:
            finding = _detect_resource_drift(
                source,
                resource_name,
                columns,
                runner=runner,
                dataset=dataset,
                ignored_columns=ignored_columns,
                naming=naming,
                load_timestamp_column=load_timestamp_column,
            )
        except Exception as exc:
            sink.emit_error(exc, source_name=source.name, resource_name=resource_name, context="detect_resource_drift")
            logger.exception("additive detection failed for %s.%s", source.name, resource_name)
            continue

        if finding is None:
            continue

        findings.append(finding)
    return findings


def _reconcile_source_inner(
    source_name: str,
    *,
    dry_run: bool,
    fetcher: "SchemaFetcher | None",
    runner: "QueryRunner | None",
    dataset: str | None,
    sources: dict[str, SourceInfo] | None,
    project_root: "Any | None",
    project_config: ProjectConfig | None,
    sink: "AlertSink",
) -> ReconcileResult:
    """Detection + emission for one source WITHOUT flushing the sink.

    The caller owns the flush. ``reconcile_source`` flushes on return per
    call; ``reconcile_all`` batches N calls and flushes once at the end so
    a full sweep pays the drain cost once rather than per-source.

    No ``source_error_context``: ``_detect_source_drift`` already reported any
    failure through the sink (per-resource errors under
    ``detect_resource_drift``, source-level fetcher failure under
    ``fetch_schemas``), so re-emitting would produce two events per one bug.
    """

    def _detect(ctx: DetectionContext) -> list[DriftFinding]:
        return _detect_source_drift(
            ctx.source,
            # ``needs_fetcher=True`` means the driver resolved one — either the
            # injected fetcher or the destination boundary's default.
            fetcher=cast("SchemaFetcher", ctx.fetcher),
            runner=ctx.runner,
            dataset=ctx.dataset,
            project_config=ctx.project_config,
            sink=ctx.sink,
        )

    return run_detection(
        source_name,
        detect=_detect,
        dry_run=dry_run,
        sink=sink,
        runner=runner,
        fetcher=fetcher,
        needs_fetcher=True,
        dataset=dataset,
        sources=sources,
        project_root=project_root,
        project_config=project_config,
    )


def reconcile_source(
    source_name: str,
    *,
    dry_run: bool = False,
    fetcher: "SchemaFetcher | None" = None,
    runner: "QueryRunner | None" = None,
    dataset: str | None = None,
    sources: dict[str, SourceInfo] | None = None,
    project_root: "Any | None" = None,
    project_config: ProjectConfig | None = None,
    sink: "AlertSink | None" = None,
) -> ReconcileResult:
    """Run additive-drift detection against one discovered source.

    ``dry_run=True`` suppresses all sink emission (both drift and
    reconciler-error paths); the returned ReconcileResult still carries the
    findings tuple so local verification can inspect them.

    Injection points (``fetcher``, ``runner``, ``dataset``, ``sources``,
    ``project_root``, ``project_config``, ``sink``) exist for tests — prod
    callers pass nothing and everything resolves from the project config
    chain + ``discover_sources`` + the DestinationAdapter-backed defaults.
    ``project_root`` lets the CLI's project-root option reach discovery
    without an env-var round-trip.
    """
    return with_resolved_sink(
        sink,
        dry_run=dry_run,
        project_config=project_config,
        project_root=project_root,
        run=lambda resolved_sink: _reconcile_source_inner(
            source_name,
            dry_run=dry_run,
            fetcher=fetcher,
            runner=runner,
            dataset=dataset,
            sources=sources,
            project_root=project_root,
            project_config=project_config,
            sink=resolved_sink,
        ),
    )


def reconcile_all(
    *,
    dry_run: bool = False,
    fetcher: "SchemaFetcher | None" = None,
    runner: "QueryRunner | None" = None,
    dataset: str | None = None,
    project_root: "Any | None" = None,
    project_config: ProjectConfig | None = None,
    sink: "AlertSink | None" = None,
) -> list[ReconcileResult]:
    """Iterate every discovered source and reconcile each.

    Each source resolves — and reconciles against — its own destination +
    dataset from the config chain, so a multi-destination project sweeps
    per destination with no cross-destination auth. A failure on one source
    never blocks the others — each source produces its own
    ``ReconcileResult`` (potentially with ``error=<message>``). One sink
    flush at the end covers the whole sweep, not one per source.
    """
    resolved_sink = resolve_sink(sink, dry_run=dry_run, project_config=project_config, project_root=project_root)
    try:
        root = project_root if project_root is not None else find_project_root()
        sources = discover_sources(root)
        if project_config is None:
            project_config = load_project_config(root)
    except Exception as exc:
        resolved_sink.emit_error(exc, source_name="<all>", context="discover_sources")
        resolved_sink.flush()
        # Return an empty list rather than raising — the caller (CLI /
        # orchestrator) gets a signal via the log message and the sink.
        logger.exception("discover_sources failed inside reconcile_all — returning empty list")
        return []

    results: list[ReconcileResult] = []
    try:
        for source_name in sorted(sources):
            result = _reconcile_source_inner(
                source_name,
                dry_run=dry_run,
                fetcher=fetcher,
                runner=runner,
                dataset=dataset,
                sources=sources,
                project_root=project_root,
                project_config=project_config,
                sink=resolved_sink,
            )
            results.append(result)
    finally:
        resolved_sink.flush()
    return results

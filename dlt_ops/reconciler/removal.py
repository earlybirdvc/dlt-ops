"""Removal-drift detection: a model column's non-null coverage collapsed.

Windowed non-null-coverage comparison against the resource's destination
table, on the configured ``[dlt_ops] load_timestamp_column``:

- **recent**: last ``recent_window_hours`` (default 6) of loads.
- **baseline**: ``baseline_window_days``-day window (default 7) ending
  ``recent_window_hours`` ago.

If ``baseline > baseline_threshold AND recent < recent_threshold``: the
field went dark. Removal findings share the additive fingerprint root
(``["schema-drift", pipeline, source, resource]`` on fingerprinting sinks) —
additive + removal on the same resource collapse into one alert issue by
design (one PR closes both). The ``drift_type`` tag disambiguates.

Runs against every source (freeze OR evolve) on whatever cadence the caller
schedules. The ingest layer accepts null on both contract modes, so removal
drift is silent at ingest — the reconciler is the only observability layer.

**Requires a load-timestamp column.** Windowed coverage needs a time axis;
when ``load_timestamp_column`` is unset, detection is skipped and the result
carries a warning (``ReconcileResult.warnings``) so ``validate``/CLI flows
can surface the degradation.

Batching: per resource, one query computes recent+baseline coverage for
every column in one shot — ``1`` query per resource instead of ``N_cols``.
The canonical (DuckDB-dialect) SQL is transpiled/executed through the
DestinationAdapter boundary; window bounds are parameter-bound, and the
outer ``WHERE`` carries the baseline lower bound so time-partitioned
destinations prune to the trailing window instead of full-scanning every
historical partition.

**Windows + thresholds are function parameters, not module constants.**
Callers (orchestrator tasks, local verification) can widen the baseline or
lower the trip threshold without patching this module; canonical defaults
are exported as ``DEFAULT_*``.

**Detection ⟂ emission.** ``_detect_removal_drift`` returns a pure
``list[DriftFinding]``; alert emission happens in ``detect_removal``
after the traversal, gated by ``dry_run``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

from dlt_ops.config import ProjectConfig
from dlt_ops.discovery.models import SourceInfo
from dlt_ops.reconciler.common import (
    DetectionContext,
    build_reproduce_sql,
    canonical_ident,
    canonical_table_ref,
    configured_load_timestamp_column,
    destination_column_names,
    resolve_source_naming,
    resource_pydantic_model,
    run_detection,
    with_resolved_sink,
)
from dlt_ops.reconciler.models import DriftFinding, DriftKind, ReconcileResult

if TYPE_CHECKING:
    from typing import Any

    from dlt_ops.reconciler.protocols import AlertSink, QueryRunner


logger = logging.getLogger(__name__)


# Defaults exported so callers (orchestrator task, tests) can reference the
# canonical values without importing them as private module state.
DEFAULT_BASELINE_THRESHOLD = 0.20
DEFAULT_RECENT_THRESHOLD = 0.01
DEFAULT_RECENT_WINDOW_HOURS = 6
DEFAULT_BASELINE_WINDOW_DAYS = 7

LOAD_TIMESTAMP_UNSET_WARNING = (
    "removal detection skipped: [dlt_ops] load_timestamp_column is not set — "
    "windowed coverage needs a time axis. Set it to enable removal-drift detection."
)


def _build_coverage_query(
    dataset: str,
    resource_name: str,
    columns: tuple[str, ...],
    *,
    load_timestamp_column: str,
    recent_start: datetime,
    baseline_start: datetime,
) -> "tuple[str, tuple[Any, ...]]":
    """One canonical query: all columns' coverage for recent + baseline windows.

    An inner projection tags every row with ``in_recent`` / ``in_baseline``
    window flags (bounds parameter-bound, so the SQL text is value-free);
    the outer SELECT computes NULL-safe coverage ratios per column in
    transpilable form (``CAST(SUM(CASE ...) AS DOUBLE) / NULLIF(SUM(...), 0)``
    — a zero-row window divides by NULL and yields NULL, matching the
    "can't distinguish idle from dropped" contract in ``_is_removal``).

    The inner ``WHERE <ts> >= ?`` (baseline start) is a hard lower bound on
    the load-timestamp column so time-partitioned destinations prune to the
    trailing baseline window — every reconcile pass then reads at most
    ``baseline_window_days``' worth of partitions per resource.

    Result columns come in pairs, positionally: ``(recent, baseline)`` per
    input column, in input order.
    """
    ts = canonical_ident(load_timestamp_column)
    inner_projection = ", ".join(canonical_ident(col) for col in columns)
    inner = (
        f"SELECT {inner_projection}, "
        f"CASE WHEN {ts} >= ? THEN 1 ELSE 0 END AS in_recent, "
        f"CASE WHEN {ts} >= ? AND {ts} <= ? THEN 1 ELSE 0 END AS in_baseline "
        f"FROM {canonical_table_ref(dataset, resource_name)} "
        f"WHERE {ts} >= ?"
    )
    per_col = []
    for col in columns:
        safe = canonical_ident(col)
        alias = _ident_alias(col)
        per_col.append(
            f"CAST(SUM(CASE WHEN {safe} IS NOT NULL AND in_recent = 1 THEN 1 ELSE 0 END) AS DOUBLE)"
            f" / NULLIF(SUM(in_recent), 0) AS recent_{alias}"
        )
        per_col.append(
            f"CAST(SUM(CASE WHEN {safe} IS NOT NULL AND in_baseline = 1 THEN 1 ELSE 0 END) AS DOUBLE)"
            f" / NULLIF(SUM(in_baseline), 0) AS baseline_{alias}"
        )
    select_list = ",\n  ".join(per_col)
    sql = f"SELECT\n  {select_list}\nFROM ({inner}) AS windowed"
    params = (recent_start, baseline_start, recent_start, baseline_start)
    return sql, params


def _ident_alias(col: str) -> str:
    """Sanitise a column name for use as a SQL alias.

    ``a-b`` / ``a.b`` / ``a b`` all become ``a_b``. Purely alias-level — the
    original column reference in the coverage expression stays intact via
    canonical quoting. Callers feed in destination-normalized names via
    ``destination_column_names``, which is already snake_case-clean; this
    pass is defensive against a future naming-convention override that might
    leak a non-identifier character.
    """
    return "".join(c if c.isalnum() or c == "_" else "_" for c in col)


def _detect_removal_for_resource(
    source: SourceInfo,
    resource_name: str,
    *,
    runner: "QueryRunner",
    dataset: str,
    load_timestamp_column: str,
    baseline_threshold: float,
    recent_threshold: float,
    recent_window_hours: int,
    baseline_window_days: int,
    naming: "Any",
) -> DriftFinding | None:
    """Compute coverage windows for one resource; return a finding if drift.

    ``naming`` is the destination-side NamingConvention resolved once at the
    source level via ``resolve_source_naming`` — threaded down so every
    resource's model-column set is normalized with the same convention dlt
    actually uses on the write path.
    """
    model = resource_pydantic_model(source, resource_name)
    if model is None:
        logger.debug("resource %s.%s has no Pydantic columns= — skipping removal detection", source.name, resource_name)
        return None

    # Destination-side (post-dlt-normalize) so the coverage projection targets
    # the real destination column names — a camelCase Pydantic field
    # `startTime` would otherwise compile as `"startTime" IS NOT NULL`, which
    # the destination rejects because the persisted column is `start_time`.
    known_columns = tuple(sorted(destination_column_names(model, naming)))
    if not known_columns:
        return None

    now = datetime.now(tz=UTC)
    sql, params = _build_coverage_query(
        dataset,
        resource_name,
        known_columns,
        load_timestamp_column=load_timestamp_column,
        recent_start=now - timedelta(hours=recent_window_hours),
        baseline_start=now - timedelta(days=baseline_window_days),
    )
    # A per-resource query failure is not fatal — the outer loop wraps
    # this call and reports through the sink's error path.
    rows = runner.query(sql, params)

    if not rows:
        return None

    row = rows[0]
    drifted: list[str] = []
    # Result columns pair up positionally: (recent, baseline) per known
    # column, in the same order the SELECT list was built.
    for position, col in enumerate(known_columns):
        recent = row[2 * position]
        baseline = row[2 * position + 1]
        if _is_removal(baseline, recent, baseline_threshold=baseline_threshold, recent_threshold=recent_threshold):
            drifted.append(col)

    if not drifted:
        return None

    drifted_tuple = tuple(sorted(drifted))
    first_seen_at = datetime.now(tz=UTC)
    return DriftFinding(
        kind=DriftKind.REMOVAL,
        pipeline_name=source.pipeline_name,
        source_name=source.name,
        resource_name=resource_name,
        columns=drifted_tuple,
        # Removal doesn't have a destination data_type to attach — the
        # coverage signal is what fires. Empty tuple keeps the field shape
        # stable.
        inferred_types=tuple("" for _ in drifted_tuple),
        # Empty samples on removal — by definition there are no recent
        # non-null values to sample.
        sample_values={col: [] for col in drifted_tuple},
        first_seen_at=first_seen_at,
        reproduce_sql=build_reproduce_sql(
            dataset,
            resource_name,
            drifted_tuple,
            first_seen_at=first_seen_at,
            load_timestamp_column=load_timestamp_column,
        ),
    )


def _is_removal(
    baseline: float | None,
    recent: float | None,
    *,
    baseline_threshold: float,
    recent_threshold: float,
) -> bool:
    """Threshold check: baseline had coverage, recent lost it.

    ``None`` baseline (NULL-safe division by zero — the resource had no loads
    in the baseline window) means we can't distinguish a new column from a
    dropped one, so we do NOT flag. Same for ``None`` recent — the resource
    might be idle.
    """
    if baseline is None or recent is None:
        return False
    return baseline > baseline_threshold and recent < recent_threshold


def _detect_removal_drift(
    source: SourceInfo,
    *,
    runner: "QueryRunner",
    dataset: str,
    load_timestamp_column: str,
    baseline_threshold: float,
    recent_threshold: float,
    recent_window_hours: int,
    baseline_window_days: int,
    sink: "AlertSink",
) -> list[DriftFinding]:
    """Pure detection: iterate resources, return removal findings.

    Never emits findings — the caller (``detect_removal``) handles emission
    outside the traversal. Per-resource failures are trapped and reported
    through the sink's error path but don't stop the sweep.
    """
    # Resolve the source's own NamingConvention once so every resource shares
    # a single lookup and every SQL projection targets the exact column names
    # dlt actually wrote — not a hardcoded default.
    naming = resolve_source_naming(source)
    findings: list[DriftFinding] = []
    for resource_name in source.resources:
        try:
            finding = _detect_removal_for_resource(
                source,
                resource_name,
                runner=runner,
                dataset=dataset,
                load_timestamp_column=load_timestamp_column,
                baseline_threshold=baseline_threshold,
                recent_threshold=recent_threshold,
                recent_window_hours=recent_window_hours,
                baseline_window_days=baseline_window_days,
                naming=naming,
            )
        except Exception as exc:
            sink.emit_error(exc, source_name=source.name, resource_name=resource_name, context="detect_removal")
            logger.exception("removal detection failed for %s.%s", source.name, resource_name)
            continue

        if finding is None:
            continue

        findings.append(finding)
    return findings


def _require_load_timestamp_column(project_config: ProjectConfig) -> str | None:
    """Precheck: no time axis, no windowed coverage.

    Skipping is deliberate and never a failure (00-current-state decision 3),
    so a project without the knob still sweeps additive drift; the returned
    message becomes the result's warning.
    """
    if configured_load_timestamp_column(project_config) is not None:
        return None
    logger.warning(LOAD_TIMESTAMP_UNSET_WARNING)
    return LOAD_TIMESTAMP_UNSET_WARNING


def _detect_removal_inner(
    source_name: str,
    *,
    dry_run: bool,
    runner: "QueryRunner | None",
    dataset: str | None,
    sources: dict[str, SourceInfo] | None,
    project_root: "Any | None",
    project_config: ProjectConfig | None,
    baseline_threshold: float,
    recent_threshold: float,
    recent_window_hours: int,
    baseline_window_days: int,
    sink: "AlertSink",
) -> ReconcileResult:
    """Detection + emission for one source WITHOUT flushing the sink.

    Runs on the shared driver with no ``needs_fetcher``: coverage windows are
    computed by query, so an injected runner alone is enough and no destination
    boundary is opened for a schema fetch this detector never makes.
    """

    def _detect(ctx: DetectionContext) -> list[DriftFinding]:
        return _detect_removal_drift(
            ctx.source,
            runner=ctx.runner,
            dataset=ctx.dataset,
            # The precheck already rejected an unset column for this run.
            load_timestamp_column=cast("str", configured_load_timestamp_column(ctx.project_config)),
            baseline_threshold=baseline_threshold,
            recent_threshold=recent_threshold,
            recent_window_hours=recent_window_hours,
            baseline_window_days=baseline_window_days,
            sink=ctx.sink,
        )

    return run_detection(
        source_name,
        detect=_detect,
        dry_run=dry_run,
        sink=sink,
        runner=runner,
        dataset=dataset,
        sources=sources,
        project_root=project_root,
        project_config=project_config,
        precheck=_require_load_timestamp_column,
        source_error_context="reconcile_removal",
    )


def detect_removal(
    source_name: str,
    *,
    dry_run: bool = False,
    runner: "QueryRunner | None" = None,
    dataset: str | None = None,
    sources: dict[str, SourceInfo] | None = None,
    project_root: "Any | None" = None,
    project_config: ProjectConfig | None = None,
    sink: "AlertSink | None" = None,
    baseline_threshold: float = DEFAULT_BASELINE_THRESHOLD,
    recent_threshold: float = DEFAULT_RECENT_THRESHOLD,
    recent_window_hours: int = DEFAULT_RECENT_WINDOW_HOURS,
    baseline_window_days: int = DEFAULT_BASELINE_WINDOW_DAYS,
) -> ReconcileResult:
    """Run removal-drift detection against one discovered source.

    Contract matches ``additive.reconcile_source`` — ``dry_run=True``
    suppresses all sink emission; source-level failures land in
    ``result.error``; per-resource failures are trapped and reported through
    the sink's error path but don't stop the sweep. When no
    ``load_timestamp_column`` is configured, detection is skipped and the
    result carries a warning instead.

    Callers can widen or tighten the windows and thresholds without
    patching this module — every knob is a keyword-only parameter with the
    canonical default exported as ``DEFAULT_*``. Prod callers pass
    ``runner=None`` and the reconciler opens the source's own resolved
    destination through the DestinationAdapter boundary.
    """
    return with_resolved_sink(
        sink,
        dry_run=dry_run,
        project_config=project_config,
        project_root=project_root,
        run=lambda resolved_sink: _detect_removal_inner(
            source_name,
            dry_run=dry_run,
            runner=runner,
            dataset=dataset,
            sources=sources,
            project_root=project_root,
            project_config=project_config,
            baseline_threshold=baseline_threshold,
            recent_threshold=recent_threshold,
            recent_window_hours=recent_window_hours,
            baseline_window_days=baseline_window_days,
            sink=resolved_sink,
        ),
    )

"""Destination-agnostic run path.

``run_pipeline`` resolves destination + dataset through the config chain
(``[dlt_ops].default_*`` overridden by ``[sources.<X>.dlt_ops]``,
both outranked by explicit arguments), runs the Tier-2 preflight, applies the
runtime halves of Rules 10 (canonical schema contract on undeclared resources)
and 12 (``TimeIntervalContext`` injection around every run), stamps the
configured load-timestamp column, wires configured pre-load assertions
(streaming gate per resource + the staged ``extract → finalize →
flush-quarantine → normalize → load`` split), records the run in the
``_dlt_ops_runs`` ledger (start row before extract, terminal row on
completion/failure), and persists the run trace to the same resolved
destination + dataset (both best effort).

Runs are destination-tiered: a destination with no registered
``DestinationAdapter`` executes the same loop in core mode — one WARNING at
run start names the adapter-gated features going dark, and the ledger writes
skip instead of erroring.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dlt
from dlt.common.configuration.container import Container
from dlt.extract.incremental.context import TimeIntervalContext
from dlt.extract.items_transform import MapItem

from dlt_ops.assertions.engine import AssertionEngine
from dlt_ops.assertions.models import AssertionFailedError
from dlt_ops.assertions.quarantine import REJECTED_TABLE, QuarantineWriteError, QuarantineWriter
from dlt_ops.config import (
    find_project_root,
    load_project_config,
    load_raw_config,
    resolve_dataset,
    resolve_destination,
)
from dlt_ops.destinations import core_mode_notice, has_adapter
from dlt_ops.discovery.models import SourceInfo
from dlt_ops.preflight import run_preflight
from dlt_ops.runs.writer import (
    RunStatus,
    RunsWriter,
    dlt_run_id_from_load_info,
    pipeline_name_for_source,
    record_counts_from_trace,
    summarize_error,
)
from dlt_ops.schema_contracts import CANONICAL_SCHEMA_CONTRACT

logger = logging.getLogger(__name__)

LOG_SEPARATOR = "=" * 60

# Destinations whose runs count as the local dev loop and get _LOCAL_DEFAULTS.
# DuckDB is the sanctioned universal dev-loop destination;
# orchestrated destinations keep the values from .dlt/config.toml.
_LOCAL_DESTINATIONS = frozenset({"duckdb"})

_LOCAL_DEFAULTS = {
    "NORMALIZE__WORKERS": "4",
    "LOAD__WORKERS": "3",
}


def _set_env_override(env_var: str, value: int | None, label: str, is_local: bool) -> None:
    """Set env var with value or local default."""
    if value is not None:
        os.environ[env_var] = str(value)
        logger.info(f"{label}: {value}")
    elif is_local and env_var in _LOCAL_DEFAULTS:
        default = _LOCAL_DEFAULTS[env_var]
        os.environ.setdefault(env_var, default)
        logger.info(f"{label}: {default} (local default)")


def apply_dlt_overrides(
    normalize_workers: int | None = None,
    load_workers: int | None = None,
    file_max_items: int | None = None,
    is_local: bool = True,
) -> None:
    """Apply dlt config overrides via environment variables.

    Documented dlt-native env passthrough for worker tuning: local (dev-loop)
    runs can increase normalize workers and file sizes while orchestrated runs
    keep conservative defaults in config.toml.
    """
    _set_env_override("NORMALIZE__WORKERS", normalize_workers, "Normalize workers", is_local)
    _set_env_override("LOAD__WORKERS", load_workers, "Load workers", is_local)
    _set_env_override("NORMALIZE__DATA_WRITER__FILE_MAX_ITEMS", file_max_items, "File max items", is_local)


def _log_section(title: str, content: Any) -> None:
    """Log a section with separator and title."""
    logger.info(f"\n{LOG_SEPARATOR}")
    logger.info(title)
    logger.info(LOG_SEPARATOR)
    logger.info(content)


def _validate_resources(source: Any, resources: tuple[str, ...]) -> None:
    """Validate requested resources exist in source, exit if not."""
    available = set(source.resources.keys())
    missing = set(resources) - available
    if missing:
        logger.error(f"Unknown resources: {missing}. Available: {sorted(available)}")
        sys.exit(1)


def _apply_canonical_schema_contract(source_instance: Any) -> None:
    """Rule 10 runtime half: undeclared resources get the canonical contract.

    A resource that declares its own ``schema_contract`` is left untouched.
    """
    for name, resource in source_instance.selected_resources.items():
        if resource.schema_contract is None:
            resource.apply_hints(schema_contract=dict(CANONICAL_SCHEMA_CONTRACT))
            logger.info(f"Applied canonical schema contract to resource {name!r}")


def _make_row_stamper(column: str, stamp: datetime) -> Any:
    """Single-parameter map transform (dlt passes meta to higher-arity callables)."""

    def _stamp_row(row: Any) -> Any:
        return {**row, column: stamp}

    return _stamp_row


class _LoadTimestampStamper(MapItem):
    """Stamp step pinned to the very end of the resource pipe.

    dlt places pipe steps by ``placement_affinity``: a plain ``add_map`` step
    (0) lands BEFORE the resource's PydanticValidator (0.9), the incremental
    filter (1), and any limit (1.1). The stamp column is infrastructure —
    never part of the resource's Pydantic model by design — so stamping
    before validation makes every ``columns=freeze`` model (extra="forbid")
    reject its own rows. A value above 1.1 runs the stamp after validation
    and only on rows that actually survive the incremental/limit steps.
    """

    placement_affinity = 1.2


def _apply_load_timestamp(source_instance: Any, load_timestamp_column: Any) -> None:
    """Stamp UTC-now on every row when ``[dlt_ops] load_timestamp_column`` is set.

    One timestamp per run (captured here) so every row of the run carries the
    same value. Unset / empty / non-string = off, nothing stamped.
    """
    if not (isinstance(load_timestamp_column, str) and load_timestamp_column.strip()):
        return
    stamp = datetime.now(UTC)
    for resource in source_instance.selected_resources.values():
        resource.add_step(_LoadTimestampStamper(_make_row_stamper(load_timestamp_column, stamp)))
    logger.info(f"Stamping load timestamp column {load_timestamp_column!r} on every row")


def _assertion_abort(exc: BaseException) -> BaseException | None:
    """The assertion-driven failure buried in ``exc``'s chain, if any.

    dlt wraps exceptions raised inside pipe steps (``PipelineStepFailed`` →
    ``ResourceExtractionError`` → the gate's ``AssertionFailedError``); the
    runner surfaces the typed error and applies the spec-§3 failure hygiene.
    ``QuarantineWriteError`` counts too: quarantined rows were removed from
    the stream and could not be recorded, so the pending package must go.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, (AssertionFailedError, QuarantineWriteError)):
            return current
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return None


def _drop_pending_packages(pipeline: Any) -> None:
    """Failure hygiene on assertion failure (assertions spec §3).

    dlt persists the extracted load package in the pipeline working dir, and
    the NEXT run would auto-load it — silently defeating the assertion. Best
    effort only in the sense that a failed drop is logged loudly; the run is
    already failing either way.
    """
    try:
        pipeline.drop_pending_packages()
        logger.info("Dropped pending load package(s) after assertion failure")
    except Exception as exc:
        logger.error(f"Failed to drop pending packages after assertion failure: {exc}")


def _flush_quarantine(engine: AssertionEngine, pipeline: Any, dataset: str, source_section: str, run_id: str) -> None:
    """Write buffered quarantined rows to ``_dlt_rejected`` before normalize/load.

    Raises:
        QuarantineWriteError: the write failed — the run aborts (spec §4:
            write failure is run failure, the deliberate opposite of the
            best-effort runs writer).
    """
    if not engine.has_quarantined:
        return
    writer = QuarantineWriter(pipeline, dataset=dataset, source_section=source_section, run_id=run_id)
    count = engine.flush_quarantine(writer)
    logger.info(f"Quarantined {count} row(s) to {REJECTED_TABLE}")


def _persist_trace(trace: Any, destination: str, dataset_name: str) -> None:
    """Persist the run trace to the run's own destination + dataset (best effort, non-fatal).

    Uses a separate pipeline to avoid polluting the run's _dlt_loads metrics.
    """
    try:
        trace_pipeline = dlt.pipeline(
            pipeline_name="_dlt_traces",
            destination=destination,
            dataset_name=dataset_name,
            dev_mode=False,
        )
        trace_pipeline.run(
            [dlt.resource([trace.asdict()], name="_dlt_trace", max_table_nesting=0)],
            loader_file_format="jsonl",
        )
        logger.info("Trace persisted to _dlt_trace")
    except Exception as e:
        logger.warning(f"Failed to persist trace (non-fatal): {e}")


def run_pipeline(
    source: SourceInfo,
    resources: tuple[str, ...] | None = None,
    *,
    project_root: Path | None = None,
    destination: str | None = None,
    dataset_name: str | None = None,
    bounds: tuple[datetime, datetime] | None = None,
    normalize_workers: int | None = None,
    load_workers: int | None = None,
    file_max_items: int | None = None,
    run_id: str | None = None,
    backfill_id: str | None = None,
    trigger_source: str = "cli",
) -> dlt.Pipeline:
    """Run a dlt pipeline for the given source.

    Args:
        source: SourceInfo from discovery (Phase-2 introspected).
        resources: Specific resources to run, or None for all.
        project_root: Project root; found by walking up from cwd when None.
        destination: Explicit destination override; outranks the config chain
            ([dlt_ops].default_destination -> [sources.<X>.dlt_ops].destination).
        dataset_name: Explicit dataset override; outranks the config chain.
        bounds: ``[from, to)`` run-window bounds injected via TimeIntervalContext
            (backfill entry). None = unbounded plain run.
        normalize_workers: Override normalize workers.
        load_workers: Override load workers.
        file_max_items: Override file max items.
        run_id: Extension run id stamped into ``_dlt_ops_runs``;
            generated when None. Backfill passes its deterministic per-chunk id.
        backfill_id: ``_dlt_backfills`` reference for the runs ledger; None for
            plain runs.
        trigger_source: Ledger trigger_source value ("cli" | "airflow" |
            "y-scheduler" | "backfill").

    Returns:
        The dlt.Pipeline instance after running.

    Raises:
        UnresolvedDestinationError / UnresolvedDatasetError: the config chain
            resolves nothing and no explicit override was given — raised
            before any pipeline is constructed (no silent fallback).
        PreflightError: a Tier-2 preflight condition is violated.
    """
    root = project_root if project_root is not None else find_project_root()
    project_config = load_project_config(root)
    raw_config = load_raw_config(root)
    resolved_destination = destination or resolve_destination(source.config, project_config)
    resolved_dataset = dataset_name or resolve_dataset(source.config, project_config)
    logger.info(f"Destination: {resolved_destination}, dataset: {resolved_dataset}")

    source_instance = source.source_fn()
    if resources:
        _validate_resources(source_instance, resources)
        source_instance = source_instance.with_resources(*resources)
        logger.info(f"Running resources: {list(resources)}")
    else:
        logger.info(f"Running all resources: {list(source_instance.resources.keys())}")

    run_preflight(
        destination=resolved_destination,
        project_config=project_config,
        source=source_instance,
        bounds=bounds,
        raw_config=raw_config,
        source_section=source.name,
        uses_checkpoints=source.uses_checkpoints,
    )
    # Core mode is loud by contract: nothing is wrong, but every degradation
    # announces itself — one WARNING naming the dark features at run start.
    if not has_adapter(resolved_destination):
        logger.warning(
            f"{core_mode_notice(resolved_destination)}; "
            "extract/load, fail/warn assertions, and trace persistence run normally"
        )

    # Assertion engine construction re-runs the cheap static checks and
    # imports custom predicates — a bad config hard-fails here, next to the
    # preflight, before any pipeline is constructed (Tier-2 defense in depth,
    # assertions spec §7). The gate steps land after the load-timestamp
    # stamper by placement_affinity, so assertions observe the final row shape.
    assertion_engine = AssertionEngine.from_config(
        source_section=source.name,
        raw_config=raw_config,
        source_instance=source_instance,
        project_root=root,
    )
    assertion_engine.attach(source_instance)

    apply_dlt_overrides(
        normalize_workers=normalize_workers,
        load_workers=load_workers,
        file_max_items=file_max_items,
        is_local=resolved_destination in _LOCAL_DESTINATIONS,
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name_for_source(source.name),
        destination=resolved_destination,
        dataset_name=resolved_dataset,
        dev_mode=False,
        progress="log",
    )
    logger.info(f"Pipeline working directory: {pipeline.working_dir}")

    _apply_canonical_schema_contract(source_instance)
    _apply_load_timestamp(source_instance, project_config.raw.get("load_timestamp_column"))

    runs_writer = RunsWriter(
        destination=resolved_destination,
        dataset=resolved_dataset,
        source_section=source.name,
        resource_name=resources[0] if resources and len(resources) == 1 else None,
        run_id=run_id,
        backfill_id=backfill_id,
        trigger_source=trigger_source,
    )
    runs_writer.write_start()

    # Rule 12 runtime half: every run executes under an injected
    # TimeIntervalContext, so incremental resources honor run-window bounds
    # without source authors writing allow_external_schedulers. The True
    # override applies only when an interval actually exists (explicit bounds,
    # DLT_INTERVAL_* env, or an orchestrator context): dlt raises
    # ExternalSchedulerNotAvailable at bind when the flag is forced with no
    # interval to join, which would fail every plain local run.
    interval_ctx = TimeIntervalContext(interval=bounds, allow_external_schedulers=True)
    if interval_ctx.interval is None:
        interval_ctx = TimeIntervalContext()
    try:
        with Container().injectable_context(interval_ctx):
            if assertion_engine.active:
                # Staged split (assertions spec §3): batch verdicts only exist
                # after the last row, so the run gates between dlt's public
                # extract() and normalize()/load() steps. Quarantined rows
                # flush to _dlt_rejected before anything loads.
                pipeline.extract(source_instance)
                assertion_engine.finalize()
                _flush_quarantine(assertion_engine, pipeline, resolved_dataset, source.name, runs_writer.run_id)
                pipeline.normalize()
                load_info = pipeline.load()
            else:
                load_info = pipeline.run(source_instance)
    except Exception as exc:
        abort = _assertion_abort(exc)
        if abort is not None:
            # CRITICAL failure hygiene: without the drop, the next run
            # auto-loads the rejected batch (spec §3).
            _drop_pending_packages(pipeline)
            runs_writer.write_end(status=RunStatus.FAILED, error_summary=summarize_error(abort))
            if abort is exc:
                raise
            raise abort from exc
        runs_writer.write_end(status=RunStatus.FAILED, error_summary=summarize_error(exc))
        raise
    _log_section("LOAD SUMMARY", load_info)

    trace = pipeline.last_trace
    records_extracted, records_loaded = record_counts_from_trace(trace)
    runs_writer.write_end(
        status=RunStatus.COMPLETED,
        dlt_run_id=dlt_run_id_from_load_info(load_info),
        records_extracted=records_extracted,
        records_loaded=records_loaded,
    )
    if trace:
        _persist_trace(trace, resolved_destination, resolved_dataset)
        _log_section("EXTRACT INFO", trace.last_extract_info)
        _log_section("NORMALIZE INFO", trace.last_normalize_info)
        _log_section("LOAD INFO", trace.last_load_info)

    return pipeline

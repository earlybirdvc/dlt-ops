"""Core orchestrator interface — the contract orchestrator adapters consume.

The X→Y ladder: v0.1 ships the Airflow adapter (X); the
v0.2 self-scheduler (``run-due``, Y) implements this same interface instead
of rewriting the discovery/secrets/runner wiring. Core owns the
orchestrator-neutral halves — WHAT runs once an orchestrator decides to fire:

- :func:`scheduled_sources` — Phase-1 (pure AST) sources grouped by
  :class:`Schedule`, safe wherever project code must never execute
  (DAG parse time).
- :func:`filtering_decision` / :func:`resolve_window` — the manual-trigger
  selection and date-window override DECISIONS as plain data; adapters map
  them onto native mechanics (skip exceptions, context intervals).
- :func:`run_source` — the run entry: Phase-2 introspection of one source,
  secrets through the secret-backend axis, then ``runner.run_pipeline``.

Adapters keep only genuinely orchestrator-native mechanics: task shapes,
skip exceptions, native data intervals, cron materialization of ``Schedule``
values. No scheduling logic and no orchestrator import appears here.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import attrs
import dlt
import pendulum

from dlt_ops.discovery.models import Schedule, SourceInfo
from dlt_ops.discovery.phase1 import discover
from dlt_ops.discovery.phase2 import introspect
from dlt_ops.discovery.runner import run_pipeline
from dlt_ops.discovery.scanner import get_sources_by_schedule
from dlt_ops.secrets import setup_secrets

__all__ = [
    "RunDecision",
    "filtering_decision",
    "resolve_window",
    "run_source",
    "scheduled_sources",
]


@attrs.frozen
class RunDecision:
    """Plain-data filtering verdict; adapters map ``run=False`` onto their native skip."""

    run: bool
    reason: str = ""


def scheduled_sources(project_root: Path) -> dict[Schedule, list[SourceInfo]]:
    """Phase-1 sources grouped by ``Schedule`` (no/invalid config groups under MANUAL).

    Pure AST — never imports project code, so orchestrator adapters may call
    it at parse/collection time (the DAG-parse foot-gun Phase 1 exists to
    close).
    """
    return get_sources_by_schedule(discover(project_root))


def filtering_decision(
    selection: Mapping[str, Any],
    *,
    source_name: str,
    resource: str | None = None,
    known_sources: Collection[str] | None = None,
) -> RunDecision:
    """Should this (source, resource) unit run under a manual-trigger selection?

    Selection contract (shared by adapters' manual-trigger payloads):
    ``source`` names one source by its config section; ``resources`` narrows
    the run to specific resources and only applies when ``source`` matches.
    An absent/empty ``source`` selects everything — ``resources`` alone is
    ignored. ``resource=None`` marks a whole-source unit: it runs whenever its
    source is selected (the caller applies any resource narrowing itself).

    Raises:
        ValueError: ``source`` names no known source — a typo'd selection
            must fail the run, not skip the world silently.
    """
    selected_source = selection.get("source")
    if not selected_source:
        return RunDecision(run=True)
    if known_sources is not None and selected_source not in known_sources:
        raise ValueError(f"Invalid source '{selected_source}'. Available: {sorted(known_sources)}")
    if selected_source != source_name:
        return RunDecision(run=False, reason=f"Source '{source_name}' not selected")
    resources = selection.get("resources")
    if not resources or resource is None:
        return RunDecision(run=True)
    if resource not in resources:
        return RunDecision(run=False, reason=f"Resource '{resource}' not selected (selection: {list(resources)})")
    return RunDecision(run=True)


def _parse_edge(value: Any, field: str) -> datetime:
    try:
        parsed = pendulum.parse(str(value))
        if not isinstance(parsed, pendulum.DateTime):
            raise ValueError(f"expected a datetime, got {type(parsed).__name__}")
    except Exception as exc:
        raise ValueError(f"Invalid {field} format: {value!r}") from exc
    return parsed


def resolve_window(
    selection: Mapping[str, Any],
    *,
    native: tuple[datetime, datetime] | None = None,
) -> tuple[datetime, datetime] | None:
    """The run's ``[start, end)`` window: explicit overrides outrank the native interval.

    ``start_date`` / ``end_date`` are ISO-8601 strings from a manual-trigger
    selection; ``native`` is the orchestrator's own interval (Airflow's
    ``data_interval_start/end``) or None when the trigger carries no
    meaningful interval (manual-only schedules). No overrides = the native
    window unchanged; a partial override replaces just its edge. Feed the
    result to ``run_pipeline(bounds=...)``.

    Raises:
        ValueError: an unparseable date, or a partial override with no native
            edge to fall back on.
    """
    raw_start = selection.get("start_date")
    raw_end = selection.get("end_date")
    if not raw_start and not raw_end:
        return native
    native_start, native_end = native if native is not None else (None, None)
    start = _parse_edge(raw_start, "start_date") if raw_start else native_start
    end = _parse_edge(raw_end, "end_date") if raw_end else native_end
    if start is None or end is None:
        missing = "start_date" if start is None else "end_date"
        raise ValueError(f"Date override needs both edges: no native interval supplies '{missing}'")
    return (start, end)


def run_source(
    source_name: str,
    *,
    project_root: Path,
    resources: tuple[str, ...] | None = None,
    window: tuple[datetime, datetime] | None = None,
    trigger_source: str,
) -> dlt.Pipeline:
    """Run one discovered source through the shared runner (the adapter run entry).

    Phase-2 introspects just the named source, resolves and fetches its
    secrets through the secret-backend axis, and delegates to
    ``run_pipeline`` with the window as ``bounds`` — preflight, the runs
    ledger, trace persistence and Rule 12's ``TimeIntervalContext`` injection
    all live in the runner, not per adapter.

    Args:
        source_name: Source config section, as discovered.
        project_root: Project root holding ``.dlt/config.toml`` + pipelines.
        resources: Specific resources to run, or None for all.
        window: ``[start, end)`` bounds from :func:`resolve_window`.
        trigger_source: Runs-ledger provenance ("airflow", "y-scheduler", ...).

    Raises:
        LookupError: no discovered source has this name.
        RuntimeError: the source's module failed Phase-2 import.
    """
    static = discover(project_root)
    if source_name not in static:
        raise LookupError(f"Unknown source '{source_name}'. Discovered: {sorted(static)}")
    source = introspect(project_root, {source_name: static[source_name]})[source_name]
    if not source.is_introspected:
        raise RuntimeError(f"Source '{source_name}' failed to import: {source.import_error}")
    setup_secrets(sources={source_name: source}, project_root=project_root)
    return run_pipeline(
        source,
        resources,
        project_root=project_root,
        bounds=window,
        trigger_source=trigger_source,
    )

"""Tier-1 staleness rule — sources that HAD run history and then stopped.

Reads the per-destination ``_dlt_ops_runs`` ledger. Exact locked
semantics: a source with run history and no run in the last N days
(``[dlt_ops] staleness_days``, default 7) gets a warning
(ingested-then-orphaned); brand-new sources with zero history are SKIPPED,
never fail validation — they have no history to be stale relative to.

Degrades gracefully: an unresolved destination/dataset, an unregistered
adapter, an unreachable destination, or a missing ledger table all skip the
source silently — Tier-1 must stay usable without destination access. The
Tier-2 runtime preflight deliberately excludes this rule: staleness never
gates ``run``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from dlt_ops.config import (
    ProjectConfig,
    ProjectConfigError,
    load_project_config,
    resolve_dataset,
    resolve_destination,
)
from dlt_ops.discovery.models import ValidationContext, ValidationError
from dlt_ops.runs.reader import latest_run_started_at
from dlt_ops.runs.writer import pipeline_name_for_source

logger = logging.getLogger(__name__)

STALENESS_DAYS_KEY = "staleness_days"
DEFAULT_STALENESS_DAYS = 7


def _staleness_days(project_config: ProjectConfig) -> int:
    """Configured threshold; non-int / non-positive values fall back to the default."""
    days = project_config.raw.get(STALENESS_DAYS_KEY, DEFAULT_STALENESS_DAYS)
    if isinstance(days, bool) or not isinstance(days, int) or days <= 0:
        return DEFAULT_STALENESS_DAYS
    return days


def validate_stale_sources(ctx: ValidationContext) -> list[ValidationError]:
    """Warn when a source with run history has no run in the last N days.

    N comes from ``[dlt_ops] staleness_days`` (default 7). Zero-history
    sources and sources whose ledger cannot be reached are skipped silently.
    """
    try:
        project_config = load_project_config(ctx.project_root)
    except ProjectConfigError:
        return []

    days = _staleness_days(project_config)
    now = datetime.now(UTC)
    findings: list[ValidationError] = []

    sources = ctx.introspected or ctx.sources
    for name, source in sorted(sources.items()):
        try:
            destination = resolve_destination(source.config, project_config)
            dataset = resolve_dataset(source.config, project_config)
        except ProjectConfigError:
            continue
        try:
            last = latest_run_started_at(
                pipeline_name_for_source(name),
                destination,
                dataset,
                source_section=name,
            )
        except Exception as exc:
            # No adapter / unreachable destination / missing dataset: Tier 1
            # stays quiet rather than demanding destination access.
            logger.debug(f"stale_sources: skipping {name!r} ({destination}/{dataset}): {exc}")
            continue
        if last is None:
            continue  # zero history — never fails validation
        if last.tzinfo is None:
            last = last.replace(tzinfo=UTC)
        if now - last > timedelta(days=days):
            findings.append(
                ValidationError(
                    source_name=name,
                    field="runs",
                    message=(
                        f"stale source: last run started {last.isoformat()}, more than {days} day(s) ago "
                        f"([dlt_ops] {STALENESS_DAYS_KEY} = {days})"
                    ),
                    is_warning=True,
                )
            )
    return findings

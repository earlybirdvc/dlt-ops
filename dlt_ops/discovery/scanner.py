"""Composite discovery: Phase 1 (AST) + Phase 2 (sandboxed import).

``discover_sources`` keeps the historical contract its callers rely on
(runner, cleanup, reconciler, Airflow secrets all call ``source_fn()``):
every returned source has its callable attached. Sources whose module failed
Phase-2 import are logged and dropped here — call ``phase1.discover`` /
``phase2.introspect`` directly to see failures and Rule 15 findings (that is
what ``pipeline validate`` does).
"""

import logging
from collections import defaultdict
from pathlib import Path

from dlt_ops.discovery.models import Schedule, SourceInfo
from dlt_ops.discovery.phase1 import discover
from dlt_ops.discovery.phase2 import introspect

logger = logging.getLogger(__name__)


def discover_sources(project_root: Path) -> dict[str, SourceInfo]:
    """Discover dlt sources and attach their imported callables.

    Supports multiple sources per directory. Each source is keyed by its
    config_section (source name), not the directory name.

    Args:
        project_root: Path to the project root (holds .dlt/config.toml and
            one subdirectory per pipeline)

    Returns:
        Dict mapping source name (config_section) to a fully-introspected
        SourceInfo. Sources whose module could not be imported are excluded
        (with a warning); Phase 1 still lists them via ``phase1.discover``.

    Raises:
        ProjectConfigParseError: .dlt/config.toml exists but is broken TOML.
    """
    introspected = introspect(project_root, discover(project_root))
    sources: dict[str, SourceInfo] = {}
    for name, info in introspected.items():
        if info.is_introspected:
            sources[name] = info
        else:
            logger.warning(f"Skipping source '{name}': {info.import_error}")
    return sources


def get_sources_by_schedule(sources: dict[str, SourceInfo]) -> dict[Schedule, list[SourceInfo]]:
    """Group sources by their schedule.

    Sources without valid config are placed under Schedule.MANUAL.
    """
    by_schedule: dict[Schedule, list[SourceInfo]] = defaultdict(list)

    for source in sources.values():
        if source.config:
            by_schedule[source.config.schedule].append(source)
        else:
            by_schedule[Schedule.MANUAL].append(source)

    return dict(by_schedule)

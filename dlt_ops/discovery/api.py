from dlt_ops.config import find_project_root
from dlt_ops.discovery.models import SourceInfo
from dlt_ops.discovery.scanner import discover_sources


def get_sources() -> dict[str, SourceInfo]:
    """Return all discovered sources with full metadata.

    The project root is resolved by walking up from the current working
    directory (see dlt_ops.config.find_project_root).

    Returns:
        Dict mapping source name to SourceInfo.
    """
    return discover_sources(find_project_root())


def get_source_schedules() -> dict[str, str]:
    """Return {source_name: schedule_value} for all discovered sources.

    Example: {"github_api": "@daily", "stripe_api": "@weekly"}

    Sources without config are excluded.
    """
    sources = get_sources()
    return {info.config_section: info.config.schedule.value for info in sources.values() if info.config}


def get_source_resources() -> dict[str, tuple[str, ...]]:
    """Return {source_name: (resource1, resource2, ...)} for all sources.

    Example: {"github_api": ("issues", "pulls", "commits")}
    """
    sources = get_sources()
    return {info.config_section: info.resources for info in sources.values()}

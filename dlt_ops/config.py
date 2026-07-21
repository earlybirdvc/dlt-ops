import tomllib
from pathlib import Path
from typing import Any

import attrs

from dlt_ops.discovery.models import SourceConfig
from dlt_ops.plugins import set_disambiguation

# The implicit project marker: a directory IS a dlt-ops project iff
# this file exists, parses, and has a top-level [dlt_ops] table.
PROJECT_MARKER = Path(".dlt") / "config.toml"

# Mandatory pipeline-directory layout (singular names, never pluralised):
# <root>/<pipeline>/SOURCE_DIR holds the source modules, RESOURCE_DIR the
# shared @dlt.resource modules. Single definition site — discovery and the
# `init` scaffold both import these so they can never drift apart.
SOURCE_DIR = "source"
RESOURCE_DIR = "resource"

_INIT_HINT = "Run `dlt-ops init` to scaffold one."

# The alert sinks engaged when [dlt_ops] sets no `alert_sinks` key: the
# core structured-logging sink, always installed via the package's own
# `dlt_ops.alert_sink` entry point.
DEFAULT_ALERT_SINKS: tuple[str, ...] = ("logging",)

# Top-level [dlt_ops] keys with a parser today, plus keys reserved for
# planned features (readable via ProjectConfig.raw until their parsers land).
# Anything else is a probable typo and lands in ProjectConfig.unknown_keys.
_KNOWN_PROJECT_KEYS = frozenset(
    {
        "default_destination",
        "default_dataset",
        "rules",
        "plugins",
        "load_timestamp_column",
        "injected_columns",
        "alert_sinks",
        "alert_sink",
        "staleness_days",
        "require_destination_adapter",
    }
)


class ProjectConfigError(Exception):
    """Base class for project discovery / config-loading / resolution failures."""


class ProjectConfigParseError(ProjectConfigError):
    """The marker file exists but is not valid TOML.

    Raised instead of treating the directory as "not a project" — a broken
    marker must fail loudly, not silently widen the root search.
    """


class ProjectRootNotFoundError(ProjectConfigError):
    """No directory with a [dlt_ops]-marked config.toml was found."""


class UnresolvedDestinationError(ProjectConfigError):
    """No destination after the project-default -> per-source override chain."""


class UnresolvedDatasetError(ProjectConfigError):
    """No dataset after the project-default -> per-source override chain."""


@attrs.frozen
class ProjectConfig:
    """Parsed top-level [dlt_ops] table from <root>/.dlt/config.toml.

    - default_destination / default_dataset: project-wide defaults, overridable
      per source in [sources.<X>.dlt_ops] (see resolve_destination /
      resolve_dataset).
    - rules: per-rule on/off knob ([dlt_ops.rules]); missing entry = on.
    - plugins: collision disambiguation ([dlt_ops.plugins.<axis>]).
    - alert_sinks: the `alert_sinks = [...]` list of alert-sink plugin names,
      or None when the key is unset (sink resolution then applies
      DEFAULT_ALERT_SINKS). None-vs-default is meaningful: validate/preflight
      only enforce explicitly configured names.
    - alert_sink_options: per-sink non-secret options from the
      [dlt_ops.alert_sink.<name>] tables, passed to the sink's
      constructor as keyword arguments. (Table key is singular — the axis
      name — because TOML forbids `alert_sinks` being both the list above
      and a table.) Secrets, e.g. the Sentry DSN, live in .dlt/secrets.toml
      under [alert_sinks.<name>] instead.
    - require_destination_adapter: `require_destination_adapter = true` makes
      a resolved destination without a registered DestinationAdapter a hard
      failure instead of core-mode degradation — for teams that operate on
      the adapter-backed surfaces (ledger, checkpoints) and want absence to
      be fatal. Default false: the core run loop works on any destination
      dlt resolves.
    - raw: the whole table, so consumers of not-yet-parsed keys
      (load_timestamp_column, injected_columns, ...) read one source of truth.
    - unknown_keys: top-level keys the package does not understand. Surfaced
      by validate as typo warnings, never raised here.
    """

    default_destination: str | None = None
    default_dataset: str | None = None
    rules: dict[str, bool] = attrs.field(factory=dict)
    plugins: dict[str, dict[str, str]] = attrs.field(factory=dict)
    alert_sinks: tuple[str, ...] | None = None
    alert_sink_options: dict[str, dict[str, Any]] = attrs.field(factory=dict)
    require_destination_adapter: bool = False
    raw: dict[str, Any] = attrs.field(factory=dict)
    unknown_keys: tuple[str, ...] = ()


def _read_marker(directory: Path) -> dict[str, Any] | None:
    """Parse the marker file under directory; None when absent.

    Raises:
        ProjectConfigParseError: the file exists but is broken TOML.
    """
    marker = directory / PROJECT_MARKER
    if not marker.is_file():
        return None
    try:
        with marker.open("rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ProjectConfigParseError(f"Failed to parse {marker}: {e}") from e


def _is_project(data: dict[str, Any] | None) -> bool:
    return data is not None and isinstance(data.get("dlt_ops"), dict)


def find_project_root(start: Path | None = None, explicit: Path | None = None) -> Path:
    """Locate the project root.

    An explicitly given root wins and is only verified, never widened.
    Otherwise walk up from start (default: cwd) until a directory qualifies:
    .dlt/config.toml exists, parses, and has a top-level [dlt_ops] table.

    Raises:
        ProjectRootNotFoundError: no qualifying directory (message carries the
            `dlt-ops init` hint).
        ProjectConfigParseError: a candidate's config.toml is broken TOML.
    """
    if explicit is not None:
        if _is_project(_read_marker(explicit)):
            return explicit
        raise ProjectRootNotFoundError(
            f"{explicit} is not a dlt-ops project root: "
            f"{PROJECT_MARKER} with a [dlt_ops] table is required. {_INIT_HINT}"
        )

    origin = start if start is not None else Path.cwd()
    for candidate in (origin, *origin.parents):
        if _is_project(_read_marker(candidate)):
            return candidate
    raise ProjectRootNotFoundError(
        f"No dlt-ops project found at {origin} or any parent directory: "
        f"looked for {PROJECT_MARKER} with a [dlt_ops] table. {_INIT_HINT}"
    )


def load_raw_config(root: Path) -> dict[str, Any]:
    """Whole parsed .dlt/config.toml under root; {} when the file is absent.

    Raises:
        ProjectConfigParseError: the file exists but is broken TOML.
    """
    return _read_marker(root) or {}


def load_project_config(root: Path) -> ProjectConfig:
    """Load and parse the [dlt_ops] table for a project root.

    Loading also installs the [dlt_ops.plugins] disambiguation mapping into the
    process-wide plugin registry — resolution context follows the loaded
    project. Re-loading is idempotent; a different project's load replaces the
    mapping (the registry is process-global, one project per process).

    Raises:
        ProjectRootNotFoundError: root has no [dlt_ops] table.
        ProjectConfigParseError: broken TOML.
        ProjectConfigError: [dlt_ops.plugins] names an unknown plugin axis.
    """
    table = load_raw_config(root).get("dlt_ops")
    if not isinstance(table, dict):
        raise ProjectRootNotFoundError(
            f"{root} is not a dlt-ops project root: {PROJECT_MARKER} has no [dlt_ops] table. {_INIT_HINT}"
        )

    raw_rules = table.get("rules")
    raw_plugins = table.get("plugins")
    raw_sinks = table.get("alert_sinks")
    raw_sink_options = table.get("alert_sink")
    config = ProjectConfig(
        default_destination=table.get("default_destination"),
        default_dataset=table.get("default_dataset"),
        rules=dict(raw_rules) if isinstance(raw_rules, dict) else {},
        plugins=(
            {axis: dict(names) for axis, names in raw_plugins.items() if isinstance(names, dict)}
            if isinstance(raw_plugins, dict)
            else {}
        ),
        # Lenient parse (non-string entries dropped, malformed tables ignored);
        # the `alert_sink_registered` validate rule surfaces malformed config.
        alert_sinks=(
            tuple(name for name in raw_sinks if isinstance(name, str)) if isinstance(raw_sinks, list) else None
        ),
        alert_sink_options=(
            {name: dict(options) for name, options in raw_sink_options.items() if isinstance(options, dict)}
            if isinstance(raw_sink_options, dict)
            else {}
        ),
        require_destination_adapter=table.get("require_destination_adapter") is True,
        raw=dict(table),
        unknown_keys=tuple(sorted(key for key in table if key not in _KNOWN_PROJECT_KEYS)),
    )
    try:
        set_disambiguation(config.plugins)
    except ValueError as exc:
        raise ProjectConfigError(f"[dlt_ops.plugins]: {exc}") from exc
    return config


def resolve_destination(source_config: SourceConfig | None, project_config: ProjectConfig) -> str:
    """Resolve a source's destination.

    Precedence (lowest -> highest):
    1. [dlt_ops].default_destination
    2. [sources.<X>.dlt_ops].destination

    Raises:
        UnresolvedDestinationError: neither key is set. No silent fallback —
            the package is destination-agnostic.
    """
    override = source_config.destination if source_config is not None else None
    if override:
        return override
    if project_config.default_destination:
        return project_config.default_destination
    raise UnresolvedDestinationError(
        "No destination configured: set [dlt_ops].default_destination "
        "or [sources.<section>.dlt_ops].destination in .dlt/config.toml"
    )


def resolve_dataset(source_config: SourceConfig | None, project_config: ProjectConfig) -> str:
    """Resolve a source's dataset.

    Precedence (lowest -> highest):
    1. [dlt_ops].default_dataset
    2. [sources.<X>.dlt_ops].dataset

    Raises:
        UnresolvedDatasetError: neither key is set.
    """
    override = source_config.dataset if source_config is not None else None
    if override:
        return override
    if project_config.default_dataset:
        return project_config.default_dataset
    raise UnresolvedDatasetError(
        "No dataset configured: set [dlt_ops].default_dataset "
        "or [sources.<section>.dlt_ops].dataset in .dlt/config.toml"
    )

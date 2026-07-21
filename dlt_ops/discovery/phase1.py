"""Phase-1 discovery: pure AST scan of the project tree.

Never imports project code — safe to call from anything that must not execute
user modules: CLI ``pipeline list`` / ``pipeline resources`` and the Airflow
DAG factory at scheduler-parse time (the DAG-parse foot-gun this phase exists
to close). ``discovery.phase2.introspect`` enriches the result with imported
callables and the authoritative resource list.

The static resource list is an approximation: it unions ``@dlt.resource``
declarations found anywhere in the source's own module (including resources
defined inside function bodies, an idiomatic dlt pattern) with declarations in
the pipeline's ``resource/*.py`` siblings. Resources built dynamically (loops,
factories) or imported from elsewhere only resolve in Phase 2.

Checkpoint usage is detected the same way: ``uses_checkpoints`` is a name
match on decorators whose terminal name is ``with_checkpoints`` across the
source's own module and the ``resource/*.py`` siblings — an aliased import
escapes it.
"""

import ast
import logging
from collections.abc import Container
from pathlib import Path
from typing import Any

import attrs

from dlt_ops.config import RESOURCE_DIR, SOURCE_DIR, load_raw_config
from dlt_ops.discovery.models import Schedule, SourceConfig, SourceInfo

logger = logging.getLogger(__name__)

# Terminal name of the package's checkpoint decorator, matched statically.
_CHECKPOINT_DECORATOR_NAME = "with_checkpoints"


def _is_valid_source_dir(subdir: Path) -> bool:
    """Check if a directory contains a valid dlt source.

    Two structural requirements decide it, and nothing else: the directory name
    must not start with ``.`` or ``_``, and it must hold a ``SOURCE_DIR`` with
    at least one non-underscore ``.py``. No name is excluded — a pipeline
    directory called ``common`` or ``logs`` is as discoverable as any other.

    Every rejected directory is logged at DEBUG with its reason. A directory an
    operator believes is a pipeline, silently absent from ``pipeline list``, is
    the hardest discovery failure to diagnose, and this is the only place that
    knows why.
    """
    if not subdir.is_dir():
        return False
    if subdir.name.startswith((".", "_")):
        logger.debug(f"Not a pipeline dir: {subdir.name} — names starting with '.' or '_' are skipped")
        return False
    source_dir = subdir / SOURCE_DIR
    if not source_dir.is_dir():
        logger.debug(f"Not a pipeline dir: {subdir.name} — no {SOURCE_DIR}/ subdirectory")
        return False
    if not any(f.suffix == ".py" and not f.name.startswith("_") for f in source_dir.iterdir()):
        logger.debug(f"Not a pipeline dir: {subdir.name} — {SOURCE_DIR}/ holds no non-underscore .py file")
        return False
    return True


def _is_dlt_attribute(node: ast.AST, attr: str) -> bool:
    """Check if AST node is a ``dlt.<attr>`` attribute (e.g. dlt.source)."""
    return (
        isinstance(node, ast.Attribute)
        and node.attr == attr
        and isinstance(node.value, ast.Name)
        and node.value.id == "dlt"
    )


def _match_dlt_decorator(decorator: ast.expr, attr: str) -> tuple[bool, str | None]:
    """Match one decorator node against ``@dlt.<attr>`` / ``@dlt.<attr>(...)``.

    Returns:
        (matched, explicit name= string or None). Non-string name= values are
        ignored (returned as None) — the name validator flags them.
    """
    if isinstance(decorator, ast.Call) and _is_dlt_attribute(decorator.func, attr):
        for keyword in decorator.keywords:
            if (
                keyword.arg == "name"
                and isinstance(keyword.value, ast.Constant)
                and isinstance(keyword.value.value, str)
            ):
                return True, keyword.value.value
        return True, None
    if _is_dlt_attribute(decorator, attr):
        return True, None
    return False, None


def _decorator_terminal_name(decorator: ast.expr) -> str | None:
    """Terminal name of a decorator: ``@name`` / ``@some.path.name``, called or bare."""
    node = decorator.func if isinstance(decorator, ast.Call) else decorator
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


@attrs.frozen
class _ModuleScan:
    """Static facts about one Python module."""

    sources: tuple[tuple[str, str | None], ...]  # (function_name, explicit decorator name or None)
    resources: tuple[str, ...]  # declared @dlt.resource names (name= or function-name fallback)
    uses_checkpoints: bool  # any decorator whose terminal name is `with_checkpoints`


def _scan_module(py_file: Path) -> _ModuleScan:
    """AST-scan one file for @dlt.source / @dlt.resource declarations and checkpoint usage.

    Source functions are collected from module top level only (Phase 2 attaches
    them via ``getattr(module, function_name)``); resource declarations are
    collected from the whole tree, nested definitions included.
    ``uses_checkpoints`` is a name match over the same tree: any decorator
    whose terminal name is ``with_checkpoints``, bare or attribute form,
    called or not.

    Raises:
        ValueError: the file cannot be read or parsed. The message carries the
            bare reason (``SyntaxError: ...``) so call sites can frame it —
            ``discover`` turns it into a ``SourceInfo.import_error``.
    """
    try:
        tree = ast.parse(py_file.read_text(encoding="utf-8"))
    except (OSError, SyntaxError) as e:
        raise ValueError(f"{type(e).__name__}: {e}") from e

    sources: list[tuple[str, str | None]] = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            matched, explicit_name = _match_dlt_decorator(decorator, "source")
            if matched:
                sources.append((node.name, explicit_name))
                break

    resources: list[str] = []
    uses_checkpoints = False
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        # Separate pass: the resource match below stops at the first hit, and
        # @with_checkpoints sits under @dlt.resource on the same function.
        if any(_decorator_terminal_name(d) == _CHECKPOINT_DECORATOR_NAME for d in node.decorator_list):
            uses_checkpoints = True
        for decorator in node.decorator_list:
            matched, explicit_name = _match_dlt_decorator(decorator, "resource")
            if matched:
                resources.append(explicit_name or node.name)
                break

    return _ModuleScan(sources=tuple(sources), resources=tuple(resources), uses_checkpoints=uses_checkpoints)


def _scan_shared_resources(resource_dir: Path) -> tuple[tuple[str, ...], bool]:
    """Collect @dlt.resource names and checkpoint usage from a pipeline's resource/*.py."""
    if not resource_dir.is_dir():
        return (), False
    names: list[str] = []
    uses_checkpoints = False
    for py_file in sorted(resource_dir.glob("*.py")):
        if py_file.name.startswith("_"):
            continue
        try:
            scan = _scan_module(py_file)
        except ValueError as e:
            logger.warning(f"Skipping {py_file.name}: {e}")
            continue
        names.extend(scan.resources)
        uses_checkpoints = uses_checkpoints or scan.uses_checkpoints
    return tuple(names), uses_checkpoints


def _dedupe(items: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return tuple(out)


def _parse_source_config(config: dict[str, Any], config_section: str) -> SourceConfig | None:
    """Parse SourceConfig from config.toml section.

    All custom config keys are under [sources.X.dlt_ops]:
    - schedule, destination, dataset, airflow_var

    Only keys core itself acts on are parsed here (plus ``airflow_var``, which
    the CLI displays). Plugin-owned keys stay unparsed: a backend reads its own
    trigger keys off the raw ext table, so core never has to know them.
    """
    sources_config = config.get("sources", {})
    section = sources_config.get(config_section)

    if not section:
        return None

    # All our custom keys live under dlt_ops namespace
    ext = section.get("dlt_ops", {})
    schedule_str = ext.get("schedule")
    if not schedule_str:
        return None

    try:
        schedule = Schedule.from_string(schedule_str)
    except ValueError as e:
        logger.warning(f"Invalid schedule in [sources.{config_section}.dlt_ops]: {schedule_str} - {e}")
        return None

    # `injected_columns` is TOML-array shaped; anything else (missing / bad
    # type) collapses to an empty tuple rather than raising — the reconciler
    # treats absence as "no source-side injected keys" and only special-cases
    # the universal `loaded_at`.
    raw_injected = ext.get("injected_columns", ())
    if isinstance(raw_injected, (list, tuple)):
        injected_columns = tuple(str(c) for c in raw_injected if isinstance(c, str))
    else:
        injected_columns = ()

    return SourceConfig(
        schedule=schedule,
        destination=ext.get("destination"),
        dataset=ext.get("dataset"),
        airflow_var=ext.get("airflow_var"),
        schema_contract_evolve_reason=ext.get("schema_contract_evolve_reason"),
        injected_columns=injected_columns,
    )


def _unloadable_record(subdir: Path, py_file: Path, reason: str, taken: Container[str]) -> SourceInfo:
    """Placeholder for a source module that cannot be read or parsed.

    A module that does not parse declares no ``@dlt.source``, so its config
    section is unknowable; the file stem is the best guess and the one
    ``module_name_matches_section`` already expects. ``function_name`` stays
    empty — nothing may call it, because ``import_error`` keeps the record out
    of Phase 2's import path and out of every ``is_introspected`` consumer.
    """
    name = py_file.stem if py_file.stem not in taken else f"{subdir.name}.{py_file.stem}"
    return SourceInfo(
        name=name,
        pipeline_name=subdir.name,
        path=subdir,
        function_name="",
        resources=(),
        module_stem=py_file.stem,
        module_path=py_file,
        import_error=f"module could not be parsed: {reason}",
    )


def discover(project_root: Path, *, include_unloadable: bool = False) -> dict[str, SourceInfo]:
    """Discover dlt sources with a pure AST scan — zero project-code imports.

    Requirements for a source to be discovered:
    1. Directory under project_root (not starting with . or _)
    2. Contains source/ subdirectory with a non-underscore Python file
    3. That file defines a top-level function decorated with @dlt.source

    Each source is keyed by its config_section: the explicit
    ``@dlt.source(name=...)`` value, or the function name minus its
    ``_source`` suffix. A file that cannot be read or parsed is logged and
    skipped; siblings are unaffected.

    Args:
        project_root: Path to the project root (holds .dlt/config.toml and
            one subdirectory per pipeline)
        include_unloadable: Also return a placeholder record per source module
            that could not be read or parsed, carrying the reason as
            ``import_error`` so ``validate`` reports it through the same
            always-on import-error path a module that raises at import takes.
            Off by default: the parse-free consumers (``pipeline list``, the
            orchestrator DAG factory) list runnable sources, and a file that
            does not parse is not one.

    Returns:
        Dict mapping source name (config_section) to a Phase-1 SourceInfo
        (no source_fn; static resource approximation).

    Raises:
        ProjectConfigParseError: .dlt/config.toml exists but is broken TOML.
    """
    config = load_raw_config(project_root)
    sources: dict[str, SourceInfo] = {}
    unloadable: list[tuple[Path, Path, str]] = []

    for subdir in sorted(project_root.iterdir()):
        if not _is_valid_source_dir(subdir):
            continue

        shared_resources, shared_uses_checkpoints = _scan_shared_resources(subdir / RESOURCE_DIR)

        for py_file in sorted((subdir / SOURCE_DIR).glob("*.py")):
            if py_file.name.startswith("_"):
                continue

            try:
                scan = _scan_module(py_file)
            except ValueError as e:
                logger.warning(f"Skipping {py_file.name}: {e}")
                unloadable.append((subdir, py_file, str(e)))
                continue

            for function_name, decorator_name in scan.sources:
                # config_section = decorator_name (if explicit) or function_name minus "_source" suffix
                config_section = decorator_name or function_name.removesuffix("_source")

                if config_section in sources:
                    logger.warning(
                        f"Duplicate source name '{config_section}' - "
                        f"overwriting {sources[config_section].path.name} with {subdir.name}"
                    )

                sources[config_section] = SourceInfo(
                    name=config_section,
                    pipeline_name=subdir.name,
                    path=subdir,
                    function_name=function_name,
                    resources=_dedupe([*scan.resources, *shared_resources]),
                    module_stem=py_file.stem,
                    config=_parse_source_config(config, config_section),
                    decorator_name=decorator_name,
                    module_path=py_file,
                    uses_checkpoints=scan.uses_checkpoints or shared_uses_checkpoints,
                )

    # Merged last so the stem-vs-pipeline key choice sees every real source,
    # and a broken sibling can never displace one that parsed.
    if include_unloadable:
        for subdir, py_file, reason in unloadable:
            record = _unloadable_record(subdir, py_file, reason, sources)
            sources[record.name] = record

    return sources

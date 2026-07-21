"""Phase-2 discovery: sandbox-checked import + enrichment of Phase-1 output.

Two deliberately separate steps per source module:

1. Import-safety check in a SUBPROCESS (``_sandbox_child``): the child
   installs ``sys.addaudithook`` before executing the module and reports
   Rule 15 findings (network I/O, disk writes, dlt.pipeline construction,
   process spawns) plus any import exception as JSON on stdout. Audit hooks
   are process-global and irremovable, hence the throwaway child process.
2. In-process import to attach ``source_fn`` — callables cannot cross the
   process boundary, so the check and the attach must be separate steps.
   This import is NOT sandboxed and runs only after a CLEAN child verdict.
   Anything short of clean — the module raised, the child itself failed, or
   the child recorded Rule 15 violations — excludes the module with a
   recorded ``import_error`` and it is never imported here. Containment is
   the point: the child already ran the offending side effects in a
   throwaway process, so importing the module again would fire the real
   ``requests.get(...)`` inside whatever called ``introspect``. The
   project-wide ``[dlt_ops.rules] import_safety = false`` knob is the only
   opt-out; it skips the child, and with it the containment.

Per-module isolation: any child or in-process failure is recorded on the
affected sources' ``import_error``; sibling modules are unaffected and
Phase 1 still lists everything.
"""

import importlib.machinery
import importlib.util
import json
import logging
import os
import subprocess
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from types import ModuleType
from typing import Any

import attrs

from dlt_ops.config import SOURCE_DIR, ProjectRootNotFoundError, load_project_config
from dlt_ops.discovery.models import ImportViolation, SourceInfo

logger = logging.getLogger(__name__)

# Synthetic namespace user source modules are registered under. Names mirror
# the on-disk layout (<namespace>.<pipeline>.source.<stem>) so intra-pipeline
# relative imports keep working: `from ..resource.x import y` inside a source
# module resolves through the synthetic pipeline package's __path__.
SOURCE_MODULE_NAMESPACE = "dlt_ops._sources"

_SANDBOX_TIMEOUT_SECONDS = 30
# The child prints its verdict as the last marker-prefixed stdout line; the
# marker keeps the JSON separable from anything the user module printed.
_VERDICT_MARKER = "@@dlt-ops-import-safety@@ "


def import_safety_enabled(rules: Mapping[str, Any]) -> bool:
    """Rule 15 knob semantics: on unless [dlt_ops.rules] import_safety = false."""
    return rules.get("import_safety") is not False


def _register_synthetic_package(name: str, directory: Path | None) -> None:
    """Register a synthetic package searching `directory` for submodules.

    File-path import machinery: the package's __path__ scopes submodule
    lookup to the project tree — no sys.path mutation, so a project dir
    can never shadow site-packages. Re-registering the same name for a
    different directory overwrites (same pipeline name under a different
    project root within one process).
    """
    expected_path = [] if directory is None else [str(directory)]
    cached = sys.modules.get(name)
    if cached is not None and list(getattr(cached, "__path__", [])) == expected_path:
        return
    spec = importlib.machinery.ModuleSpec(name, None, is_package=True)
    spec.submodule_search_locations = expected_path
    sys.modules[name] = importlib.util.module_from_spec(spec)


def _register_pipeline_packages(pipeline_dir: Path) -> str:
    """Register the synthetic packages for one pipeline dir; return its package name."""
    pipeline_package = f"{SOURCE_MODULE_NAMESPACE}.{pipeline_dir.name}"
    _register_synthetic_package(SOURCE_MODULE_NAMESPACE, None)
    _register_synthetic_package(pipeline_package, pipeline_dir)
    _register_synthetic_package(f"{pipeline_package}.{SOURCE_DIR}", pipeline_dir / SOURCE_DIR)
    return pipeline_package


def _load_source_module(module_name: str, py_file: Path) -> ModuleType:
    """Load py_file under a synthetic module name via file-path import.

    The cached module is reused when it came from the same file; a
    same-named module from a different file is re-loaded and overwritten.
    """
    cached = sys.modules.get(module_name)
    if cached is not None and getattr(cached, "__file__", None) == str(py_file):
        return cached
    spec = importlib.util.spec_from_file_location(module_name, py_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot build an import spec for {py_file}")
    module = importlib.util.module_from_spec(spec)
    # Must be visible in sys.modules while executing so the module's own
    # relative imports can resolve back to it.
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(module_name, None)
        raise
    return module


def _get_source_resources(source_fn: Callable[..., Any]) -> tuple[str, ...]:
    """Get available resource names from a source function."""
    source = source_fn()
    return tuple(source.resources.keys())


@attrs.frozen
class _SandboxVerdict:
    """Outcome of one sandbox child run."""

    violations: tuple[ImportViolation, ...] = ()
    import_error: str | None = None  # the user module raised inside the child
    sandbox_error: str | None = None  # the child itself failed (crash / timeout / protocol)


def _spawn_sandbox_child(payload: dict[str, str], *, project_root: Path) -> _SandboxVerdict:
    """Run the audit-hook child on one payload and parse its verdict.

    -B stops the loader from writing __pycache__/*.pyc for the target —
    a bytecode-cache write would otherwise register as a false disk-write.
    """
    env = {**os.environ, "DLT_PROJECT_DIR": os.environ.get("DLT_PROJECT_DIR", str(project_root))}
    try:
        proc = subprocess.run(
            [sys.executable, "-B", "-m", "dlt_ops.discovery._sandbox_child", json.dumps(payload)],
            capture_output=True,
            text=True,
            timeout=_SANDBOX_TIMEOUT_SECONDS,
            cwd=str(project_root),
            env=env,
        )
    except subprocess.TimeoutExpired:
        return _SandboxVerdict(sandbox_error=f"import-safety check timed out after {_SANDBOX_TIMEOUT_SECONDS}s")

    for line in reversed(proc.stdout.splitlines()):
        if not line.startswith(_VERDICT_MARKER):
            continue
        try:
            data = json.loads(line[len(_VERDICT_MARKER) :])
            return _SandboxVerdict(
                violations=tuple(
                    ImportViolation(kind=v["kind"], event=v["event"], target=v["target"]) for v in data["violations"]
                ),
                import_error=data["import_error"],
            )
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            return _SandboxVerdict(sandbox_error=f"corrupt sandbox verdict: {e}")

    stderr_tail = " | ".join(proc.stderr.strip().splitlines()[-3:])
    return _SandboxVerdict(sandbox_error=f"sandbox child exited {proc.returncode} without a verdict: {stderr_tail}")


def _run_sandbox_check(*, project_root: Path, pipeline_dir: Path, module_name: str, py_file: Path) -> _SandboxVerdict:
    """Execute a source module in the audit-hook child."""
    return _spawn_sandbox_child(
        {
            "project_root": str(project_root),
            "pipeline_dir": str(pipeline_dir),
            "module_name": module_name,
            "py_file": str(py_file),
        },
        project_root=project_root,
    )


def run_predicate_sandbox_check(predicate: str, *, project_root: Path) -> _SandboxVerdict:
    """Resolve a custom assertion predicate inside the Rule-15 audit child.

    The child calls the engine's own ``assertions.config.resolve_predicate``
    with the hook armed, so an unresolvable predicate surfaces as
    ``import_error`` (worded exactly as ``run`` would fail) and import-time
    network/disk-write/pipeline-run/process-spawn events surface as
    ``violations``.
    """
    return _spawn_sandbox_child(
        {"project_root": str(project_root), "predicate": predicate},
        project_root=project_root,
    )


def introspect(project_root: Path, sources: dict[str, SourceInfo]) -> dict[str, SourceInfo]:
    """Enrich Phase-1 sources: sandbox-check, import, attach callables.

    With the ``import_safety`` rule on (default), each module first runs in
    the audit-hook child and the in-process import happens only on a clean
    verdict: a module that raised, that the child could not verify, or that
    the child caught violating Rule 15 is recorded and never imported here.
    With the rule off, the child is skipped and the in-process import is
    merely guarded per module.

    Phase-1 records that already carry an ``import_error`` (a module that
    does not parse) pass straight through — there is nothing to import.

    Args:
        project_root: Path to the project root.
        sources: Phase-1 output (``discovery.phase1.discover``).

    Returns:
        Dict keyed like the input: every source enriched with ``source_fn``
        and the authoritative resource list, or carrying ``import_error``
        (and any ``import_violations``) when its module could not be loaded.
    """
    # dlt resolves its own config/secrets relative to DLT_PROJECT_DIR; point
    # it at the same root (the package SETS this env var for dlt, never
    # reads it for its own configuration).
    os.environ.setdefault("DLT_PROJECT_DIR", str(project_root))

    try:
        rules = load_project_config(project_root).rules
    except ProjectRootNotFoundError:
        rules = {}
    sandbox = import_safety_enabled(rules)

    result: dict[str, SourceInfo] = {}
    by_module: dict[Path, list[SourceInfo]] = {}
    for info in sources.values():
        if info.import_error is not None:
            # Phase 1 already ruled the module out (unreadable / unparseable).
            result[info.name] = info
            continue
        if info.module_path is None:
            result[info.name] = attrs.evolve(info, import_error="Phase-1 record has no module_path; cannot import")
            continue
        by_module.setdefault(info.module_path, []).append(info)

    for py_file, infos in by_module.items():
        pipeline_dir = infos[0].path
        module_name = f"{SOURCE_MODULE_NAMESPACE}.{pipeline_dir.name}.{SOURCE_DIR}.{py_file.stem}"

        violations: tuple[ImportViolation, ...] = ()
        error: str | None = None
        if sandbox:
            verdict = _run_sandbox_check(
                project_root=project_root, pipeline_dir=pipeline_dir, module_name=module_name, py_file=py_file
            )
            violations = verdict.violations
            if verdict.sandbox_error is not None:
                error = f"import-safety check failed: {verdict.sandbox_error}"
            elif verdict.import_error is not None:
                error = f"module raised at import: {verdict.import_error}"
            elif violations:
                # Containment: the child already executed these side effects
                # once, in a process built to be thrown away. Importing the
                # module here would run them for real in the caller.
                findings = ", ".join(f"{v.kind} ({v.event}: {v.target})" for v in violations)
                error = (
                    f"not imported — violates import safety (Rule 15) at import time: {findings}. "
                    f"Fix the module or opt out via [dlt_ops.rules] import_safety = false."
                )

        module: ModuleType | None = None
        if error is None:
            _register_pipeline_packages(pipeline_dir)
            try:
                module = _load_source_module(module_name, py_file)
            except (Exception, SystemExit) as e:  # user code can raise anything; isolate siblings
                error = f"module raised at import: {type(e).__name__}: {e}"

        for info in infos:
            if module is None:
                logger.warning(f"Source '{info.name}' excluded from Phase 2: {error}")
                result[info.name] = attrs.evolve(info, import_error=error, import_violations=violations)
                continue

            source_fn = getattr(module, info.function_name, None)
            if not callable(source_fn):
                result[info.name] = attrs.evolve(
                    info,
                    import_error=f"module loaded but '{info.function_name}' is missing or not callable",
                    import_violations=violations,
                )
                continue

            resources = info.resources
            try:
                resources = _get_source_resources(source_fn)
            except Exception as e:
                # Keep the Phase-1 static approximation rather than dropping to ().
                logger.warning(f"Failed to instantiate resources for {info.name}: {e}")

            result[info.name] = attrs.evolve(
                info, source_fn=source_fn, resources=resources, import_violations=violations
            )

    return result

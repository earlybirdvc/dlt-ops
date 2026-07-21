"""Import-safety sandbox child (Rule 15): audit-hooked module execution.

Runs as ``python -B -m dlt_ops.discovery._sandbox_child <json>`` with
one of two payload shapes:

- source module: ``{"project_root", "pipeline_dir", "module_name", "py_file"}``
  — executes the module the way Phase-2 introspection would.
- assertion predicate: ``{"project_root", "predicate"}`` — resolves the
  ``module:attr`` path through the engine's own ``resolve_predicate``, so the
  probe's import semantics can never drift from what ``run`` does.

Either way the child installs ``sys.addaudithook`` (process-global and
irremovable — which is why this runs in a throwaway child, never in the
caller process), executes the target with the hook armed, and prints a
verdict as the last marker-prefixed stdout line:
``{"import_error": str | null, "violations": [{kind, event, target}]}``.
Exit code 0 means the protocol ran; a user-module import failure is data in
the verdict, not an exit code.

What the hook flags while the module executes:

- network:      socket.connect / socket.getaddrinfo / socket.gethostbyname /
                socket.bind / socket.sendto / socket.sendmsg / urllib.Request
                (the ATTEMPT is the violation — a refused localhost connect
                still counts).
- disk-write:   ``open`` audit events whose mode/flags request write access
                (w/a/x/+ string modes; O_WRONLY / O_RDWR / O_APPEND / O_CREAT /
                O_TRUNC / O_EXCL flags — covers built-in open, io.open,
                os.open, Path.write_text/write_bytes), plus os.mkdir /
                os.rename / os.remove / os.rmdir / os.truncate / os.link /
                os.symlink / shutil.rmtree / shutil.move / tempfile.mkstemp /
                tempfile.mkdtemp. Reads are permitted.
- pipeline-run: ``dlt.pipeline(...)`` constructed at import time, caught via a
                marker wrapped around the ``dlt.pipeline`` module attribute
                before the module executes (best effort: a direct
                ``from dlt.pipeline import pipeline`` bypasses the marker, but
                the run's disk writes still trip the hook).
- process-spawn: subprocess.Popen / os.system / os.posix_spawn / os.exec —
                a spawned process escapes the audit hook entirely, so spawning
                one at import is itself a finding.

Known gaps: C extensions doing raw syscalls bypass CPython audit events;
``os.write`` on an inherited fd has no event; anything a spawned subprocess
does is invisible (the spawn itself is flagged instead).

dlt and this package are imported (and dlt decorator machinery warmed up)
BEFORE the hook arms, so their own import-time file access is never
attributed to the user module.
"""

import json
import os
import sys
from pathlib import Path

_NETWORK_EVENTS = {
    "socket.connect",
    "socket.getaddrinfo",
    "socket.gethostbyname",
    "socket.bind",
    "socket.sendto",
    "socket.sendmsg",
    "urllib.Request",
}
_FS_MUTATION_EVENTS = {
    "os.mkdir",
    "os.rename",
    "os.remove",
    "os.rmdir",
    "os.truncate",
    "os.link",
    "os.symlink",
    "shutil.rmtree",
    "shutil.move",
    "tempfile.mkstemp",
    "tempfile.mkdtemp",
}
_PROCESS_EVENTS = {"subprocess.Popen", "os.system", "os.posix_spawn", "os.exec"}
_WRITE_FLAG_MASK = os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC | os.O_EXCL
_MAX_VIOLATIONS = 50

# The two accepted payload shapes, matched exactly: dispatch is key-based, so
# a drifted or malformed payload must fail loud here instead of silently
# taking whichever branch its keys happen to satisfy.
_PAYLOAD_SHAPES = (
    frozenset({"project_root", "predicate"}),
    frozenset({"project_root", "pipeline_dir", "module_name", "py_file"}),
)

_armed = False
_violations: list[dict[str, str]] = []


def _record(kind: str, event: str, target: str) -> None:
    entry = {"kind": kind, "event": event, "target": target}
    if entry not in _violations and len(_violations) < _MAX_VIOLATIONS:
        _violations.append(entry)


def _is_write_open(mode: object, flags: object) -> bool:
    """True when an `open` audit event requests write access.

    io.open passes the string mode + computed O_* flags; os.open passes
    mode=None + raw flags. '+' (update) counts as write. O_RDONLY is 0, so a
    read-only open has no write bit set (O_CLOEXEC and friends don't match
    the mask).
    """
    if isinstance(mode, str) and any(c in mode for c in "wax+"):
        return True
    return isinstance(flags, int) and bool(flags & _WRITE_FLAG_MASK)


def _fmt_target(event: str, args: tuple) -> str:
    try:
        if event == "socket.connect" and len(args) >= 2:
            return repr(args[1])
        if event == "socket.getaddrinfo" and len(args) >= 2:
            return f"{args[0]}:{args[1]}"
        return ", ".join(repr(a) for a in args)[:200]
    except Exception:
        return "<unprintable>"


def _audit_hook(event: str, args: tuple) -> None:
    if not _armed:
        return
    # The hook fires for every runtime event in the process; it must never
    # raise (that would corrupt unrelated code paths mid-flight).
    try:
        if event in _NETWORK_EVENTS:
            _record("network", event, _fmt_target(event, args))
        elif event == "open" and len(args) >= 3:
            if _is_write_open(args[1], args[2]):
                _record("disk-write", event, str(args[0]))
        elif event in _FS_MUTATION_EVENTS:
            _record("disk-write", event, _fmt_target(event, args))
        elif event in _PROCESS_EVENTS:
            _record("process-spawn", event, _fmt_target(event, args))
    except Exception:
        pass


def _prepare_dlt() -> None:
    """Pre-import dlt and warm its decorator machinery, then plant the
    dlt.pipeline marker.

    Warming up (applying @dlt.source/@dlt.resource to throwaway functions)
    forces dlt's lazy imports and config-provider setup to happen before the
    hook arms, so none of it is attributed to the user module. Best effort:
    a project may legitimately not have dlt importable here.
    """
    try:
        import dlt
    except Exception:
        return

    try:

        @dlt.source(name="_import_safety_warmup")
        def _warmup_source():
            return []

        @dlt.resource(name="_import_safety_warmup_rows")
        def _warmup_rows():
            yield {}

    except Exception:
        pass

    original_pipeline = getattr(dlt, "pipeline", None)
    if callable(original_pipeline):

        def _marked_pipeline(*args: object, **kwargs: object) -> object:
            if _armed:
                _record("pipeline-run", "dlt.pipeline", str(kwargs.get("pipeline_name") or args or "<unnamed>"))
            return original_pipeline(*args, **kwargs)

        dlt.pipeline = _marked_pipeline  # type: ignore[assignment]


def main() -> int:
    global _armed

    payload = json.loads(sys.argv[1])
    if set(payload) not in _PAYLOAD_SHAPES:
        print(f"unrecognized sandbox payload keys: {sorted(payload)}", file=sys.stderr)
        return 2
    os.environ.setdefault("DLT_PROJECT_DIR", payload["project_root"])

    # Everything the child itself needs gets imported before the hook arms.
    from dlt_ops.discovery.phase2 import _VERDICT_MARKER

    if "predicate" in payload:
        from dlt_ops.assertions.config import resolve_predicate

        def _execute() -> None:
            resolve_predicate(payload["predicate"], project_root=Path(payload["project_root"]))
    else:
        from dlt_ops.discovery.phase2 import _load_source_module, _register_pipeline_packages

        _register_pipeline_packages(Path(payload["pipeline_dir"]))

        def _execute() -> None:
            _load_source_module(payload["module_name"], Path(payload["py_file"]))

    _prepare_dlt()

    sys.addaudithook(_audit_hook)

    # The target's own prints must not contaminate the verdict stream;
    # open devnull before arming so it isn't flagged.
    real_stdout = sys.stdout
    devnull = open(os.devnull, "w")

    import_error: str | None = None
    sys.stdout = devnull
    _armed = True
    try:
        _execute()
    except BaseException as e:  # user code can raise anything, including SystemExit
        import_error = f"{type(e).__name__}: {e}"
    finally:
        _armed = False
        sys.stdout = real_stdout
        devnull.close()

    print(_VERDICT_MARKER + json.dumps({"import_error": import_error, "violations": _violations}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

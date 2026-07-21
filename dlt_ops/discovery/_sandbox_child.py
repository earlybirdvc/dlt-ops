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

Every candidate event is attributed to a module body before it is recorded
(:func:`_event_is_project_code`) — the rule targets what the PROJECT's module
does at import, not what the libraries it imports do to initialise themselves.

Known gaps: C extensions doing raw syscalls bypass CPython audit events;
``os.write`` on an inherited fd has no event; anything a spawned subprocess
does is invisible (the spawn itself is flagged instead); an event raised from
a thread the target started carries no module body to attribute it to, and
code ``exec``-ed from a string is not attributable to a project file.

dlt and this package are imported (and dlt decorator machinery warmed up)
BEFORE the hook arms, so their own import-time file access is never
attributed to the user module.
"""

import json
import os
import sys
from pathlib import Path
from types import FrameType

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

# Path parts marking installed third-party code that lives inside the project
# tree anyway — a virtualenv under the project root is the common case, and its
# packages are libraries, not project source.
_VENDOR_PATH_PARTS = frozenset({"site-packages", "dist-packages"})

# The two accepted payload shapes, matched exactly: dispatch is key-based, so
# a drifted or malformed payload must fail loud here instead of silently
# taking whichever branch its keys happen to satisfy.
_PAYLOAD_SHAPES = (
    frozenset({"project_root", "predicate"}),
    frozenset({"project_root", "pipeline_dir", "module_name", "py_file"}),
)

_armed = False
_violations: list[dict[str, str]] = []
_project_prefixes: tuple[str, ...] = ()


def _record(kind: str, event: str, target: str) -> None:
    entry = {"kind": kind, "event": event, "target": target}
    if entry not in _violations and len(_violations) < _MAX_VIOLATIONS:
        _violations.append(entry)


def _set_project_prefixes(project_root: str) -> None:
    """Pin the path prefixes that count as project code. Called before arming.

    Both the literal root and its realpath, because discovery hands paths
    through unresolved and a frame's ``co_filename`` may spell the same file
    either way (on macOS ``/tmp/...`` is a symlink to ``/private/tmp/...``).
    Everything is ``normcase``-folded so the Windows lane compares paths the way
    Windows means them — case-insensitively, with separators normalised — since
    a prefix that fails to match would silently drop findings rather than
    produce them.
    """
    global _project_prefixes
    roots = {project_root, os.path.realpath(project_root)}
    _project_prefixes = tuple(sorted(os.path.normcase(root).rstrip(os.sep) + os.sep for root in roots))


def _is_project_file(filename: str) -> bool:
    """True when ``filename`` is the project's own source rather than an installed library.

    Pure string work over a prefix computed before arming: the hook must not
    touch the filesystem, since a stat from inside it would fire further audit
    events. With no prefixes pinned the answer is True — attribution is the
    filter in front of a safety rule, so losing it must make the rule noisy,
    never silently dead.
    """
    if not _project_prefixes:
        return True
    folded = os.path.normcase(filename)
    if not folded.startswith(_project_prefixes):
        return False
    return not _VENDOR_PATH_PARTS.intersection(folded.split(os.sep))


def _event_is_project_code() -> bool:
    """Whether the module body currently executing is the project's own.

    The attribution boundary. Walking out from the event, the first frame
    running a module body (``co_name == "<module>"``) says whose import-time
    work this is: importing a module always interposes that module's own body
    frame, so a urllib3 body probing IPv6 while the project module runs
    ``import requests`` is urllib3 initialising itself. Reaching a PROJECT body
    frame first means the call chain got to the event without crossing into
    another module's import — the module-level ``requests.get(...)`` the rule
    exists to catch. No module body on the stack at all (an event from a thread
    the target started) leaves nothing to attribute the event to.
    """
    frame: FrameType | None = sys._getframe(1)
    while frame is not None:
        if frame.f_code.co_name == "<module>":
            return _is_project_file(frame.f_code.co_filename)
        frame = frame.f_back
    return False


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


def _event_kind(event: str, args: tuple) -> str | None:
    """The violation kind an audit event represents, or None when it is not one."""
    if event in _NETWORK_EVENTS:
        return "network"
    if event == "open":
        return "disk-write" if len(args) >= 3 and _is_write_open(args[1], args[2]) else None
    if event in _FS_MUTATION_EVENTS:
        return "disk-write"
    if event in _PROCESS_EVENTS:
        return "process-spawn"
    return None


def _audit_hook(event: str, args: tuple) -> None:
    if not _armed:
        return
    # The hook fires for every runtime event in the process; it must never
    # raise (that would corrupt unrelated code paths mid-flight). Classify
    # first, attribute second, format last — the cheap checks gate the ones
    # that walk frames or repr() arbitrary objects.
    try:
        kind = _event_kind(event, args)
        if kind is None or not _event_is_project_code():
            return
        _record(kind, event, str(args[0]) if event == "open" else _fmt_target(event, args))
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
            if _armed and _event_is_project_code():
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
    _set_project_prefixes(payload["project_root"])

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

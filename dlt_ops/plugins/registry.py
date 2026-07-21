from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from importlib.metadata import entry_points
from typing import Any, TypeVar

import attrs

_T = TypeVar("_T")

# Closed set of extension axes (extensible only in later majors). Order is the
# canonical display order for `plugins doctor`. Entry-point group per axis is
# `dlt_ops.<axis>` — frozen at first release; renames break plugins.
AXES: tuple[str, ...] = (
    "destination",
    "orchestrator",
    "assertion",
    "validators",
    "secret_backend",
    "alert_sink",
)


class PluginCollisionError(RuntimeError):
    """Distinct plugins claim the same ``<axis>/<name>`` and no disambiguation picks a winner."""


class UnknownPluginError(LookupError):
    """No distribution or runtime registration provides the requested plugin name."""


@attrs.frozen
class PluginSource:
    """Provenance of a plugin candidate: shipping distribution + object path."""

    dist: str | None
    """Distribution name; ``None`` for runtime (decorator) registrations."""

    value: str
    """``module:attr`` object path of the registered object."""

    @property
    def label(self) -> str:
        """Human-readable origin for CLI output."""
        return self.dist if self.dist is not None else "<runtime>"


@attrs.frozen
class FailedPlugin:
    """A plugin whose load raised; recorded instead of crashing (soft-fail policy)."""

    axis: str
    name: str
    dist: str | None
    error: str


@attrs.frozen
class PluginCollision:
    """An unresolved ``<axis>/<name>`` claimed by several distinct plugins."""

    axis: str
    name: str
    sources: tuple[PluginSource, ...]

    def disambiguation_toml(self) -> str:
        """The exact config block that picks a winner (hard-error, no silent first-wins)."""
        winner = self.sources[0]
        pick = winner.dist if winner.dist is not None else winner.value
        return f'[dlt_ops.plugins.{self.axis}]\n{self.name} = "{pick}"'


@attrs.frozen
class _Candidate:
    source: PluginSource
    loader: Callable[[], Any]


def _check_axis(axis: str) -> None:
    if axis not in AXES:
        raise ValueError(f"unknown plugin axis {axis!r}; axes: {', '.join(AXES)}")


def _matches(source: PluginSource, pick: str) -> bool:
    """A disambiguation value may name the distribution or the object qualname."""
    return pick in (source.dist, source.value, source.value.replace(":", "."))


def _collision_message(collision: PluginCollision) -> str:
    claimants = ", ".join(f"{source.label!r} ({source.value})" for source in collision.sources)
    return (
        f"multiple plugins register {collision.axis}/{collision.name}: {claimants}. "
        f"Pick a winner (distribution name or qualified object path) in .dlt/config.toml:\n\n"
        f"{collision.disambiguation_toml()}"
    )


class _Registry:
    """Process-wide plugin registry: entry points + runtime registrations, one lookup path.

    Scans are lazy and per-axis; a scan reads entry-point metadata only — plugin
    modules are imported by ``get`` for the specific plugin asked for.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._scanned: dict[str, dict[str, list[_Candidate]]] = {}
        self._runtime: dict[str, dict[str, list[_Candidate]]] = {axis: {} for axis in AXES}
        self._loaded: dict[tuple[str, str], Any] = {}
        self._load_errors: dict[tuple[str, str], BaseException] = {}
        self._failures: list[FailedPlugin] = []
        self._disambiguation: dict[str, dict[str, str]] = {}

    @staticmethod
    def _add_candidate(by_name: dict[str, list[_Candidate]], name: str, candidate: _Candidate) -> None:
        candidates = by_name.setdefault(name, [])
        # Same distribution + object path == the same object re-exported, not a collision.
        if any(existing.source == candidate.source for existing in candidates):
            return
        candidates.append(candidate)

    def _scan(self, axis: str) -> dict[str, list[_Candidate]]:
        scanned = self._scanned.get(axis)
        if scanned is not None:
            return scanned
        by_name: dict[str, list[_Candidate]] = {}
        for ep in entry_points(group=f"dlt_ops.{axis}"):
            dist = getattr(ep, "dist", None)
            source = PluginSource(dist=dist.name if dist is not None else None, value=ep.value)
            self._add_candidate(by_name, ep.name, _Candidate(source=source, loader=ep.load))
        self._scanned[axis] = by_name
        return by_name

    def _candidates(self, axis: str, name: str) -> list[_Candidate]:
        merged: list[_Candidate] = []
        for candidate in self._scan(axis).get(name, []) + self._runtime[axis].get(name, []):
            if not any(existing.source == candidate.source for existing in merged):
                merged.append(candidate)
        return merged

    def _resolve(self, axis: str, name: str) -> _Candidate | PluginCollision | None:
        candidates = self._candidates(axis, name)
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        pick = self._disambiguation.get(axis, {}).get(name)
        if pick is not None:
            matched = [candidate for candidate in candidates if _matches(candidate.source, pick)]
            if len(matched) == 1:
                return matched[0]
        return PluginCollision(axis=axis, name=name, sources=tuple(candidate.source for candidate in candidates))

    def register_runtime(self, axis: str, name: str, obj: Any) -> None:
        _check_axis(axis)
        if not name:
            raise ValueError("plugin name must be a non-empty string")
        value = f"{getattr(obj, '__module__', '<unknown>')}:{getattr(obj, '__qualname__', repr(obj))}"
        candidate = _Candidate(source=PluginSource(dist=None, value=value), loader=lambda: obj)
        with self._lock:
            self._add_candidate(self._runtime[axis], name, candidate)

    def set_disambiguation(self, mapping: Mapping[str, Mapping[str, str]]) -> None:
        unknown = set(mapping) - set(AXES)
        if unknown:
            raise ValueError(f"unknown plugin axes in disambiguation mapping: {', '.join(sorted(unknown))}")
        with self._lock:
            self._disambiguation = {axis: dict(names_) for axis, names_ in mapping.items()}

    def names(self, axis: str) -> tuple[str, ...]:
        _check_axis(axis)
        with self._lock:
            return tuple(sorted(set(self._scan(axis)) | set(self._runtime[axis])))

    def get(self, axis: str, name: str) -> Any:
        _check_axis(axis)
        with self._lock:
            key = (axis, name)
            if key in self._loaded:
                return self._loaded[key]
            if key in self._load_errors:
                raise self._load_errors[key]
            resolved = self._resolve(axis, name)
            if resolved is None:
                registered = ", ".join(repr(known) for known in self.names(axis)) or "(none)"
                raise UnknownPluginError(f"no plugin {name!r} registered for axis {axis!r}; registered: {registered}")
            if isinstance(resolved, PluginCollision):
                raise PluginCollisionError(_collision_message(resolved))
            try:
                obj = resolved.loader()
            except Exception as exc:
                self._failures.append(
                    FailedPlugin(
                        axis=axis,
                        name=name,
                        dist=resolved.source.dist,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )
                self._load_errors[key] = exc
                raise
            self._loaded[key] = obj
            return obj

    def source(self, axis: str, name: str) -> PluginSource | None:
        """Resolved provenance for ``<axis>/<name>``; ``None`` when unknown or contested."""
        _check_axis(axis)
        with self._lock:
            resolved = self._resolve(axis, name)
            if isinstance(resolved, _Candidate):
                return resolved.source
            return None

    def failures(self) -> tuple[FailedPlugin, ...]:
        with self._lock:
            return tuple(self._failures)

    def collisions(self) -> tuple[PluginCollision, ...]:
        with self._lock:
            found: list[PluginCollision] = []
            for axis in AXES:
                for name in self.names(axis):
                    resolved = self._resolve(axis, name)
                    if isinstance(resolved, PluginCollision):
                        found.append(resolved)
            return tuple(found)


_registry = _Registry()


def register(axis: str, name: str) -> Callable[[_T], _T]:
    """Register the decorated object as plugin ``<axis>/<name>``.

    Runtime twin of the ``dlt_ops.<axis>`` entry-point groups — both feed
    the same process-wide registry, so ``get``/``names`` behave identically
    regardless of how a plugin arrived::

        @dlt_ops.register("destination", "duckdb")
        class DuckDBAdapter: ...
    """

    def _decorator(obj: _T) -> _T:
        _registry.register_runtime(axis, name, obj)
        return obj

    return _decorator


def get(axis: str, name: str) -> Any:
    """Resolve and load plugin ``<axis>/<name>``.

    Raises ``UnknownPluginError`` (with a registered-names hint) for unknown
    names, ``PluginCollisionError`` (with the exact disambiguation TOML) for
    contested names, and re-raises load errors after recording a
    ``FailedPlugin``. Loaded objects are cached per process.
    """
    return _registry.get(axis, name)


def names(axis: str) -> tuple[str, ...]:
    """Sorted plugin names registered for an axis (no plugin imports triggered)."""
    return _registry.names(axis)


def source(axis: str, name: str) -> PluginSource | None:
    """Resolved provenance for ``<axis>/<name>``; ``None`` when unknown or contested."""
    return _registry.source(axis, name)


def failures() -> tuple[FailedPlugin, ...]:
    """Plugins whose load raised so far in this process (soft-fail records)."""
    return _registry.failures()


def collisions() -> tuple[PluginCollision, ...]:
    """Unresolved ``<axis>/<name>`` collisions across all axes (metadata-only scan)."""
    return _registry.collisions()


def set_disambiguation(mapping: Mapping[str, Mapping[str, str]]) -> None:
    """Install the collision-disambiguation mapping: ``{axis: {name: distribution-or-qualname}}``.

    Mirrors ``[dlt_ops.plugins.<axis>]`` config tables; callers pass the
    parsed mapping directly, so the registry has no config-loader dependency.
    Set it before lookups — loaded objects are cached per process.
    """
    _registry.set_disambiguation(mapping)


def _reset_for_tests() -> None:
    """Drop the process-wide registry so tests start from a clean scan/load state.

    Not part of the public API. Tests import this to reset module-level state
    between cases.
    """
    global _registry
    _registry = _Registry()

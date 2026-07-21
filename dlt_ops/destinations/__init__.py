"""Destination adapters — the only layer that speaks destination-native SQL.

Adapter implementations (``duckdb.py``, ``bigquery.py``) are loaded lazily via
the ``dlt_ops.destination`` entry-point group; importing this package
pulls neither sqlglot nor any destination SDK.

Adapter registration is the capability-tier switch: a destination whose engine
name has a registered adapter runs at full tier; every other destination dlt
can resolve runs in core mode, where the run loop works and the
:data:`ADAPTER_GATED_FEATURES` are unavailable.
"""

import tempfile
from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from typing import Any

import dlt
from dlt.common.destination import Destination

from dlt_ops.destinations.protocol import ColumnInfo, Cursor, DestinationAdapter
from dlt_ops.plugins import UnknownPluginError
from dlt_ops.plugins import get as _get_plugin
from dlt_ops.plugins import names as _plugin_names
from dlt_ops.plugins import registry as _default_registry

__all__ = [
    "ADAPTER_GATED_FEATURES",
    "ColumnInfo",
    "Cursor",
    "DestinationAdapter",
    "UnregisteredDestinationError",
    "adapter_for_pipeline",
    "core_mode_notice",
    "engine_name",
    "get_adapter",
    "has_adapter",
    "open_client",
    "open_destination_boundary",
]

ADAPTER_GATED_FEATURES: tuple[str, ...] = (
    "runs ledger and status",
    "checkpoints",
    "backfill",
    "clean (remote)",
    "reconcile",
    "assertion quarantine",
)
"""The canonical adapter-gated feature list.

Every surface that names the gated features (preflight errors, runner and
validate warnings, verb refusals, docs) renders from this tuple — never a
local copy, so the lists cannot drift.
"""


class UnregisteredDestinationError(LookupError):
    """An adapter-gated operation reached a destination running in core mode (no adapter)."""


def engine_name(destination: Any) -> str:
    """Registry key for a resolved dlt destination: its engine name.

    Derived from ``destination_type`` (``duckdb``, ``bigquery``, ...) — a
    custom ``destination_name`` on a dlt destination changes config sections,
    not the SQL dialect the adapter must speak. The one normalization every
    adapter lookup shares, so ``"duckdb"`` and ``"dlt.destinations.duckdb"``
    land on the same registry entry.
    """
    return Destination.to_name(destination.destination_type)


def has_adapter(destination_ref: str, *, registry: Any = _default_registry) -> bool:
    """Whether a ``DestinationAdapter`` is registered for ``destination_ref``'s engine name.

    Registry-membership check only — the adapter is never loaded. The ref is
    anything dlt itself resolves (shorthand ``duckdb``, module path
    ``dlt.destinations.duckdb``); an unresolvable ref is False, because
    resolvability is preflight's question, not this one's.
    """
    try:
        resolved = Destination.from_reference(destination_ref)
    except Exception:
        return False
    return engine_name(resolved) in registry.names("destination")


def core_mode_notice(destination: str) -> str:
    """The one-line core-mode statement every reporting surface renders.

    Single rendering site so the run-start warning, the validate warning, and
    the refusal errors can never drift on what core mode means or which
    features it darkens.
    """
    return (
        f"destination {destination!r} has no registered DestinationAdapter — running in core mode; "
        f"adapter-gated features unavailable: {', '.join(ADAPTER_GATED_FEATURES)}"
    )


def get_adapter(name: str) -> DestinationAdapter:
    """Resolve destination plugin ``name`` from the registry and instantiate it.

    Entry points conventionally register the adapter class; already-constructed
    adapter objects (e.g. runtime registrations) pass through as-is.
    """
    plugin: Any = _get_plugin("destination", name)
    return plugin() if isinstance(plugin, type) else plugin


def adapter_for_pipeline(pipeline: Any) -> DestinationAdapter:
    """Resolve the adapter for a live dlt pipeline's destination.

    The registry key is the destination's engine name (see
    :func:`engine_name`).

    Raises:
        UnregisteredDestinationError: the destination runs in core mode; the
            caller's operation is adapter-gated.
    """
    name = engine_name(pipeline.destination)
    try:
        return get_adapter(name)
    except UnknownPluginError as exc:
        registered = ", ".join(repr(known) for known in _plugin_names("destination")) or "(none)"
        raise UnregisteredDestinationError(
            f"pipeline {pipeline.pipeline_name!r}: {core_mode_notice(name)}. Registered adapters: {registered}. "
            f"Install one under the 'dlt_ops.destination' entry-point group to reach full tier; "
            f"see docs/reference/destinations.md."
        ) from exc


def open_client(pipeline: Any) -> AbstractContextManager[Any]:
    """Open the pipeline's live SQL client for handing into adapter calls.

    The one sanctioned acquisition point outside adapter implementations —
    ci/sql_boundary_guard.sh bans raw acquisition elsewhere in the package.
    """
    return pipeline.sql_client()


@contextmanager
def throwaway_pipeline(pipeline_name: str, destination: Any, dataset: str) -> Iterator[Any]:
    """A dlt pipeline that exists only as a client-acquisition vehicle.

    Its working dir points at a temp dir, so constructing it never creates or
    mutates the user's real local pipeline state. ``pipeline_name`` still
    matters on file-based destinations (DuckDB keys the physical database on
    it) — pass the same name the data run used.
    """
    with tempfile.TemporaryDirectory() as tmp:
        yield dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination,
            dataset_name=dataset,
            pipelines_dir=tmp,
        )


@contextmanager
def open_destination_boundary(
    pipeline_name: str, destination: Any, dataset: str
) -> Iterator[tuple[DestinationAdapter, Any]]:
    """Adapter + live client for one physical (destination, dataset) location.

    The shared acquisition for verbs that touch a destination without a live
    run (ledger reads, cleanup, reconciliation, backfill state): resolves the
    adapter and opens the client on a :func:`throwaway_pipeline`. The client
    closes when the context exits, so all destination work happens inside it.
    """
    with throwaway_pipeline(pipeline_name, destination, dataset) as pipeline:
        adapter = adapter_for_pipeline(pipeline)
        with open_client(pipeline) as client:
            yield adapter, client

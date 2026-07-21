"""Capability-derived adapters — full tier for a destination nobody hand-wrote.

Most of what a ``DestinationAdapter`` needs is already published by dlt per
destination (see ``_capabilities.py``), so for any destination declaring a
``sqlglot_dialect`` the whole class can be derived. :func:`register_derived_adapter`
puts one on the ``destination`` plugin axis, and every tier check, preflight
gate and gated feature then treats that destination as full tier — no new
switch, no second code path.

**Opt-in, and deliberately so.** Deriving an adapter proves the SQL will be
*shaped* for the right dialect. It proves nothing about whether the driver
binds parameters that way, whether the destination's ``information_schema``
has the standard shape, or whether its SQL client writes anywhere durable —
and for at least one dlt destination (an object store whose client is an
in-memory query engine over files) the derivation is dialect-correct and
operationally wrong. Registering on the user's say-so keeps "dlt publishes
enough to derive this" from being read as "this package supports it"::

    from dlt_ops.destinations import register_derived_adapter

    register_derived_adapter("snowflake")

The registration logs a warning naming the destination as derived and
unverified. Deriving what dlt cannot describe raises rather than guessing.
"""

from __future__ import annotations

import logging
from typing import Any

from dlt_ops.destinations._base import SqlAdapterBase
from dlt_ops.destinations._capabilities import DerivedCapabilities, require_capabilities
from dlt_ops.plugins import registry as _default_registry

__all__ = ["DerivedAdapter", "derived_adapter", "register_derived_adapter"]

logger = logging.getLogger(__name__)


class DerivedAdapter(SqlAdapterBase):
    """A ``DestinationAdapter`` built entirely from dlt's published capabilities.

    Declares nothing the base cannot derive, which is the point: the same
    inherited defaults that make a hand-written adapter small are the whole
    implementation here. What it cannot know is exactly what the base cannot
    derive — driver paramstyle, NULL binding, ``information_schema`` scope,
    who owns schema creation — so a destination that differs from the standard
    shape on any of those needs a hand-written adapter overriding just that.
    """

    def __init__(self, engine: str) -> None:
        self.name = engine
        self.capabilities: DerivedCapabilities = require_capabilities(engine)
        super().__init__()
        logger.warning(
            "destination %r is running on a capability-derived DestinationAdapter "
            "(sqlglot dialect %r, from dlt's DestinationCapabilitiesContext). It is not exercised by "
            "dlt-ops CI, and derivation cannot see driver paramstyle, NULL binding, information_schema "
            "scope, or whether the destination's SQL client writes anywhere durable. Verify the "
            "adapter-gated features against your destination before relying on them.",
            engine,
            self.dialect,
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r}, dialect={self.dialect!r})"


def derived_adapter(engine: str) -> DerivedAdapter:
    """Build (without registering) a capability-derived adapter for ``engine``.

    Raises:
        UnderivableDestinationError: dlt publishes no basis for one — an
            unresolvable destination, no declared dialect, or a dialect whose
            placeholder syntax sqlglot cannot render as a token.
    """
    return DerivedAdapter(engine)


def register_derived_adapter(engine: str, *, registry: Any = _default_registry) -> DerivedAdapter:
    """Register a capability-derived adapter for ``engine`` on the ``destination`` axis.

    The runtime twin of shipping an adapter under the ``dlt_ops.destination``
    entry-point group: after this call ``has_adapter`` answers True for
    ``engine`` and the adapter-gated features run against it. Registering the
    same engine twice is harmless.

    Raises:
        UnderivableDestinationError: see :func:`derived_adapter`.
    """
    adapter = derived_adapter(engine)
    registry.register("destination", engine)(adapter)
    return adapter

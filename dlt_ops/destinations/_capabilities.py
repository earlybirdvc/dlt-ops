"""Per-destination facts read from dlt's own ``DestinationCapabilitiesContext``.

Internal. dlt already publishes, per destination factory, facts an adapter
would otherwise hand-write — the sqlglot dialect to transpile into, and whether
``CREATE TABLE IF NOT EXISTS`` is valid DDL. Reading them here means
:class:`~dlt_ops.destinations._base.SqlAdapterBase` declares a fact only where
dlt publishes none.

Deliberately *not* read: dlt's ``escape_identifier``. Quoting is already handled
by transpiling canonically-quoted identifiers into the target dialect, and
dlt's escaper is per-destination policy rather than a defence — at least one
destination sets it to ``str``, which escapes nothing. The validate-then-quote
rule in ``protocol.py`` stays the boundary's only identifier defence.

Two properties make this safe to call from adapter construction:

- **No live connection, no credentials.** ``Destination.capabilities()``
  synthesizes mock credentials rather than resolving real ones.
- **No destination SDK.** Resolving a factory and reading its capabilities
  imports neither the warehouse client library nor its auth stack, so adapter
  import hygiene (locked in the adapter tests) survives.

Nothing here guesses. A destination dlt cannot resolve, or one that publishes
no ``sqlglot_dialect``, yields ``None`` — the caller degrades to core mode and
says so, rather than transpiling into a dialect nobody declared.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import TYPE_CHECKING

import attrs
import sqlglot

if TYPE_CHECKING:
    from dlt.common.destination.capabilities import DestinationCapabilitiesContext

__all__ = [
    "DerivedCapabilities",
    "UnderivableDestinationError",
    "derivable_destinations",
    "derive_capabilities",
    "require_capabilities",
]

PLACEHOLDER_TOKEN_RE = re.compile(r"[^\s{}(),;'\"]+")
"""What a usable positional placeholder token may look like.

Deliberately open — the set of placeholder styles a DB-API driver can use is
not this package's to close. It rejects only renderings that are structurally
unusable as a substituted token: whitespace, braces, or statement punctuation
mean the dialect writer rewrote ``?`` into something that is not a placeholder.
"""


class UnderivableDestinationError(LookupError):
    """Capabilities for this destination give no basis for an adapter.

    Raised instead of falling back to a plausible-looking dialect: guessing one
    would produce SQL that parses and silently means something else.
    """


@attrs.frozen
class DerivedCapabilities:
    """The subset of dlt's capabilities an adapter can be built from."""

    engine: str
    """dlt engine name — the adapter registry key (``mssql``, ``snowflake``, ...)."""

    dialect: str
    """sqlglot dialect to transpile canonical SQL into.

    Distinct from ``engine``: several engines share a dialect (both T-SQL
    engines) and object stores borrow one, so the registry key and the
    transpile target are two facts, not one.
    """

    placeholder_style: str
    """How this dialect renders a positional placeholder.

    A *dialect* fact, derived by asking sqlglot to write one. The *driver's*
    paramstyle can disagree with its own dialect's convention, and where it
    does the adapter must declare it — see ``SqlAdapterBase.placeholder_style``.
    """

    supports_if_exists: bool
    """``IF EXISTS`` / ``IF NOT EXISTS`` are valid on table DDL.

    From dlt's ``supports_create_table_if_not_exists``. dlt publishes the CREATE
    half only; treating DROP as the same fact is the conservative reading —
    a False sends the caller down the probe-then-drop path, which is correct
    either way.
    """


@lru_cache(maxsize=None)
def _capabilities(engine: str) -> DestinationCapabilitiesContext | None:
    """dlt's capabilities context for ``engine``, or ``None`` if unobtainable.

    Never raises: an unresolvable reference, a factory that fails to import,
    and a factory that cannot describe itself are all the same answer to the
    caller — no capabilities, so nothing to derive.
    """
    from dlt.common.destination import Destination

    try:
        factory = Destination.from_reference(engine)
        return factory.capabilities() if factory is not None else None
    except Exception:
        return None


@lru_cache(maxsize=None)
def _dialect_placeholder(dialect: str) -> str | None:
    """The token ``dialect``'s sqlglot writer renders a positional placeholder as.

    Asks the writer rather than tabulating per dialect, so the answer tracks
    whatever sqlglot version is installed. ``None`` when the writer produces
    something unusable as a substituted token (a dialect whose placeholder
    syntax carries structure sqlglot renders inline) — the caller must then
    treat the destination as underivable rather than emit a broken token.
    """
    try:
        rendered = sqlglot.parse_one("SELECT ?", read="duckdb").sql(dialect=dialect)
    except Exception:
        return None
    token = rendered.removeprefix("SELECT ").strip()
    return token if PLACEHOLDER_TOKEN_RE.fullmatch(token) else None


def derive_capabilities(engine: str) -> DerivedCapabilities | None:
    """Everything an adapter for ``engine`` can be derived from, or ``None``.

    ``None`` means one of: dlt cannot resolve the reference, the destination
    declares no ``sqlglot_dialect``, or sqlglot cannot render a usable
    placeholder for that dialect. All three are "there is no honest basis for
    an adapter here", and callers degrade to core mode.
    """
    caps = _capabilities(engine)
    if caps is None or not caps.sqlglot_dialect:
        return None
    placeholder = _dialect_placeholder(caps.sqlglot_dialect)
    if placeholder is None:
        return None
    return DerivedCapabilities(
        engine=engine,
        dialect=caps.sqlglot_dialect,
        placeholder_style=placeholder,
        # dlt defaults this True on the base capabilities context, so an
        # engine that never considered the question reads as supporting it —
        # matching the pre-derivation behaviour of every hand-written adapter.
        supports_if_exists=bool(caps.supports_create_table_if_not_exists),
    )


def require_capabilities(engine: str) -> DerivedCapabilities:
    """:func:`derive_capabilities`, raising instead of returning ``None``.

    Raises:
        UnderivableDestinationError: with the reason — unresolvable reference,
            no declared dialect, or no renderable placeholder.
    """
    derived = derive_capabilities(engine)
    if derived is not None:
        return derived
    caps = _capabilities(engine)
    if caps is None:
        reason = "dlt cannot resolve it as a destination"
    elif not caps.sqlglot_dialect:
        reason = "its dlt capabilities declare no sqlglot_dialect, so there is no dialect to transpile into"
    else:
        reason = "sqlglot renders no usable positional placeholder for its dialect"
    raise UnderivableDestinationError(
        f"cannot derive a DestinationAdapter for {engine!r}: {reason}. "
        f"Destinations that publish enough to derive from: {', '.join(derivable_destinations()) or '(none)'}. "
        f"Without one this destination runs in core mode."
    )


def derivable_destinations() -> tuple[str, ...]:
    """Engine names dlt publishes enough capabilities to derive an adapter for.

    Diagnostic surface: what a user could opt into, not what is supported.
    Derivation says the SQL will be shaped for the right dialect; it says
    nothing about whether anyone has run it there.
    """
    from dlt.common.destination import Destination
    from dlt.common.destination.reference import DestinationReference

    engines = {Destination.to_name(ref) for ref in DestinationReference.DESTINATIONS}
    return tuple(sorted(engine for engine in engines if derive_capabilities(engine) is not None))

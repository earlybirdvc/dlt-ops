"""Verified dlt minors, for the one feature that rewrites dlt-internal storage.

The package itself never caps dlt (floor-only dependency); only remote cleanup
consults this list, because it edits dlt's state tables and refuses to guess
against an unverified layout. ``SUPPORTED_DLT_MINORS`` mirrors
``ci/dlt-versions.txt`` (the single source of truth for the verified set);
tests/test_cleanup.py asserts the two stay in sync. Extend the tuple together
with the pin file after the state-schema diff verifies the new minor
(procedure in COMPATIBILITY.md).
"""

from __future__ import annotations

import importlib.metadata

SUPPORTED_DLT_MINORS: tuple[str, ...] = ("1.27", "1.28", "1.29")
"""dlt minors (``"X.Y"``) whose state/schema table layout is verified."""


def installed_dlt_version() -> str:
    """Installed dlt distribution version (e.g. ``"1.29.0"``)."""
    return importlib.metadata.version("dlt")


def dlt_minor(version: str) -> str:
    """``"X.Y.Z..." -> "X.Y"``; raises ``ValueError`` for versions without a minor."""
    parts = version.split(".")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise ValueError(f"cannot extract a minor from dlt version {version!r}")
    return f"{parts[0]}.{parts[1]}"


def supported_dlt_range() -> str:
    """Human-readable verified range, e.g. ``"1.27.x-1.29.x"``."""
    return f"{SUPPORTED_DLT_MINORS[0]}.x-{SUPPORTED_DLT_MINORS[-1]}.x"


def is_dlt_version_supported(version: str) -> bool:
    """True iff ``version``'s minor is in the verified range."""
    try:
        return dlt_minor(version) in SUPPORTED_DLT_MINORS
    except ValueError:
        return False

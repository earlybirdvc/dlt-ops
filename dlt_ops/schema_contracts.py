"""The package's schema-contract vocabulary: canonical literals and contract reading.

`CANONICAL_SCHEMA_CONTRACT` is the literal the `schema_contract_declared` rule
accepts and the one the runtime applies to resources dlt left without a
contract (dict `columns=`, or no `columns=`). It is deliberately identical to
what dlt derives from a Pydantic model declaring `extra="forbid"` — a resource
reaches the same contract from either direction, so the two paths cannot drift
into two different project policies.
"""

from collections.abc import Mapping
from typing import Any

CANONICAL_SCHEMA_CONTRACT: dict[str, str] = {
    "tables": "evolve",
    "columns": "freeze",
    "data_type": "freeze",
}

# Per-source opt-in via a non-empty `schema_contract_evolve_reason` under
# [sources.<X>.dlt_ops].
EVOLVE_SCHEMA_CONTRACT: dict[str, str] = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "freeze",
}

# dlt's column-evolution mode for "drop unknown columns and say nothing". It is
# what dlt derives for a Pydantic model that leaves `extra` unset, so it is the
# one mode this package refuses: silent data loss is the opposite of the
# fail-fast contract every other gate here enforces.
DISCARD_COLUMN_MODE = "discard_value"


def effective_column_mode(schema_contract: Any) -> str | None:
    """Column-evolution mode a resource's `schema_contract` hint resolves to.

    dlt accepts a contract as a bare string (one mode for every entity) or as a
    per-entity dict, so reading `["columns"]` alone is not enough. `None` means
    the resource carries no contract this can be read from — either no contract
    at all, or a shape (callable, malformed) no static reading can resolve.
    """
    if isinstance(schema_contract, str):
        return schema_contract
    if isinstance(schema_contract, Mapping):
        mode = schema_contract.get("columns")
        return mode if isinstance(mode, str) else None
    return None


def discards_unknown_columns(schema_contract: Any) -> bool:
    """True when this contract makes dlt drop unknown columns without an error."""
    return effective_column_mode(schema_contract) == DISCARD_COLUMN_MODE

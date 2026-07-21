"""Canonical dlt schema-contract literals.

Single source for the validator (`schema_contract_declared`) and the runtime
auto-apply on resources that declare no contract.
"""

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

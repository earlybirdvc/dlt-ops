"""Alias-safe Pydantic-model column-name utilities.

Shared home for the "which keys can a raw payload legitimately carry" walk.
`drop_unknown_nulls` uses it to strip unknown-null keys before dlt's normalize
stage (defer column birth until the first typed value); the reconciler uses it
to compute the known-column set for both additive detection
(`live_columns - known`) and removal detection ("columns worth checking for a
coverage drop").

Handles Pydantic v2 alias forms:
- `field_info.alias` — the primary alias.
- `field_info.validation_alias` — only when it's a plain str. `AliasChoices` /
  `AliasPath` are skipped (no single stable key to include).

Pydantic v2 models with `populate_by_name=True` accept BOTH the attribute
name AND the alias on input, so both go into `known`.
"""

from collections.abc import Callable
from typing import Any

import pydantic


def extract_model_column_names(model: type[pydantic.BaseModel]) -> set[str]:
    """Return the set of keys a raw payload can carry for a known model field.

    Includes attribute names + aliases + validation_aliases so callers do not
    need to know Pydantic-v2 alias forms. Shared by drop_unknown_nulls (below),
    reconciler/additive.py (known-set for `live_columns - known` diff), and
    reconciler/removal.py (columns-to-check-for-coverage-drop).
    """
    known: set[str] = set()
    for name, field_info in model.model_fields.items():
        known.add(name)
        if field_info.alias:
            known.add(field_info.alias)
        if field_info.validation_alias and isinstance(field_info.validation_alias, str):
            known.add(field_info.validation_alias)
    return known


def drop_unknown_nulls(model: type[pydantic.BaseModel]) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Strip None-valued keys not in the model's known-fields set.

    Prevents the seen-null-first freeze trap: dlt would otherwise register an
    incomplete column on the first null observation; when the first non-null
    arrives, dlt tries to complete-with-type and data_type: "freeze" rejects
    it. Stripping unknown nulls means the column is born typed on its first
    real value.
    """
    known = extract_model_column_names(model)

    def _map(record: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in record.items() if v is not None or k in known}

    return _map

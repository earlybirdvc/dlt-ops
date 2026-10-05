"""Alias-safe Pydantic-model column-name utilities.

Two views of a model's field names:

- `extract_model_column_names` — the keys a raw payload can legitimately
  carry. `drop_unknown_nulls` uses it to strip unknown-null keys before dlt's
  normalize stage (defer column birth until the first typed value). It covers
  the attribute name, `field_info.alias`, and a plain-str
  `field_info.validation_alias` (`AliasChoices` / `AliasPath` are skipped: no
  single stable key to include). Pydantic v2 models with
  `populate_by_name=True` accept both the attribute name and the alias on
  input, so both are known.
- `destination_field_names` — the one column name dlt writes per field,
  before destination-side normalization. The reconciler diffs and queries
  live destination columns, so it uses this view.
"""

from collections.abc import Callable
from typing import Any

import pydantic


def extract_model_column_names(model: type[pydantic.BaseModel]) -> set[str]:
    """Return the set of keys a raw payload can carry for a known model field.

    Includes attribute names + aliases + validation_aliases so callers do not
    need to know Pydantic-v2 alias forms.
    """
    known: set[str] = set()
    for name, field_info in model.model_fields.items():
        known.add(name)
        if field_info.alias:
            known.add(field_info.alias)
        if field_info.validation_alias and isinstance(field_info.validation_alias, str):
            known.add(field_info.validation_alias)
    return known


def destination_field_names(model: type[pydantic.BaseModel]) -> set[str]:
    """Return the column name dlt writes for each model field: the alias if set, else the attribute name.

    This is the rule dlt's ``pydantic_to_table_schema_columns`` applies.
    ``validation_alias`` and ``serialization_alias`` change parsing and
    dumping, not the schema column, so they are not names here.
    """
    return {field_info.alias or name for name, field_info in model.model_fields.items()}


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

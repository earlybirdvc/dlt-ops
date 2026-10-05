"""Tests for the shared alias-safe Pydantic-model column-name utilities.

Covers Pydantic v2 alias forms, the seen-null-first mitigation
(`drop_unknown_nulls`) that sources wire into their `add_map`, and the
destination-side name rule (`destination_field_names`) the reconciler diffs
against.
"""

import pydantic
import pytest
from dlt.common.libs.pydantic import pydantic_to_table_schema_columns
from pydantic.alias_generators import to_camel

from dlt_ops import drop_unknown_nulls, extract_model_column_names
from dlt_ops import pydantic_fields as pydantic_fields_mod


class TestExtractModelColumnNames:
    """Tests for extract_model_column_names."""

    def test_attribute_only_fields(self):
        """Plain fields → set of attribute names."""

        class M(pydantic.BaseModel):
            a: str
            b: int | None = None
            c: str | None = None

        assert extract_model_column_names(M) == {"a", "b", "c"}

    def test_alias_only_field(self):
        """A field with only `alias` returns both attribute name AND alias."""

        class M(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(populate_by_name=True)
            snake_case: str = pydantic.Field(alias="camelCase")

        # populate_by_name=True means both names are legal input keys.
        assert extract_model_column_names(M) == {"snake_case", "camelCase"}

    def test_validation_alias_string_field(self):
        """String validation_alias joins the known set alongside attribute name."""

        class M(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(populate_by_name=True)
            resolved: str = pydantic.Field(validation_alias="incoming_key")

        assert extract_model_column_names(M) == {"resolved", "incoming_key"}

    def test_validation_alias_non_string_skipped(self):
        """AliasChoices (non-str) is skipped — no single stable key to include.

        The attribute name still lands in the known set; only the AliasChoices
        payload itself is excluded from `known`.
        """

        class M(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(populate_by_name=True)
            resolved: str = pydantic.Field(
                validation_alias=pydantic.AliasChoices("primary", "secondary"),
            )

        # AliasChoices is not a str; only the attribute name comes through.
        assert extract_model_column_names(M) == {"resolved"}

    def test_alias_and_populate_by_name(self):
        """With populate_by_name=True + alias, both are known keys."""

        class M(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(populate_by_name=True)
            snake: str = pydantic.Field(alias="camel")
            other: int | None = None

        assert extract_model_column_names(M) == {"snake", "camel", "other"}


class TestDropUnknownNulls:
    """Tests for drop_unknown_nulls."""

    def _model(self):
        class M(pydantic.BaseModel):
            model_config = pydantic.ConfigDict(populate_by_name=True)
            id: str
            name: str | None = None
            snake_case: str | None = pydantic.Field(default=None, alias="camelCase")

        return M

    def test_strips_unknown_null(self):
        """Payload key not in known set + value=None → stripped."""

        fn = drop_unknown_nulls(self._model())
        assert fn({"id": "1", "unknown_new_field": None}) == {"id": "1"}

    def test_preserves_known_null(self):
        """Payload key in known set + value=None → kept (dlt will type it later)."""

        fn = drop_unknown_nulls(self._model())
        assert fn({"id": "1", "name": None}) == {"id": "1", "name": None}

    def test_preserves_unknown_non_null(self):
        """Unknown key with a non-null value is NOT stripped — dlt will surface it.

        The stripper only targets nulls; freeze-contract enforcement on unknown
        NON-nulls stays intact so drift is loud.
        """

        fn = drop_unknown_nulls(self._model())
        assert fn({"id": "1", "unknown_new_field": "surprise"}) == {
            "id": "1",
            "unknown_new_field": "surprise",
        }

    def test_preserves_known_non_null(self):
        """Attribute-name known field with a real value passes through untouched."""

        fn = drop_unknown_nulls(self._model())
        assert fn({"id": "1", "name": "hello"}) == {"id": "1", "name": "hello"}

    def test_alias_safe_path(self):
        """An aliased known field's null MUST be preserved when the payload
        uses the alias form — the alias walk (`extract_model_column_names`) is
        what makes this safe."""

        fn = drop_unknown_nulls(self._model())
        assert fn({"id": "1", "camelCase": None}) == {"id": "1", "camelCase": None}


class _AttributeOnly(pydantic.BaseModel):
    a: str
    b: int | None = None


class _AliasOnly(pydantic.BaseModel):
    snake_case: str = pydantic.Field(alias="camelCase")


class _AliasPopulateByName(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(populate_by_name=True)
    snake_case: str = pydantic.Field(alias="camelCase")
    other: int | None = None


class _ValidationAliasOnly(pydantic.BaseModel):
    resolved: str = pydantic.Field(validation_alias="incoming_key")


class _SerializationAliasOnly(pydantic.BaseModel):
    renamed: str = pydantic.Field(serialization_alias="outgoing_key")


class _AliasChoicesOnly(pydantic.BaseModel):
    chosen: str = pydantic.Field(validation_alias=pydantic.AliasChoices("primary", "secondary"))


class _AliasGenerator(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(alias_generator=to_camel)
    start_time: str
    end_time: str | None = None


class _Mixed(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(populate_by_name=True)
    plain: str
    snake_case: str = pydantic.Field(alias="camelCase")
    resolved: str = pydantic.Field(validation_alias="incoming_key")
    renamed: str = pydantic.Field(serialization_alias="outgoing_key")
    chosen: str = pydantic.Field(validation_alias=pydantic.AliasChoices("primary", "secondary"))
    cursor_ts: str | None = pydantic.Field(default=None, alias="_cursor_ts")


_NAME_CASES = [
    pytest.param(_AttributeOnly, {"a", "b"}, id="attribute-only"),
    pytest.param(_AliasOnly, {"camelCase"}, id="alias-only"),
    pytest.param(_AliasPopulateByName, {"camelCase", "other"}, id="alias-populate-by-name"),
    pytest.param(_ValidationAliasOnly, {"resolved"}, id="validation-alias-only"),
    pytest.param(_SerializationAliasOnly, {"renamed"}, id="serialization-alias-only"),
    pytest.param(_AliasChoicesOnly, {"chosen"}, id="alias-choices"),
    pytest.param(_AliasGenerator, {"startTime", "endTime"}, id="alias-generator"),
    pytest.param(
        _Mixed,
        {"plain", "camelCase", "resolved", "renamed", "chosen", "_cursor_ts"},
        id="mixed",
    ),
]

_PARITY_MODELS = [pytest.param(case.values[0], id=case.id) for case in _NAME_CASES]


class TestDestinationFieldNames:
    """`destination_field_names`: one name per field — the alias if set, else the attribute name.

    Only `alias` names the written column. `validation_alias` and
    `serialization_alias` change input parsing and output dumping, not the
    column dlt creates, so the attribute name stays the column name.
    """

    @pytest.mark.parametrize(("model", "expected"), _NAME_CASES)
    def test_one_name_per_field(self, model, expected):
        assert pydantic_fields_mod.destination_field_names(model) == expected

    @pytest.mark.parametrize("model", _PARITY_MODELS)
    def test_matches_dlt_schema_columns(self, model):
        """The rule must equal the column names dlt derives from the same model.

        Runs against the installed dlt, so a dlt release that changes its
        naming rule fails here instead of producing false drift findings.
        """
        assert pydantic_fields_mod.destination_field_names(model) == set(pydantic_to_table_schema_columns(model))

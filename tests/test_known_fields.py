"""Tests for the shared alias-safe Pydantic-model column-name utilities.

Covers Pydantic v2 alias forms plus the seen-null-first mitigation
(`drop_unknown_nulls`) that sources wire into their `add_map`.
"""

import pydantic

from dlt_ops import drop_unknown_nulls, extract_model_column_names


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

"""Compatibility of the public ``ColumnInfo`` value type for third-party adapters."""

import attrs

from dlt_ops.destinations import ColumnInfo


def test_subclass_may_add_a_field_without_a_default():
    @attrs.frozen
    class NullableColumnInfo(ColumnInfo):
        nullable: bool

    column = NullableColumnInfo("id", "BIGINT", True)

    assert (column.name, column.data_type, column.nullable) == ("id", "BIGINT", True)
    assert column.is_partition_column is False


def test_positional_construction_keeps_its_meaning_in_a_subclass_with_defaults():
    @attrs.frozen
    class CommentedColumnInfo(ColumnInfo):
        comment: str = ""

    column = CommentedColumnInfo("id", "BIGINT", "primary key")

    assert column.comment == "primary key"
    assert column.is_partition_column is False

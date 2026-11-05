"""Tests for helpers/schema.py — column schema derivation from DLO metadata."""

import pytest

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.schema import (
    CONTROL_COLUMNS,
    build_schema,
    normalize_column_name,
    to_snake_case,
)


class TestToSnakeCase:
    def test_camel_case(self) -> None:
        assert to_snake_case("SystemModstamp") == "system_modstamp"

    def test_pascal_case(self) -> None:
        assert to_snake_case("FirstName") == "first_name"

    def test_all_caps_acronym(self) -> None:
        assert to_snake_case("CPFNumber") == "cp_f_number"

    def test_already_lower(self) -> None:
        assert to_snake_case("already_lower") == "already_lower"


class TestNormalizeColumnName:
    def test_strips_ssot_prefix(self) -> None:
        assert normalize_column_name("ssot__Name__dll") == "name"

    def test_strips_dll_suffix(self) -> None:
        assert normalize_column_name("WorkOrder_Home__dll") == "work_order_home"

    def test_strips_custom_c_suffix_for_custom_field(self) -> None:
        # Custom field: original name ends in __c, so the residual _c is stripped.
        assert normalize_column_name("CPF_c__c") == "cpf"

    def test_does_not_strip_c_for_standard_field(self) -> None:
        # Standard field ending in _c (not __c): residual _c must NOT be stripped.
        result = normalize_column_name("billing_arc")
        assert result.endswith("_arc") or result == "billing_arc"

    def test_applies_rename_map(self) -> None:
        result = normalize_column_name("SomeField__dll", rename={"some_field": "my_field"})
        assert result == "my_field"

    def test_no_rename_map(self) -> None:
        result = normalize_column_name("Status__c")
        assert result == "status"


class TestBuildSchema:
    SAMPLE_COLS = [
        {"name": "Id__c", "type": "Varchar"},
        {"name": "SystemModstamp__c", "type": "TimestampTZ"},
        {"name": "CreatedDate__c", "type": "TimestampTZ"},
        {"name": "Amount__c", "type": "Numeric"},
        {"name": "IsDeleted__c", "type": "Bool"},
    ]

    def test_produces_expected_column_count(self) -> None:
        schema = build_schema(self.SAMPLE_COLS)
        # 5 source columns + 3 control columns
        assert len(schema.columns) == 5 + 3

    def test_control_columns_at_end(self) -> None:
        schema = build_schema(self.SAMPLE_COLS)
        last_names = [name for name, _ in schema.columns[-3:]]
        assert last_names == list(CONTROL_COLUMNS)

    def test_timestamp_columns_tracked(self) -> None:
        schema = build_schema(self.SAMPLE_COLS)
        assert "system_modstamp" in schema.timestamp_columns
        assert "created_date" in schema.timestamp_columns

    def test_boolean_override(self) -> None:
        schema = build_schema(
            [{"name": "Flag__c", "type": "Varchar"}],
            boolean_overrides=["flag"],
        )
        flag_col = next((name, col_type) for name, col_type in schema.columns if name == "flag")
        assert flag_col[1] == "boolean"

    def test_raises_on_unknown_type(self) -> None:
        with pytest.raises(ValueError, match="Unmapped"):
            build_schema([{"name": "Weird__c", "type": "UnknownType"}])

    def test_raises_on_control_column_collision(self) -> None:
        with pytest.raises(ValueError, match="collides with a control column"):
            build_schema([{"name": "loaded_at__c", "type": "Varchar"}])

    def test_deduplicates_columns(self) -> None:
        # Two DLO names that normalise to the same final name.
        cols = [
            {"name": "Foo__c", "type": "Varchar"},
            {"name": "ssot__Foo__c", "type": "Varchar"},
        ]
        schema = build_schema(cols)
        names = [name for name, _ in schema.columns]
        assert names.count("foo") == 1

    def test_all_columns_matches_columns(self) -> None:
        schema = build_schema(self.SAMPLE_COLS)
        assert schema.all_columns == tuple(name for name, _ in schema.columns)

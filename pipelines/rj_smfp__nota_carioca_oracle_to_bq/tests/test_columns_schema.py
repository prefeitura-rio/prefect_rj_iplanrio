from decimal import Decimal

import pyarrow as pa
import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import (
    OracleColumn,
    bq_type,
    fetch_schema,
    fetch_type,
    output_type,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import (
    BqField,
    SchemaChangeError,
    assert_compatible,
    build_fields,
    diff_schemas,
    meta_default,
    parquet_schema,
)


def column(name: str, data_type: str, precision: int | None = None, scale: int | None = None) -> OracleColumn:
    return OracleColumn(name=name, data_type=data_type, precision=precision, scale=scale)


@pytest.mark.parametrize(
    ("oracle", "expected"),
    [
        (column("A", "NUMBER", 17, 2), "NUMERIC"),
        (column("A", "NUMBER", 20, 0), "NUMERIC"),
        (column("A", "VARCHAR2"), "STRING"),
        (column("A", "CHAR"), "STRING"),
        (column("A", "RAW"), "STRING"),
        (column("A", "DATE"), "STRING"),
    ],
)
def test_bq_type_follows_airbyte_contract(oracle: OracleColumn, expected: str) -> None:
    assert bq_type(oracle) == expected


def test_number_without_precision_is_refused() -> None:
    with pytest.raises(NotImplementedError, match="sem precisão"):
        bq_type(column("VALOR", "NUMBER"))


@pytest.mark.parametrize(
    "unsupported",
    [
        column("A", "FLOAT", 126),
        column("A", "TIMESTAMP(6)"),
        column("A", "CLOB"),
        column("A", "BLOB"),
        column("A", "NUMBER", 38, 0),
        column("A", "NUMBER", 20, 12),
        column("A", "NUMBER", 5, -2),
    ],
)
def test_unsupported_types_fail_loudly(unsupported: OracleColumn) -> None:
    with pytest.raises(NotImplementedError):
        bq_type(unsupported)


def test_fetch_types_keep_numbers_exact() -> None:
    assert fetch_type(column("TELEFONE", "NUMBER", 20, 0)) == pa.decimal128(20, 0)
    assert fetch_type(column("VALOR", "NUMBER", 17, 2)) == pa.decimal128(17, 2)
    assert fetch_type(column("PESSOA", "RAW")) == pa.binary()
    assert fetch_type(column("DATA", "DATE")) == pa.timestamp("us")
    assert output_type(column("DATA", "DATE")) == pa.string()
    assert output_type(column("PESSOA", "RAW")) == pa.string()
    assert fetch_schema((column("N", "NUMBER", 1, 0), column("T", "VARCHAR2"))).names == ["N", "T"]


def test_column_name_with_quote_cannot_be_used_in_sql() -> None:
    with pytest.raises(ValueError, match="citado"):
        _ = column('A"; DROP TABLE X; --', "VARCHAR2").quoted
    assert column("DPS", "VARCHAR2").quoted == '"DPS"'


def test_build_fields_adds_airbyte_columns_before_oracle_columns() -> None:
    fields = build_fields((column("DPS", "VARCHAR2"), column("VALOR", "NUMBER", 17, 2)), sync_id=875)

    assert [(f.name, f.field_type, f.mode) for f in fields] == [
        ("_airbyte_extracted_at", "TIMESTAMP", "REQUIRED"),
        ("_airbyte_meta", "JSON", "REQUIRED"),
        ("_airbyte_generation_id", "INTEGER", "NULLABLE"),
        ("DPS", "STRING", "NULLABLE"),
        ("VALOR", "NUMERIC", "NULLABLE"),
    ]
    assert fields[1].default == meta_default(875) == 'JSON \'{"changes":[],"sync_id":875}\''


def test_parquet_schema_omits_json_column_and_marks_required_columns_not_null() -> None:
    schema = parquet_schema((column("DPS", "VARCHAR2"),))

    assert schema.names == ["_airbyte_extracted_at", "_airbyte_generation_id", "DPS"]
    assert not schema.field("_airbyte_extracted_at").nullable
    assert schema.field("_airbyte_extracted_at").type == pa.timestamp("us", tz="UTC")
    assert schema.field("DPS").nullable


def test_decimal_20_0_survives_arrow_cast_exactly() -> None:
    biggest = Decimal("99999999999999999999")
    values = pa.array([biggest, None], pa.decimal128(20, 0))

    assert values.cast(output_type(column("TELEFONE", "NUMBER", 20, 0)), safe=True).to_pylist() == [biggest, None]


EXISTING = (BqField("A", "STRING"), BqField("B", "NUMERIC"))


def test_new_column_is_accepted() -> None:
    changes = diff_schemas(EXISTING, (*EXISTING, BqField("C", "STRING")))

    assert changes.added == ("C",)
    assert_compatible("T", changes)


def test_removed_column_is_refused() -> None:
    changes = diff_schemas(EXISTING, (BqField("A", "STRING"),))

    with pytest.raises(SchemaChangeError, match="Removidas: \\['B'\\]"):
        assert_compatible("T", changes)


def test_retired_airbyte_raw_id_may_disappear_from_the_destination() -> None:
    existing = (BqField("_airbyte_raw_id", "STRING", "REQUIRED"), *EXISTING)

    changes = diff_schemas(existing, EXISTING)

    assert changes.removed == ()
    assert_compatible("T", changes)


def test_other_removed_columns_are_still_refused_alongside_the_retired_one() -> None:
    existing = (BqField("_airbyte_raw_id", "STRING", "REQUIRED"), *EXISTING)

    changes = diff_schemas(existing, (BqField("A", "STRING"),))

    assert changes.removed == ("B",)
    with pytest.raises(SchemaChangeError, match="Removidas: \\['B'\\]"):
        assert_compatible("T", changes)


def test_changed_type_is_refused() -> None:
    changes = diff_schemas(EXISTING, (BqField("A", "NUMERIC"), BqField("B", "NUMERIC")))

    assert changes.changed == ("A: STRING → NUMERIC",)
    with pytest.raises(SchemaChangeError):
        assert_compatible("T", changes)


def test_column_order_and_mode_do_not_count_as_changes() -> None:
    reordered = (BqField("B", "NUMERIC", "REQUIRED"), BqField("A", "STRING"))

    changes = diff_schemas(EXISTING, reordered)

    assert (changes.added, changes.removed, changes.changed) == ((), (), ())

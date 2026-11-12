from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import CHECKSUM_COLUMNS, DEFAULT_TABLES
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.checksum import (
    ChecksumColumnError,
    ChecksumMismatchError,
    ColumnChecksum,
    assert_checksums_match,
    chunk_checksums,
    merge_checksums,
    parse_checksum_row,
    render_checksum_select,
    validate_checksum_columns,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.convert import to_output_table

MONEY = OracleColumn("VALOR", "NUMBER", 17, 2)
PHONE = OracleColumn("TELEFONE", "NUMBER", 20, 0)
COLUMNS = (MONEY, PHONE, OracleColumn("NOME", "VARCHAR2", None, None))
NAMES = ("VALOR", "TELEFONE")


def batch(money: list[str | None], phones: list[int | None]) -> pa.Table:
    return pa.table(
        {
            "VALOR": pa.array([None if v is None else Decimal(v) for v in money], pa.decimal128(17, 2)),
            "TELEFONE": pa.array([None if p is None else Decimal(p) for p in phones], pa.decimal128(20, 0)),
            "NOME": pa.array(["a"] * len(money), pa.string()),
        }
    )


def checksums_of(table: pa.Table) -> dict[str, ColumnChecksum]:
    return chunk_checksums(to_output_table(table, COLUMNS, datetime(2026, 1, 1, tzinfo=UTC)), NAMES)


def test_chunk_checksum_counts_non_nulls_and_sums_decimals_exactly_on_the_converted_table() -> None:
    result = checksums_of(batch(["0.10", "0.20", None], [11, None, 22]))

    assert result["VALOR"] == ColumnChecksum(count=2, total="0.30")
    assert result["TELEFONE"] == ColumnChecksum(count=2, total="33")


def test_all_null_column_counts_zero_and_sums_zero() -> None:
    result = checksums_of(batch([None, None], [None, None]))

    assert result["VALOR"].count == 0
    assert result["VALOR"].amount == 0


def test_merge_sums_beyond_28_digits_without_rounding() -> None:
    big = "9" * 20
    first = checksums_of(batch(["1.00"], [int(big)]))
    second = checksums_of(batch(["2.50", "0.01"], [int(big), 1]))

    merged = merge_checksums([first, second], NAMES)

    assert merged["TELEFONE"].amount == Decimal(big) * 2 + 1
    assert merged["TELEFONE"].count == 3
    assert merged["VALOR"].amount == Decimal("3.51")


def test_merge_of_no_chunks_is_zero_for_every_column() -> None:
    merged = merge_checksums([], NAMES)

    assert merged == {name: ColumnChecksum(0, "0") for name in NAMES}


def test_match_is_exact_and_ignores_decimal_formatting() -> None:
    extracted = {"VALOR": ColumnChecksum(2, "0.30")}

    assert_checksums_match("T", extracted, {"VALOR": ColumnChecksum(2, "0.3")})
    for wrong in (ColumnChecksum(2, "0.31"), ColumnChecksum(3, "0.30")):
        with pytest.raises(ChecksumMismatchError, match="não foi alterada"):
            assert_checksums_match("T", extracted, {"VALOR": wrong})


def test_query_row_without_values_means_zero_sum() -> None:
    row = {"c_VALOR": 0, "s_VALOR": None, "c_TELEFONE": 2, "s_TELEFONE": "33"}

    assert parse_checksum_row(row, NAMES) == {"VALOR": ColumnChecksum(0, "0"), "TELEFONE": ColumnChecksum(2, "33")}
    with pytest.raises(TypeError):
        parse_checksum_row({**row, "s_TELEFONE": 33}, NAMES)


def test_checksum_sql_quotes_every_column_and_scans_the_temp_table_once() -> None:
    sql = render_checksum_select("p", "d", "T__oracle_to_bq_tmp", NAMES)

    assert sql.count("FROM") == 1
    assert "COUNT(`VALOR`) AS c_VALOR" in sql
    assert "CAST(SUM(CAST(`TELEFONE` AS BIGNUMERIC)) AS STRING) AS s_TELEFONE" in sql
    assert "`p.d.T__oracle_to_bq_tmp`" in sql
    with pytest.raises(ValueError, match="inválido"):
        render_checksum_select("p", "d", "t", ("A`; DROP",))


def test_plan_time_check_requires_the_column_to_exist_and_be_number() -> None:
    validate_checksum_columns("T", COLUMNS, NAMES)
    with pytest.raises(ChecksumColumnError, match="não existe"):
        validate_checksum_columns("T", COLUMNS, ("OUTRA",))
    with pytest.raises(ChecksumColumnError, match="não NUMBER"):
        validate_checksum_columns("T", COLUMNS, ("NOME",))


def test_every_default_table_has_checksum_columns() -> None:
    assert set(CHECKSUM_COLUMNS) == set(DEFAULT_TABLES)
    assert all(len(names) == 2 for names in CHECKSUM_COLUMNS.values())  # noqa: PLR2004

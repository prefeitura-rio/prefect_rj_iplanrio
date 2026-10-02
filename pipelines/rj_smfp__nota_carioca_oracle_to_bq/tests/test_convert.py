# ruff: noqa: PLR2004, DTZ001
import base64
import random
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.convert import (
    base64_strings,
    iso_date_strings,
    to_output_table,
    uuid_strings,
)

EXTRACTED_AT = datetime(2026, 10, 1, 20, 38, 44, tzinfo=UTC)


def expected_base64(values: Sequence[bytes | None]) -> list[str | None]:
    return [None if value is None else base64.b64encode(value).decode() for value in values]


def test_raw16_matches_stdlib_base64_with_airbyte_example() -> None:
    raw = bytes.fromhex("44c8358692e86143ad18480d064bdeff")

    assert base64_strings(pa.array([raw], pa.binary())).to_pylist() == ["RMg1hpLoYUOtGEgNBkve/w=="]


@pytest.mark.parametrize("length", [1, 2, 3, 4, 16, 32, 33])
def test_uniform_length_with_nulls_matches_stdlib(length: int) -> None:
    rng = random.Random(length)
    values = [None if index % 4 == 0 else rng.randbytes(length) for index in range(40)]

    assert base64_strings(pa.array(values, pa.binary())).to_pylist() == expected_base64(values)


def test_variable_length_falls_back_and_matches_stdlib() -> None:
    rng = random.Random(7)
    values = [None, rng.randbytes(16), rng.randbytes(5), None, rng.randbytes(31)]

    assert base64_strings(pa.array(values, pa.binary())).to_pylist() == expected_base64(values)


def test_all_null_and_empty_input() -> None:
    assert base64_strings(pa.array([None, None], pa.binary())).to_pylist() == [None, None]
    assert base64_strings(pa.array([], pa.binary())).to_pylist() == []


def test_large_binary_and_sliced_arrays_are_handled() -> None:
    rng = random.Random(3)
    values = [rng.randbytes(16) for _ in range(10)]
    sliced = pa.array(values, pa.large_binary()).slice(3, 4)

    assert base64_strings(sliced).to_pylist() == expected_base64(values[3:7])


def test_dates_become_iso_strings_without_fraction_or_timezone() -> None:
    dates = pa.array([datetime(2025, 5, 15, 2, 44), datetime(2025, 10, 16, 0, 0), None], pa.timestamp("us"))

    assert iso_date_strings(dates).to_pylist() == ["2025-05-15T02:44:00", "2025-10-16T00:00:00", None]


def test_uuids_are_valid_version4_and_unique() -> None:
    values = uuid_strings(1000).to_pylist()

    parsed = [uuid.UUID(value) for value in values]
    assert {item.version for item in parsed} == {4}
    assert len(set(values)) == 1000
    assert all(len(value) == 36 for value in values)


def test_batch_becomes_airbyte_shaped_table() -> None:
    columns = (
        OracleColumn("PESSOA_NACIONAL", "RAW", None, None),
        OracleColumn("TELEFONE", "NUMBER", 20, 0),
        OracleColumn("VALOR", "NUMBER", 17, 2),
        OracleColumn("DATA", "DATE", None, None),
        OracleColumn("NOME", "VARCHAR2", None, None),
    )
    batch = pa.table(
        {
            "PESSOA_NACIONAL": pa.array([bytes(range(16)), None], pa.binary()),
            "TELEFONE": pa.array([Decimal("21999999999"), None], pa.decimal128(20, 0)),
            "VALOR": pa.array([Decimal("1234.50"), Decimal("0.07")], pa.decimal128(17, 2)),
            "DATA": pa.array([datetime(2025, 10, 16, 17, 59, 5), None], pa.timestamp("us")),
            "NOME": pa.array(["João", None], pa.string()),
        }
    )

    table = to_output_table(batch, columns, EXTRACTED_AT)

    assert table.column_names[:3] == ["_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_generation_id"]
    assert table["_airbyte_extracted_at"].to_pylist() == [EXTRACTED_AT, EXTRACTED_AT]
    assert table["_airbyte_generation_id"].to_pylist() == [1, 1]
    assert table["PESSOA_NACIONAL"].to_pylist() == [base64.b64encode(bytes(range(16))).decode(), None]
    assert table["TELEFONE"].to_pylist() == [Decimal("21999999999"), None]
    assert table["VALOR"].to_pylist() == [Decimal("1234.50"), Decimal("0.07")]
    assert table["DATA"].to_pylist() == ["2025-10-16T17:59:05", None]
    assert table["NOME"].to_pylist() == ["João", None]


def test_batch_with_unexpected_columns_is_rejected() -> None:
    columns = (OracleColumn("A", "VARCHAR2", None, None),)

    with pytest.raises(ValueError, match="diferem"):
        to_output_table(pa.table({"B": pa.array(["x"])}), columns, EXTRACTED_AT)

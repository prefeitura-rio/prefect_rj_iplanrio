from datetime import date
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest
from psycopg.sql import Identifier
from pyarrow import Table as ArrowTable
from pyarrow.parquet import read_table, write_table

from toolkit.chunks import NULL_CHUNK, Chunk, Rows
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.requests import Changes, Files
from toolkit.executor.sources.backends.files import FilesSource

DAY = Chunk("2024-03-01", date(2024, 3, 1), date(2024, 3, 2))
ROWS = [
    {"id": 1, "dia": date(2024, 3, 1), "status": "ok"},
    {"id": 2, "dia": date(2024, 3, 1), "status": "x"},
    {"id": 3, "dia": date(2024, 3, 2), "status": "ok"},
    {"id": 4, "dia": None, "status": "ok"},
]
ID = [{"source": Identifier("id"), "target": Identifier("id")}]


@pytest.fixture
def parquet(tmp_path) -> Files:
    """Write the sample rows to a Parquet file, and return the origin that reads it."""
    path = tmp_path / "rows.parquet"
    write_table(ArrowTable.from_pylist(ROWS), path)

    return Files(str(path), "parquet")


async def written(
    engine: DuckDBEngine, origin: Files, target: Path, rows: Rows
) -> list[int]:
    """Write a chunk of the origin, and return the ids of the file that it writes."""
    await engine.materialize_origin(origin, target, rows, ID)

    return sorted(cast("list[int]", read_table(target)["id"].to_pylist()))


@pytest.mark.parametrize(
    ("rows", "ids"),
    [
        pytest.param(Rows(), [1, 2, 3, 4], id="no_filter_and_no_chunk"),
        pytest.param(
            Rows(filters=["status <> 'x'"]), [1, 3, 4], id="a_filter_in_parentheses"
        ),
        pytest.param(
            Rows(filters=["id = 1 OR id = 4", "status = 'ok'"]),
            [1, 4],
            id="an_or_stays_inside_its_filter",
        ),
        pytest.param(Rows(column="dia", chunk=DAY), [1, 2], id="a_chunk_of_dates"),
        pytest.param(Rows(column="dia", chunk=NULL_CHUNK), [4], id="the_null_chunk"),
        pytest.param(
            Rows(filters=["status = 'ok'"], column="dia", chunk=DAY),
            [1],
            id="filters_and_chunk_together",
        ),
    ],
)
async def test_a_chunk_of_a_file_is_written_with_its_filters(
    duckdb_memory, parquet, tmp_path, rows, ids
):
    """Keep the rows that pass every filter and the chunk, as a Parquet file."""
    target = tmp_path / "out.parquet"

    assert await written(duckdb_memory, parquet, target, rows) == ids


async def test_no_columns_write_every_column(duckdb_memory, parquet, tmp_path):
    """Select `*` when the request lists no column."""
    target = tmp_path / "out.parquet"

    await duckdb_memory.materialize_origin(parquet, target, Rows())

    assert read_table(target).column_names == ["id", "dia", "status"]


async def test_an_empty_read_keeps_the_schema(duckdb_memory, parquet, tmp_path):
    """Write a file with the columns and no row, so a dump learns the schema."""
    target = tmp_path / "out.parquet"

    await duckdb_memory.materialize_origin(parquet, target, Rows(empty=True))
    table = read_table(target)

    assert (table.column_names, table.num_rows) == (["id", "dia", "status"], 0)


async def test_a_filter_with_a_second_statement_is_refused(
    duckdb_memory, parquet, tmp_path
):
    """Raise before DuckDB runs, so a `;` never reaches the source."""
    rows = Rows(filters=["id = 1; DROP TABLE t"])

    with pytest.raises(ValueError, match="one predicate"):
        await duckdb_memory.materialize_origin(parquet, tmp_path / "out.parquet", rows)

    assert not (tmp_path / "out.parquet").exists()


async def test_describe_gives_the_name_and_the_type_of_each_column(
    duckdb_memory, parquet
):
    """Read the columns of an origin without its rows."""
    assert await duckdb_memory.describe_origin(parquet) == {
        "id": "BIGINT",
        "dia": "DATE",
        "status": "VARCHAR",
    }


async def test_an_origin_that_duckdb_cannot_read_is_refused(duckdb_memory, tmp_path):
    """Raise `UnsupportedRequestError` for changes, which only CouchDB answers."""
    with pytest.raises(TypeError, match="Changes"):
        await duckdb_memory.materialize_origin(
            Changes("0"), tmp_path / "out.parquet", Rows()
        )


async def test_an_engine_reads_the_origin_of_its_source(parquet, tmp_path):
    """Describe, materialize and read the origin that the source names."""
    target = tmp_path / "out.parquet"

    async with DuckDBEngine(FilesSource(origin=parquet)) as engine:
        described = await engine.describe()
        await engine.materialize(target, Rows(filters=["id < 3"]), ID)
        [dataframe] = [dataframe async for dataframe in engine.read()]

    assert described == {"id": "BIGINT", "dia": "DATE", "status": "VARCHAR"}
    assert read_table(target)["id"].to_pylist() == [1, 2]
    assert dataframe.height == len(ROWS)


async def test_files_are_written_to_a_sheet_with_the_header_in_the_first_row():
    """Render the write of a sheet, with the spreadsheet and the sheet as literals."""
    connection = MagicMock()
    engine = DuckDBEngine()
    engine.connection = connection

    await engine.write_sheet(Files("rows.parquet", "parquet"), "id1", "Aba 1")
    sql = connection.execute.call_args.args[0]

    assert "read_parquet('rows.parquet'" in sql
    assert "TO 'id1' (FORMAT gsheet, SHEET 'Aba 1')" in sql

from pathlib import Path
from typing import TypedDict

import polars as pl
import pytest
from psycopg.sql import Identifier

from toolkit.executor.engines.duckdb import DuckDBEngine, Extension
from toolkit.executor.executor import Executor
from toolkit.executor.schema import SchemaError
from toolkit.types import Geometry


class Number(TypedDict):
    n: int


class Pair(TypedDict):
    a: int
    b: str


class WrongPair(TypedDict):
    a: int
    b: int


class Place(TypedDict):
    geom: Geometry


def write(directory: Path, name: str, sql: str) -> Path:
    (directory / f"{name}.sql").write_text(sql)
    return directory


@pytest.mark.parametrize(
    ("batch_size", "sizes"),
    [
        pytest.param(4, [4, 4, 2], id="in_batches"),
        pytest.param(50, [10], id="one_batch"),
    ],
)
async def test_a_query_is_read_whole_or_batch_by_batch(
    tmp_path: Path, duckdb_memory, batch_size, sizes
):
    templates = write(tmp_path, "numbers", "SELECT i AS n FROM range(10) r(i)\n")
    executor = Executor(duckdb_memory, templates)

    batches = [
        len(batch)
        async for batch in executor.query(
            "numbers", expect=Number, batch_size=batch_size
        )
    ]
    whole = await executor.query("numbers", expect=Number, batch_size=batch_size)

    assert batches == sizes
    assert whole.frame["n"].to_list() == list(range(10))
    assert whole.frame.schema == pl.Schema({"n": pl.Int64})


async def test_a_result_with_no_rows_keeps_the_declared_columns(
    tmp_path: Path, duckdb_memory
):
    templates = write(tmp_path, "none", "SELECT 1::BIGINT AS a, 'x' AS b WHERE false\n")

    dataset = await Executor(duckdb_memory, templates).query("none", expect=Pair)

    assert len(dataset) == 0
    assert dataset.frame.schema == pl.Schema({"a": pl.Int64, "b": pl.String})


async def test_rows_that_do_not_fit_the_schema_are_rejected(
    tmp_path: Path, duckdb_memory
):
    """Regression: the data-proxy executor skipped validation (unresolved TypeVar)."""
    templates = write(tmp_path, "pair", "SELECT 1::BIGINT AS a, 'x' AS b\n")

    with pytest.raises(SchemaError, match="b: String is not Int64"):
        await Executor(duckdb_memory, templates).query("pair", expect=WrongPair)


async def test_an_unsupported_schema_fails_before_anything_runs(
    fake_engine, tmp_path: Path
):
    class Bad(TypedDict):
        value: set[int]

    client = fake_engine()

    with pytest.raises(TypeError, match="value"):
        Executor(client, write(tmp_path, "q", "SELECT 1\n")).query("q", expect=Bad)

    assert client.statements == []


async def test_execute_renders_the_values_into_the_statement(
    tmp_path: Path, duckdb_memory
):
    templates = write(tmp_path, "make", "CREATE TABLE {{ table }} AS SELECT 1 AS n\n")

    await Executor(duckdb_memory, templates).execute("make", {"table": Identifier("t")})

    [frame] = [f async for f in duckdb_memory.query("SELECT n FROM t")]
    assert frame.rows() == [(1,)]


async def test_a_pipeline_template_overrides_a_toolkit_one_of_the_same_name(
    fake_engine, tmp_path: Path
):
    (tmp_path / "duckdb").mkdir()
    write(tmp_path / "duckdb", "load_extensions", "-- mine\n")
    client = fake_engine()

    await Executor(client, tmp_path).execute(
        "duckdb/load_extensions", {"extensions": []}
    )

    assert client.statements == ["-- mine\n"]


@pytest.mark.extensions
async def test_a_duckdb_geometry_is_read_with_its_crs(tmp_path: Path):
    templates = write(
        tmp_path, "place", "SELECT ST_Point(1, 2)::GEOMETRY('EPSG:4326') AS geom\n"
    )

    async with DuckDBEngine.connect([Extension("spatial")]) as engine:
        dataset = await Executor(engine, templates).query("place", expect=Place)

    dtype = str(dataset.frame.schema["geom"])
    assert dtype.startswith("Extension('geoarrow.wkb'")
    assert "EPSG" in dtype

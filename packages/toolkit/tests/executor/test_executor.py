from pathlib import Path
from typing import TypedDict

import duckdb
import polars as pl
import pytest
from psycopg.sql import Identifier, Literal

from toolkit.executor.engines.duckdb import DuckDBEngine, Extension
from toolkit.executor.executor import Executor
from toolkit.executor.schema import SchemaError
from toolkit.types import Geometry
from toolkit.utils import column_aliases


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
    """Write a template and return its directory."""
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
    """Read a result whole with `await`, or batch by batch with `async for`."""
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
    """Keep the declared columns in a result with no rows."""
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
    """Fail before the query runs if the schema has no Polars type."""

    class Bad(TypedDict):
        value: set[int]

    client = fake_engine()

    with pytest.raises(TypeError, match="value"):
        Executor(client, write(tmp_path, "q", "SELECT 1\n")).query("q", expect=Bad)

    assert client.statements == []


async def test_execute_renders_the_values_into_the_statement(
    tmp_path: Path, duckdb_memory
):
    """Render the values into the statement."""
    templates = write(tmp_path, "make", "CREATE TABLE {{ table }} AS SELECT 1 AS n\n")

    await Executor(duckdb_memory, templates).execute("make", {"table": Identifier("t")})

    [frame] = [f async for f in duckdb_memory.query("SELECT n FROM t")]
    assert frame.rows() == [(1,)]


async def test_a_pipeline_template_overrides_a_toolkit_one_of_the_same_name(
    fake_engine, tmp_path: Path
):
    """Prefer a pipeline template to a toolkit template of the same name."""
    (tmp_path / "duckdb").mkdir()
    write(tmp_path / "duckdb", "load_extensions", "-- mine\n")
    client = fake_engine()

    await Executor(client, tmp_path).execute(
        "duckdb/load_extensions", {"extensions": []}
    )

    assert client.statements == ["-- mine\n"]


@pytest.mark.extensions
async def test_a_duckdb_geometry_is_read_with_its_crs(tmp_path: Path):
    """Read a DuckDB geometry with its CRS."""
    templates = write(
        tmp_path, "place", "SELECT ST_Point(1, 2)::GEOMETRY('EPSG:4326') AS geom\n"
    )

    async with DuckDBEngine.connect([Extension("spatial")]) as engine:
        dataset = await Executor(engine, templates).query("place", expect=Place)

    dtype = str(dataset.frame.schema["geom"])
    assert dtype.startswith("Extension('geoarrow.wkb'")
    assert "EPSG" in dtype


async def test_a_csv_becomes_parquet_with_every_column_as_text(
    tmp_path: Path, duckdb_memory
):
    """Keep every column as text, and add no column from a Hive folder in the path."""
    csv = tmp_path / "ano=2024" / "in.csv"
    csv.parent.mkdir()
    csv.write_text("code,amount\n007,1.50\n010,2\n")
    parquet = tmp_path / "out.parquet"

    await Executor(duckdb_memory).execute(
        "duckdb/csv_to_parquet",
        {"source": Literal(str(csv)), "target": Literal(str(parquet))},
    )

    frame = pl.read_parquet(parquet)
    assert frame.schema == {"code": pl.String, "amount": pl.String}
    assert frame.rows() == [("007", "1.50"), ("010", "2")]


async def stand_in_for_postgres(engine: DuckDBEngine) -> None:
    """Make `postgres_query` return a small table, as a real database would."""
    await engine.execute(
        "CREATE TABLE stub AS SELECT 1 AS id, 'a' AS \"Nome Completo\", 2024 AS ano"
    )
    await engine.execute(
        "CREATE MACRO postgres_query(alias, query) AS TABLE SELECT * FROM stub"
    )


async def dump(engine: DuckDBEngine, target: Path, **changes) -> None:
    """Dump the stand-in to `target`, with the given changes to the values."""
    values = {
        "type": "postgres",
        "alias": Literal("src"),
        "secret": Literal("src_secret"),
        "query": Literal("SELECT 1"),
        "target": Literal(str(target)),
        "columns": False,
        "partition_by": [],
    }

    await Executor(engine).execute("duckdb/dump_to_bucket", values | changes)


async def test_a_dump_writes_parquet_files_with_unique_names(
    tmp_path: Path, duckdb_memory
):
    """Write the rows as `part-<uuid>` files, so a second dump adds files."""
    await stand_in_for_postgres(duckdb_memory)

    await dump(duckdb_memory, tmp_path / "out")
    await dump(duckdb_memory, tmp_path / "out")

    files = sorted((tmp_path / "out").glob("part-*.parquet"))
    assert [pl.read_parquet(file).rows() for file in files] == [[(1, "a", 2024)]] * 2


async def test_a_dump_renames_the_columns_and_partitions_by_the_new_name(
    tmp_path: Path, duckdb_memory
):
    """Rename the columns to slugs, and name the folders after a column."""
    await stand_in_for_postgres(duckdb_memory)

    await dump(
        duckdb_memory,
        tmp_path / "out",
        columns=column_aliases(["id", "Nome Completo", "ano"]),
        partition_by=[Identifier("ano")],
    )

    [file] = (tmp_path / "out").glob("ano=2024/part-*.parquet")
    assert pl.read_parquet(file).columns == ["id", "nome_completo"]


@pytest.mark.parametrize(
    ("columns", "separator", "lowercase", "changed", "names"),
    [
        pytest.param(
            None,
            "_",
            True,
            {"Código do Órgão": "codigo_do_orgao", "Nome": "nome"},
            ["codigo_do_orgao", "nome", "ok"],
            id="all_columns",
        ),
        pytest.param(
            ["Nome"],
            "_",
            True,
            {"Nome": "nome"},
            ["Código do Órgão", "nome", "ok"],
            id="only_the_given_columns",
        ),
        pytest.param(
            None,
            "-",
            False,
            {"Código do Órgão": "Codigo-do-Orgao"},
            ["Codigo-do-Orgao", "Nome", "ok"],
            id="the_separator_and_the_case_are_parameters",
        ),
        pytest.param(
            ["ok"],
            "_",
            True,
            {},
            ["Código do Órgão", "Nome", "ok"],
            id="nothing_to_rename",
        ),
    ],
)
async def test_normalize_columns_renames_the_columns_of_a_table(
    duckdb_memory, columns, separator, lowercase, changed, names
):
    """Rename all the columns or only the given ones, with the separator and the case."""
    await duckdb_memory.execute(
        'CREATE TABLE t ("Código do Órgão" INT, "Nome" INT, ok INT)'
    )
    executor = Executor(duckdb_memory)

    result = await executor.normalize_columns("t", columns, separator, lowercase)

    [frame] = [f async for f in duckdb_memory.query("SELECT * FROM t")]
    assert result == changed
    assert frame.columns == names


async def test_normalize_columns_never_reaches_an_attached_source(duckdb_memory):
    """Treat the table name as one identifier, so `src.t` is not the table of `src`."""
    await duckdb_memory.execute("ATTACH ':memory:' AS src")
    await duckdb_memory.execute('CREATE TABLE src.t ("A b" INT)')

    with pytest.raises(duckdb.CatalogException):
        await Executor(duckdb_memory).normalize_columns("src.t")

    [frame] = [f async for f in duckdb_memory.query("SELECT * FROM src.t")]
    assert frame.columns == ["A b"]

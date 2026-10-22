from pathlib import Path
from typing import TypedDict

import pytest
from duckdb import CatalogException
from polars import Int64, Schema, String, read_parquet
from psycopg.sql import Identifier, Literal

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.schema import Column, SchemaError, read_columns
from toolkit.executor.sources.duckdb import Extension
from toolkit.types import Geometry
from toolkit.utils import select_columns


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


@pytest.mark.parametrize(
    ("batch_size", "sizes"),
    [
        pytest.param(4, [4, 4, 2], id="in_batches"),
        pytest.param(50, [10], id="one_batch"),
    ],
)
async def test_a_query_is_read_whole_or_batch_by_batch(
    tmp_path: Path, duckdb_memory, batch_size, sizes, write
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
    assert whole.dataframe["n"].to_list() == list(range(10))
    assert whole.dataframe.schema == Schema({"n": Int64})


async def test_a_result_with_no_rows_keeps_the_declared_columns(
    tmp_path: Path, duckdb_memory, write
):
    """Keep the declared columns in a result with no rows."""
    templates = write(tmp_path, "none", "SELECT 1::BIGINT AS a, 'x' AS b WHERE false\n")

    dataset = await Executor(duckdb_memory, templates).query("none", expect=Pair)

    assert len(dataset) == 0
    assert dataset.dataframe.schema == Schema({"a": Int64, "b": String})


async def test_rows_that_do_not_fit_the_schema_are_rejected(
    tmp_path: Path, duckdb_memory, write
):
    """Regression: the data-proxy executor skipped validation (unresolved TypeVar)."""
    templates = write(tmp_path, "pair", "SELECT 1::BIGINT AS a, 'x' AS b\n")

    with pytest.raises(SchemaError, match="b: String is not Int64"):
        await Executor(duckdb_memory, templates).query("pair", expect=WrongPair)


async def test_an_unsupported_schema_fails_before_anything_runs(
    fake_engine, tmp_path: Path, write
):
    """Fail before the query runs if the schema has no Polars type."""

    class Bad(TypedDict):
        value: set[int]

    client = fake_engine()

    with pytest.raises(TypeError, match="value"):
        Executor(client, write(tmp_path, "q", "SELECT 1\n")).query("q", expect=Bad)

    assert client.statements == []


async def test_execute_renders_the_values_into_the_statement(
    tmp_path: Path, duckdb_memory, write
):
    """Render the values into the statement."""
    templates = write(tmp_path, "make", "CREATE TABLE {{ table }} AS SELECT 1 AS n\n")

    await Executor(duckdb_memory, templates).execute("make", {"table": Identifier("t")})

    [dataframe] = [f async for f in duckdb_memory.query("SELECT n FROM t")]
    assert dataframe.rows() == [(1,)]


async def test_a_pipeline_template_overrides_a_toolkit_one_of_the_same_name(
    fake_engine, tmp_path: Path, write
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
async def test_a_duckdb_geometry_is_read_with_its_crs(tmp_path: Path, write):
    """Read a DuckDB geometry with its CRS."""
    templates = write(
        tmp_path, "place", "SELECT ST_Point(1, 2)::GEOMETRY('EPSG:4326') AS geom\n"
    )

    async with DuckDBEngine(extensions=[Extension("spatial")]) as engine:
        dataset = await Executor(engine, templates).query("place", expect=Place)

    dtype = str(dataset.dataframe.schema["geom"])
    assert dtype.startswith("Extension('geoarrow.wkb'")
    assert "EPSG" in dtype


async def test_a_chunk_is_one_local_file_with_the_schema_even_when_it_is_empty(
    tmp_path: Path, duckdb_memory, stand_in_for_postgres
):
    """Keep the schema of an empty chunk, so Delta can remove its rows."""
    await stand_in_for_postgres(duckdb_memory)
    values = {
        "type": "postgres",
        "alias": Literal("src"),
        "secret": Literal("src_secret"),
        "table": Identifier("stub"),
        "columns": select_columns(
            {"id": "INTEGER", "Nome Completo": "VARCHAR", "ano": "INTEGER"},
            normalize_names=False,
        ),
    }
    executor = Executor(duckdb_memory)

    for name, predicates, rows in (
        ("some", [{"column": Identifier("id"), "op": "=", "value": Literal(1)}], 1),
        ("none", [{"column": Identifier("id"), "op": "=", "value": Literal(99)}], 0),
    ):
        target = tmp_path / f"{name}.parquet"
        await executor.execute(
            "duckdb/extract_chunk",
            {**values, "predicates": predicates, "target": Literal(str(target))},
        )

        dataframe = read_parquet(target)
        assert dataframe.height == rows
        assert dataframe.columns == ["id", "Nome Completo", "ano"]


@pytest.mark.parametrize(
    ("file_type", "name", "content", "rows"),
    [
        pytest.param(
            "csv",
            "in.csv",
            "code,amount\n007,1.50\n010,2\n",
            [("007", "1.50"), ("010", "2")],
            id="a_csv_keeps_every_column_as_text",
        ),
        pytest.param(
            "json",
            "in.json",
            '[{"code": "007", "amount": 1.5}]',
            [("007", 1.5)],
            id="a_json_keeps_its_types",
        ),
    ],
)
async def test_a_chunk_reads_local_files_in_a_folder_with_a_hive_name(
    tmp_path: Path, duckdb_memory, file_type, name, content, rows
):
    """Read the files, and add no column from a Hive folder in their path."""
    folder = tmp_path / "ano=2024"
    folder.mkdir()
    (folder / name).write_text(content)
    target = tmp_path / "out.parquet"

    await Executor(duckdb_memory).execute(
        "duckdb/extract_chunk",
        {
            "type": file_type,
            "alias": Literal("src"),
            "secret": Literal("src_secret"),
            "query": Literal(f"{tmp_path}/**/*.{file_type}"),
            "columns": select_columns(
                {"code": "VARCHAR", "amount": "VARCHAR"}, normalize_names=False
            ),
            "target": Literal(str(target)),
        },
    )

    dataframe = read_parquet(target)
    assert dataframe.columns == ["code", "amount"]
    assert dataframe.rows() == rows


async def test_a_chunk_cleans_and_transforms_the_values_with_the_functions(
    tmp_path: Path, duckdb_memory
):
    """Compute the values of the columns with the functions, inside the `COPY`."""
    await duckdb_memory.execute(
        "CREATE TABLE stub AS SELECT 'x' || chr(10) || 'y ' AS nome, "
        "'529.982.247-25' AS cpf, DATE '2024-03-05' AS dia"
    )
    target = tmp_path / "out.parquet"

    await Executor(duckdb_memory).execute(
        "duckdb/extract_chunk",
        {
            "type": "postgres",
            "alias": Literal("src"),
            "secret": Literal("src_secret"),
            "table": Identifier("stub"),
            "columns": select_columns(
                {"nome": "VARCHAR", "cpf": "VARCHAR", "dia": "DATE"},
                normalize_values=True,
                transforms={"cpf": "normalize_cpf", "dia": "ano_particao"},
            ),
            "target": Literal(str(target)),
        },
    )

    assert read_parquet(target).rows() == [("x y", "52998224725", 2024)]


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
    """Rename all the columns or only the given ones."""
    await duckdb_memory.execute(
        'CREATE TABLE t ("Código do Órgão" INT, "Nome" INT, ok INT)'
    )
    executor = Executor(duckdb_memory)

    result = await executor.normalize_columns("t", columns, separator, lowercase)

    [dataframe] = [f async for f in duckdb_memory.query("SELECT * FROM t")]
    assert result == changed
    assert dataframe.columns == names


async def test_normalize_columns_never_reaches_an_attached_source(duckdb_memory):
    """Treat the table name as one identifier, so `src.t` is not the table of `src`."""
    await duckdb_memory.execute("ATTACH ':memory:' AS src")
    await duckdb_memory.execute('CREATE TABLE src.t ("A b" INT)')

    with pytest.raises(CatalogException):
        await Executor(duckdb_memory).normalize_columns("src.t")

    [dataframe] = [f async for f in duckdb_memory.query("SELECT * FROM src.t")]
    assert dataframe.columns == ["A b"]


async def test_a_query_with_no_expect_yields_the_dataframes_unchecked(
    duckdb_memory, tmp_path: Path, write
):
    """Read a result whose columns the caller does not declare, batch by batch."""
    executor = Executor(
        duckdb_memory,
        templates=write(
            tmp_path, "numbers", "SELECT i, i::VARCHAR AS t FROM range(5) r(i)"
        ),
    )

    datasets = [dataset async for dataset in executor.query("numbers", batch_size=2)]

    assert [len(dataset) for dataset in datasets] == [2, 2, 1]
    assert datasets[0].dataframe.columns == ["i", "t"]


async def test_a_query_with_no_expect_counts_the_rows_like_any_other(
    duckdb_memory, tmp_path: Path, caplog, write
):
    """Log the template and the rows when the result ends."""
    executor = Executor(
        duckdb_memory,
        templates=write(tmp_path, "numbers", "SELECT i FROM range(3) r(i)"),
    )

    with caplog.at_level("INFO"):
        await executor.query("numbers")

    assert "Query finished: template=numbers rows=3" in caplog.text


async def test_the_columns_of_a_first_read_check_the_next_ones(
    duckdb_memory, tmp_path: Path, write
):
    """Keep the columns of the first read, so a column that changes later raises."""
    first = write(tmp_path, "first", "SELECT 1 AS id, 'a' AS name")
    changed = write(tmp_path, "changed", "SELECT 1 AS id, 2 AS name")
    columns = read_columns(
        (await Executor(duckdb_memory, templates=first).query("first")).dataframe
    )

    same = await Executor(duckdb_memory, templates=first).query("first", expect=columns)
    with pytest.raises(SchemaError, match="name"):
        await Executor(duckdb_memory, templates=changed).query(
            "changed", expect=columns
        )

    assert same.dataframe.columns == ["id", "name"]


async def test_a_polars_schema_checks_the_result_of_a_query(
    duckdb_memory, tmp_path, write
):
    """Check a query against a `polars.Schema`, whose columns have no nulls."""
    templates = write(tmp_path, "pair", "SELECT 1::BIGINT AS a, 'x' AS b\n")
    executor = Executor(duckdb_memory, templates)

    good = await executor.query("pair", expect=Schema({"a": Int64, "b": String}))
    assert good.dataframe.rows() == [(1, "x")]

    with pytest.raises(SchemaError, match="b: String is not Int64"):
        await executor.query("pair", expect=Schema({"a": Int64, "b": Int64}))


async def test_a_mixed_map_lets_one_column_hold_nulls(duckdb_memory, tmp_path, write):
    """Allow the nulls of the column that says so, and refuse them in the others."""
    templates = write(tmp_path, "nulls", "SELECT 1::BIGINT AS a, NULL::VARCHAR AS b\n")
    executor = Executor(duckdb_memory, templates)

    allowed = await executor.query(
        "nulls", expect={"a": Int64, "b": Column(String, nullable=True)}
    )
    assert allowed.dataframe.rows() == [(1, None)]

    with pytest.raises(SchemaError, match="b: 1 null values"):
        await executor.query("nulls", expect=Schema({"a": Int64, "b": String}))


class NumberRow(TypedDict):
    id: int
    nome: str | None


async def test_read_checks_every_batch_against_the_schema(numbers):
    """Yield checked datasets, for a map of Polars types or for a schema class."""
    async with numbers as executor:
        mixed = {"id": Int64, "nome": Column(String, nullable=True)}
        by_map = [batch async for batch in executor.read(expect=mixed, batch_size=2)]
        by_class = [
            batch async for batch in executor.read(expect=NumberRow, batch_size=2)
        ]

    assert [len(batch) for batch in by_map] == [2, 1]
    assert [len(batch) for batch in by_class] == [2, 1]


async def test_read_refuses_a_batch_that_does_not_fit(numbers):
    """Raise `SchemaError` for a null in a column with no nulls, or a wrong type."""
    async with numbers as executor:
        with pytest.raises(SchemaError, match="nome: 1 null values"):
            _ = [
                batch
                async for batch in executor.read(
                    expect=Schema({"id": Int64, "nome": String})
                )
            ]

        with pytest.raises(SchemaError, match="id: Int64 is not String"):
            _ = [
                batch
                async for batch in executor.read(
                    expect=Schema({"id": String, "nome": String})
                )
            ]


async def test_read_without_expect_yields_every_row_unchecked(numbers):
    """Check nothing when there is no `expect`."""
    async with numbers as executor:
        batches = [batch async for batch in executor.read()]

    assert sum(len(batch) for batch in batches) == 3

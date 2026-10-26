"""The same checks on every engine: the answers must be the same, or the difference is on the list."""

from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from parity_dataset import Order
from parity_helpers import DOCUMENTS, canonical, column, dia, expected, filters_for, stage
from parity_lab import available, build_conn, build_source, environment
from pyarrow.parquet import read_table
from pymongoarrow.api import Schema
from toolkit.chunks import NULL_CHUNK, Chunk, Rows
from toolkit.executor.engines.base import Engine
from toolkit.executor.requests import Aggregate, UnsupportedRequestError
from toolkit.executor.sources.backends.mongodb import MongoDBSource
from toolkit.types import Chunks

pytestmark = pytest.mark.parity


async def describe(engine_name: str) -> dict[str, str]:
    async with Engine.from_source(build_source(engine_name)) as engine:
        return await engine.describe()


async def test_the_engine_describes_the_columns_of_orders(engine) -> None:
    """Name the four columns of `orders`, in the case of the engine, and nothing odd."""
    described = await describe(engine)

    names = {name.lower() for name in described} - {"_id"}
    assert names == {"id", "nome", "dia", "valor"}


async def test_every_row_arrives_with_the_same_values(engine) -> None:
    """Return the 304 rows of the dataset, equal after the types are put in one form."""
    table = await stage(engine, Rows())

    assert canonical(table) == expected()


async def test_a_chunk_of_ids_keeps_exactly_its_rows(engine) -> None:
    """Keep the ids from 100 up to, and not including, 200."""
    rows = Rows(column=column(engine), chunk=Chunk("w", 100, 200))

    table = await stage(engine, rows)

    assert canonical(table) == expected(lambda row: 100 <= row["id"] < 200)


@pytest.mark.parametrize("kind", ["lt10", "ge5", "or12"])
async def test_a_filter_keeps_exactly_its_rows(engine, kind) -> None:
    """Apply one filter in the dialect of the engine, with an OR inside it."""
    keep = {
        "lt10": lambda row: row["id"] < 10,
        "ge5": lambda row: row["id"] >= 5,
        "or12": lambda row: row["id"] in (1, 2),
    }[kind]

    table = await stage(engine, Rows(filters=filters_for(engine, kind)))

    assert canonical(table) == expected(keep)


async def test_two_filters_are_joined_by_and(engine) -> None:
    """Keep the rows that pass both filters, which are the ids from 5 to 9."""
    filters = [*filters_for(engine, "lt10"), *filters_for(engine, "ge5")]

    table = await stage(engine, Rows(filters=filters))

    assert [row[0] for row in canonical(table)] == [5, 6, 7, 8, 9]


async def test_a_chunk_of_dates_keeps_exactly_its_rows(engine) -> None:
    """Keep February of 2024, with the chunk half open at the end."""
    if engine == "couchdb":
        pytest.skip("CouchDB chunks cut only the id")
    chunk = Chunk("2024-02-01", date(2024, 2, 1), date(2024, 3, 1))

    table = await stage(engine, Rows(column=dia(engine), chunk=chunk))

    def in_february(row: Order) -> bool:
        """Tell if a row has a date in February."""
        return isinstance(row["dia"], date) and date(2024, 2, 1) <= row["dia"] < date(2024, 3, 1)

    assert canonical(table) == expected(in_february)


async def test_the_null_chunk_keeps_the_rows_without_a_date(engine) -> None:
    """Keep the three rows whose date is NULL."""
    if engine == "couchdb":
        pytest.skip("CouchDB chunks cut only the id")
    table = await stage(engine, Rows(column=dia(engine), chunk=NULL_CHUNK))

    assert [row[0] for row in canonical(table)] == [301, 302, 303]


async def test_couchdb_cuts_only_its_ids() -> None:
    """Refuse a chunk over another column, because CouchDB chunks cut the id."""
    with pytest.raises(ValueError, match="cuts the id"):
        await stage("couchdb", Rows(column="dia", chunk=NULL_CHUNK))


async def test_an_empty_read_keeps_the_columns(engine) -> None:
    """Write a file with no row and the same columns as a full read."""
    full = await stage(engine, Rows())
    empty = await stage(engine, Rows(empty=True))

    assert empty.num_rows == 0
    assert empty.column_names == full.column_names


async def test_a_filter_that_is_not_one_predicate_is_refused(engine) -> None:
    """Raise `ValueError` for a filter that cannot be one predicate, before any read."""
    bad = "id = 1; DROP TABLE orders" if engine not in DOCUMENTS else "not a selector"

    with pytest.raises(ValueError, match=r"predicate|selector|JSON"):
        await stage(engine, Rows(filters=[bad]))


@pytest.mark.parametrize(
    ("width", "fits"),
    [pytest.param(100, True, id="a_number_of_ids"), pytest.param("month", False, id="a_date_width")],
)
async def test_the_chunk_column_check_follows_the_type_of_the_id(engine, width, fits) -> None:
    """Accept the integer id for a number of ids and refuse it for a date width."""
    described = await describe(engine)
    chunks = Chunks(column=column(engine), width=width, begin=0)

    async with Engine.from_source(build_source(engine)) as opened:
        if fits:
            opened.check_chunks(chunks, described)
        else:
            with pytest.raises(ValueError, match=r"column|width"):
                opened.check_chunks(chunks, described)


async def test_sql_runs_where_the_system_has_sql(engine) -> None:
    """Run a statement on the SQL engines, and refuse it on the document stores."""
    sql = "SELECT 1 FROM DUAL" if engine == "oracle" else "SELECT 1"

    async with Engine.from_source(build_source(engine)) as opened:
        if engine in DOCUMENTS:
            with pytest.raises(UnsupportedRequestError):
                await opened.execute(sql)
        else:
            await opened.execute(sql) if engine != "oracle" else None
            datadataframes = [dataframe async for dataframe in opened.query(sql)]
            assert datadataframes[0].height == 1


async def test_the_decimal_keeps_every_digit(engine) -> None:
    """Return 99999999.99 exactly, for the row with the big value."""
    table = await stage(engine, Rows(column=column(engine), chunk=Chunk("big", 3_000_000_000, 3_000_000_001)))

    assert [row[3] for row in canonical(table)] == [Decimal("99999999.99")]


async def test_read_returns_every_row_in_datadataframes_where_the_engine_reads_whole(engine) -> None:
    """Read the 304 rows as datadataframes of at most `batch_size`, and refuse on Oracle, which only writes chunks."""
    async with Engine.from_source(build_source(engine)) as opened:
        if engine == "oracle":
            with pytest.raises(UnsupportedRequestError):
                opened.read()
            return

        datadataframes = [dataframe async for dataframe in opened.read(batch_size=100)]

    assert sum(dataframe.height for dataframe in datadataframes) == 304
    if engine not in DOCUMENTS:
        assert max(dataframe.height for dataframe in datadataframes) <= 100


async def test_a_clone_is_a_new_engine_that_works(engine) -> None:
    """Open a clone of an engine, and get the same columns from it."""
    async with Engine.from_source(build_source(engine)) as opened:
        first = await opened.describe()
        async with opened.clone() as other:
            assert other is not opened
            assert await other.describe() == first


async def test_a_mongodb_pipeline_returns_the_rows_of_a_plain_read() -> None:
    """Project two fields with a pipeline, and get the same rows as a plain read gives."""
    if "mongodb" not in available():
        pytest.skip("MongoDB is not up")

    entry = environment()["mongodb"]
    source = MongoDBSource(
        build_conn("mongodb"),
        shape=Schema({"id": int, "nome": str}),
        auth_source=str(entry["auth_source"]),
        tls=False,
        origin=Aggregate('[{"$project": {"_id": 0, "id": 1, "nome": 1}}]', str(entry["table"])),
    )

    async with Engine.from_source(source) as engine:
        with TemporaryDirectory() as folder:
            target = Path(folder) / "out.parquet"
            await engine.materialize(target, Rows())
            projected = read_table(target)

    plain = await stage("mongodb", Rows())
    assert sorted(zip(projected["id"].to_pylist(), projected["nome"].to_pylist(), strict=True)) == sorted(
        zip(plain["id"].to_pylist(), plain["nome"].to_pylist(), strict=True)
    )

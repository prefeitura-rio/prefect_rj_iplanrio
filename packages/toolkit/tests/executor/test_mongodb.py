import sys
from datetime import UTC, date, datetime
from types import ModuleType, SimpleNamespace
from typing import cast

import pytest
from bson import Decimal128, ObjectId
from pyarrow import ExtensionArray, Table, array, binary
from pyarrow import string as arrow_string
from pyarrow.parquet import read_table
from pymongo.database import Database
from pymongo.errors import OperationFailure
from pymongoarrow.api import Schema
from pymongoarrow.types import Decimal128Type, ObjectIdType

from toolkit.chunks import NULL_CHUNK, Chunk, Rows
from toolkit.executor.engines import mongodb
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.engines.mongodb import MongoDBEngine
from toolkit.executor.requests import Aggregate, Query, UnsupportedRequestError
from toolkit.executor.requests import Table as TableOrigin
from toolkit.executor.sources.backends.mongodb import MongoDBSource
from toolkit.executor.sources.protocols import DBConn
from toolkit.executor.sources.registry import get_source
from toolkit.executor.types import Origin
from toolkit.types import Chunks

SCHEMA = Schema({"_id": ObjectIdType(), "nome": str, "valor": Decimal128Type()})
DAY = Chunk("2024-03-01", date(2024, 3, 1), date(2024, 3, 2))
CONN = DBConn("db.example", 27017, "app", "user", "p@ss/word")


def identifier(number: int) -> ObjectId:
    """Return the `ObjectId` of a number, so the ids sort as the numbers do."""
    return ObjectId(f"{number:024x}")


def page(numbers: list[int]) -> Table:
    """Return a page of documents as PyMongoArrow reads them: ids, names, values."""
    ids = [identifier(number).binary for number in numbers]
    values = [Decimal128(f"{number}.50").bid for number in numbers]

    return Table.from_arrays(
        [
            ExtensionArray.from_storage(ObjectIdType(), array(ids, binary(12))),
            array([f"nome {number}" for number in numbers], arrow_string()),
            ExtensionArray.from_storage(Decimal128Type(), array(values, binary(16))),
        ],
        names=["_id", "nome", "valor"],
    )


class FakeCollection:
    """A collection that the fake fetch ignores, except for its name."""

    def __init__(self, name: str) -> None:
        """Keep the name of the collection."""
        self.name = name


class FakeDatabase:
    """A database that gives a fake collection for each name."""

    def __getitem__(self, name: str) -> FakeCollection:
        """Return the fake collection of a name."""
        return FakeCollection(name)


class Fetcher:
    """A fetch that answers with the canned pages, and records each query."""

    def __init__(self, pages: list[list[int]]) -> None:
        """Keep the pages, from the first to the last."""
        self.pages = pages
        self.calls: list[tuple[str, dict[str, object], int]] = []

    def __call__(self, collection, query, schema, limit):
        """Record the call, and return the next page, or an empty one."""
        self.calls.append((collection.name, dict(query), limit))
        numbers = (
            self.pages[len(self.calls) - 1]
            if len(self.calls) <= len(self.pages)
            else []
        )

        return page(numbers)


class Aggregator:
    """An aggregation that answers with a canned table, and records each call."""

    def __init__(self, table: Table) -> None:
        """Keep the table that every call returns."""
        self.table = table
        self.calls: list[tuple[str, list[dict[str, object]]]] = []

    def __call__(self, collection, pipeline, schema):
        """Record the collection and the stages, and return the table."""
        self.calls.append((collection.name, [dict(stage) for stage in pipeline]))

        return self.table


def mongo_engine(
    duckdb,
    fetcher,
    batch_size=2,
    origin: Origin | None = None,
    shape: Schema = SCHEMA,
    aggregator: Aggregator | None = None,
) -> MongoDBEngine:
    """Return an open engine over the fake database and the canned pages."""
    source = MongoDBSource(
        CONN, shape, batch_size=batch_size, origin=origin or TableOrigin("c")
    )
    engine = (
        MongoDBEngine(source, fetcher, aggregator)
        if aggregator
        else MongoDBEngine(source, fetcher)
    )
    engine.database = cast(
        "Database[dict[str, object]]", cast("object", FakeDatabase())
    )
    engine.duckdb = duckdb

    return engine


@pytest.mark.parametrize(
    ("rows", "query"),
    [
        pytest.param(Rows(), {}, id="no_filter"),
        pytest.param(
            Rows(filters=['{"situacao": "ativo"}', '{"idade": {"$gte": 18}}']),
            {"$and": [{"situacao": "ativo"}, {"idade": {"$gte": 18}}]},
            id="filters_joined_by_and",
        ),
        pytest.param(
            Rows(column="dia", chunk=DAY),
            {
                "$and": [
                    {
                        "dia": {
                            "$gte": datetime(2024, 3, 1, tzinfo=UTC),
                            "$lt": datetime(2024, 3, 2, tzinfo=UTC),
                        }
                    }
                ]
            },
            id="a_chunk_of_dates_is_a_range_of_utc_midnights",
        ),
        pytest.param(
            Rows(column="id", chunk=Chunk("k", 10, 20)),
            {"$and": [{"id": {"$gte": 10, "$lt": 20}}]},
            id="a_chunk_of_numbers",
        ),
        pytest.param(
            Rows(column="dia", chunk=NULL_CHUNK),
            {"$and": [{"dia": None}]},
            id="the_null_chunk_keeps_the_documents_with_no_value",
        ),
    ],
)
async def test_the_first_page_is_asked_with_the_filters_and_the_chunk(
    duckdb_memory, rows, query, collect_pages
) -> None:
    """Join the filters and the chunk in one query, in the collection of the table."""
    fetcher = Fetcher([[1]])

    await collect_pages(
        mongo_engine(duckdb_memory, fetcher), TableOrigin("pessoas"), rows
    )

    assert fetcher.calls == [("pessoas", query, 2)]


async def test_the_pages_follow_the_ids_until_a_short_page(
    duckdb_memory, collect_pages
) -> None:
    """Ask for the ids after the last one of the page before, until a short page."""
    fetcher = Fetcher([[1, 2], [3, 4], [5]])

    tables = await collect_pages(
        mongo_engine(duckdb_memory, fetcher), TableOrigin("c"), Rows()
    )

    assert [table.num_rows for table in tables] == [2, 2, 1]
    assert [call[1] for call in fetcher.calls] == [
        {},
        {"_id": {"$gt": identifier(2)}},
        {"_id": {"$gt": identifier(4)}},
    ]


async def test_the_pages_follow_the_ids_inside_the_query(
    duckdb_memory, collect_pages
) -> None:
    """Join the id bound with the filters, so a page never leaves the chunk."""
    fetcher = Fetcher([[1, 2], [3]])
    rows = Rows(filters=['{"a": 1}'])

    await collect_pages(mongo_engine(duckdb_memory, fetcher), TableOrigin("c"), rows)

    assert fetcher.calls[1][1] == {
        "$and": [{"$and": [{"a": 1}]}, {"_id": {"$gt": identifier(2)}}]
    }


async def test_a_selector_of_a_query_is_a_filter_of_its_collection(
    duckdb_memory, collect_pages
) -> None:
    """Read the collection that the query names, with its selector as a condition."""
    fetcher = Fetcher([[1]])
    origin = Query('{"situacao": "ativo"}', "mongodb", "pessoas")

    await collect_pages(mongo_engine(duckdb_memory, fetcher), origin, Rows())

    assert fetcher.calls == [("pessoas", {"$and": [{"situacao": "ativo"}]}, 2)]


async def test_a_query_without_a_collection_is_refused(
    duckdb_memory, collect_pages
) -> None:
    """Raise before a read, because a selector needs a collection to run on."""
    engine = mongo_engine(duckdb_memory, Fetcher([]))

    with pytest.raises(ValueError, match="collection"):
        await collect_pages(engine, Query("{}", "mongodb"), Rows())


async def test_a_filter_that_is_not_a_selector_is_refused(
    duckdb_memory, collect_pages
) -> None:
    """Raise for a filter that is not a JSON object."""
    engine = mongo_engine(duckdb_memory, Fetcher([[1]]))

    with pytest.raises(ValueError, match="not a JSON selector"):
        await collect_pages(engine, TableOrigin("c"), Rows(filters=["not json"]))


async def test_an_empty_read_asks_for_nothing(duckdb_memory, collect_pages) -> None:
    """Read no page, so a dump learns the schema without a query."""
    fetcher = Fetcher([[1]])

    tables = await collect_pages(
        mongo_engine(duckdb_memory, fetcher), TableOrigin("c"), Rows(empty=True)
    )

    assert (tables, fetcher.calls) == ([], [])


async def test_the_bson_types_reach_the_file_as_text(duckdb_memory, tmp_path) -> None:
    """Write an `ObjectId` as its hex text and a `Decimal128` as its exact text."""
    engine = mongo_engine(duckdb_memory, Fetcher([[1, 2]]), batch_size=5)
    target = tmp_path / "out.parquet"

    await engine.materialize(target, Rows())
    table = read_table(target)

    assert table["_id"].to_pylist() == [str(identifier(1)), str(identifier(2))]
    assert table["valor"].to_pylist() == ["1.50", "2.50"]
    assert table["nome"].to_pylist() == ["nome 1", "nome 2"]


async def test_an_empty_collection_keeps_the_schema(duckdb_memory, tmp_path) -> None:
    """Write a file with the columns and no row when there is no document."""
    engine = mongo_engine(duckdb_memory, Fetcher([]))
    target = tmp_path / "out.parquet"

    await engine.materialize(target, Rows())
    table = read_table(target)

    assert (table.column_names, table.num_rows) == (["_id", "nome", "valor"], 0)
    assert str(table.schema.field("_id").type) == "string"


async def test_a_dump_describes_the_columns_of_the_schema(duckdb_memory) -> None:
    """Describe the schema through an empty file, with the types that DuckDB reads."""
    engine = mongo_engine(duckdb_memory, Fetcher([]))

    assert await engine.describe() == {
        "_id": "VARCHAR",
        "nome": "VARCHAR",
        "valor": "VARCHAR",
    }


@pytest.fixture
def shapes(monkeypatch):
    """Put a module with a schema, one without `_id` and a value that is no schema."""
    module = ModuleType("mongo_shapes_of_the_test")
    setattr(module, "GOOD", SCHEMA)  # noqa: B010
    setattr(module, "NO_ID", Schema({"nome": str}))  # noqa: B010
    setattr(module, "NOT_A_SCHEMA", "text")  # noqa: B010
    monkeypatch.setitem(sys.modules, "mongo_shapes_of_the_test", module)

    return module


def test_the_options_choose_the_schema_the_page_size_and_the_login(shapes) -> None:
    """Take the schema by its path, and keep the other options as they come."""
    options = {
        "shape": "mongo_shapes_of_the_test:GOOD",
        "batch_size": 500,
        "auth_source": "admin",
        "tls": True,
    }

    source = get_source("mongodb")(conn=CONN, **options)

    assert isinstance(source, MongoDBSource)
    assert (source.shape, source.batch_size) == (SCHEMA, 500)
    assert (source.auth_source, source.tls) == ("admin", True)


@pytest.mark.parametrize(
    ("options", "message"),
    [
        pytest.param({}, "shape", id="no_shape"),
        pytest.param({"shape": "no_colon"}, "module:name", id="a_path_without_a_colon"),
        pytest.param(
            {"shape": "mongo_missing_module:X"},
            "cannot be imported",
            id="a_module_that_does_not_exist",
        ),
        pytest.param(
            {"shape": "mongo_shapes_of_the_test:NOT_A_SCHEMA"},
            "not a PyMongoArrow Schema",
            id="a_name_of_another_type",
        ),
        pytest.param(
            {"shape": "mongo_shapes_of_the_test:GOOD", "batch_size": 0},
            "batch_size",
            id="a_batch_size_of_zero",
        ),
        pytest.param(
            {"shape": "mongo_shapes_of_the_test:GOOD", "nope": 1},
            "nope",
            id="an_option_that_does_not_exist",
        ),
    ],
)
def test_options_that_do_not_fit_are_an_error(shapes, options, message) -> None:
    """Raise for a missing or bad schema, and for an option that does not fit."""
    with pytest.raises(ValueError, match=message):
        get_source("mongodb")(conn=CONN, **options)


def test_the_connection_string_quotes_the_login_and_sets_the_options(shapes) -> None:
    """Quote the password, and send the authentication database and the TLS flag."""
    source = MongoDBSource(CONN, SCHEMA, auth_source="admin", tls=False)

    assert source.uri == (
        "mongodb://user:p%40ss%2Fword@db.example:27017/?authSource=admin&tls=false"
    )


def test_a_source_that_needs_a_connection_names_it() -> None:
    """Name the missing `conn` in the error when it has no connection."""
    with pytest.raises(ValueError, match="conn"):
        get_source("mongodb")()


def test_mongodb_reads_two_chunks_at_a_time_unless_the_options_say_otherwise() -> None:
    """Default `concurrency` to 2, and take the number that the options give."""
    assert MongoDBEngine(MongoDBSource(CONN, SCHEMA)).concurrency == 2
    assert MongoDBEngine(MongoDBSource(CONN, SCHEMA, concurrency=5)).concurrency == 5


async def test_the_mongodb_engine_refuses_sql(duckdb_memory: DuckDBEngine) -> None:
    """Raise `UnsupportedRequestError`, naming the engine, for `execute` and `query`."""
    engine = mongo_engine(duckdb_memory, Fetcher([]))

    with pytest.raises(UnsupportedRequestError, match="MongoDBEngine"):
        await engine.execute("SELECT 1")

    with pytest.raises(UnsupportedRequestError, match="MongoDBEngine"):
        engine.query("SELECT 1")


async def test_a_server_that_refuses_the_ping_fails_when_the_engine_opens(
    monkeypatch,
) -> None:
    """Ping on open, so a wrong host or login fails before the first chunk."""
    commands: list[str] = []

    class RefusingClient:
        """A client whose server refuses the ping."""

        def __init__(self, uri: str) -> None:
            """Keep the uri and give the admin database a command that refuses."""
            self.uri = uri
            self.admin = SimpleNamespace(command=self.refuse)

        @classmethod
        def __class_getitem__(cls, item: object) -> type[RefusingClient]:
            """Accept the type argument of the real client."""
            return cls

        def refuse(self, command: str) -> None:
            """Record the command and refuse it, as a server does with a wrong login."""
            commands.append(command)
            raise OperationFailure("Authentication failed.")

        def close(self) -> None:
            """Close nothing."""

    monkeypatch.setattr(mongodb, "MongoClient", RefusingClient)

    with pytest.raises(OperationFailure, match="Authentication failed"):
        async with MongoDBEngine(MongoDBSource(CONN, SCHEMA, origin=TableOrigin("c"))):
            pass

    assert commands == ["ping"]


PROJECTED = Schema({"id": str, "criado": str})
STAGES = '[{"$project": {"id": {"$toString": "$_id"}, "criado": 1}}]'


def projected(count: int) -> Table:
    """Return the result of the stages above: text columns, and no `_id`."""
    return Table.from_pydict(
        {
            "id": [f"id{number}" for number in range(count)],
            "criado": ["2024-03-01"] * count,
        }
    )


async def test_an_aggregation_runs_its_stages_after_a_match_of_the_filters_and_the_chunk(
    duckdb_memory, collect_pages
) -> None:
    """Put the filters and the chunk in a leading `$match`, then the stages."""
    aggregator = Aggregator(projected(3))
    origin = Aggregate(STAGES, "corridas")
    engine = mongo_engine(
        duckdb_memory,
        Fetcher([]),
        origin=origin,
        shape=PROJECTED,
        aggregator=aggregator,
    )
    rows = Rows(filters=['{"ativo": true}'], column="criado", chunk=DAY)

    tables = await collect_pages(engine, origin, rows)

    match = {
        "$and": [
            {"ativo": True},
            {
                "criado": {
                    "$gte": datetime(2024, 3, 1, tzinfo=UTC),
                    "$lt": datetime(2024, 3, 2, tzinfo=UTC),
                }
            },
        ]
    }
    assert [table.num_rows for table in tables] == [3]
    assert aggregator.calls == [
        (
            "corridas",
            [
                {"$match": match},
                {"$project": {"id": {"$toString": "$_id"}, "criado": 1}},
            ],
        )
    ]


async def test_an_aggregation_without_filters_has_no_match(
    duckdb_memory, collect_pages
) -> None:
    """Send only the stages when nothing narrows the read."""
    aggregator = Aggregator(projected(1))
    origin = Aggregate(STAGES, "corridas")
    engine = mongo_engine(
        duckdb_memory,
        Fetcher([]),
        origin=origin,
        shape=PROJECTED,
        aggregator=aggregator,
    )

    await collect_pages(engine, origin, Rows())

    assert [stage.keys() for stage in aggregator.calls[0][1]] == [{"$project"}]


async def test_an_aggregation_does_not_need_the_id_in_the_shape(
    duckdb_memory, tmp_path
) -> None:
    """Write the projected columns, though the shape has no `_id`."""
    origin = Aggregate(STAGES, "corridas")
    engine = mongo_engine(
        duckdb_memory,
        Fetcher([]),
        origin=origin,
        shape=PROJECTED,
        aggregator=Aggregator(projected(2)),
    )
    target = tmp_path / "out.parquet"

    await engine.materialize(target, Rows())

    assert read_table(target)["id"].to_pylist() == ["id0", "id1"]


async def test_an_empty_aggregation_describes_the_shape_without_running(
    duckdb_memory,
) -> None:
    """Describe the columns of the shape and run nothing."""
    aggregator = Aggregator(projected(1))
    engine = mongo_engine(
        duckdb_memory,
        Fetcher([]),
        origin=Aggregate(STAGES, "corridas"),
        shape=PROJECTED,
        aggregator=aggregator,
    )

    assert await engine.describe() == {"id": "VARCHAR", "criado": "VARCHAR"}
    assert aggregator.calls == []


def test_an_aggregation_may_cut_chunks_by_a_field_that_the_result_holds_as_text(
    duckdb_memory,
) -> None:
    """Accept a text column for a date chunk: the stored field is a date."""
    engine = mongo_engine(
        duckdb_memory,
        Fetcher([]),
        origin=Aggregate(STAGES, "corridas"),
        shape=PROJECTED,
    )
    chunks = Chunks(column="criado", width="day", begin="2024-01-01")

    engine.check_chunks(chunks, {"id": "VARCHAR", "criado": "VARCHAR"})


def test_a_read_of_a_collection_still_checks_the_type_of_the_chunk_column(
    duckdb_memory,
) -> None:
    """Refuse a text column for a date chunk when the read is not an aggregation."""
    engine = mongo_engine(duckdb_memory, Fetcher([]))
    chunks = Chunks(column="nome", width="day", begin="2024-01-01")

    with pytest.raises(ValueError, match="need a date or a timestamp"):
        engine.check_chunks(chunks, {"_id": "VARCHAR", "nome": "VARCHAR"})


async def test_a_paged_read_needs_the_id_in_the_shape(
    duckdb_memory, collect_pages
) -> None:
    """Raise before a query if the shape has no `_id` to page by."""
    engine = mongo_engine(duckdb_memory, Fetcher([[1]]), shape=Schema({"nome": str}))

    with pytest.raises(ValueError, match="needs the field _id"):
        await collect_pages(engine, TableOrigin("c"), Rows())

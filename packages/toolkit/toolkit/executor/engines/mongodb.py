"""The MongoDB engine: it reads a collection in pages and writes chunks as Parquet."""

from asyncio import to_thread
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time
from json import loads
from pathlib import Path
from typing import ClassVar, Self, override

from polars import DataFrame
from pyarrow import Schema as ArrowSchema
from pyarrow import Table, array, string
from pyarrow.parquet import ParquetWriter
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongoarrow.api import Schema, aggregate_arrow_all, find_arrow_all
from pymongoarrow.types import Decimal128Type, ObjectIdType

from ...chunks import NULL_CHUNK, Chunk, Rows
from ...types import Chunks, Columns
from ...utils import filter_selectors
from ..constants import DEFAULT_BATCH_SIZE, MONGO_ID_FIELD
from ..requests import (
    Aggregate,
    Query,
    UnsupportedRequestError,
)
from ..requests import Table as TableOrigin
from ..sources.backends.mongodb import MongoDBSource
from ..sources.protocols import Source
from ..types import Fetch, Origin, Run
from .base import Engine, require_origin
from .duckdb import DuckDBEngine, describe_extracted, materialize_extracted


def fetch_page(
    collection: Collection[dict[str, object]],
    query: Mapping[str, object],
    schema: Schema,
    limit: int,
) -> Table:
    """Read one page of a collection with PyMongoArrow, sorted by `_id`."""
    return find_arrow_all(
        collection, query, schema=schema, sort=[(MONGO_ID_FIELD, 1)], limit=limit
    )


def run_aggregate(
    collection: Collection[dict[str, object]],
    pipeline: Sequence[Mapping[str, object]],
    schema: Schema,
) -> Table:
    """Run an aggregation with PyMongoArrow and return the whole result."""
    return aggregate_arrow_all(collection, list(pipeline), schema=schema)


def make_portable(table: Table) -> Table:
    """Return the table with the BSON types as types that Parquet and Delta hold.

    An `ObjectId` becomes its hex text and a `Decimal128` becomes its exact text.
    """
    for index, schema_field in enumerate(table.schema):
        values = table.column(index).to_pylist()

        match schema_field.type:
            case ObjectIdType():
                texts = [str(value) for value in values]
            case Decimal128Type():
                texts = [
                    None if value is None else str(value.to_decimal())
                    for value in values
                ]
            case _:
                continue

        table = table.set_column(
            index, schema_field.with_type(string()), array(texts, string())
        )

    return table


def convert_bound(value: date | int) -> datetime | int:
    """Return a bound of a chunk as a value that BSON holds: a date is midnight UTC."""
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=UTC)

    return value


def build_chunk_selector(column: str, chunk: Chunk) -> dict[str, object]:
    """Return the query that keeps the documents of a chunk, over a field.

    The null chunk keeps the documents whose field is null or missing.
    """
    if chunk == NULL_CHUNK:
        return {column: None}

    return {
        column: {"$gte": convert_bound(chunk.start), "$lt": convert_bound(chunk.stop)}
    }


def read_collection(origin: Origin) -> tuple[str, dict[str, object]]:
    """Return the collection of an origin and the selector of its query.

    Raise `ValueError` for a query with no collection.
    """
    match origin:
        case TableOrigin(name):
            return name, {}
        case Query(text, _, table):
            if table is None:
                raise ValueError("A MongoDB query needs the collection: set the table")
            return table, loads(text)
        case Aggregate(_, table):
            return table, {}
        case _:
            raise UnsupportedRequestError("MongoDBEngine", origin)


def combine_conditions(
    selector: Mapping[str, object], rows: Rows
) -> Mapping[str, object]:
    """Join the selector, the filters and the chunk with AND, or return no query."""
    conditions: list[Mapping[str, object]] = [
        *([selector] if selector else []),
        *filter_selectors(rows.filters),
        *(
            [build_chunk_selector(rows.column, rows.chunk)]
            if rows.column is not None and rows.chunk is not None
            else []
        ),
    ]

    return {"$and": conditions} if conditions else {}


def page_after(query: Mapping[str, object], last: object) -> Mapping[str, object]:
    """Return the query of the page after the last `_id` read, or of the first."""
    if last is None:
        return query

    after = {MONGO_ID_FIELD: {"$gt": last}}

    return {"$and": [query, after]} if query else after


@dataclass
class MongoDBEngine(Engine):
    """An open MongoDB database that reads a collection in pages of `_id`.

    The filters are query documents joined with `$and`. A chunk is a range of a field,
    and the null chunk keeps the documents with no value. The `shape` of the source is
    a PyMongoArrow schema with the field `_id`, and its `batch_size` is the documents of
    one page. The pages go to a raw Parquet file, and a DuckDB engine of its own writes
    the final file from it. `fetch` reads one page, and a test replaces it. An
    aggregation reads a chunk in one result, with no pages, because its stages can
    change `_id`, and `aggregate` runs it. `batch_size` does not apply to it.
    """

    source_type: ClassVar[type[Source]] = MongoDBSource

    source: MongoDBSource
    fetch: Fetch = field(default=fetch_page)
    aggregate: Run = field(default=run_aggregate)
    duckdb: DuckDBEngine = field(init=False, repr=False)
    database: Database[dict[str, object]] = field(init=False, repr=False)

    @property
    @override
    def concurrency(self) -> int:
        """Return the chunks of the source that are read at the same time."""
        return self.source.concurrency

    @property
    def schema(self) -> Schema:
        """Return the PyMongoArrow schema of the collection."""
        return self.source.shape

    @property
    def batch_size(self) -> int:
        """Return the documents of one page."""
        return self.source.batch_size

    @override
    async def open(self, stack: AsyncExitStack) -> None:
        """Open a DuckDB engine for the final files, and the client.

        Ping the server once, so a wrong host or login fails here and not on the first
        chunk. Raise `PyMongoError` if the server refuses.
        """
        self.duckdb = await stack.enter_async_context(DuckDBEngine())

        client = await to_thread(MongoClient[dict[str, object]], self.source.uri)
        stack.push_async_callback(to_thread, client.close)
        await to_thread(client.admin.command, "ping")

        self.database = client[self.source.conn.database]

    @override
    def clone(self) -> Self:
        """Return a new unopened engine of the source."""
        return replace(self)

    @override
    async def read(
        self, *, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> AsyncGenerator[DataFrame]:
        """Yield the pages of the origin as dataframes.

        `batch_size` does not apply: a page has the `batch_size` of the source. Raise
        `UnsupportedRequestError` for an origin that MongoDB does not read, and
        `ValueError` for a source with no origin.
        """
        async for table in self.read_pages(require_origin(self.source), Rows()):
            yield DataFrame(table)

    @override
    def check_chunks(self, chunks: Chunks, described: Mapping[str, str]) -> None:
        """Raise `ValueError` unless the chunk column fits the width of the chunks.

        An aggregation names the field of the stored documents, and its result can hold
        the field with another type, so only a read of a collection is checked.
        """
        if not isinstance(self.source.origin, Aggregate):
            super().check_chunks(chunks, described)

    @override
    async def describe(self) -> dict[str, str]:
        """Return the columns of the origin.

        Raise `ValueError` for a source with no origin.
        """
        origin = require_origin(self.source)

        return await describe_extracted(self.duckdb, self.extract, origin)

    @override
    async def materialize(
        self, target: Path, rows: Rows, columns: Columns = ()
    ) -> None:
        """Write the chunk of the origin as Parquet.

        Raise `ValueError` for a source with no origin, and for filters that are not
        JSON objects.
        """
        origin = require_origin(self.source)

        await materialize_extracted(
            self.duckdb, self.extract, origin, target, rows, columns
        )

    async def extract(self, origin: Origin, folder: Path, rows: Rows) -> Path:
        """Write the pages of the chunk to a raw Parquet file, and return its path.

        The file goes in a `_stage` folder of `folder`, and keeps the schema when there
        are no pages.
        """
        target = folder / "_stage" / "mongodb.parquet"
        writer: ParquetWriter | None = None

        target.parent.mkdir(exist_ok=True)

        try:
            async for table in self.read_pages(origin, rows):
                writer = writer or ParquetWriter(target, table.schema)
                await to_thread(writer.write_table, table)

            if writer is None:
                schema: ArrowSchema = make_portable(
                    self.schema.to_arrow().empty_table()
                ).schema
                writer = ParquetWriter(target, schema)
        finally:
            if writer is not None:
                writer.close()

        return target

    async def read_pages(self, origin: Origin, rows: Rows) -> AsyncGenerator[Table]:
        """Yield the pages of the chunk of an origin, and none for an empty read.

        Raise `ValueError` for filters that are not JSON objects, for a query with no
        collection, and for a paged read when the shape has no `_id`.
        """
        collection_name, selector = read_collection(origin)
        query = combine_conditions(selector, rows)

        if rows.empty:
            return

        collection = self.database[collection_name]

        if isinstance(origin, Aggregate):
            stages = [*([{"$match": query}] if query else []), *loads(origin.stages)]
            result = await to_thread(self.aggregate, collection, stages, self.schema)
            if result.num_rows:
                yield make_portable(result)
            return

        if MONGO_ID_FIELD not in self.schema.typemap:
            raise ValueError(
                f"A paged read needs the field {MONGO_ID_FIELD} in the shape"
            )

        last: object = None

        while True:
            page_query = page_after(query, last)
            table = await to_thread(
                self.fetch, collection, page_query, self.schema, self.batch_size
            )

            if table.num_rows == 0:
                return

            yield make_portable(table)

            if table.num_rows < self.batch_size:
                return

            last = table.column(MONGO_ID_FIELD)[-1].as_py()

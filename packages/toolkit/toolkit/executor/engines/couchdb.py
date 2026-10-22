"""The CouchDB engine: it reads a database over HTTP and writes chunks as Parquet."""

from asyncio import to_thread
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import ClassVar, Self, override

from polars import DataFrame
from pyarrow import Table
from pyarrow.parquet import ParquetWriter
from pydantic import JsonValue, TypeAdapter, ValidationError

from ...chunks import Chunk, Rows
from ...types import Chunks, Columns
from ...utils import filter_selectors
from ..constants import COUCHDB_ID_COLUMN, DEFAULT_BATCH_SIZE
from ..couchdb import CouchDBClient
from ..requests import (
    Changes,
    Query,
    UnsupportedRequestError,
)
from ..requests import (
    Table as TableOrigin,
)
from ..sources.backends.couchdb import CouchDBSource
from ..sources.models import (
    CouchDBItem,
)
from ..sources.protocols import Source
from ..sources.shapes import Shape
from ..types import Origin, Page
from .base import Engine, require_origin
from .duckdb import DuckDBEngine, describe_extracted, materialize_extracted


def build_id_range(start: str | None, stop: str | None) -> dict[str, JsonValue] | None:
    """Return the selector of the ids from `start` to `stop`, or `None` for all ids."""
    if start is None and stop is None:
        return None

    bounds: dict[str, JsonValue] = {}

    if start is not None:
        bounds["$gte"] = start
    if stop is not None:
        bounds["$lt"] = stop

    return {"_id": bounds}


def write_page(
    writer: ParquetWriter,
    shape: Shape,
    items: Sequence[CouchDBItem],
    update_seq: str | None,
) -> None:
    """Write the rows of one page to the file."""
    writer.write_table(build_page_table(shape, items, update_seq))


def build_page_table(
    shape: Shape, items: Sequence[CouchDBItem], update_seq: str | None
) -> Table:
    """Return the rows of one page as a table with the columns of the shape."""
    rows = [
        shape.row(item.model_copy(update={"last_seq": update_seq})) for item in items
    ]

    return Table.from_pylist(rows, schema=shape.columns)


@dataclass
class CouchDBEngine(Engine):
    """An open HTTP client of CouchDB that reads the pages of a database.

    A chunk sends `startkey` and `endkey` over the ids, cut by the first `key_width`
    digits of the source, or by the whole id when it is `None`. The null chunk reads
    the ids that do not start with a digit. A read with filters goes through `_find`
    with the chunk as a range of the ids. The width of the ids is read once, on the
    first chunk that needs it. The pages go to a raw Parquet file, and a DuckDB engine
    of its own writes the final file from it. The database is the one of the connection.
    """

    source_type: ClassVar[type[Source]] = CouchDBSource

    source: CouchDBSource
    duckdb: DuckDBEngine = field(init=False, repr=False)
    client: CouchDBClient = field(init=False, repr=False)

    @property
    @override
    def concurrency(self) -> int:
        """Return the chunks of the source that are read at the same time."""
        return self.source.concurrency

    @property
    def database(self) -> str:
        """Return the database of the connection."""
        return self.source.conn.database

    @property
    def shape(self) -> Shape:
        """Return the columns of a row."""
        return self.source.shape

    @override
    async def open(self, stack: AsyncExitStack) -> None:
        """Open a DuckDB engine for the final files, and the HTTP client.

        Ask for the database once, so a wrong host, login or name fails here and not on
        the first chunk. Raise `HTTPStatusError` if the server refuses.
        """
        self.duckdb = await stack.enter_async_context(DuckDBEngine())
        http = await stack.enter_async_context(self.source.build_client())
        self.client = CouchDBClient(http, self.source.batch_size, self.source.key_width)
        await self.client.ping(self.database)

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
        `UnsupportedRequestError` for an origin that CouchDB does not read, and
        `ValueError` for a source with no origin.
        """
        empty = True

        async for items, update_seq in self.read_pages(
            require_origin(self.source), Rows()
        ):
            empty = False

            yield DataFrame(build_page_table(self.shape, items, update_seq))

        if empty:
            yield DataFrame(build_page_table(self.shape, [], None))

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

        Raise `ValueError` for a source with no origin, for a chunk over a column
        other than the id, for filters that are not Mango selectors, and for filters
        with a query or a sequence.
        """
        origin = require_origin(self.source)

        await materialize_extracted(
            self.duckdb, self.extract, origin, target, rows, columns
        )

    @override
    def check_chunks(self, chunks: Chunks, described: Mapping[str, str]) -> None:
        """Raise `ValueError` unless the chunks cut the id by a number of ids.

        The ids are text that a chunk cuts by its digits, so the type of the column
        does not matter, and a date width has no meaning.
        """
        if chunks["column"] != COUCHDB_ID_COLUMN:
            raise ValueError(
                f"A CouchDB chunk cuts the id: set chunks.column to {COUCHDB_ID_COLUMN!r}"
            )

        match chunks["width"]:
            case int():
                pass
            case _:
                raise ValueError(
                    "A CouchDB chunk cuts ids, so chunks.width must be a number of ids"
                )

    async def extract(self, origin: Origin, folder: Path, rows: Rows) -> Path:
        """Write the pages of the chunk to a raw Parquet file, and return its path.

        The file goes in a `_stage` folder of `folder`.
        """
        target = folder / "_stage" / "couchdb.parquet"

        target.parent.mkdir(exist_ok=True)

        with ParquetWriter(target, self.shape.columns) as writer:
            async for items, update_seq in self.read_pages(origin, rows):
                await to_thread(write_page, writer, self.shape, items, update_seq)

        return target

    async def read_pages(self, origin: Origin, rows: Rows) -> AsyncGenerator[Page]:
        """Yield the pages of the chunk of an origin, and none for an empty read.

        Raise `ValueError` for a chunk over a column other than the id, for filters
        that are not Mango selectors, and for filters with a query or a sequence.
        """
        if rows.column not in (None, COUCHDB_ID_COLUMN):
            raise ValueError(
                f"A CouchDB chunk cuts the id: set chunks.column to {COUCHDB_ID_COLUMN!r}"
            )
        if rows.filters and isinstance(origin, Query | Changes):
            raise ValueError(
                "Filters are selectors that read the database: drop the query or the since"
            )

        match origin:
            case Changes(since, table):
                reader = self.client.read_changes(
                    table or self.database, since, rows.chunk
                )
            case Query(text, _, table):
                reader = self.read_selector_pages(
                    table or self.database, text, rows.chunk
                )
            case TableOrigin(name):
                reader = self.read_database_pages(name, rows)
            case _:
                raise UnsupportedRequestError("CouchDBEngine", origin)

        if rows.empty:
            return

        async for page in reader:
            yield page

    async def read_database_pages(
        self, database: str, rows: Rows
    ) -> AsyncGenerator[Page]:
        """Yield the pages of a database in the ids of the chunk, with the filters."""
        if rows.filters:
            ranges = await self.client.list_key_ranges(database, rows.chunk)
            conditions: list[JsonValue] = [*filter_selectors(rows.filters)]
            bounds = [
                bound
                for start, stop in ranges
                if (bound := build_id_range(start, stop))
            ]

            if bounds:
                conditions.append(bounds[0] if len(bounds) == 1 else {"$or": [*bounds]})

            async for page in self.client.find(database, {"$and": conditions}):
                yield page

            return

        for start, stop in await self.client.list_key_ranges(database, rows.chunk):
            async for page in self.client.read_all_docs(database, start, stop):
                yield page

    async def read_selector_pages(
        self, database: str, query: str, chunk: Chunk | None
    ) -> AsyncGenerator[Page]:
        """Yield the pages that a Mango selector returns.

        Raise `ValueError` if the read also asks for a chunk, or if the query is not
        JSON.
        """
        if chunk is not None:
            raise ValueError(
                "A Mango selector is one chunk: read the database to cut it into chunks"
            )

        try:
            selector = TypeAdapter(JsonValue).validate_json(query)
        except ValidationError as error:
            raise ValueError(f"The query is not a JSON selector: {error}") from error

        async for page in self.client.find(database, selector):
            yield page

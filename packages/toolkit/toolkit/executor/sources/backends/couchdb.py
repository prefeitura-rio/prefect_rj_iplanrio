"""CouchDB databases, read in pages and staged as Parquet, as the Oracle source does."""

from asyncio import to_thread
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from importlib import import_module
from json import dumps
from pathlib import Path
from typing import ClassVar, Self, override
from urllib.parse import quote

from httpx2 import AsyncClient, Timeout
from psycopg.sql import Literal
from pyarrow import Schema, Table, json_, schema, string
from pyarrow.parquet import ParquetWriter
from pydantic import BaseModel, JsonValue, TypeAdapter, ValidationError

from ....batches import NULL_WINDOW, Rows, Window
from ....settings import settings
from ....types import TemplateValue
from ...constants import (
    ALIAS,
    COUCHDB_ALL_WINDOW,
    COUCHDB_DESIGN_PREFIXES,
    COUCHDB_PLAIN_PORT,
    SECRET,
)
from ...engines.duckdb import Extension
from ...engines.protocol import Engine
from ..base import SourceBase
from ..models import (
    CouchDBAllDocs,
    CouchDBChange,
    CouchDBChanges,
    CouchDBFind,
    CouchDBItem,
    CouchDBOptions,
)
from ..protocols import DBConn, SourceContext


def json_text(value: JsonValue) -> str:
    """Return a value as compact JSON text."""
    return dumps(value, ensure_ascii=False, separators=(",", ":"))


def number_key(number: int, width: int) -> str:
    """Return the key of a number of ids: padded to `width`, `0` below, `:` above.

    `:` sorts after every digit, so a number past the ids of the width ends the range
    before the ids that start with a letter. `0` sorts before every padded number.
    """
    if number >= 10**width:
        return ":"

    return f"{number:0{width}d}" if number > 0 else "0"


def item_of(doc: dict[str, JsonValue]) -> CouchDBItem:
    """Return the item of a `_find` document, or raise `ValidationError`."""
    return CouchDBItem.model_validate(
        {
            "id": doc.get("_id"),
            "key": doc.get("_id"),
            "value": {"rev": doc.get("_rev")},
            "doc": doc,
        }
    )


def change_item(change: CouchDBChange) -> CouchDBItem:
    """Return the item of a change, with its sequence and its deletion mark."""
    revision = change.changes[0].get("rev") if change.changes else None

    return CouchDBItem(
        id=change.id,
        key=change.id,
        value={"rev": revision},
        doc=change.doc,
        seq=change.seq,
        deleted=change.deleted,
    )


def document_row(item: CouchDBItem) -> dict[str, JsonValue]:
    """Return the id and the document of one item."""
    return {"id": item.id, "doc": json_text(item.doc)}


@dataclass(frozen=True, slots=True)
class Shape:
    """The columns of the stage file, and how one item of a page becomes a row.

    A database with other columns takes its own shape, with no new source class.
    """

    columns: Schema
    row: Callable[[CouchDBItem], dict[str, JsonValue]]


def load_shape(path: str) -> Shape:
    """Return the shape that a path `module:name` names, or raise `ValueError`."""
    module, _, name = path.partition(":")

    if not module or not name:
        raise ValueError(f"The shape {path!r} is not a path of the form module:name")

    try:
        found: object = getattr(import_module(module), name, None)
    except ImportError as error:
        raise ValueError(f"The shape {path!r} cannot be imported: {error}") from error

    match found:
        case Shape():
            return found
        case _:
            raise ValueError(f"The shape {path!r} is not a Shape")


@dataclass(slots=True)
class CouchDBSource(SourceBase):
    """A source that reads the documents of a database in pages, over HTTP.

    The database is the `table` value, or `conn.database` without it.

    A `query` in the values is a Mango selector, and a `since` is the sequence from
    which `_changes` reads. A window sends `startkey` and `endkey` over the ids, cut by
    the first `key_width` digits, or by the whole id when `key_width` is `None`. The
    null window reads the ids that do not start with a digit.
    """

    extensions: ClassVar[tuple[Extension, ...]] = ()
    text_windows: ClassVar[bool] = True

    conn: DBConn
    shape: Shape = Shape(schema([("id", string()), ("doc", json_())]), document_row)
    page_size: int = field(default_factory=lambda: settings.COUCHDB_PAGE_SIZE)
    verify_tls: bool = field(default_factory=lambda: settings.COUCHDB_VERIFY_TLS)
    secure: bool | None = field(default_factory=lambda: settings.COUCHDB_SECURE)
    path_prefix: str = field(default_factory=lambda: settings.COUCHDB_PATH_PREFIX)
    timeout: int = field(default_factory=lambda: settings.COUCHDB_TIMEOUT_SECONDS)
    key_width: int | None = None
    width: int | None = field(default=None, init=False, repr=False)

    @override
    @classmethod
    def from_context(cls, context: SourceContext) -> Self:
        """Build the source from the connection data and the options.

        Raise `ValueError` for a context with no connection, or with options that do not
        fit.
        """
        conn = context.require_conn(cls.name)
        options = CouchDBOptions.model_validate(context.options)
        source = cls(conn, key_width=options.key_width)

        return (
            replace(source, shape=load_shape(options.shape))
            if options.shape
            else source
        )

    @property
    def base_url(self) -> str:
        """Return the URL of the server, with its path prefix and without the login."""
        secure = (
            self.conn.port != COUCHDB_PLAIN_PORT if self.secure is None else self.secure
        )
        prefix = (
            f"/{self.path_prefix.strip('/')}" if self.path_prefix.strip("/") else ""
        )

        return f"{'https' if secure else 'http'}://{self.conn.hostname}:{self.conn.port}{prefix}"

    def path(self, database: str, endpoint: str) -> str:
        """Return the path of an endpoint of a database, with the name quoted."""
        return f"/{quote(database, safe='')}/{endpoint}"

    def client(self) -> AsyncClient:
        """Return a client with the login in the header and the TLS setting."""
        return AsyncClient(
            base_url=self.base_url,
            auth=(self.conn.user, self.conn.password),
            verify=self.verify_tls,
            timeout=Timeout(self.timeout),
        )

    async def stage(
        self,
        client: Engine,
        values: Mapping[str, TemplateValue],
        rows: Rows,
        folder: Path,
    ) -> Mapping[str, TemplateValue]:
        """Write the pages to one Parquet file, and return the values that read it.

        Raise `ValueError` for filters, which belong in a Mango selector, and for a
        `query` and a `since` together. The engine of the caller is not used: it reads
        the staged file afterwards.
        """
        del client

        if rows.filters:
            raise ValueError(
                "A CouchDB read takes no filters: write a Mango selector as the query"
            )

        table = values.get("table")
        database = table if isinstance(table, str) else self.conn.database
        selector = values.get("query")
        since = values.get("since")

        if selector is not None and since is not None:
            raise ValueError("Give one of query and since, not both")
        target = folder / "_stage" / f"{self.name}.parquet"

        target.parent.mkdir(exist_ok=True)

        with ParquetWriter(target, self.shape.columns) as writer:
            if not rows.empty:
                async with self.client() as http:
                    if since is not None:
                        await self.copy_changes(
                            http, database, str(since), rows.window, writer
                        )
                    elif selector is not None:
                        await self.copy_selector(
                            http, database, str(selector), rows.window, writer
                        )
                    else:
                        await self.copy_database(http, database, rows.window, writer)

        return {
            "type": "parquet",
            "alias": Literal(ALIAS),
            "secret": Literal(SECRET),
            "query": Literal(str(target)),
        }

    async def copy_database(
        self,
        http: AsyncClient,
        database: str,
        window: Window | None,
        writer: ParquetWriter,
    ) -> None:
        """Copy every page of the database in the key ranges of the window."""
        for start, stop in await self.key_ranges(http, database, window):
            await self.copy_range(http, database, start, stop, writer)

    async def copy_range(
        self,
        http: AsyncClient,
        database: str,
        start: str | None,
        stop: str | None,
        writer: ParquetWriter,
    ) -> None:
        """Copy every page of the database between two keys.

        Raise `ValueError` if the server repeats a key, because the read cannot advance.
        """
        update_seq = None
        cursor = start
        skip = 0

        while True:
            params = {
                **self.page_params(cursor, stop),
                **({"skip": str(skip)} if skip else {}),
            }
            page = await self.get(
                http, self.path(database, "_all_docs"), params, CouchDBAllDocs
            )
            update_seq = update_seq or page.update_seq
            items = [
                item
                for item in page.rows
                if not item.id.startswith(COUCHDB_DESIGN_PREFIXES)
            ]

            if items:
                await to_thread(self.write, writer, items, update_seq)
            if len(page.rows) < self.page_size:
                return

            last = page.rows[-1].id

            if last == cursor and skip:
                raise ValueError(
                    f"The server repeated the key {last} of {database}, so the read cannot advance"
                )

            cursor, skip = last, 1

    async def copy_selector(
        self,
        http: AsyncClient,
        database: str,
        query: str,
        window: Window | None,
        writer: ParquetWriter,
    ) -> None:
        """Copy the documents that a Mango selector returns, page by page.

        Raise `ValueError` if the read also asks for a window, or if the query is not
        JSON.
        """
        if window is not None:
            raise ValueError(
                "A Mango selector is one window: read the database to cut it into windows"
            )

        try:
            selector = TypeAdapter(JsonValue).validate_json(query)
        except ValidationError as error:
            raise ValueError(f"The query is not a JSON selector: {error}") from error

        bookmark = None

        while True:
            body: dict[str, JsonValue] = {"selector": selector, "limit": self.page_size}

            if bookmark:
                body["bookmark"] = bookmark

            page = await self.post(
                http, self.path(database, "_find"), body, CouchDBFind
            )
            items = [item_of(doc) for doc in page.docs]

            if items:
                await to_thread(self.write, writer, items, None)
            if len(items) < self.page_size or not (bookmark := page.bookmark):
                return

    async def copy_changes(
        self,
        http: AsyncClient,
        database: str,
        since: str,
        window: Window | None,
        writer: ParquetWriter,
    ) -> None:
        """Copy the changes after a sequence, page by page, with deletions marked.

        `since` is `0`, `now` or the `last_seq` of an earlier read. Raise `ValueError`
        if the read also asks for a window.
        """
        if window is not None:
            raise ValueError(
                "A changes read is one window: read the database to cut it into windows"
            )

        while True:
            params = {
                "feed": "normal",
                "include_docs": "true",
                "limit": str(self.page_size),
                "since": since,
            }
            page = await self.get(
                http, self.path(database, "_changes"), params, CouchDBChanges
            )
            items = [
                change_item(change)
                for change in page.results
                if not change.id.startswith(COUCHDB_DESIGN_PREFIXES)
            ]

            if items:
                await to_thread(self.write, writer, items, page.last_seq)
            if len(page.results) < self.page_size:
                return

            since = page.last_seq

    def page_params(self, start: str | None, stop: str | None) -> dict[str, str]:
        """Return the parameters of one `_all_docs` page."""
        params = {
            "include_docs": "true",
            "limit": str(self.page_size),
            "update_seq": "true",
        }

        if start is not None:
            params["startkey"] = json_text(start)
        if stop is not None:
            params["endkey"] = json_text(stop)
            params["inclusive_end"] = "false"

        return params

    async def key_ranges(
        self, http: AsyncClient, database: str, window: Window | None
    ) -> list[tuple[str | None, str | None]]:
        """Return the key ranges of a window: one for ids, two for the null window.

        No window reads every key. The null window reads the keys below `0` and from
        `:`. Raise `ValueError` for a window of dates, because the ranges cut ids.
        """
        if window is None or window.key == COUCHDB_ALL_WINDOW:
            return [(None, None)]
        if window == NULL_WINDOW:
            return [(None, "0"), (":", None)]

        match window.start, window.stop:
            case int() as start, int() as stop:
                width = self.key_width or await self.id_width(http, database)

                return [(number_key(start, width), number_key(stop, width))]
            case _:
                raise ValueError(
                    "A CouchDB window cuts ids, so it needs numbers, not dates"
                )

    async def id_width(self, http: AsyncClient, database: str) -> int:
        """Return the width of the ids, and keep it for the next window.

        Raise `ValueError` if the ids are not numbers padded to one width. The first id
        is read from the key `0` and the last below the key `:`, so the ids that start
        with a letter or a symbol, and the design documents, stay out.
        """
        if self.width is not None:
            return self.width

        first = await self.first_key(http, database, {"startkey": json_text("0")})
        last = await self.first_key(
            http, database, {"descending": "true", "startkey": json_text(":")}
        )

        if not (first.isdigit() and last.isdigit() and len(first) == len(last)):
            message = f"The ids of {database} are not zero-padded numbers ({first!r} to {last!r})"
            raise ValueError(f"{message}, so a window cannot be a range")

        self.width = len(first)

        return self.width

    async def first_key(
        self, http: AsyncClient, database: str, params: Mapping[str, str]
    ) -> str:
        """Return the first key of a page of one row."""
        page = await self.get(
            http,
            self.path(database, "_all_docs"),
            {**params, "limit": "1"},
            CouchDBAllDocs,
        )

        return page.rows[0].id if page.rows else ""

    def write(
        self,
        writer: ParquetWriter,
        items: Sequence[CouchDBItem],
        update_seq: str | None,
    ) -> None:
        """Write the rows of one page to the file, off the event loop."""
        rows = [
            self.shape.row(item.model_copy(update={"last_seq": update_seq}))
            for item in items
        ]

        writer.write_table(Table.from_pylist(rows, schema=self.shape.columns))

    async def get[Page: BaseModel](
        self,
        http: AsyncClient,
        path: str,
        params: Mapping[str, str],
        page: type[Page],
    ) -> Page:
        """Return the page that a `GET` answers, or raise `ValidationError`."""
        response = await http.get(path, params=params)
        response.raise_for_status()

        return page.model_validate_json(response.content)

    async def post[Page: BaseModel](
        self,
        http: AsyncClient,
        path: str,
        body: Mapping[str, JsonValue],
        page: type[Page],
    ) -> Page:
        """Return the page that a `POST` answers, or raise `ValidationError`."""
        response = await http.post(path, json=body)
        response.raise_for_status()

        return page.model_validate_json(response.content)

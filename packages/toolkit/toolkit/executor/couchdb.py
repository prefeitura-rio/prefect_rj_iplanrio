"""The CouchDB HTTP client: it reads the pages of `_all_docs`, `_find`, `_changes`."""

from collections.abc import AsyncGenerator, Mapping
from dataclasses import dataclass, field
from urllib.parse import quote

from httpx2 import AsyncClient
from pydantic import BaseModel, JsonValue

from ..chunks import NULL_CHUNK, Chunk
from .constants import COUCHDB_DESIGN_PREFIX
from .sources.models import (
    CouchDBAllDocs,
    CouchDBChange,
    CouchDBChanges,
    CouchDBFind,
    CouchDBItem,
)
from .sources.shapes import convert_to_json_text
from .types import Page


def format_number_key(number: int, width: int) -> str:
    """Return the key of a number of ids: padded to `width`, `0` below, `:` above.

    `:` sorts after every digit, so a number past the ids of the width ends the range
    before the ids that start with a letter. `0` sorts before every padded number.
    """
    if number >= 10**width:
        return ":"

    return f"{number:0{width}d}" if number > 0 else "0"


def convert_document(doc: dict[str, JsonValue]) -> CouchDBItem:
    """Return the item of a `_find` document, or raise `ValidationError`."""
    return CouchDBItem.model_validate(
        {
            "id": doc.get("_id"),
            "key": doc.get("_id"),
            "value": {"rev": doc.get("_rev")},
            "doc": doc,
        }
    )


def convert_change(change: CouchDBChange) -> CouchDBItem:
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


def build_database_path(database: str, endpoint: str) -> str:
    """Return the path of an endpoint of a database, with the name quoted."""
    return f"/{quote(database, safe='')}/{endpoint}"


def build_page_params(
    page_size: int, start: str | None, stop: str | None
) -> dict[str, str]:
    """Return the parameters of one `_all_docs` page."""
    params = {
        "include_docs": "true",
        "limit": str(page_size),
        "update_seq": "true",
    }

    if start is not None:
        params["startkey"] = convert_to_json_text(start)
    if stop is not None:
        params["endkey"] = convert_to_json_text(stop)
        params["inclusive_end"] = "false"

    return params


@dataclass
class CouchDBClient:
    """An open HTTP client of one CouchDB server, with the size of a page.

    A page is at most `page_size` rows. `key_width` is the number of digits of an id
    that a chunk cuts, or `None` to read the width of the ids once, on the first chunk
    that needs it.
    """

    http: AsyncClient
    page_size: int
    key_width: int | None = None
    width: int | None = field(default=None, init=False)

    async def ping(self, database: str) -> None:
        """Ask for the database, and raise `HTTPStatusError` if the server refuses."""
        response = await self.http.get(build_database_path(database, ""))
        response.raise_for_status()

    async def read_all_docs(
        self, database: str, start: str | None, stop: str | None
    ) -> AsyncGenerator[Page]:
        """Yield every page of the database between two keys.

        Raise `ValueError` if the server repeats a key, because the read cannot advance.
        """
        update_seq = None
        cursor = start
        skip = 0

        while True:
            params = {
                **build_page_params(self.page_size, cursor, stop),
                **({"skip": str(skip)} if skip else {}),
            }
            page = await self.get(
                build_database_path(database, "_all_docs"), params, CouchDBAllDocs
            )
            update_seq = update_seq or page.update_seq
            items = [
                item
                for item in page.rows
                if not item.id.startswith(COUCHDB_DESIGN_PREFIX)
            ]

            if items:
                yield items, update_seq
            if len(page.rows) < self.page_size:
                return

            last = page.rows[-1].id

            if last == cursor and skip:
                raise ValueError(
                    f"The server repeated the key {last} of {database}, so the read cannot advance"
                )

            cursor, skip = last, 1

    async def find(self, database: str, selector: JsonValue) -> AsyncGenerator[Page]:
        """Yield the pages that a selector returns, by `_find` and its bookmark."""
        bookmark = None

        while True:
            body: dict[str, JsonValue] = {"selector": selector, "limit": self.page_size}

            if bookmark:
                body["bookmark"] = bookmark

            page = await self.post(
                build_database_path(database, "_find"), body, CouchDBFind
            )
            items = [
                item
                for item in map(convert_document, page.docs)
                if not item.id.startswith(COUCHDB_DESIGN_PREFIX)
            ]

            if items:
                yield items, None
            if len(page.docs) < self.page_size or not (bookmark := page.bookmark):
                return

    async def read_changes(
        self, database: str, since: str, chunk: Chunk | None
    ) -> AsyncGenerator[Page]:
        """Yield the changes after a sequence, page by page, with deletions marked.

        `since` is `0`, `now` or the `last_seq` of an earlier read. Raise `ValueError`
        if the read also asks for a chunk.
        """
        if chunk is not None:
            raise ValueError(
                "A changes read is one chunk: read the database to cut it into chunks"
            )

        while True:
            params = {
                "feed": "normal",
                "include_docs": "true",
                "limit": str(self.page_size),
                "since": since,
            }
            page = await self.get(
                build_database_path(database, "_changes"), params, CouchDBChanges
            )
            items = [
                convert_change(change)
                for change in page.results
                if not change.id.startswith(COUCHDB_DESIGN_PREFIX)
            ]

            if items:
                yield items, page.last_seq
            if len(page.results) < self.page_size:
                return

            since = page.last_seq

    async def list_key_ranges(
        self, database: str, chunk: Chunk | None
    ) -> list[tuple[str | None, str | None]]:
        """Return the key ranges of a chunk: one for ids, two for the null chunk.

        No chunk reads every key. The null chunk reads the keys below `0` and from
        `:`. Raise `ValueError` for a chunk of dates, because the ranges cut ids.
        """
        if chunk is None:
            return [(None, None)]
        if chunk == NULL_CHUNK:
            return [(None, "0"), (":", None)]

        match chunk.start, chunk.stop:
            case int() as start, int() as stop:
                width = self.key_width or await self.find_id_width(database)

                return [
                    (format_number_key(start, width), format_number_key(stop, width))
                ]
            case _:
                raise ValueError(
                    "A CouchDB chunk cuts ids, so it needs numbers, not dates"
                )

    async def find_id_width(self, database: str) -> int:
        """Return the width of the ids, and keep it for the next chunk.

        Raise `ValueError` if the ids are not numbers padded to one width. The first id
        is read from the key `0` and the last below the key `:`, so the ids that start
        with a letter or a symbol, and the design documents, stay out.
        """
        if self.width is not None:
            return self.width

        first = await self.read_first_key(
            database, {"startkey": convert_to_json_text("0")}
        )
        last = await self.read_first_key(
            database, {"descending": "true", "startkey": convert_to_json_text(":")}
        )

        if not (first.isdigit() and last.isdigit() and len(first) == len(last)):
            message = f"The ids of {database} are not zero-padded numbers ({first!r} to {last!r})"
            raise ValueError(f"{message}, so a chunk cannot be a range")

        self.width = len(first)

        return self.width

    async def read_first_key(self, database: str, params: Mapping[str, str]) -> str:
        """Return the first key of a page of one row."""
        page = await self.get(
            build_database_path(database, "_all_docs"),
            {**params, "limit": "1"},
            CouchDBAllDocs,
        )

        return page.rows[0].id if page.rows else ""

    async def get[Model: BaseModel](
        self,
        path: str,
        params: Mapping[str, str],
        page: type[Model],
    ) -> Model:
        """Return the page that a `GET` answers, or raise `ValidationError`."""
        response = await self.http.get(path, params=params)
        response.raise_for_status()

        return page.model_validate_json(response.content)

    async def post[Model: BaseModel](
        self,
        path: str,
        body: Mapping[str, JsonValue],
        page: type[Model],
    ) -> Model:
        """Return the page that a `POST` answers, or raise `ValidationError`."""
        response = await self.http.post(path, json=body)
        response.raise_for_status()

        return page.model_validate_json(response.content)

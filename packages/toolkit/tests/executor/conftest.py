import sys
from base64 import b64decode, b64encode
from collections.abc import Iterator
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import import_module
from importlib.util import find_spec
from json import dumps, loads
from pathlib import Path
from threading import Thread
from types import ModuleType
from typing import TypedDict, override
from urllib.parse import parse_qs, urlparse

import pytest
from polars import DataFrame
from psycopg.sql import Literal
from pyarrow import Table
from pyarrow import schema as arrow_schema
from pyarrow import string as arrow_string
from pyarrow.parquet import read_table
from pydantic import JsonValue

from toolkit.chunks import Rows
from toolkit.executor.engines import base
from toolkit.executor.engines.base import Engine
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.engines.mongodb import MongoDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.requests import (
    Changes,
    Files,
    Query,
)
from toolkit.executor.requests import Table as TableOrigin
from toolkit.executor.sources import backends, registry
from toolkit.executor.sources.backends.arcgis import ArcGISSource
from toolkit.executor.sources.backends.couchdb import CouchDBSource
from toolkit.executor.sources.backends.files import FilesSource
from toolkit.executor.sources.base import Registered
from toolkit.executor.sources.models import CouchDBItem
from toolkit.executor.sources.protocols import DBConn
from toolkit.executor.sources.shapes import Shape, convert_to_json_text
from toolkit.types import Geometry, TemplateValue
from toolkit.utils import build_column_aliases

collect_ignore: list[str] = (
    [] if find_spec("pymongoarrow") else ["test_mongodb.py", "test_probes.py"]
)
"""The MongoDB tests need the `mongodb` extra, so they are skipped without it."""

USER = "user"
PASSWORD = "segredo"
UPDATE_SEQ = "9-g1AAAAKTeJzLYWBgEMhgTmHgzcvPy09J"
DESIGN = "_design/cac500ad"
POLL_SECONDS = 0.01
KEYS = [f"{number:011d}" for number in range(1, 2501)]
FIND_WARNING = "No matching index found, create an index to optimize query time."
NGINX_UNAUTHORIZED = (
    b"<html>\r\n<head><title>401 Authorization Required</title></head>\r\n"
    b"<body>\r\n<center><h1>401 Authorization Required</h1></center>\r\n"
    b"<hr><center>nginx/1.28.1</center>\r\n</body>\r\n</html>\r\n"
)
"""What the proxy in front of the real instance answers to a wrong login."""
NOT_FOUND = b'{"error":"not_found","reason":"Database does not exist."}'


def document(key: str) -> dict[str, object]:
    """Return one document of the fake database."""
    return {"_id": key, "_rev": f"1-{key}", "name": f"NAME {key}"}


def compact(value: object) -> str:
    """Return a value as the compact JSON that CouchDB writes."""
    return dumps(value, ensure_ascii=False, separators=(",", ":"))


class Store:
    """The documents of a fake CouchDB, and the requests that it received."""

    def __init__(self, keys: list[str]) -> None:
        """Start with one document for each key."""
        self.docs = {key: document(key) for key in keys}
        self.update_seq = UPDATE_SEQ
        self.sequence = {key: number for number, key in enumerate(keys, 1)}
        self.counter = len(keys)
        self.tombstones: dict[str, str] = {}
        self.requests: list[tuple[str, dict[str, str], dict[str, object] | None]] = []

    def all_docs(self, params: dict[str, str]) -> bytes:
        """Return `_all_docs` as CouchDB writes it: a head, then one row per line.

        The head holds `total_rows`, `offset` and, when asked, `update_seq`.
        """
        descending = params.get("descending") == "true"
        everything = sorted(self.docs, reverse=descending)
        keys = everything
        start = loads(params["startkey"]) if "startkey" in params else None
        stop = loads(params["endkey"]) if "endkey" in params else None
        inclusive = params.get("inclusive_end") == "true"

        if start is not None:
            keys = [
                key for key in keys if (key <= start if descending else key >= start)
            ]
        if stop is not None:
            keys = [key for key in keys if key < stop or (inclusive and key == stop)]

        skip = int(params.get("skip", "0"))
        limit = int(params.get("limit", len(keys)))
        page = keys[skip : skip + limit]
        head: dict[str, object] = {
            "total_rows": len(self.docs),
            "offset": everything.index(page[0]) if page else len(everything),
        }

        if params.get("update_seq") == "true":
            head["update_seq"] = self.update_seq

        rows = ",\n".join(
            compact(self.row(key, params.get("include_docs") == "true")) for key in page
        )

        return f'{compact(head)[:-1]},"rows":[\n{rows}\n]}}\n'.encode()

    def row(self, key: str, include_doc: bool) -> dict[str, object]:
        """Return the row of `_all_docs` for one document."""
        doc = self.docs[key]
        row: dict[str, object] = {"id": key, "key": key, "value": {"rev": doc["_rev"]}}

        if include_doc:
            row["doc"] = doc

        return row

    def touch(self, key: str) -> None:
        """Update a document, which moves it to the end of the changes."""
        self.docs[key] = {**self.docs[key], "_rev": f"2-{key}", "name": "RENAMED"}
        self.counter += 1
        self.sequence[key] = self.counter

    def remove(self, key: str) -> None:
        """Delete a document, which leaves a tombstone at the end of the changes."""
        del self.docs[key]
        self.tombstones[key] = f"2-{key}"
        self.counter += 1
        self.sequence[key] = self.counter

    @staticmethod
    def sequence_text(number: int) -> str:
        """Return the opaque sequence of a position, as `_changes` hands it out."""
        return f"{number}-g1AAAA"

    def changes(self, params: dict[str, str]) -> bytes:
        """Return `_changes` as CouchDB writes it, with `last_seq` and `pending`.

        Each document shows once, at its latest change. A deleted one is a tombstone.
        """
        since = params.get("since", "0")
        after = self.counter if since == "now" else int(since.split("-")[0])
        entries = sorted(
            (number, key) for key, number in self.sequence.items() if number > after
        )
        page = entries[: int(params.get("limit", len(entries)))]
        results = []

        for number, key in page:
            result: dict[str, object] = {"seq": self.sequence_text(number), "id": key}

            if key in self.docs:
                result["changes"] = [{"rev": self.docs[key]["_rev"]}]
                doc = self.docs[key]
            else:
                result["changes"] = [{"rev": self.tombstones[key]}]
                result["deleted"] = True
                doc = {"_id": key, "_rev": self.tombstones[key], "_deleted": True}

            if params.get("include_docs") == "true":
                result["doc"] = doc

            results.append(result)

        last = self.sequence_text(page[-1][0]) if page else since
        lines = ",\n".join(compact(result) for result in results)
        tail = compact({"last_seq": last, "pending": len(entries) - len(page)})

        return f'{{"results":[\n{lines}\n],\n{tail[1:]}\n'.encode()

    @staticmethod
    def bookmark(offset: int) -> str:
        """Return the opaque bookmark of a position, as `_find` hands it out."""
        return b64encode(f"g1AAAA{offset}".encode()).decode()

    def find(self, body: dict[str, object]) -> bytes:
        """Return `_find` as CouchDB writes it: the documents, a bookmark and a warning.

        The bookmark never ends: after the last page it still names a position.
        """
        keys = sorted(self.docs)
        bookmark = body.get("bookmark")
        offset = int(b64decode(str(bookmark))[6:]) if bookmark else 0
        page = keys[offset : offset + int(str(body.get("limit", len(keys))))]
        answer = {
            "docs": [self.docs[key] for key in page],
            "bookmark": self.bookmark(offset + len(page)),
            "warning": FIND_WARNING,
        }

        return compact(answer).encode()


def handler(store: Store, prefix: str) -> type[BaseHTTPRequestHandler]:
    """Return a handler class that serves the store under a path prefix."""
    login = b64encode(f"{USER}:{PASSWORD}".encode()).decode()

    class Handler(BaseHTTPRequestHandler):
        def reply(
            self, body: bytes, status: int = 200, kind: str = "application/json"
        ) -> None:
            """Send one response."""
            self.send_response(status)
            self.send_header("Content-Type", kind)
            self.send_header("Cache-Control", "must-revalidate")
            self.end_headers()
            self.wfile.write(body)

        def unauthorized(self) -> bool:
            """Answer 401 as the proxy does, and tell whether the login was wrong."""
            wrong = self.headers.get("Authorization") != f"Basic {login}"

            if wrong:
                self.reply(NGINX_UNAUTHORIZED, 401, "text/html")

            return wrong

        def do_GET(self) -> None:
            """Serve `_all_docs` and `_changes`."""
            url = urlparse(self.path)
            params = {name: values[0] for name, values in parse_qs(url.query).items()}
            store.requests.append((url.path, params, None))

            if not url.path.startswith(f"{prefix}/"):
                self.reply(NOT_FOUND, 404)
            elif not self.unauthorized():
                served = (
                    store.changes if url.path.endswith("/_changes") else store.all_docs
                )
                self.reply(served(params))

        def do_POST(self) -> None:
            """Serve `_find`."""
            url = urlparse(self.path)
            body = loads(self.rfile.read(int(self.headers["Content-Length"])))
            store.requests.append((url.path, {}, body))

            if not url.path.startswith(f"{prefix}/"):
                self.reply(NOT_FOUND, 404)
            elif not self.unauthorized():
                self.reply(store.find(body))

        @override
        def log_message(self, format: str, *args: object) -> None:
            """Silence the log of the test server."""

    return Handler


def serve(keys: list[str], prefix: str = "") -> Iterator[tuple[Store, DBConn]]:
    """Serve a fake CouchDB from a thread, and yield its store and connection."""
    store = Store(keys)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler(store, prefix))
    Thread(target=server.serve_forever, args=(POLL_SECONDS,), daemon=True).start()
    yield store, DBConn("127.0.0.1", server.server_port, "people", USER, PASSWORD)
    server.shutdown()


@pytest.fixture
def couchdb() -> Iterator[tuple[Store, DBConn]]:
    """Serve the 2500 documents of the fake database."""
    yield from serve(KEYS)


@pytest.fixture
def small_couchdb() -> Iterator[tuple[Store, DBConn]]:
    """Serve seven documents, so a page repeats the key of the one before."""
    yield from serve(KEYS[:7])


@pytest.fixture
def empty_couchdb() -> Iterator[tuple[Store, DBConn]]:
    """Serve a database with no document."""
    yield from serve([])


@pytest.fixture
def proxied_couchdb() -> Iterator[tuple[Store, DBConn]]:
    """Serve seven documents under `/couchdb`, as a reverse proxy publishes CouchDB."""
    yield from serve(KEYS[:7], "/couchdb")


class Point(TypedDict):
    id: int
    name: str
    geom: Geometry


@pytest.fixture
def read_layer():
    """Provide the `read_layer` helper."""

    async def read_layer(
        url: str, directory: Path, where: str = "1=1", srid: str | None = None
    ) -> list[int]:
        """Read the ids of the layer with the macro, through the executor."""
        (directory / "points.sql").write_text(
            '{% from "duckdb/macros.sql" import arcgis_layer %}'
            "SELECT id, name, geom FROM {{ arcgis_layer(url, where, srid | default(none)) }} ORDER BY id\n"
        )
        source = ArcGISSource()

        async with DuckDBEngine(extensions=source.extensions) as engine:
            dataset = await Executor(engine, directory).query(
                "points",
                {"url": Literal(url), "where": Literal(where)}
                | ({"srid": Literal(srid)} if srid else {}),
                expect=Point,
            )

        assert dataset.dataframe.schema["geom"].is_extension()
        return dataset.dataframe["id"].to_list()

    return read_layer


@pytest.fixture
def build_origin():
    """Provide the `build_origin` helper."""

    def build_origin(source, values):
        """Return the origin that read values name: a table, a query or changes."""
        if "since" in values:
            return Changes(values["since"], values.get("table"))
        if "query" in values:
            return Query(values["query"], "couchdb", values.get("table"))

        return TableOrigin(values.get("table", source.conn.database))

    return build_origin


@pytest.fixture
def stage_rows(build_origin):
    """Provide the `stage_rows` helper."""

    async def stage_rows(source, values, rows, folder):
        """Write the chunk through the engine of the source, and return the file."""
        target = folder / "out.parquet"

        reading = replace(source, origin=build_origin(source, values))

        async with Engine.from_source(reading) as engine:
            await engine.materialize(target, rows)

        return target

    return stage_rows


@pytest.fixture
def shout_row():
    """Provide the `shout_row` helper."""

    def shout_row(item: CouchDBItem) -> dict[str, JsonValue]:
        """Return the id of one item in capitals, for a source with another shape."""
        return {"id": convert_to_json_text(item.id.upper())}

    return shout_row


@pytest.fixture
def stage(stage_rows):
    """Provide the `stage` helper."""

    async def stage(
        source: CouchDBSource, folder, values=None
    ) -> tuple[list[str], int]:
        """Stage a read and return the columns and the count of rows."""
        target = await stage_rows(source, values or {}, Rows(), folder)
        table = read_table(target)

        return table.schema.names, table.num_rows

    return stage


@pytest.fixture
def shapes(monkeypatch, shout_row) -> Shape:
    """Offer a module of shapes that a context can name, and return the shape in it."""
    shape = Shape(arrow_schema([("id", arrow_string())]), shout_row)
    module = ModuleType("shapes_of_the_test")
    module.__dict__.update(SHOUT=shape, NOT_A_SHAPE="text")
    monkeypatch.setitem(sys.modules, "shapes_of_the_test", module)

    return shape


@pytest.fixture
def collect():
    """Provide the `collect` helper."""

    async def collect(engine, sql: str, **options) -> list[DataFrame]:
        """Read every batch of a query."""
        return [dataframe async for dataframe in engine.query(sql, **options)]

    return collect


@pytest.fixture
def fail_to_import_engine():
    """Provide the `fail_to_import_engine` helper."""

    def fail_to_import_engine(monkeypatch, module: str, missing: str) -> None:
        """Make an engine fail to import, as if `missing` were not installed."""

        def failing_import(name: str):
            """Raise for the engine, and import every other module as usual."""
            if name == f"{base.engines.__name__}.{module}":
                raise ModuleNotFoundError(f"No module named {missing!r}", name=missing)

            return import_module(name)

        monkeypatch.setattr(base, "import_module", failing_import)

    return fail_to_import_engine


@pytest.fixture
def write():
    """Provide the `write` helper."""

    def write(directory: Path, name: str, sql: str) -> Path:
        """Write a template and return its directory."""
        (directory / f"{name}.sql").write_text(sql)
        return directory

    return write


@pytest.fixture
def stand_in_for_postgres():
    """Provide the `stand_in_for_postgres` helper."""

    async def stand_in_for_postgres(engine: DuckDBEngine) -> None:
        """Make `postgres_query` return a small table, as a real database would."""
        await engine.execute(
            "CREATE TABLE stub AS SELECT 1 AS id, 'a' AS \"Nome Completo\", 2024 AS ano"
        )
        await engine.execute(
            "CREATE MACRO postgres_query(alias, query) AS TABLE SELECT * FROM stub"
        )

    return stand_in_for_postgres


@pytest.fixture
def numbers(tmp_path) -> Executor:
    """Return an executor over a Parquet file of three ids and a nullable name."""
    path = tmp_path / "numbers.parquet"
    DataFrame({"id": [1, 2, 3], "nome": ["a", None, "c"]}).write_parquet(path)
    source = FilesSource(origin=Files(str(path), "parquet"))

    return Executor(DuckDBEngine(source))


@pytest.fixture
def collect_pages():
    """Provide the `collect_pages` helper."""

    async def collect_pages(engine: MongoDBEngine, origin, rows: Rows) -> list[Table]:
        """Return every page that the engine reads."""
        return [table async for table in engine.read_pages(origin, rows)]

    return collect_pages


@pytest.fixture
def fail_to_import_backend():
    """Provide the `fail_to_import_backend` helper."""

    def fail_to_import_backend(monkeypatch, module: str, missing: str) -> None:
        """Make a backend fail to import, as if `missing` were not installed.

        The registry forgets the backend too, because an earlier import added it.
        """
        monkeypatch.delitem(Registered.sources, module, raising=False)

        def failing_import(name: str):
            """Raise for the backend, and import every other module as usual."""
            if name == f"{backends.__name__}.{module}":
                raise ModuleNotFoundError(f"No module named {missing!r}", name=missing)

            return import_module(name)

        monkeypatch.setattr(registry, "import_module", failing_import)

    return fail_to_import_backend


@pytest.fixture
def dump_values():
    """Provide the `dump_values` helper."""

    def dump_values(**changes):
        """Return the values of a dump, with the given changes."""
        values: dict[str, TemplateValue] = {
            "type": "postgres",
            "alias": Literal("src"),
            "secret": Literal("src_secret"),
            "query": Literal("q"),
            "target": Literal("out.parquet"),
            "columns": build_column_aliases(["a"]),
        }
        return values | changes

    return dump_values

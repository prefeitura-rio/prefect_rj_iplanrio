"""The fake services that the tests use, each one with its fixture.

They are the fakes of GCS, BigQuery (one that detects the schema of a Delta table and one that
records calls), an HTTP API, a database, Oracle and an object store. They run in the process, with no network and no
container, and they follow what the real services did in the runs of `test_cloud.py` with
`-m gcp`. `conftest.py` registers this module as a plugin, so a test asks for a fixture by name.
"""

from collections import deque
from collections.abc import AsyncGenerator, Callable, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import md5
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from json import dumps, loads
from multiprocessing import get_context
from multiprocessing.connection import Connection
from re import findall
from threading import Lock, Thread
from types import SimpleNamespace
from typing import ClassVar, cast, override
from urllib.parse import parse_qs, unquote, urlparse
from xml.sax.saxutils import escape, unescape

import pytest
from deltalake import DeltaTable
from obstore.store import MemoryStore
from oracledb import Connection as oracle_Connection
from oracledb import DatabaseError
from pyarrow import Schema, Table
from pyarrow import array as arrow_array
from pyarrow import int64 as arrow_int64
from pyarrow import table as arrow_table
from pydantic import SecretStr
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.engines.protocol import Engine
from toolkit.executor.sources.protocols import DBConn
from toolkit.oracle import Oracle
from toolkit.settings import settings

from utils import tasks, utils

READABLE = {"timestampNtz", "columnMapping", "deletionVectors"}
"""The reader features that BigQuery supports, with reader version 3 as the most it reads."""

BIGQUERY_TYPES = {
    "long": "INTEGER",
    "integer": "INTEGER",
    "short": "INTEGER",
    "byte": "INTEGER",
    "string": "STRING",
    "double": "FLOAT",
    "float": "FLOAT",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp_ntz": "DATETIME",
    "timestamp": "TIMESTAMP",
    "binary": "BYTES",
}
"""The BigQuery type that each Delta type had in the real runs."""

NOT_FOUND = 404
NO_CONTENT = 204
PRECONDITION_FAILED = 412
PARTIAL = 206
NOT_IMPLEMENTED = 501


@dataclass
class FakeBlob:
    """One stored object."""

    data: bytes
    generation: int
    modified: datetime

    @property
    def etag(self) -> str:
        """Return the entity tag of the content."""
        return md5(self.data, usedforsecurity=False).hexdigest()


@dataclass
class FakeBucket:
    """The objects of a fake GCS, and the lock that keeps a conditional write atomic."""

    blobs: dict[tuple[str, str], FakeBlob] = field(default_factory=dict)
    lock: Lock = field(default_factory=Lock)
    generation: int = 0
    writes: list[tuple[str, str | None]] = field(default_factory=list)


def byte_range(header: str, size: int) -> tuple[int, int]:
    """Return the first and the last byte that a `Range: bytes=a-b` header asks for."""
    start, _, end = header.removeprefix("bytes=").partition("-")

    if not start:
        return max(size - int(end), 0), size - 1

    return int(start), min(int(end), size - 1) if end else size - 1


def listing(blobs: Mapping[str, FakeBlob], bucket: str, query: Mapping[str, str]) -> bytes:
    """Return the XML of the `list-type=2` answer, with the `delimiter` folded into prefixes."""
    prefix, delimiter = query.get("prefix", ""), query.get("delimiter", "")
    after = query.get("start-after", "")
    contents: list[tuple[str, FakeBlob]] = []
    prefixes: set[str] = set()

    for key in sorted(blobs):
        rest = key[len(prefix) :]

        if not key.startswith(prefix) or key <= after:
            continue

        if delimiter and delimiter in rest:
            prefixes.add(prefix + rest.split(delimiter)[0] + delimiter)
        else:
            contents.append((key, blobs[key]))

    xml = [f"<ListBucketResult><Name>{bucket}</Name><Prefix>{escape(prefix)}</Prefix>"]
    xml.append(f"<KeyCount>{len(contents)}</KeyCount><IsTruncated>false</IsTruncated>")

    for key, blob in contents:
        stamp = blob.modified.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        xml.append(f"<Contents><Key>{escape(key)}</Key><LastModified>{stamp}</LastModified>")
        xml.append(f'<ETag>"{blob.etag}"</ETag><Size>{len(blob.data)}</Size></Contents>')

    xml.extend(f"<CommonPrefixes><Prefix>{escape(item)}</Prefix></CommonPrefixes>" for item in sorted(prefixes))
    xml.append("</ListBucketResult>")

    return "".join(xml).encode()


def make_handler(state: FakeBucket) -> type[BaseHTTPRequestHandler]:
    """Build the request handler of a fake GCS over its state."""

    class FakeHandler(BaseHTTPRequestHandler):
        """The XML API of GCS that `deltalake` and `obstore` call."""

        protocol_version = "HTTP/1.1"

        @override
        def log_message(self, format: str, *args: object) -> None:
            """Keep the test output quiet."""

        def target(self) -> tuple[str, str, dict[str, str]]:
            """Return the bucket, the key and the query of the request."""
            url = urlparse(self.path)
            bucket, _, key = unquote(url.path).lstrip("/").partition("/")

            return (
                bucket,
                key,
                {name: values[0] for name, values in parse_qs(url.query, keep_blank_values=True).items()},
            )

        def answer(self, status: int, body: bytes = b"", headers: Mapping[str, str] | None = None) -> None:
            """Send a response with its body."""
            self.send_response(status)

            for name, value in (headers or {}).items():
                self.send_header(name, value)

            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def describe(self, blob: FakeBlob) -> dict[str, str]:
            """Return the headers that describe an object."""
            return {
                "ETag": f'"{blob.etag}"',
                "x-goog-generation": str(blob.generation),
                "Last-Modified": blob.modified.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            }

        def do_PUT(self) -> None:
            """Store an object, or copy one, and honor `x-goog-if-generation-match`."""
            bucket, key, _ = self.target()
            size = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(size) if size else b""
            source = self.headers.get("x-goog-copy-source")
            match = self.headers.get("x-goog-if-generation-match")

            with state.lock:
                if source:
                    origin = state.blobs.get(cast("tuple[str, str]", tuple(unquote(source).lstrip("/").split("/", 1))))

                    if origin is None:
                        return self.answer(NOT_FOUND)

                    body = origin.data

                state.writes.append((key, match))
                current = state.blobs.get((bucket, key))

                if match is not None and int(match) != (current.generation if current else 0):
                    return self.answer(PRECONDITION_FAILED, b"<Error><Code>PreconditionFailed</Code></Error>")

                state.generation += 1
                blob = FakeBlob(body, state.generation, datetime.now(UTC))
                state.blobs[(bucket, key)] = blob

            return self.answer(200, b"", self.describe(blob))

        def do_GET(self) -> None:
            """Read an object, with a range if asked, or list a prefix."""
            bucket, key, query = self.target()

            if not key:
                with state.lock:
                    mine = {name: blob for (owner, name), blob in state.blobs.items() if owner == bucket}

                return self.answer(200, listing(mine, bucket, query), {"Content-Type": "application/xml"})

            with state.lock:
                blob = state.blobs.get((bucket, key))

            if blob is None:
                return self.answer(NOT_FOUND, b"<Error><Code>NoSuchKey</Code></Error>")

            data, status, headers = blob.data, 200, self.describe(blob)

            if span := self.headers.get("Range"):
                first, last = byte_range(span, len(data))
                headers["Content-Range"] = f"bytes {first}-{last}/{len(data)}"
                data, status = data[first : last + 1], PARTIAL

            return self.answer(status, data, headers)

        def do_HEAD(self) -> None:
            """Describe an object without its body."""
            bucket, key, _ = self.target()

            with state.lock:
                blob = state.blobs.get((bucket, key))

            if blob is None:
                self.send_response(NOT_FOUND)
                self.send_header("Content-Length", "0")
            else:
                self.send_response(200)

                for name, value in self.describe(blob).items():
                    self.send_header(name, value)

                self.send_header("Content-Length", str(len(blob.data)))

            self.end_headers()

        def do_POST(self) -> None:
            """Delete many objects with one request, as `obstore` does for a list of keys."""
            bucket, _, query = self.target()
            size = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(size).decode() if size else ""

            if "delete" not in query:
                return self.answer(NOT_IMPLEMENTED)

            keys = [unescape(key) for key in findall(r"<Key>(.*?)</Key>", body)]

            with state.lock:
                for key in keys:
                    state.blobs.pop((bucket, key), None)

            deleted = "".join(f"<Deleted><Key>{escape(key)}</Key></Deleted>" for key in keys)

            return self.answer(
                200, f"<DeleteResult>{deleted}</DeleteResult>".encode(), {"Content-Type": "application/xml"}
            )

        def do_DELETE(self) -> None:
            """Delete an object."""
            bucket, key, _ = self.target()

            with state.lock:
                existed = state.blobs.pop((bucket, key), None)

            self.answer(NO_CONTENT if existed else NOT_FOUND)

    return FakeHandler


def serve(connection: Connection) -> None:
    """Serve a fake GCS on a free port, and send the port to the parent. It runs in a process."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(FakeBucket()))

    connection.send(server.server_port)
    server.serve_forever()


class FakeGcs:
    """An object store with the XML API of GCS, served on a free local port.

    It keeps the semantics that the real runs showed: a write with
    `x-goog-if-generation-match` fails with a 412 when the generation differs, so two
    writers cannot create the same commit file. It runs in its own process, because
    `deltalake` holds the GIL during some calls, and a server in a thread would never answer.
    """

    def __init__(self) -> None:
        """Start the server process and wait for its port."""
        context = get_context("spawn")
        parent, child = context.Pipe()

        self.process = context.Process(target=serve, args=(child,), daemon=True)
        self.process.start()

        self.port = cast("int", parent.recv())

    @property
    def endpoint(self) -> str:
        """Return `host:port` of the server."""
        return f"127.0.0.1:{self.port}"

    @property
    def url(self) -> str:
        """Return the base URL of the server."""
        return f"http://{self.endpoint}"

    def stop(self) -> None:
        """Stop the server process."""
        self.process.terminate()
        self.process.join()


def bigquery_type(delta_type: str) -> str:
    """Return the BigQuery type of a Delta type, as the real runs showed."""
    return "NUMERIC" if delta_type.startswith("decimal") else BIGQUERY_TYPES[delta_type]


@dataclass(frozen=True)
class FakeTable:
    """A BigQuery table over a Delta folder. Like the real one, it reads the log for its schema."""

    uri: str

    @property
    def schema(self) -> dict[str, str]:
        """Return the BigQuery type of each column, from the schema of the Delta log now."""
        fields = loads(DeltaTable(self.uri).schema().to_json())["fields"]

        return {field["name"]: bigquery_type(field["type"]) for field in fields}


class FakeBigQuery:
    """BigQuery over Delta tables, with the schema read from the Delta log on every query.

    It refuses what the real BigQuery refused: a Delta table without a connection, a
    folder with no Delta log, and a protocol that it cannot read.
    """

    tables: ClassVar[dict[str, FakeTable]] = {}
    datasets: ClassVar[set[str]] = set()

    @classmethod
    @asynccontextmanager
    async def connect(cls, *_: str | None) -> AsyncGenerator["FakeBigQuery"]:
        """Yield a client. It takes the project of the real one, and serves every project."""
        yield cls()

    async def create_dataset(self, dataset: str) -> None:
        """Create a dataset when it does not exist."""
        self.datasets.add(dataset)

    async def execute(self, sql: str) -> int:
        """Refuse a statement: this fake only serves reads over Delta tables."""
        raise NotImplementedError(sql)

    async def sync_delta_table(self, table: str, uri: str, connection_id: str) -> None:
        """Create the table over a Delta folder, or check it again when it exists."""
        if len(connection_id.split(".")) != 3:
            raise ValueError(f"Not a connection id: {connection_id}")

        if not DeltaTable.is_deltatable(uri):
            raise LookupError(f"No Delta log at {uri}")

        delta = DeltaTable(uri)
        protocol = delta.protocol()

        if protocol.min_reader_version > 3 or not set(protocol.reader_features or []) <= READABLE:
            raise ValueError("BigQuery cannot read this Delta protocol")

        self.tables[table] = FakeTable(uri)


@dataclass
class FakeDatabase:
    """A database in memory, attached as `src` the way the real extension attaches one.

    A table named `schema.name` is read as `"src"."schema"."name"`, so a dump that reads
    an attached table runs the same SQL as it does against Postgres.
    """

    name: ClassVar[str] = "fake"
    tables: dict[str, Table] = field(default_factory=dict)

    @asynccontextmanager
    async def open(self, engine: Engine) -> AsyncGenerator[Engine]:
        """Yield the engine itself."""
        yield engine

    async def setup(self, client: Engine) -> None:
        """Attach an empty database as `src`, and create the tables in it."""
        connection = cast("DuckDBEngine", client).connection
        connection.execute("ATTACH ':memory:' AS src")

        for name, table in self.tables.items():
            schema, _, short = name.partition(".")

            connection.execute(f'CREATE SCHEMA IF NOT EXISTS src."{schema}"')
            connection.register("pending", table)
            connection.execute(f'CREATE TABLE src."{schema}"."{short}" AS SELECT * FROM pending')
            connection.unregister("pending")


class FakeBigQueryCalls:
    """A BigQuery that only records the calls that a publish or a statement makes."""

    calls: ClassVar[list[tuple[str | None, ...]]] = []
    changed: ClassVar[int] = 3
    """The number of rows that every statement reports."""

    @classmethod
    @asynccontextmanager
    async def connect(cls, project: str) -> AsyncGenerator["FakeBigQueryCalls"]:
        """Yield a fake client for the project."""
        cls.calls = [("connect", project)]

        yield cls()

    async def create_dataset(self, dataset: str) -> None:
        """Record the dataset."""
        self.calls.append(("dataset", dataset))

    async def sync_delta_table(self, table: str, uri: str, connection_id: str) -> None:
        """Record the Delta table."""
        self.calls.append(("delta", table, uri, connection_id))

    async def execute(self, sql: str) -> int:
        """Record the statement and report `changed` rows."""
        self.calls.append(("execute", sql))
        return self.changed


@pytest.fixture
def bigquery(monkeypatch: pytest.MonkeyPatch) -> type[FakeBigQueryCalls]:
    """Replace BigQuery with the one that records the calls."""
    monkeypatch.setattr(utils, "BigQuery", FakeBigQueryCalls)
    monkeypatch.setattr(tasks, "BigQuery", FakeBigQueryCalls)

    return FakeBigQueryCalls


@pytest.fixture
def store() -> MemoryStore:
    """Return an in-memory store, in place of the S3 store of a bucket."""
    return MemoryStore()


@pytest.fixture(scope="module")
def fake_gcs() -> Iterator[FakeGcs]:
    """Serve a fake GCS, and point `deltalake` (environment) and `obstore` (settings) at it."""
    server = FakeGcs()

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("GOOGLE_BASE_URL", server.url)
        patch.setenv("GOOGLE_SKIP_SIGNATURE", "true")
        patch.setattr(settings, "GCS_ENDPOINT", server.endpoint)
        patch.setattr(settings, "GCS_USE_SSL", False)
        patch.setattr(settings, "GCS_HMAC_ACCESS_ID", "key")
        patch.setattr(settings, "GCS_HMAC_SECRET", SecretStr("secret"))

        try:
            yield server
        finally:
            server.stop()


@pytest.fixture(scope="module")
def fake_bigquery() -> Iterator[type[FakeBigQuery]]:
    """Replace BigQuery with the one that detects the schema of a Delta table."""
    FakeBigQuery.tables.clear()

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(utils, "BigQuery", FakeBigQuery)

        yield FakeBigQuery


@dataclass(frozen=True)
class SeenRequest:
    """One request that the fake API received."""

    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    body: bytes


class FakeApi:
    """An HTTP API on a free local port that answers from a queue of replies.

    A reply is a status and a JSON body. With an empty queue it answers 200 with `{"ok": true}`.
    It records every request, so a test sees what was sent.
    """

    def __init__(self) -> None:
        """Start the server in a thread."""
        self.replies: deque[tuple[int, object]] = deque()
        self.requests: list[SeenRequest] = []
        api = self

        class Handler(BaseHTTPRequestHandler):
            """Record the request and send the next reply."""

            def handle_request(self) -> None:
                """Answer any method."""
                size = int(self.headers.get("Content-Length") or 0)
                parsed = urlparse(self.path)
                api.requests.append(
                    SeenRequest(
                        self.command,
                        parsed.path,
                        parse_qs(parsed.query),
                        {name.lower(): value for name, value in self.headers.items()},
                        self.rfile.read(size),
                    )
                )
                status, body = api.replies.popleft() if api.replies else (200, {"ok": True})
                data = dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = do_PUT = handle_request

            @override
            def log_message(self, format: str, *args: object) -> None:
                """Keep the test output clean."""

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        """Return the base URL of the server."""
        return f"http://127.0.0.1:{self.server.server_port}"


@pytest.fixture
def fake_api() -> Iterator[FakeApi]:
    """Serve a fake HTTP API, and stop it after the test."""
    api = FakeApi()

    yield api

    api.server.shutdown()


@pytest.fixture
def fake_database() -> FakeDatabase:
    """Return an empty database in memory."""
    return FakeDatabase()


class FakeOracleError(Exception):
    """The error of an Oracle call, with the number that `oracledb` reports."""

    def __init__(self, code: int) -> None:
        """Keep the number of the error."""
        super().__init__(code)
        self.code = code


class FakeOracle:
    """An Oracle connection that describes canned columns and answers each read with `rows_for`.

    It records every read. `snapshot_error` fails the read of the system change number,
    and `read_errors` fail the next windows that are read as of one.
    """

    def __init__(
        self,
        columns: Sequence[tuple[str, str, int | None, int | None]],
        rows_for: Callable[[str, Sequence[object]], Table],
    ) -> None:
        """Start with the description of the columns and the function that answers a read."""
        self.columns = [self.column(*column) for column in columns]
        self.description = self.columns
        self.rows_for = rows_for
        self.reads: list[tuple[str, list[object]]] = []
        self.snapshot_error: int | None = None
        self.read_errors: list[int] = []
        self.closed = False

    @staticmethod
    def column(name: str, kind: str, precision: int | None, scale: int | None) -> SimpleNamespace:
        """Return the description of one column, as a cursor gives it."""
        return SimpleNamespace(name=name, type=SimpleNamespace(name=kind), precision=precision, scale=scale)

    def cursor(self) -> "FakeOracle":
        """Return itself as the cursor."""
        return self

    def __enter__(self) -> "FakeOracle":
        """Enter the cursor."""
        return self

    def __exit__(self, *error: object) -> None:
        """Leave the cursor."""

    def parse(self, sql: str) -> None:
        """Describe a statement: the system change number has one column, and a read has the table's."""
        if self.snapshot_error is not None and ("v$database" in sql or "DBMS_FLASHBACK" in sql):
            raise DatabaseError(FakeOracleError(self.snapshot_error))
        self.description = [self.column("scn", "DB_TYPE_NUMBER", 18, 0)] if '"scn"' in sql else self.columns

    def execute(self, sql: str, params: Sequence[object]) -> None:
        """Run a statement of the session, which the fake only needs to accept."""

    def fetch_df_batches(
        self, sql: str, params: Sequence[object], size: int, requested_schema: Schema
    ) -> Iterator[Table]:
        """Record the read, fail if asked to, and yield what `rows_for` answers."""
        if '"scn"' in sql:
            yield arrow_table({"scn": arrow_array([4242], arrow_int64())})
            return
        if self.read_errors and "AS OF SCN" in sql:
            raise DatabaseError(FakeOracleError(self.read_errors.pop(0)))
        if "WHERE 1 = 0" in sql:
            yield self.rows_for(sql, params).slice(0, 0)
            return
        self.reads.append((sql, list(params)))
        yield self.rows_for(sql, params)

    def close(self) -> None:
        """Record that the connection closed."""
        self.closed = True


@pytest.fixture
def fake_oracle(monkeypatch: pytest.MonkeyPatch) -> Callable[[FakeOracle], FakeOracle]:
    """Return a function that makes the Oracle source connect to a fake."""

    def install(connection: FakeOracle) -> FakeOracle:
        """Replace `Oracle.connect` with one that yields the fake, and close it on exit."""

        @asynccontextmanager
        async def connect(_: DBConn) -> AsyncGenerator[Oracle]:
            """Yield an `Oracle` over the fake connection."""
            try:
                yield Oracle(connection=cast("oracle_Connection", cast("object", connection)))
            finally:
                connection.close()

        monkeypatch.setattr(Oracle, "connect", staticmethod(connect))
        return connection

    return install

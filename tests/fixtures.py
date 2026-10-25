"""The fake services that the tests use, each one with its fixture.

They are the fakes of GCS, BigQuery (one that detects the schema of a Delta table and one that
records calls), an HTTP API, a database, Oracle and an object store, and the pages that the
bcadastro CouchDB sends. They run in the process, with no network and no container.
`conftest.py` registers this module as a plugin, so a test asks for a fixture by name.
"""

import sys
from collections import deque
from collections.abc import AsyncGenerator, Callable, Iterator, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from hashlib import md5
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from json import dumps, loads
from multiprocessing import get_context
from pathlib import Path
from re import findall, search
from threading import Thread
from types import ModuleType, SimpleNamespace
from typing import ClassVar, Self, TypedDict, Unpack, cast, override
from urllib.parse import parse_qs, urlparse

import pytest
from deltalake import DeltaTable
from duckdb import DuckDBPyConnection, Error
from duckdb import connect as duckdb_connect
from fake_gcs_server import serve
from obstore.store import MemoryStore, ObjectStore
from oracledb import Connection as oracle_Connection
from oracledb import DatabaseError
from polars import DataFrame, from_arrow, read_parquet
from pyarrow import Schema, Table
from pyarrow import array as arrow_array
from pyarrow import int64 as arrow_int64
from pyarrow import table as arrow_table
from pyarrow.parquet import ParquetWriter
from pydantic import BaseModel, JsonValue
from pydantic.dataclasses import dataclass as pydantic_dataclass
from toolkit import gcs
from toolkit.chunks import Probe, Rows
from toolkit.executor.constants import DEFAULT_BATCH_SIZE, SOURCE_CONFIG
from toolkit.executor.engines.base import Engine
from toolkit.executor.engines.couchdb import write_page
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.requests import Files
from toolkit.executor.sources.backends.files import FilesSource
from toolkit.executor.sources.duckdb import DuckDBSource
from toolkit.executor.sources.models import CouchDBAllDocs
from toolkit.executor.sources.protocols import DBConn, Source
from toolkit.executor.sources.registry import get_source
from toolkit.executor.types import Origin
from toolkit.gcs import create_store
from toolkit.oracle import Oracle
from toolkit.types import Columns, QueryParams, TableReport

from utils import delta, tasks
from utils.couchdb import BCADASTRO
from utils.delta import Destination, dump_to_delta, publish
from utils.origins import read_request
from utils.records import write_json_lines
from utils.types import DatabaseDumpOptions, DumpMode, DumpOptions, Read

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

POLL_SECONDS = 0.01


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


@pydantic_dataclass(frozen=True, config=SOURCE_CONFIG)
class FakeDatabaseSource(DuckDBSource):
    """A database in memory, attached as `src` the way the real extension attaches one.

    A table named `schema.name` is read as `"src"."schema"."name"`, so a dump that reads
    an attached table runs the same SQL as it does against Postgres. It is a source that
    DuckDB reads in place, so the `DuckDBEngine` serves it.
    """

    tables: dict[str, Table] = field(default_factory=dict[str, Table])

    @override
    async def setup(self, engine: Engine) -> None:
        """Attach an empty database as `src`, and create the tables in it."""
        connection = cast("DuckDBEngine", engine).connection
        connection.execute("ATTACH ':memory:' AS src")

        for name, table in self.tables.items():
            schema, _, short = name.partition(".")

            connection.execute(f'CREATE SCHEMA IF NOT EXISTS src."{schema}"')
            connection.register("pending", table)
            connection.execute(f'CREATE TABLE src."{schema}"."{short}" AS SELECT * FROM pending')
            connection.unregister("pending")


@dataclass(frozen=True)
class ConcurrentFilesSource:
    """Local files that an engine reads with `workers` chunks at the same time."""

    probe_kind: ClassVar[str] = ""
    range_column_needed: ClassVar[bool] = False

    workers: int
    origin: Origin | None = None


@dataclass
class ConcurrentFilesEngine(Engine):
    """An engine that reads the files of its source through DuckDB, in `workers` chunks."""

    source_type: ClassVar[type[Source]] = ConcurrentFilesSource

    source: ConcurrentFilesSource
    inner: DuckDBEngine = field(init=False, repr=False)

    @property
    @override
    def concurrency(self) -> int:
        """Return the chunks that the source reads at the same time."""
        return self.source.workers

    @override
    async def open(self, stack: AsyncExitStack) -> None:
        """Open the DuckDB engine that reads the files."""
        self.inner = await stack.enter_async_context(DuckDBEngine(FilesSource(origin=self.source.origin)))

    @override
    def clone(self) -> Self:
        """Return a new unopened engine of the source."""
        return replace(self)

    @override
    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run the statement on the DuckDB engine."""
        await self.inner.execute(sql, params=params)

    @override
    def query(
        self, sql: str, *, params: QueryParams | None = None, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> AsyncGenerator[DataFrame]:
        """Run the query on the DuckDB engine."""
        return self.inner.query(sql, params=params, batch_size=batch_size)

    @override
    def read(self, *, batch_size: int = DEFAULT_BATCH_SIZE) -> AsyncGenerator[DataFrame]:
        """Read the files whole."""
        return self.inner.read(batch_size=batch_size)

    @override
    async def describe(self) -> dict[str, str]:
        """Describe the files."""
        return await self.inner.describe()

    @override
    async def materialize(self, target: Path, rows: Rows, columns: Columns = ()) -> None:
        """Write a chunk of the files."""
        await self.inner.materialize(target, rows, columns)


@dataclass(frozen=True)
class ProbedFilesSource:
    """Local files with a probe that the test controls.

    `tokens` gives the token of each chunk, by its key, and `downloads` and `probes` record
    the chunks that were read and probed. They are shared by every engine of the source.
    """

    tokens: dict[str, int | None]
    downloads: list[str] = field(default_factory=list[str])
    probes: list[str] = field(default_factory=list[str])
    origin: Origin | None = None
    probe_kind: ClassVar[str] = "test-1"
    range_column_needed: ClassVar[bool] = False


@dataclass(frozen=True)
class ProbedFilesSourceV2(ProbedFilesSource):
    """The same source with another kind of token."""

    probe_kind: ClassVar[str] = "test-2"


@dataclass
class ProbedFilesEngine(Engine):
    """An engine that reads local files through DuckDB, and probes with canned tokens."""

    source_type: ClassVar[type[Source]] = ProbedFilesSource

    source: ProbedFilesSource
    inner: DuckDBEngine = field(init=False, repr=False)

    @override
    async def probe(self, rows: Rows, known: int | None, probe: Probe) -> bool:
        """Take the canned token of the chunk, and download unless it is the known one."""
        key = rows.chunk.key if rows.chunk else "all"
        self.source.probes.append(key)
        self.token = self.source.tokens.get(key)

        return self.token is None or self.token != known

    @override
    async def open(self, stack: AsyncExitStack) -> None:
        """Open the DuckDB engine that reads the files."""
        self.inner = await stack.enter_async_context(DuckDBEngine(FilesSource(origin=self.source.origin)))

    @override
    def clone(self) -> Self:
        """Return a new unopened engine of the source."""
        return replace(self)

    @override
    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run the statement on the DuckDB engine."""
        await self.inner.execute(sql, params=params)

    @override
    def query(
        self, sql: str, *, params: QueryParams | None = None, batch_size: int = DEFAULT_BATCH_SIZE
    ) -> AsyncGenerator[DataFrame]:
        """Run the query on the DuckDB engine."""
        return self.inner.query(sql, params=params, batch_size=batch_size)

    @override
    def read(self, *, batch_size: int = DEFAULT_BATCH_SIZE) -> AsyncGenerator[DataFrame]:
        """Read the files whole."""
        return self.inner.read(batch_size=batch_size)

    @override
    async def describe(self) -> dict[str, str]:
        """Describe the files."""
        return await self.inner.describe()

    @override
    async def materialize(self, target: Path, rows: Rows, columns: Columns = ()) -> None:
        """Write a chunk of the files, and record it."""
        self.source.downloads.append(rows.chunk.key if rows.chunk else "all")
        await self.inner.materialize(target, rows, columns)


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
    monkeypatch.setattr(delta, "BigQuery", FakeBigQueryCalls)
    monkeypatch.setattr(tasks, "BigQuery", FakeBigQueryCalls)

    return FakeBigQueryCalls


@pytest.fixture
def store() -> MemoryStore:
    """Return an in-memory store, in place of the S3 store of a bucket."""
    return MemoryStore()


@pytest.fixture(scope="session")
def fake_gcs() -> Iterator[FakeGcs]:
    """Serve a fake GCS, and point `deltalake` (environment) and `obstore` (settings) at it."""
    server = FakeGcs()

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("GOOGLE_BASE_URL", server.url)
        patch.setenv("GOOGLE_SKIP_SIGNATURE", "true")
        patch.setenv("GCS_ENDPOINT", server.endpoint)
        patch.setenv("GCS_USE_SSL", "false")
        patch.setenv("GCS_HMAC_ACCESS_ID", "key")
        patch.setenv("GCS_HMAC_SECRET", "secret")

        try:
            yield server
        finally:
            server.stop()


@pytest.fixture(scope="module")
def fake_bigquery() -> Iterator[type[FakeBigQuery]]:
    """Replace BigQuery with the one that detects the schema of a Delta table."""
    FakeBigQuery.tables.clear()

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(delta, "BigQuery", FakeBigQuery)

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
        Thread(target=self.server.serve_forever, args=(POLL_SECONDS,), daemon=True).start()

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
def fake_database() -> FakeDatabaseSource:
    """Return an empty database in memory."""
    return FakeDatabaseSource()


class FakeOracleError(Exception):
    """The error of an Oracle call, with the number that `oracledb` reports."""

    def __init__(self, code: int) -> None:
        """Keep the number of the error."""
        super().__init__(code)
        self.code = code


class FakeOracle:
    """An Oracle connection that describes canned columns and answers each read with `rows_for`.

    It records every read. `snapshot_error` fails the read of the system change number,
    and `read_errors` fail the next chunks that are read as of one.
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
        self.sizes: list[int] = []
        self.scn_reads = 0
        self.snapshot_error: int | None = None
        self.read_errors: list[int] = []
        self.closed = False

    @staticmethod
    def column(name: str, kind: str, precision: int | None, scale: int | None) -> SimpleNamespace:
        """Return the description of one column, as a cursor gives it."""
        return SimpleNamespace(name=name, type=SimpleNamespace(name=kind), precision=precision, scale=scale)

    def chosen(self, sql: str) -> list[SimpleNamespace]:
        """Return the columns that a read selects, or all of them for `SELECT *`."""
        found = search(r'SELECT ((?:"\w+"(?:, )?)+)\s+FROM "APP"', sql)
        names = findall(r'"(\w+)"', found.group(1)) if found else []

        return [column for column in self.columns if column.name in names] if names else self.columns

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
        self.description = [self.column("scn", "DB_TYPE_NUMBER", 18, 0)] if '"scn"' in sql else self.chosen(sql)

    def execute(self, sql: str, params: Sequence[object]) -> None:
        """Run a statement of the session, which the fake only needs to accept."""

    def fetch_df_batches(
        self, sql: str, params: Sequence[object], size: int, requested_schema: Schema
    ) -> Iterator[Table]:
        """Record the size of a read, fail if asked to, and yield what `rows_for` answers."""
        if '"scn"' in sql:
            self.scn_reads += 1
            yield arrow_table({"scn": arrow_array([4242], arrow_int64())})
            return
        self.sizes.append(size)
        if self.read_errors and "AS OF SCN" in sql:
            raise DatabaseError(FakeOracleError(self.read_errors.pop(0)))
        if "WHERE 1 = 0" in sql:
            yield self.rows_for(sql, params).slice(0, 0)
            return
        self.reads.append((sql, list(params)))
        yield self.rows_for(sql, params).select([column.name for column in self.chosen(sql)])

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


UPDATE_SEQ = "616823122-g1AAAAKTeJzLYWBgEMhgTmHgzcvPy09JdcjLz8gvLskBCjPlsQBJhgdA6v___--zkph8Eo_iUJpkACST7IHqQMr"
"""An `update_seq` in the form of the real instance: a number, a dash and opaque text."""

PEOPLE = {
    272: ("JOSÉ AUGUSTO DE SÁ", "MARIA DA CONCEIÇÃO DE SÁ"),
    353: ("ANA BEATRIZ LIMA", "LÚCIA HELENA LIMA"),
    8001: ("CARLOS EDUARDO NUNES", "ROSA MARIA NUNES"),
}
"""Made-up people and their mothers, with accents like the real names have."""


def revision(key: str) -> str:
    """Return a revision as CouchDB writes it: a number, a dash and 32 hexadecimal digits."""
    return f"1-{md5(key.encode(), usedforsecurity=False).hexdigest()}"


def cpf_document(number: int) -> dict[str, JsonValue]:
    """Return a person of `chcpf_bcadastros`, with the fields of the real documents."""
    key = f"{number:011d}"
    name, mother = PEOPLE[number]

    return {
        "_id": key,
        "_rev": revision(key),
        "anoExerc": "2007",
        "bairro": "AURORA OESTE",
        "cep": "74000000",
        "codMunDomic": "9373",
        "codNatOcup": "011",
        "codOcup": "000",
        "codSexo": "1",
        "codSitCad": "8",
        "codUA": "0120100",
        "cpfId": key,
        "dtInscricao": "20070322",
        "dtNasc": "19660331",
        "dtUltAtualiz": "20230801",
        "indEstrangeiro": "N",
        "indResExt": "N",
        "logradouro": "NATIVIDADE",
        "nomeContribuinte": name,
        "nomeMae": mother,
        "nroLogradouro": "109",
        "tipoLogradouro": "RUA",
        "ufMunDomic": "GO",
        "~version": "CgYDBgsdAQ4=",
    }


def cnpj_document(number: int) -> dict[str, JsonValue]:
    """Return a company of `chcnpj_bcadastros` with two partners, as the real ones are."""
    key = f"{number:08d}"

    return {
        "_id": key,
        "_rev": revision(key),
        "capitalSocial": "00000020000000",
        "cpfResponsavel": "12831060869",
        "dataInclusaoResponsavel": "20210324",
        "enteFederativo": "",
        "naturezaJuridica": "2062",
        "nomeEmpresarial": "CONFEITARIA SÃO JOÃO LTDA",
        "porteEmpresa": "01",
        "qualificacaoResponsavel": "49",
        "socios": [
            {
                "codigoPais": "",
                "cpfCnpj": "00012831060869",
                "cpfRepresentanteLegal": "",
                "dataEntrada": "20210324",
                "nomeSocioEstrangeiro": "",
                "qualificacaoRepresentanteLegal": "00",
                "qualificacaoSocio": "49",
                "tipo": "2",
            },
            {
                "codigoPais": "",
                "cpfCnpj": "00045678901234",
                "cpfRepresentanteLegal": "",
                "dataEntrada": "20220110",
                "nomeSocioEstrangeiro": "",
                "qualificacaoRepresentanteLegal": "00",
                "qualificacaoSocio": "22",
                "tipo": "2",
            },
        ],
        "~version": "CgYDBHFNAQg=",
    }


def all_docs_page(documents: Sequence[Mapping[str, JsonValue]], total_rows: int) -> bytes:
    """Return a page of `_all_docs` with documents, as CouchDB writes it.

    The head holds `total_rows`, `offset` and `update_seq`, and each row sits on its own line.
    """
    rows = ",\n".join(
        dumps(
            {"id": document["_id"], "key": document["_id"], "value": {"rev": document["_rev"]}, "doc": document},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        for document in documents
    )
    head = dumps({"total_rows": total_rows, "offset": 0, "update_seq": UPDATE_SEQ}, separators=(",", ":"))

    return f'{head[:-1]},"rows":[\n{rows}\n]}}\n'.encode()


@pytest.fixture
def cpf_page() -> bytes:
    """Return a page of `_all_docs` of `chcpf_bcadastros`, with three people."""
    return all_docs_page([cpf_document(number) for number in PEOPLE], 279_833_107)


@pytest.fixture
def cnpj_page() -> bytes:
    """Return a page of `_all_docs` of `chcnpj_bcadastros`, with one company."""
    return all_docs_page([cnpj_document(27329521)], 144_691_433)


def changes_page(changes: Sequence[tuple[int, Mapping[str, JsonValue]]], pending: int) -> bytes:
    """Return a page of `_changes` with documents, as CouchDB writes it.

    A document with `_deleted` is a deletion: the change has `deleted` and the document is a tombstone.
    """
    results = []

    for number, document in changes:
        result: dict[str, JsonValue] = {
            "seq": f"{number}-g1AAAAKTeJzLYWBg",
            "id": document["_id"],
            "changes": [{"rev": document["_rev"]}],
        }

        if document.get("_deleted"):
            result["deleted"] = True

        results.append(dumps({**result, "doc": document}, ensure_ascii=False, separators=(",", ":")))

    last_seq = f"{changes[-1][0]}-g1AAAAKTeJzLYWBg"
    tail = dumps({"last_seq": last_seq, "pending": pending}, separators=(",", ":"))

    return f'{{"results":[\n{",\n".join(results)}\n],\n{tail[1:]}\n'.encode()


@pytest.fixture
def cpf_changes_page() -> bytes:
    """Return a page of `_changes` of `chcpf_bcadastros`: two updates and one deletion."""
    erased = {"_id": "00000008001", "_rev": f"2-{revision('00000008001')[2:]}", "_deleted": True}

    return changes_page([(2, cpf_document(272)), (4, cpf_document(353)), (12, erased)], pending=0)


@pytest.fixture(scope="module")
def read_gs_uri():
    """Provide the `read_gs_uri` helper."""

    def read_gs_uri(destination: Destination) -> str:
        """Return the `gs://` URI, which `object_store` reads from the fake bucket."""
        return f"gs://{destination.bucket}/{destination.prefix}"

    return read_gs_uri


SIMULATED_PROJECT = "simulated-project"


@dataclass(frozen=True)
class SimulatedCloud:
    """A fake GCS and a fake BigQuery."""

    gcs: FakeGcs
    bigquery: type[FakeBigQuery]
    bucket: str = "simulated-bucket"
    project: str = SIMULATED_PROJECT

    def destination(self, name: str) -> Destination:
        """Return the destination of a table of this run."""
        return Destination(self.bucket, "simulated_dataset", name)

    async def publish(self, destination: Destination) -> None:
        """Create or refresh the fake BigQuery table of a destination."""
        await publish(destination, self.project, f"{self.project}.us.connection")

    def read(self, destination: Destination) -> DataFrame:
        """Return the rows that the fake BigQuery returns, with the columns it detected."""
        detected = self.bigquery.tables[destination.table(self.project)]
        return cast("DataFrame", from_arrow(DeltaTable(detected.uri).to_pyarrow_table(columns=list(detected.schema))))

    def types(self, destination: Destination) -> dict[str, str]:
        """Return the types that the fake BigQuery detected."""
        return self.bigquery.tables[destination.table(self.project)].schema

    def store(self) -> ObjectStore:
        """Return the store of the fake bucket."""
        return create_store(self.bucket)


@pytest.fixture(scope="module")
def cloud(fake_gcs: FakeGcs, fake_bigquery: type[FakeBigQuery], read_gs_uri) -> Iterator[SimulatedCloud]:
    """Return the simulated cloud, where `deltalake`, `obstore` and BigQuery reach the fakes.

    `Destination.uri` is `gcs://` (OpenDAL), which has no setting for a fake endpoint, so it
    reads `gs://` here.
    """
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(Destination, "uri", property(read_gs_uri))
        yield SimulatedCloud(fake_gcs, fake_bigquery)


@pytest.fixture
def dump_to_cloud():
    """Provide the `dump_to_cloud` helper."""

    async def dump_to_cloud(
        database: FakeDatabaseSource,
        destination: Destination,
        dump_mode: DumpMode = "append",
        table: str = "public.orders",
        **options: Unpack[DumpOptions],
    ) -> None:
        """Dump a table of the database into the Delta table of a destination."""
        values = read_request("postgres", {"table": table}, "chunks" in options)
        await dump_to_delta(replace(database, origin=values), destination.uri, dump_mode, options)

    return dump_to_cloud


@pytest.fixture
def capture():
    """Provide the `capture` helper."""

    async def capture(destination: Destination, folder, records: list[dict[str, JsonValue]], moment: datetime) -> None:
        """Write the records as JSON lines, and add them to the table as the capture of a moment."""
        (folder / "records.json").unlink(missing_ok=True)
        write_json_lines(records, folder)
        values = Files(f"{folder}/**/*.json", "json")
        await dump_to_delta(FilesSource(origin=values), destination.uri, "append", {"capture": moment})

    return capture


@pytest.fixture
def stage_page():
    """Provide the `stage_page` helper."""

    def stage_page(page: bytes, path: Path) -> CouchDBAllDocs:
        """Write a page of `_all_docs` as the bcadastro source writes it, and return the page."""
        parsed = CouchDBAllDocs.model_validate_json(page)

        with ParquetWriter(path, BCADASTRO.columns) as writer:
            write_page(writer, BCADASTRO, parsed.rows, parsed.update_seq)

        return parsed

    return stage_page


@pytest.fixture
def query_rows():
    """Provide the `query_rows` helper."""

    async def query_rows(path: Path, sql: str) -> list[tuple[object, ...]]:
        """Run a query over the staged file, and return its rows."""
        async with DuckDBEngine() as engine:
            [dataframe] = [batch async for batch in engine.query(sql.format(path=path))]

        return dataframe.rows()

    return query_rows


@pytest.fixture
def dump_oracle():
    """Provide the `dump_oracle` helper."""

    async def dump_oracle(uri: Path, read: Read, **options: Unpack[DatabaseDumpOptions]) -> None:
        """Dump an Oracle read to a local Delta table, with the batch size as the flow gives it."""
        conn = DBConn("h", 1521, "SVC", "u", "p")
        given: dict[str, int] = {"batch_size": options["batch_size"]} if "batch_size" in options else {}
        origin = read_request("oracle", read, "chunks" in options)
        source = get_source("oracle")(conn=conn, origin=origin, **given)
        await dump_to_delta(source, str(uri), "append", {"probe": False, **options})

    return dump_oracle


@pytest.fixture
def commits():
    """Provide the `commits` helper."""

    def commits(uri: Path) -> int:
        """Return how many commits the table has."""
        return DeltaTable(str(uri)).version() + 1

    return commits


class FakeRunner:
    """A `PrefectDbtRunner` that records the settings and the arguments."""

    instances: ClassVar[list[FakeRunner]] = []
    error: ClassVar[Exception | None] = None
    result: ClassVar[SimpleNamespace] = SimpleNamespace(success=True)

    def __init__(self, **kwargs: object) -> None:
        """Record the keyword arguments and register the instance."""
        self.kwargs = kwargs
        self.invoked: list[list[str]] = []
        FakeRunner.instances.append(self)

    @property
    def settings(self):
        """Return the settings that the runner got."""
        return self.kwargs["settings"]

    def invoke(self, arguments: list[str]) -> SimpleNamespace:
        """Record the arguments, and fail if an error is set. `deps` always works."""
        self.invoked.append(arguments)
        if arguments == ["deps"]:
            return SimpleNamespace(success=True)
        if FakeRunner.error is not None:
            raise FakeRunner.error
        return FakeRunner.result


@pytest.fixture
def fake_runner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Replace the dbt runner, and clone into a folder of the test."""
    project = tmp_path / "queries"
    project.mkdir()
    FakeRunner.instances = []
    FakeRunner.error = None
    FakeRunner.result = SimpleNamespace(success=True)
    monkeypatch.setattr(tasks, "PrefectDbtRunner", FakeRunner)
    monkeypatch.setattr(tasks, "clone_repository", lambda repository, destination, branch: project)
    return FakeRunner


@pytest.fixture
def dbt_node():
    """Provide the `dbt_node` helper."""

    def dbt_node(name: str, status: str, message: str = "") -> SimpleNamespace:
        """Return a dbt result for a node, as dbt gives it."""
        return SimpleNamespace(
            status=status, message=message, node=SimpleNamespace(name=name, unique_id=f"model.x.{name}")
        )

    return dbt_node


@pytest.fixture
def report():
    """Provide the `report` helper."""

    def report(files: int) -> TableReport:
        """Return the state of a Delta table that has this many files."""
        return TableReport(version=0, files=files, bytes=0, last_commit=datetime.now(UTC), keys=[])

    return report


@pytest.fixture
def stored_rows():
    """Provide the `stored_rows` helper."""

    async def stored_rows(store, prefix: str) -> list[int]:
        """Return the ids of every Parquet object under a prefix."""
        ids: list[int] = []
        for key in await gcs.list_keys(store, prefix):
            data = bytes(await (await store.get_async(key)).bytes_async())
            ids += read_parquet(BytesIO(data))["id"].to_list()
        return sorted(ids)

    return stored_rows


@pytest.fixture
def put():
    """Provide the `put` helper."""

    async def put(store, *keys: str) -> None:
        """Store a small object under each key."""
        for key in keys:
            await store.put_async(key, b"old")

    return put


@pytest.fixture
def stamps():
    """Provide the `stamps` helper."""

    def stamps(lake) -> dict[int, datetime]:
        """Return the extraction time of each id."""
        table = DeltaTable(lake.uri).to_pyarrow_table()
        ids, times = table.column("id").to_pylist(), table.column("_prefect_extracted_at").to_pylist()
        return cast("dict[int, datetime]", dict(zip(ids, times, strict=True)))

    return stamps


@pytest.fixture
def naive():
    """Provide the `naive` helper."""

    def naive(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
        """Return a timestamp without a time zone, as a database column holds it."""
        return datetime(year, month, day, hour, minute, tzinfo=UTC).replace(tzinfo=None)

    return naive


@pytest.fixture(scope="module")
def delta_reader() -> DuckDBPyConnection:
    """Return a DuckDB connection with the delta extension, an independent reader of the log."""
    connection = duckdb_connect()
    try:
        connection.execute("INSTALL delta")
        connection.execute("LOAD delta")
    except Error:
        pytest.skip("The DuckDB delta extension is not available")
    return connection


class ItemModel(BaseModel):
    id: int
    nome: str


@dataclass
class ItemData:
    id: int
    nome: str


class ItemDict(TypedDict):
    id: int
    nome: str


@pytest.fixture
def schemas(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Put a module with the three kinds of schema, and a name that is no type."""
    module = ModuleType("schemas_of_the_test")
    module.__dict__.update(MODEL=ItemModel, DATA=ItemData, DICT=ItemDict, NOT_A_TYPE="text")
    monkeypatch.setitem(sys.modules, "schemas_of_the_test", module)

    return module

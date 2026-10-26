"""`dump_to_delta` end to end on every engine, into a local Delta table."""

import json
from asyncio import sleep as sleep_async
from base64 import b64encode
from datetime import date
from pathlib import Path
from time import sleep as sleep_blocking
from urllib.request import Request, urlopen

import psycopg
import pytest
from deltalake import DeltaTable
from parity_helpers import DOCUMENTS, canonical, column, expected
from parity_lab import available, build_source, environment
from pymongo import MongoClient
from pymongoarrow.api import find_arrow_all
from toolkit.chunks import split_chunks
from toolkit.constants import BATCH_COLUMN
from toolkit.executor import couchdb as couchdb_module
from toolkit.executor.engines import mongodb as mongodb_module
from toolkit.types import Chunks

from utils.delta import dump_to_delta

pytestmark = pytest.mark.parity

TODAY = date(2024, 6, 1)


def build_chunks(engine: str) -> Chunks:
    """Return chunks of 100 ids over the id column of an engine, from 0 up to 399."""
    return Chunks(column=column(engine), width=100, begin=0, end=399)


def load_table(uri: Path, engine: str):
    """Return the rows of the Delta table, and the chunk of each id.

    MongoDB keeps `_id` as `id` and the business id as `id_1`, because the names of
    the columns are normalized: the lab maps them back so the engines compare.
    """
    table = DeltaTable(str(uri)).to_pyarrow_table()

    if engine == "mongodb":
        table = table.drop_columns(["id"]).rename_columns(
            ["id" if name == "id_1" else name for name in table.drop_columns(["id"]).column_names]
        )

    chunks_of_ids = {int(row["id"]): row[BATCH_COLUMN] for row in table.to_pylist()}

    return table, chunks_of_ids


async def dump(engine: str, uri: Path, **options: object) -> None:
    """Dump `orders` of an engine into a Delta table."""
    await dump_to_delta(build_source(engine, **options), str(uri), "append", {"chunks": build_chunks(engine)})


async def test_every_engine_loads_the_same_rows_in_the_same_chunks(engine, tmp_path) -> None:
    """Load the 303 rows that the chunks cover, each in the chunk that holds its id."""
    await dump(engine, tmp_path / "t")

    table, chunks_of_ids = load_table(tmp_path / "t", engine)
    cut = split_chunks(build_chunks(engine), TODAY)
    owner = {number: chunk.key for chunk in cut for number in range(chunk.start, chunk.stop)}

    assert canonical(table) == expected(lambda row: row["id"] < 400)
    assert chunks_of_ids == {number: owner[number] for number in chunks_of_ids}


async def test_a_second_run_writes_nothing(engine, tmp_path) -> None:
    """Skip every chunk whose fingerprint is already in the ledger."""
    await dump(engine, tmp_path / "t")
    first = DeltaTable(str(tmp_path / "t")).version()

    await dump(engine, tmp_path / "t")

    assert DeltaTable(str(tmp_path / "t")).version() == first


@pytest.mark.parametrize("workers", [2, 4])
async def test_concurrent_chunks_load_the_same_table(tmp_path, workers) -> None:
    """Write the same rows whether CouchDB and MongoDB read one chunk or several."""
    for name in DOCUMENTS:
        await dump(name, tmp_path / f"{name}-1", concurrency=1)
        await dump(name, tmp_path / f"{name}-n", concurrency=workers)

        one, _ = load_table(tmp_path / f"{name}-1", name)
        many, _ = load_table(tmp_path / f"{name}-n", name)
        assert canonical(many) == canonical(one) == expected(lambda row: row["id"] < 400)


def couchdb_update(number: int, nome: str) -> None:
    """Rename one document of CouchDB."""
    entry = environment()["couchdb"]
    base = f"http://{entry['host']}:{entry['port']}/orders/{number:011d}"
    token = b64encode(f"{entry['user']}:{entry['password']}".encode()).decode()

    def call(method: str, body: object | None = None):
        request = Request(base, data=None if body is None else json.dumps(body).encode(), method=method)
        request.add_header("Authorization", f"Basic {token}")
        request.add_header("Content-Type", "application/json")
        with urlopen(request) as response:
            return json.loads(response.read())

    document = call("GET")
    document["nome"] = nome
    call("PUT", document)


def mongodb_update(number: int, nome: str) -> None:
    """Rename one document of MongoDB."""
    entry = environment()["mongodb"]
    client = MongoClient(
        entry["host"], entry["port"], username=entry["user"], password=entry["password"], authSource="admin"
    )
    client["lab"]["orders"].update_one({"id": number}, {"$set": {"nome": nome}})


def postgres_update(number: int, nome: str) -> None:
    """Rename one row of Postgres."""
    entry = environment()["postgres"]
    with psycopg.connect(
        host=entry["host"],
        port=entry["port"],
        dbname="lab",
        user=entry["user"],
        password=entry["password"],
        autocommit=True,
    ) as connection:
        connection.execute("UPDATE orders SET nome = %s WHERE id = %s", (nome, number))


UPDATES = {"postgres": postgres_update, "couchdb": couchdb_update, "mongodb": mongodb_update}


@pytest.mark.parametrize("name", sorted(UPDATES))
async def test_a_changed_row_replaces_its_chunk_and_only_that_one(name, tmp_path) -> None:
    """Replace the chunk of id 150 with one commit, and keep every other row as it was."""
    if name not in available():
        pytest.skip(f"{name} is not up")

    original = {1: None}
    await dump(name, tmp_path / "t")
    before = DeltaTable(str(tmp_path / "t")).version()
    original = {row[0]: row[1] for row in canonical(load_table(tmp_path / "t", name)[0])}
    nome = original[150]

    try:
        UPDATES[name](150, "alterado")
        await dump(name, tmp_path / "t")
        after = {row[0]: row[1] for row in canonical(load_table(tmp_path / "t", name)[0])}
    finally:
        UPDATES[name](150, nome)

    assert after[150] == "alterado"
    assert {key: value for key, value in after.items() if key != 150} == {
        key: value for key, value in original.items() if key != 150
    }
    assert DeltaTable(str(tmp_path / "t")).version() > before


async def test_date_chunks_load_every_row_in_the_chunk_of_its_month(engine, tmp_path) -> None:
    """Cut by month over the date column, and load the null chunk last, on every engine with a date."""
    if engine == "couchdb":
        pytest.skip("CouchDB chunks cut only the id")

    name = {"oracle": "DIA"}.get(engine, "dia")
    chunks = Chunks(column=name, width="month", begin="2024-01-01", end="2024-03-31")
    await dump_to_delta(build_source(engine), str(tmp_path / "t"), "append", {"chunks": chunks})

    table, _ = load_table(tmp_path / "t", engine)
    month = {row["id"]: row[BATCH_COLUMN] for row in table.to_pylist()}

    assert canonical(table) == expected()
    assert {key for key, value in month.items() if value == "null"} == {301, 302, 303}
    assert month[150] == f"2024-0{1 + 150 % 3}-01"


async def test_chunks_really_overlap_on_the_real_server(monkeypatch, tmp_path) -> None:
    """Hold more than one request in flight on CouchDB, and more than one fetch on MongoDB, with 4 workers."""
    flying = {"now": 0, "peak": 0}

    original_get = couchdb_module.CouchDBClient.get

    async def slow_get(self, *args, **kwargs):
        flying["now"] += 1
        flying["peak"] = max(flying["peak"], flying["now"])
        try:
            await sleep_async(0.05)
            return await original_get(self, *args, **kwargs)
        finally:
            flying["now"] -= 1

    monkeypatch.setattr(couchdb_module.CouchDBClient, "get", slow_get)
    await dump("couchdb", tmp_path / "c", concurrency=4, batch_size=5)
    couch_peak = flying["peak"]

    flying.update(now=0, peak=0)
    original_find = find_arrow_all

    def slow_find(*args, **kwargs):
        flying["now"] += 1
        flying["peak"] = max(flying["peak"], flying["now"])
        try:
            sleep_blocking(0.05)
            return original_find(*args, **kwargs)
        finally:
            flying["now"] -= 1

    monkeypatch.setattr(mongodb_module, "find_arrow_all", slow_find)
    await dump("mongodb", tmp_path / "m", concurrency=4, batch_size=5)

    assert couch_peak > 1
    assert flying["peak"] > 1


async def test_mongodb_keeps_its_business_id_when_select_leaves_the_object_id_out(tmp_path) -> None:
    """README: `select` without `_id` leaves the field `id` with the name `id`."""
    await dump_to_delta(
        build_source("mongodb"),
        str(tmp_path / "t"),
        "append",
        {"chunks": build_chunks("mongodb"), "select": ["id", "nome", "dia", "valor"]},
    )

    table = DeltaTable(str(tmp_path / "t")).to_pyarrow_table()

    assert "id_1" not in table.column_names
    assert sorted(table["id"].to_pylist())[:3] == [1, 2, 3]


async def test_select_loads_only_the_columns_that_it_names(engine, tmp_path) -> None:
    """Keep the chosen columns and the chunk key, in the order of `select`, on every engine."""
    names = [column(engine), "NOME" if engine == "oracle" else "nome"]

    await dump_to_delta(
        build_source(engine), str(tmp_path / "t"), "append", {"chunks": build_chunks(engine), "select": names}
    )

    assert DeltaTable(str(tmp_path / "t")).to_pyarrow_table().column_names == ["id", "nome", BATCH_COLUMN]

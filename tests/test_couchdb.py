from dataclasses import replace
from json import loads
from pathlib import Path

import pytest
from deltalake import DeltaTable
from fixtures import PEOPLE, UPDATE_SEQ, all_docs_page, cpf_document, revision
from pyarrow.parquet import ParquetWriter, read_table
from pydantic import JsonValue
from toolkit.executor.couchdb import convert_change
from toolkit.executor.engines.couchdb import write_page
from toolkit.executor.requests import Table
from toolkit.executor.sources.backends.couchdb import CouchDBSource
from toolkit.executor.sources.models import CouchDBChanges
from toolkit.executor.sources.protocols import DBConn
from toolkit.types import Chunks
from yaml import safe_load

from utils import tasks
from utils.couchdb import BCADASTRO
from utils.delta import dump_to_delta
from utils.origins import read_request
from utils.types import Database, Read

CPF_IDS = ["00000000272", "00000000353", "00000008001"]


def test_the_bcadastro_columns_are_those_of_the_destination_without_the_airbyte_ones():
    """Keep every column of the destination but the `_airbyte_` ones."""
    assert BCADASTRO.columns.names == ["id", "doc", "key", "seq", "value", "last_seq"]


def test_a_page_of_cpf_documents_becomes_the_rows_of_the_destination(cpf_page, tmp_path, stage_page):
    """Keep the id, the revision and the sequence of each person, and the document whole."""
    path = tmp_path / "cpf.parquet"

    page = stage_page(cpf_page, path)
    table = read_table(path)
    rows = table.to_pylist()

    assert page.update_seq == UPDATE_SEQ
    assert table["id"].to_pylist() == CPF_IDS
    assert table["key"].to_pylist() == CPF_IDS
    assert table["seq"].to_pylist() == [None, None, None]
    assert table["last_seq"].to_pylist() == [UPDATE_SEQ] * 3
    assert [loads(row["doc"]) for row in rows] == [cpf_document(number) for number in PEOPLE]
    assert [loads(row["value"]) for row in rows] == [{"rev": cpf_document(number)["_rev"]} for number in PEOPLE]


async def test_duckdb_reads_the_cpf_documents_as_json(cpf_page, tmp_path, query_rows, stage_page):
    """Read the document as `JSON`, with the accents of the names kept."""
    path = tmp_path / "cpf.parquet"

    stage_page(cpf_page, path)
    rows = await query_rows(
        path,
        "SELECT typeof(doc), doc->>'$.nomeContribuinte', value->>'$.rev' FROM read_parquet('{path}') ORDER BY id",
    )

    assert rows == [("JSON", PEOPLE[number][0], cpf_document(number)["_rev"]) for number in PEOPLE]


async def test_a_cnpj_document_keeps_its_partners_as_nested_json(cnpj_page, tmp_path, query_rows, stage_page):
    """Keep the list of partners inside the document, so DuckDB reads it by path."""
    path = tmp_path / "cnpj.parquet"

    stage_page(cnpj_page, path)
    rows = await query_rows(
        path,
        "SELECT id, json_array_length(doc->'$.socios'), doc->>'$.socios[1].cpfCnpj' FROM read_parquet('{path}')",
    )

    assert rows == [("27329521", 2, "00045678901234")]


async def test_a_page_of_changes_keeps_the_sequence_and_the_deletions(cpf_changes_page, tmp_path, query_rows):
    """Write the sequence of each change, and a deletion as the tombstone of its document."""
    path = tmp_path / "changes.parquet"
    page = CouchDBChanges.model_validate_json(cpf_changes_page)
    items = [convert_change(change) for change in page.results]

    with ParquetWriter(path, BCADASTRO.columns) as writer:
        write_page(writer, BCADASTRO, items, page.last_seq)

    rows = await query_rows(
        path, "SELECT id, seq, last_seq, doc->>'$._deleted', value->>'$.rev' FROM read_parquet('{path}') ORDER BY id"
    )

    assert [(row[0], row[1], row[3]) for row in rows] == [
        ("00000000272", "2-g1AAAAKTeJzLYWBg", None),
        ("00000000353", "4-g1AAAAKTeJzLYWBg", None),
        ("00000008001", "12-g1AAAAKTeJzLYWBg", "true"),
    ]
    assert {row[2] for row in rows} == {"12-g1AAAAKTeJzLYWBg"}
    assert rows[2][4] == f"2-{revision('00000008001')[2:]}"


async def test_a_couchdb_database_is_dumped_in_chunks_over_its_text_ids(fake_api, tmp_path):
    """Cut the text ids by number, read what is outside them last, and keep each document."""
    event: dict[str, JsonValue] = {
        "_id": "CPF-20261020011517",
        "_rev": revision("CPF-20261020011517"),
        "tipo": "evento",
    }
    people = [cpf_document(number) for number in PEOPLE]
    pages: list[list[dict[str, JsonValue]]] = [[people[0], people[1]], [people[2]], [], [event]]
    fake_api.replies.append((200, {"db_name": "ignored"}))
    fake_api.replies.extend((200, loads(all_docs_page(documents, 4))) for documents in pages)
    conn = DBConn("127.0.0.1", fake_api.server.server_port, "ignored", "user", "password")
    source = CouchDBSource(conn, secure=False, key_width=11, concurrency=1)
    chunks: Chunks = {"column": "id", "width": 5_000, "begin": 0, "end": 9_999}
    read: Read = {"table": "chcpf_bcadastros"}

    await dump_to_delta(
        replace(source, origin=read_request("couchdb", read, True)),
        str(tmp_path / "t"),
        "append",
        {"chunks": chunks, "probe": False},
    )
    rows = DeltaTable(str(tmp_path / "t")).to_pyarrow_table().to_pylist()

    assert {row["id"]: row["batch"] for row in rows} == {
        "00000000272": "000000000000",
        "00000000353": "000000000000",
        "00000008001": "000000005000",
        "CPF-20261020011517": "null",
    }
    assert loads(next(row["doc"] for row in rows if row["id"] == "00000000272")) == people[0]
    assert [(r.path, r.query.get("startkey"), r.query.get("endkey")) for r in fake_api.requests] == [
        ("/ignored/", None, None),
        ("/chcpf_bcadastros/_all_docs", ['"0"'], ['"00000005000"']),
        ("/chcpf_bcadastros/_all_docs", ['"00000005000"'], ['"00000010000"']),
        ("/chcpf_bcadastros/_all_docs", None, ['"0"']),
        ("/chcpf_bcadastros/_all_docs", ['":"'], None),
    ]


async def test_the_options_of_the_database_choose_the_bcadastro_shape(monkeypatch):
    """Build the source from the shape and the key width that the `prefect.yaml` names."""
    seen = {}

    async def load(source, destination, dump_mode, options):
        """Keep what the dump would read and where it would write."""
        seen.update(source=source, mode=dump_mode, options=options, destination=destination)

    monkeypatch.setattr(tasks, "load_to_delta", load)
    monkeypatch.setenv("DB_USERNAME", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")
    database: Database = {
        "type": "couchdb",
        "host": "10.2.231.241",
        "port": 6984,
        "database": "unused",
        "options": {"shape": "utils.couchdb:BCADASTRO", "key_width": 3},
    }

    await tasks.dump_db_to_gcs.fn(
        database, {"dataset_id": "brutos_bcadastro", "table_id": "chcpf"}, {"table": "chcpf_bcadastros"}, bucket="b"
    )

    source = seen["source"]
    assert (source.shape, source.key_width, source.conn.hostname) == (BCADASTRO, 3, "10.2.231.241")
    assert (source.conn.user, source.conn.password) == ("user", "password")
    assert source.origin == Table("chcpf_bcadastros")


@pytest.mark.parametrize(
    ("kind", "attribute"),
    [
        pytest.param("couchdb", "batch_size", id="couchdb_page_size"),
        pytest.param("oracle", "batch_size", id="oracle_fetch_size"),
    ],
)
async def test_the_batch_size_of_the_options_reaches_the_source(monkeypatch, kind, attribute):
    """Merge `options.batch_size` into the options of the source that reads in chunks."""
    seen = {}

    async def load(source, *_):
        """Keep the source that the dump would read."""
        seen["source"] = source

    monkeypatch.setattr(tasks, "load_to_delta", load)
    monkeypatch.setenv("DB_USERNAME", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")
    database: Database = {"type": kind, "host": "10.0.0.1", "port": 1, "database": "db"}

    await tasks.dump_db_to_gcs.fn(
        database, {"dataset_id": "d", "table_id": "t"}, {"table": "APP.T"}, bucket="b", options={"batch_size": 7000}
    )

    assert getattr(seen["source"], attribute) == 7000


async def test_a_batch_size_on_a_database_that_duckdb_reads_is_refused(monkeypatch):
    """Raise for `batch_size` on postgres, which DuckDB streams, before any read."""
    monkeypatch.setenv("DB_USERNAME", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")
    database: Database = {"type": "postgres", "host": "10.0.0.1", "port": 5432, "database": "db"}

    with pytest.raises(ValueError, match="batch_size"):
        await tasks.dump_db_to_gcs.fn(
            database,
            {"dataset_id": "d", "table_id": "t"},
            {"table": "public.t"},
            bucket="b",
            options={"batch_size": 7000},
        )


async def test_the_concurrency_of_the_options_reaches_the_source(monkeypatch):
    """Merge `options.concurrency` into the options of a source that reads over HTTP."""
    seen = {}

    async def load(source, *_):
        """Keep the source that the dump would read."""
        seen["source"] = source

    monkeypatch.setattr(tasks, "load_to_delta", load)
    monkeypatch.setenv("DB_USERNAME", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")
    database: Database = {"type": "couchdb", "host": "10.0.0.1", "port": 6984, "database": "db"}

    await tasks.dump_db_to_gcs.fn(
        database, {"dataset_id": "d", "table_id": "t"}, {"table": "people"}, bucket="b", options={"concurrency": 4}
    )

    assert seen["source"].concurrency == 4


async def test_a_database_that_reads_one_chunk_at_a_time_refuses_concurrency(monkeypatch):
    """Raise for `concurrency` on postgres, which one DuckDB connection serves, before any read."""
    monkeypatch.setenv("DB_USERNAME", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")
    database: Database = {"type": "postgres", "host": "10.0.0.1", "port": 5432, "database": "db"}

    with pytest.raises(ValueError, match="concurrency"):
        await tasks.dump_db_to_gcs.fn(
            database,
            {"dataset_id": "d", "table_id": "t"},
            {"table": "public.t"},
            bucket="b",
            options={"concurrency": 2},
        )


@pytest.mark.parametrize(
    ("pipeline", "database", "key_width"),
    [
        pytest.param("rj_iplanrio__bcadastro_chcpf", "chcpf_bcadastros", None, id="people"),
        pytest.param("rj_iplanrio__bcadastro_chcnpj", "chcnpj_bcadastros", 8, id="companies"),
        pytest.param("rj_iplanrio__bcadastro_chcno", "chcno_bcadastros", None, id="works"),
        pytest.param("rj_iplanrio__bcadastro_chcaepf", "chcaepf_bcadastros", None, id="activities"),
    ],
)
async def test_a_bcadastro_pipeline_dumps_what_its_prod_schedule_says(monkeypatch, pipeline, database, key_width):
    """Build the dump from the parameters of the prod schedule: the database, the shape, the chunks and the table."""
    seen = {}

    async def load(source, destination, dump_mode, options):
        """Keep what the dump would read and where it would write."""
        seen.update(source=source, destination=destination, mode=dump_mode, options=options)

    config = safe_load((Path(__file__).parent.parent / "pipelines" / pipeline / "prefect.yaml").read_text())
    [prod] = [deployment for deployment in config["deployments"] if deployment["name"].endswith("--prod")]
    [schedule] = prod["schedules"]
    parameters = schedule["parameters"]
    monkeypatch.setattr(tasks, "load_to_delta", load)
    monkeypatch.setenv("DB_USERNAME", "user")
    monkeypatch.setenv("DB_PASSWORD", "password")

    await tasks.dump_db_to_gcs.fn(
        parameters["database"], parameters["table"], parameters["read"], bucket="b", options=parameters["options"]
    )

    source = seen["source"]
    assert (source.shape, source.origin, source.conn.hostname, source.conn.port) == (
        BCADASTRO,
        Table(database),
        "10.2.231.241",
        6984,
    )
    assert (seen["destination"].dataset_id, seen["destination"].table_id, seen["mode"]) == (
        "brutos_bcadastro",
        database,
        "append",
    )
    assert source.key_width == key_width
    assert seen["options"]["chunks"]["column"] == "id"
    assert seen["options"]["columns"] == {"add_timestamp": True}

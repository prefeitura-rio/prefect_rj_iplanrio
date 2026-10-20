from json import loads
from pathlib import Path

import pytest
from deltalake import DeltaTable
from fixtures import PEOPLE, UPDATE_SEQ, all_docs_page, cpf_document, revision
from pyarrow.parquet import ParquetWriter, read_table
from pydantic import JsonValue
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.sources.backends.couchdb import CouchDBSource, change_item
from toolkit.executor.sources.models import CouchDBAllDocs, CouchDBChanges
from toolkit.executor.sources.protocols import DBConn
from toolkit.settings import DatabaseCredentials
from toolkit.types import Batches

from utils import tasks
from utils.couchdb import BCADASTRO
from utils.types import Database, Read
from utils.utils import dump_to_delta, read_values

CPF_IDS = ["00000000272", "00000000353", "00000008001"]


def stage(page: bytes, database: str, path: Path) -> CouchDBAllDocs:
    """Write a page of `_all_docs` as the bcadastro source writes it, and return the page."""
    parsed = CouchDBAllDocs.model_validate_json(page)
    source = CouchDBSource(DBConn("10.2.231.241", 6984, database, "user", "password"), shape=BCADASTRO)

    with ParquetWriter(path, BCADASTRO.columns) as writer:
        source.write(writer, parsed.rows, parsed.update_seq)

    return parsed


async def query(path: Path, sql: str) -> list[tuple[object, ...]]:
    """Run a query over the staged file, and return its rows."""
    async with DuckDBEngine.connect() as engine:
        [frame] = [batch async for batch in engine.query(sql.format(path=path))]

    return frame.rows()


def test_the_bcadastro_columns_are_those_of_the_destination_without_the_airbyte_ones():
    """Keep every column of the destination but the `_airbyte_` ones."""
    assert BCADASTRO.columns.names == ["id", "doc", "key", "seq", "value", "last_seq"]


def test_a_page_of_cpf_documents_becomes_the_rows_of_the_destination(cpf_page, tmp_path):
    """Keep the id, the revision and the sequence of each person, and the document whole."""
    path = tmp_path / "cpf.parquet"

    page = stage(cpf_page, "chcpf_bcadastros", path)
    table = read_table(path)
    rows = table.to_pylist()

    assert page.update_seq == UPDATE_SEQ
    assert table["id"].to_pylist() == CPF_IDS
    assert table["key"].to_pylist() == CPF_IDS
    assert table["seq"].to_pylist() == [None, None, None]
    assert table["last_seq"].to_pylist() == [UPDATE_SEQ] * 3
    assert [loads(row["doc"]) for row in rows] == [cpf_document(number) for number in PEOPLE]
    assert [loads(row["value"]) for row in rows] == [{"rev": cpf_document(number)["_rev"]} for number in PEOPLE]


async def test_duckdb_reads_the_cpf_documents_as_json(cpf_page, tmp_path):
    """Read the document as `JSON`, with the accents of the names kept."""
    path = tmp_path / "cpf.parquet"

    stage(cpf_page, "chcpf_bcadastros", path)
    rows = await query(
        path,
        "SELECT typeof(doc), doc->>'$.nomeContribuinte', value->>'$.rev' FROM read_parquet('{path}') ORDER BY id",
    )

    assert rows == [("JSON", PEOPLE[number][0], cpf_document(number)["_rev"]) for number in PEOPLE]


async def test_a_cnpj_document_keeps_its_partners_as_nested_json(cnpj_page, tmp_path):
    """Keep the list of partners inside the document, so DuckDB reads it by path."""
    path = tmp_path / "cnpj.parquet"

    stage(cnpj_page, "chcnpj_bcadastros", path)
    rows = await query(
        path,
        "SELECT id, json_array_length(doc->'$.socios'), doc->>'$.socios[1].cpfCnpj' FROM read_parquet('{path}')",
    )

    assert rows == [("27329521", 2, "00045678901234")]


async def test_a_page_of_changes_keeps_the_sequence_and_the_deletions(cpf_changes_page, tmp_path):
    """Write the sequence of each change, and a deletion as the tombstone of its document."""
    path = tmp_path / "changes.parquet"
    page = CouchDBChanges.model_validate_json(cpf_changes_page)
    source = CouchDBSource(DBConn("10.2.231.241", 6984, "chcpf_bcadastros", "user", "password"), shape=BCADASTRO)
    items = [change_item(change) for change in page.results]

    with ParquetWriter(path, BCADASTRO.columns) as writer:
        source.write(writer, items, page.last_seq)

    rows = await query(
        path, "SELECT id, seq, last_seq, doc->>'$._deleted', value->>'$.rev' FROM read_parquet('{path}') ORDER BY id"
    )

    assert [(row[0], row[1], row[3]) for row in rows] == [
        ("00000000272", "2-g1AAAAKTeJzLYWBg", None),
        ("00000000353", "4-g1AAAAKTeJzLYWBg", None),
        ("00000008001", "12-g1AAAAKTeJzLYWBg", "true"),
    ]
    assert {row[2] for row in rows} == {"12-g1AAAAKTeJzLYWBg"}
    assert rows[2][4] == f"2-{revision('00000008001')[2:]}"


@pytest.mark.parametrize(
    ("read", "expected"),
    [
        pytest.param({"table": "chcpf_bcadastros"}, {"type": "couchdb", "table": "chcpf_bcadastros"}, id="database"),
        pytest.param({"query": '{"a": 1}'}, {"type": "couchdb", "query": '{"a": 1}'}, id="selector"),
        pytest.param(
            {"table": "db", "query": "{}"},
            {"type": "couchdb", "table": "db", "query": "{}"},
            id="database_and_selector",
        ),
        pytest.param({"since": "0"}, {"type": "couchdb", "since": "0"}, id="changes"),
        pytest.param(
            {"table": "db", "since": "7-g1"},
            {"type": "couchdb", "table": "db", "since": "7-g1"},
            id="database_and_changes",
        ),
    ],
)
def test_a_couchdb_read_keeps_the_database_the_selector_and_the_sequence(read, expected):
    """Pass the three parts of a CouchDB read to the source as they are, even with windows."""
    assert read_values("couchdb", read, batched=True) == expected


@pytest.mark.parametrize(
    ("read", "message"),
    [
        pytest.param({"query": "{}", "since": "0"}, "one of query and since", id="selector_and_changes"),
        pytest.param({}, "Give a table, a query or a since", id="nothing"),
    ],
)
def test_a_couchdb_read_that_does_not_fit_is_an_error(read, message):
    """Raise for a read with a selector and a sequence, or with nothing to read."""
    with pytest.raises(ValueError, match=message):
        read_values("couchdb", read, batched=False)


async def test_a_couchdb_database_is_dumped_in_windows_over_its_text_ids(fake_api, tmp_path):
    """Cut the text ids by number, read what is outside them last, and keep each document."""
    event: dict[str, JsonValue] = {
        "_id": "CPF-20261020011517",
        "_rev": revision("CPF-20261020011517"),
        "tipo": "evento",
    }
    people = [cpf_document(number) for number in PEOPLE]
    pages: list[list[dict[str, JsonValue]]] = [[people[0], people[1]], [people[2]], [], [event]]
    fake_api.replies.extend((200, loads(all_docs_page(documents, 4))) for documents in pages)
    conn = DBConn("127.0.0.1", fake_api.server.server_port, "ignored", "user", "password")
    source = CouchDBSource(conn, secure=False, key_width=11)
    batches: Batches = {"column": "id", "width": 5_000, "begin": 0, "end": 9_999}
    read: Read = {"table": "chcpf_bcadastros"}

    await dump_to_delta(source, read_values("couchdb", read, True), str(tmp_path / "t"), "append", {"batches": batches})
    rows = DeltaTable(str(tmp_path / "t")).to_pyarrow_table().to_pylist()

    assert {row["id"]: row["batch"] for row in rows} == {
        "00000000272": "000000000000",
        "00000000353": "000000000000",
        "00000008001": "000000005000",
        "CPF-20261020011517": "null",
    }
    assert loads(next(row["doc"] for row in rows if row["id"] == "00000000272")) == people[0]
    assert [(r.path, r.query.get("startkey"), r.query.get("endkey")) for r in fake_api.requests] == [
        ("/chcpf_bcadastros/_all_docs", ['"0"'], ['"00000005000"']),
        ("/chcpf_bcadastros/_all_docs", ['"00000005000"'], ['"00000010000"']),
        ("/chcpf_bcadastros/_all_docs", None, ['"0"']),
        ("/chcpf_bcadastros/_all_docs", ['":"'], None),
    ]


async def test_the_options_of_the_database_choose_the_bcadastro_shape(monkeypatch):
    """Build the source from the shape and the key width that the `prefect.yaml` names."""
    seen = {}

    async def load(source, values, destination, dump_mode, options):
        """Keep what the dump would read and where it would write."""
        seen.update(source=source, values=values, mode=dump_mode, options=options, destination=destination)

    def credentials(path):
        """Keep the secret that was asked for, and give a user and a password."""
        seen["secret_path"] = path

        return DatabaseCredentials("user", "password")

    monkeypatch.setattr(tasks, "load_to_delta", load)
    monkeypatch.setattr(tasks, "read_database_credentials", credentials)
    database: Database = {
        "type": "couchdb",
        "host": "10.2.231.241",
        "port": 6984,
        "database": "unused",
        "secret_path": "/db-bcadastro",
        "options": {"shape": "utils.couchdb:BCADASTRO", "key_width": 3},
    }

    await tasks.dump_db_to_gcs.fn(
        database, {"dataset_id": "brutos_bcadastro", "table_id": "chcpf"}, {"table": "chcpf_bcadastros"}, bucket="b"
    )

    source = seen["source"]
    assert (source.shape, source.key_width, source.conn.hostname) == (BCADASTRO, 3, "10.2.231.241")
    assert (seen["secret_path"], seen["values"]) == ("/db-bcadastro", {"type": "couchdb", "table": "chcpf_bcadastros"})

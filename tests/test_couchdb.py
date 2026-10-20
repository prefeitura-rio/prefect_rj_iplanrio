from json import loads
from pathlib import Path

from fixtures import PEOPLE, UPDATE_SEQ, cpf_document, revision
from pyarrow.parquet import ParquetWriter, read_table
from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.sources.backends.couchdb import CouchDBSource, change_item
from toolkit.executor.sources.models import CouchDBAllDocs, CouchDBChanges
from toolkit.executor.sources.protocols import DBConn

from utils.couchdb import BCADASTRO

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

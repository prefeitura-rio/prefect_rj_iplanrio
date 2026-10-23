"""Checksum do slot: md5 de agregação ordenada pela chave, calculado no Postgres."""

import re
from collections.abc import Iterator

import psycopg
import pytest
from conftest import Database
from synthetic import BUCKET, MemoryObjectStore, detail_row, stage_export, summary_row

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import checksum, loader, manifest, publication


@pytest.fixture
def carregador(database: Database) -> Iterator[psycopg.Connection]:
    with psycopg.connect(database.carregador_dsn, autocommit=True) as connection:
        yield connection


def load(connection: psycopg.Connection, summary: list, detail: list, *, files_per_table: int = 2) -> str:
    staged = stage_export(MemoryObjectStore(), summary, detail, files_per_table=files_per_table)
    parsed = manifest.parse_manifest(staged.store.read_text(BUCKET, staged.manifest_name))
    exports = publication.required_exports(parsed)
    loader.load_slot(connection, loader.LoadRequest(staged.store, BUCKET, "b", exports))
    return checksum.slot_checksum(connection, "b")


def test_the_checksum_is_a_md5_hex_digest(carregador):
    assert re.fullmatch(r"[0-9a-f]{32}", load(carregador, [summary_row(1)], [detail_row(1)]))


def test_the_checksum_does_not_depend_on_the_order_the_rows_were_loaded(carregador):
    detail = [detail_row(number) for number in range(1, 8)]
    summary = [summary_row(number) for number in range(1, 5)]

    forward = load(carregador, summary, detail, files_per_table=1)
    scattered = load(carregador, summary[::-1], detail[::-1], files_per_table=3)

    assert forward == scattered


def test_changing_one_value_changes_the_checksum(carregador):
    detail = [detail_row(number) for number in range(1, 4)]
    original = load(carregador, [summary_row(1)], detail)

    changed = load(carregador, [summary_row(1)], [*detail[:2], detail_row(3, valor_issqn="9.99")])

    assert changed != original


def test_a_change_in_the_summary_table_changes_the_checksum_too(carregador):
    detail = [detail_row(1)]
    original = load(carregador, [summary_row(1)], detail)

    assert load(carregador, [summary_row(1, quantidade_notas=7)], detail) != original

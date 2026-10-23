"""Carga de um slot: COPY em staging de texto e INSERT ... SELECT com as conversões do control file do sqlldr."""

from collections.abc import Iterator
from datetime import datetime
from decimal import Decimal

import psycopg
import pytest
from conftest import Database
from synthetic import BUCKET, MemoryObjectStore, detail_row, stage_export, summary_row

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import loader, manifest, publication, tables

DETAIL_TABLE = "exigiveis_b.mvt_notas_nacionais_exigiveis_detalhes"
SUMMARY_TABLE = "exigiveis_b.mvt_notas_nacionais_exigiveis"
SECOND_SUMMARY = 2


@pytest.fixture
def carregador(database: Database) -> Iterator[psycopg.Connection]:
    with psycopg.connect(database.carregador_dsn, autocommit=True) as connection:
        yield connection


def request_for(summary: list, detail: list, slot: str = "b") -> loader.LoadRequest:
    staged = stage_export(MemoryObjectStore(), summary, detail)
    parsed = manifest.parse_manifest(staged.store.read_text(BUCKET, staged.manifest_name))
    return loader.LoadRequest(
        store=staged.store, bucket=BUCKET, slot=slot, exports=publication.required_exports(parsed)
    )


def fetch(database: Database, query: str) -> list[tuple]:
    with database.admin() as admin:
        return admin.execute(query).fetchall()


def test_both_tables_are_loaded_into_the_slot_and_the_row_counts_come_back(database, carregador):
    request = request_for([summary_row(1), summary_row(2)], [detail_row(1), detail_row(2), detail_row(3)])

    result = loader.load_slot(carregador, request)

    assert result == {tables.SUMMARY.pg_name: 2, tables.DETAIL.pg_name: 3}
    assert fetch(database, f"SELECT count(*) FROM {SUMMARY_TABLE}") == [(2,)]
    assert fetch(database, f"SELECT count(*) FROM {DETAIL_TABLE}") == [(3,)]
    assert fetch(database, "SELECT count(*) FROM exigiveis_a.mvt_notas_nacionais_exigiveis_detalhes") == [(0,)]


def test_empty_and_null_text_become_null_while_awkward_text_is_kept_exactly(database, carregador):
    awkward = 'José, "Zé" & Cia\nLtda  '
    detail = [
        detail_row(1, nome_contraparte=None),
        detail_row(2, nome_contraparte=""),
        detail_row(3, nome_contraparte=awkward),
        detail_row(4, nome_contraparte="  espaço antes"),
    ]

    loader.load_slot(carregador, request_for([summary_row(1)], detail))

    names = dict(fetch(database, f"SELECT nota_fiscal::int, nome_contraparte FROM {DETAIL_TABLE}"))
    assert names == {1001: None, 1002: None, 1003: awkward, 1004: "  espaço antes"}


def test_numbers_are_exact_decimals_and_round_to_the_column_scale(database, carregador):
    detail = [
        detail_row(1, valor_servico=Decimal("12345678901234.56")),
        detail_row(2, valor_servico=Decimal("0.10"), valor_issqn=None),
        detail_row(3, valor_servico=Decimal("1.005")),
    ]

    loader.load_slot(carregador, request_for([summary_row(1)], detail))

    values = dict(fetch(database, f"SELECT nota_fiscal::int, valor_servico::text FROM {DETAIL_TABLE}"))
    assert values == {1001: "12345678901234.56", 1002: "0.10", 1003: "1.01"}
    assert fetch(database, f"SELECT valor_issqn FROM {DETAIL_TABLE} WHERE nota_fiscal = 1002") == [(None,)]


def test_dates_in_the_export_format_become_timestamps(database, carregador):
    loader.load_slot(carregador, request_for([summary_row(1)], [detail_row(1)]))

    row = fetch(database, f"SELECT data_validacao, data_competencia_municipio FROM {DETAIL_TABLE}")[0]
    assert row == (datetime(2026, 3, 10, 14, 5, 9, tzinfo=None), datetime(2026, 3, 1, tzinfo=None))  # noqa: DTZ001


def test_a_date_outside_the_export_format_fails_the_whole_load_and_keeps_the_slot(database, carregador):
    loader.load_slot(carregador, request_for([summary_row(1)], [detail_row(1)]))
    before = fetch(database, f"SELECT nota_fiscal FROM {DETAIL_TABLE}")

    bad = request_for([summary_row(1)], [detail_row(2), detail_row(3, data_validacao="2026-03-10 14:05:09")])
    with pytest.raises(ValueError, match="data_validacao"):
        loader.load_slot(carregador, bad)

    assert fetch(database, f"SELECT nota_fiscal FROM {DETAIL_TABLE}") == before


def test_a_value_that_is_not_a_number_fails_the_load_and_keeps_the_slot(database, carregador):
    loader.load_slot(carregador, request_for([summary_row(1)], [detail_row(1)]))

    bad = request_for([summary_row(1)], [detail_row(2, valor_servico="doze")])
    with pytest.raises(psycopg.errors.InvalidTextRepresentation):
        loader.load_slot(carregador, bad)

    assert fetch(database, f"SELECT nota_fiscal::int FROM {DETAIL_TABLE}") == [(1001,)]


def test_an_empty_value_in_a_required_column_fails_like_the_oracle_load(carregador):
    bad = request_for([summary_row(1)], [detail_row(1, cpf_cnpj_responsavel="")])

    with pytest.raises(psycopg.errors.NotNullViolation):
        loader.load_slot(carregador, bad)


def test_the_load_replaces_whatever_the_slot_held(database, carregador):
    loader.load_slot(carregador, request_for([summary_row(1)], [detail_row(1), detail_row(2)]))

    loader.load_slot(carregador, request_for([summary_row(2)], [detail_row(9)]))

    assert fetch(database, f"SELECT nota_fiscal::int FROM {DETAIL_TABLE}") == [(1009,)]
    assert fetch(database, f"SELECT quantidade_notas::int FROM {SUMMARY_TABLE}") == [(SECOND_SUMMARY,)]


def test_a_competence_outside_the_partition_range_lands_in_the_default_partition(database, carregador):
    far = detail_row(1, data_competencia_municipio="2031-02-01T00:00:00")

    loader.load_slot(carregador, request_for([summary_row(1)], [far]))

    in_default = fetch(database, "SELECT count(*) FROM exigiveis_b.mvt_nn_exigiveis_detalhes_pdefault")
    assert in_default == [(1,)]


def test_the_loaded_tables_are_analyzed(database, carregador):
    loader.load_slot(carregador, request_for([summary_row(1)], [detail_row(1), detail_row(2)]))

    analyzed = fetch(
        database,
        "SELECT count(DISTINCT tablename) FROM pg_stats WHERE schemaname = 'exigiveis_b' "
        "AND tablename IN ('mvt_notas_nacionais_exigiveis', 'mvt_notas_nacionais_exigiveis_detalhes')",
    )
    assert analyzed == [(2,)]

"""Slot inativo: o que as views public.* servem agora, lido do catálogo (o carregador não lê publicacoes)."""

import psycopg
import pytest
from conftest import Database

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import slots


def test_the_slot_to_load_is_b_while_the_views_still_read_a(database: Database):
    with psycopg.connect(database.carregador_dsn, autocommit=True) as carregador:
        assert slots.inactive_slot(carregador) == "b"


def test_the_slot_to_load_flips_after_a_publication(database: Database):
    with database.admin() as admin:
        admin.execute("SELECT public.publicar_exigiveis('b', 'run', 'teste', 0, 0, NULL)")

    with psycopg.connect(database.carregador_dsn, autocommit=True) as carregador:
        assert slots.inactive_slot(carregador) == "a"


def test_views_that_disagree_about_the_slot_are_refused(database: Database):
    with database.admin() as admin:
        admin.execute(
            "CREATE OR REPLACE VIEW public.mvt_notas_nacionais_exigiveis AS "
            "SELECT cpf_cnpj_responsavel, data_competencia_municipio, tipo_retencao, opcao_simples_nacional, "
            "retencao, quantidade_notas, total_valor_servico, total_valor_ded_red, total_valor_issqn, "
            "total_valor_base_calculo, cnt_valor_servico, cnt_valor_ded_red, cnt_valor_issqn, "
            "cnt_valor_base_calculo FROM exigiveis_b.mvt_notas_nacionais_exigiveis"
        )

    with psycopg.connect(database.carregador_dsn, autocommit=True) as carregador, pytest.raises(slots.SlotError):
        slots.inactive_slot(carregador)

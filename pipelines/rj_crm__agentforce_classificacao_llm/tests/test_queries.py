# -*- coding: utf-8 -*-
"""SQL da pipeline: tudo em queries/, renderizado por load_query (string.Template, STYLEGUIDE §7)."""

import re
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest
from prefect_rj_iplanrio.sql import load_query

from pipelines.rj_crm__agentforce_classificacao_llm.constants import QUERIES_ANCHOR
from pipelines.rj_crm__agentforce_classificacao_llm.tasks import extract, load, roteamento, taxonomia

PIPELINE = Path(QUERIES_ANCHOR).parent
QUERIES = sorted((PIPELINE / "queries").glob("*.sql"))
# Valor com chaves e STRUCT<>: com str.format() quebrava ou corrompia o SQL.
HOSTIL = "proj.ds.t{x}_STRUCT<a INT64>"


def _placeholders(sql: str) -> set[str]:
    return set(re.findall(r"\$\{?(\w+)\}?", sql))


def _cliente(df: pd.DataFrame | None = None) -> mock.MagicMock:
    c = mock.MagicMock()
    c.query.return_value.to_dataframe.return_value = df if df is not None else pd.DataFrame()
    c.query.return_value.num_dml_affected_rows = 1
    c.load_table_from_dataframe.return_value.errors = None
    return c


def _sqls(c: mock.MagicMock) -> list[str]:
    return [chamada.args[0] for chamada in c.query.call_args_list]


def test_queries_existem():
    nomes = {q.stem for q in QUERIES}
    assert {
        "extract_sessoes",
        "carrega_catalogo_regras",
        "extrai_sessoes_recalculo_taxonomia",
        "merge_tema_motivo",
        "merge_classificacoes",
        "trunca_tmp",
        "carrega_macrotemas",
        "carrega_secretarias",
    } <= nomes


@pytest.mark.parametrize("arquivo", QUERIES, ids=lambda q: q.stem)
def test_query_sem_placeholder_de_str_format(arquivo: Path):
    assert not re.search(r"(?<!\$)\{\w+\}", arquivo.read_text(encoding="utf-8"))


@pytest.mark.parametrize("arquivo", QUERIES, ids=lambda q: q.stem)
def test_query_renderiza_valor_com_chaves(arquivo: Path):
    params = {nome: HOSTIL for nome in _placeholders(arquivo.read_text(encoding="utf-8"))}
    sql = load_query(QUERIES_ANCHOR, arquivo.stem, **params)
    assert "$" not in sql
    if params:
        assert HOSTIL in sql


def test_python_da_pipeline_nao_monta_sql():
    for py in (PIPELINE / "tasks").glob("*.py"):
        texto = py.read_text(encoding="utf-8")
        assert ".read_text().format(" not in texto, py.name
        assert not re.search(r"f[\"']+\s*(SELECT|MERGE|TRUNCATE)\b", texto), py.name


def test_extract_interpola_janela_e_tabelas():
    c = _cliente(pd.DataFrame(columns=["resposta_atrasada_btn"]))
    with mock.patch.object(extract, "get_bq_client", return_value=c):
        extract.extrai_sessoes_nao_classificadas.fn("proj", HOSTIL, "proj.ds.destino", 7)
    (sql,) = _sqls(c)
    assert "INTERVAL 7 DAY" in sql
    assert f"FROM `{HOSTIL}`" in sql
    assert "`proj.ds.destino` d" in sql


def test_merge_classificacoes_cobre_todas_as_colunas():
    sql = load.monta_merge_classificacoes(destino_full="proj.ds.destino", tmp_full="proj.ds.destino_tmp")
    assert "MERGE `proj.ds.destino` AS t" in sql
    assert "USING `proj.ds.destino_tmp` AS s" in sql
    assert "ON t.id_sessao = s.id_sessao" in sql
    assert "t.id_sessao = s.id_sessao," not in sql  # a chave não entra no UPDATE SET
    for coluna in load._COLUNAS:
        assert f"s.{coluna}" in sql


def test_carrega_classificacoes_faz_merge_e_trunca_tmp():
    c = _cliente()
    with mock.patch.object(load, "get_bq_client", return_value=c):
        load.carrega_classificacoes.fn(pd.DataFrame([{"id_sessao": "s1"}]), "proj", "ds", "destino", "destino_tmp")
    merge, trunca = _sqls(c)
    assert merge == load.monta_merge_classificacoes(destino_full="proj.ds.destino", tmp_full="proj.ds.destino_tmp")
    assert trunca.strip().endswith("TRUNCATE TABLE `proj.ds.destino_tmp`")


def test_atualiza_tema_motivo_trunca_merge_trunca():
    df = pd.DataFrame([{"id_sessao": "s1", **{k: None for k in taxonomia._COLUNAS_TAXONOMIA}}])
    c = _cliente()
    with mock.patch.object(taxonomia, "get_bq_client", return_value=c):
        taxonomia.atualiza_tema_motivo.fn(df, "proj", "ds", "destino", "destino_tmp")
    antes, merge, depois = _sqls(c)
    assert antes == depois
    assert antes.strip().endswith("TRUNCATE TABLE `proj.ds.destino_tmp`")
    assert "MERGE `proj.ds.destino` AS t" in merge and "USING `proj.ds.destino_tmp` AS s" in merge


def test_catalogo_e_recalculo_usam_a_tabela_pedida():
    c = _cliente(pd.DataFrame(columns=taxonomia._COLUNAS_CATALOGO))
    with mock.patch.object(taxonomia, "get_bq_client", return_value=c):
        taxonomia.carrega_catalogo_regras.fn("proj", "ds", "regras", "tema", "macrotema")
        taxonomia.extrai_sessoes_para_recalculo_taxonomia.fn("proj", "ds", "destino")
    catalogo, recalculo = _sqls(c)
    assert "FROM `proj.ds.regras`" in catalogo and "@etapa" in catalogo
    assert "FROM `proj.ds.destino`" in recalculo


def test_taxonomia_roteamento_le_as_duas_tabelas():
    c = mock.MagicMock()
    c.query.return_value.to_dataframe.side_effect = [
        pd.DataFrame([{"nome": "m", "descricao": "d", "secretarias": ["S"], "prompt_versao_macrotema": "v1"}]),
        pd.DataFrame([{"sigla": "S", "nome": "n", "descricao": "d", "prompt_versao_secretaria": "v1"}]),
    ]
    with mock.patch.object(roteamento, "get_bq_client", return_value=c):
        taxonomia_roteamento = roteamento.carrega_taxonomia_roteamento.fn("proj", "ds", "macrotemas", "secretarias")
    macrotemas, secretarias = _sqls(c)
    assert "FROM `proj.ds.macrotemas`" in macrotemas and "ORDER BY ordem" in macrotemas
    assert "FROM `proj.ds.secretarias`" in secretarias and "ORDER BY ordem" in secretarias
    assert taxonomia_roteamento.prompt_versao_macrotema == "v1"

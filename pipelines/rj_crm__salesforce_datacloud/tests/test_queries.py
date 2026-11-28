# -*- coding: utf-8 -*-
"""SQL/SOQL da pipeline: tudo em queries/, renderizado por load_query (string.Template, STYLEGUIDE §7)."""

import re
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest
from prefect_rj_iplanrio.sql import load_query

from pipelines.rj_crm__salesforce_datacloud import flow
from pipelines.rj_crm__salesforce_datacloud.constants import QUERIES_ANCHOR
from pipelines.rj_crm__salesforce_datacloud.tasks import bigquery as bq
from pipelines.rj_crm__salesforce_datacloud.utils import tabelas

PIPELINE = Path(QUERIES_ANCHOR).parent
QUERIES = sorted((PIPELINE / "queries").glob("*.sql"))
INICIO, FIM = "2026-11-20T23:00:00Z", "2026-11-21T02:00:00Z"


def _query_da_tabela(tabela: tabelas.Tabela) -> str:
    capturada: dict[str, str] = {}

    def data_cloud(dc_session, query, table_name, order_by_col):  # noqa: ANN001, ANN202
        capturada["q"] = query
        return pd.DataFrame()

    def crm_rest(crm_session, soql, table_name):  # noqa: ANN001, ANN202
        capturada["q"] = soql
        return pd.DataFrame()

    with (
        mock.patch.object(flow, "extract_from_data_cloud", side_effect=data_cloud),
        mock.patch.object(flow, "extract_from_crm_rest", side_effect=crm_rest),
    ):
        resultado = flow.processar_tabela(tabela, INICIO, FIM, date(2026, 11, 20), "proj", "ds", {})
    assert resultado == {"extraidas": 0, "inseridas": 0, "atualizadas": 0}
    return capturada["q"]


def test_nao_sobrou_diretorio_sql():
    assert not (PIPELINE / "sql").exists()
    assert not (PIPELINE / "utils" / "queries.py").exists()


@pytest.mark.parametrize("arquivo", QUERIES, ids=lambda q: q.stem)
def test_query_sem_placeholder_de_str_format(arquivo: Path):
    assert not re.search(r"(?<!\$)\{\w+\}", arquivo.read_text(encoding="utf-8"))


@pytest.mark.parametrize("tabela", tabelas.TABELAS, ids=lambda t: t.nome)
def test_query_de_cada_tabela_renderiza_a_janela(tabela: tabelas.Tabela):
    query = _query_da_tabela(tabela)
    assert "$" not in query
    if tabela.janela_utc:
        # janela convertida pra UTC (SP + 3 h) e limites de partição preenchidos
        assert "2026-11-21T02:00:00Z" in query and "2026-11-21T05:00:00Z" in query
        assert "2026-11-21T00:00:00Z" in query
    else:
        assert INICIO in query and FIM in query


def test_soql_continua_sem_comentario():
    for tabela in tabelas.TABELAS:
        if tabela.source == "crm_rest":
            assert "--" not in (PIPELINE / "queries" / f"{tabela.query}.sql").read_text(encoding="utf-8")


def test_tabelas_yaml_recusa_query_inexistente(tmp_path: Path):
    yaml = tmp_path / "tabelas.yaml"
    yaml.write_text("- nome: x\n  grupo: g\n  query: nao_existe\n", encoding="utf-8")
    with pytest.raises(ValueError, match=r"queries/nao_existe\.sql não existe"):
        tabelas._carrega(yaml)


def test_merge_staging_monta_merge_e_trunca_staging():
    c = mock.MagicMock()
    c.get_table.return_value.schema = [SimpleNamespace(name=n) for n in ("id", "a", "data_particao", "_loaded_at")]
    c.query.return_value.dml_stats = None
    with mock.patch.object(bq, "_get_bq_client", return_value=c):
        bq.merge_staging_to_target.fn("proj", "ds", "t_staging", "t", "id")
    merge, trunca = (chamada.args[0] for chamada in c.query.call_args_list)
    assert "MERGE `proj.ds.t` AS t" in merge
    assert "FROM `proj.ds.t_staging`" in merge
    assert "PARTITION BY id, data_particao" in merge
    assert "UPDATE SET t.a = s.a, t.data_particao = s.data_particao, t._loaded_at = s._loaded_at" in merge
    assert "INSERT (id, a, data_particao, _loaded_at, _first_loaded_at)" in merge
    assert trunca.strip().endswith("TRUNCATE TABLE `proj.ds.t_staging`")


def test_valor_com_chaves_nao_quebra_o_merge():
    sql = load_query(
        QUERIES_ANCHOR,
        "merge_staging",
        target="p.d.t",
        staging="p.d.t_staging",
        primary_key="id",
        partition_field="data_particao",
        set_clause="t.s = STRUCT<a INT64>(1)",
        insert_cols="id, s",
        insert_vals="s.id, ARRAY[{}]",
    )
    assert "STRUCT<a INT64>(1)" in sql and "ARRAY[{}]" in sql

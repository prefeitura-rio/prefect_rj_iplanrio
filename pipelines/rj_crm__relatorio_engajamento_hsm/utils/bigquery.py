# -*- coding: utf-8 -*-
"""Cliente BigQuery autenticado com a credencial de produção do secret do work pool —
mesmo padrão de rj_crm__agentforce_classificacao_llm/utils/bigquery.py. Escopo Drive
(default de get_bd_credentials_from_env) é necessário mesmo pra BQ aqui:
disparos_ativos é uma tabela externa (Google Sheet importada)."""

from google.cloud import bigquery
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env

from pipelines.rj_crm__relatorio_engajamento_hsm.config import PROJECT_ID


def get_bq_client() -> bigquery.Client:
    credentials = get_bd_credentials_from_env(mode="prod")
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

# -*- coding: utf-8 -*-
"""Cliente BigQuery autenticado com a credencial de produção do secret do work pool —
mesmo padrão de rj_crm__agentforce_classificacao_llm/utils/bigquery.py. Usado na
extração (rmi_conversas.chatbot) e no banco de exemplos do RAG (tasks/rag.py)."""

from google.cloud import bigquery
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env

from pipelines.rj_crm__relatorio_engajamento_hsm.config import PROJECT_ID


def get_bq_client() -> bigquery.Client:
    credentials = get_bd_credentials_from_env(mode="prod")
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

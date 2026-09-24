# -*- coding: utf-8 -*-
"""
Cliente BigQuery autenticado explicitamente com a credencial de produção do secret do
work pool (mesmo padrão de pipelines/rj_crm__get_history_data/utils/bigquery.py).

`bigquery.Client(project=project_id)` sem `credentials=` cai em Application Default
Credentials — no pod do work pool isso NÃO é a service account com acesso a
brutos_salesforce (causou 403 até em bigquery.tables.get). A credencial certa já vem
injetada no mesmo secret (BASEDOSDADOS_CREDENTIALS_PROD), só precisa ser lida
explicitamente via get_bd_credentials_from_env — por isso todo Client() desta pipeline
deve passar por get_bq_client, nunca instanciar bigquery.Client direto.
"""

from google.cloud import bigquery
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env


def get_bq_client(project_id: str) -> bigquery.Client:
    """Cria um cliente BQ autenticado com as credenciais de produção (mesmo em
    deployments de staging — get_history_data usa mode="prod" incondicionalmente,
    cada secret (prefect-jobs-crm-registry-secrets[-staging]) já define
    BASEDOSDADOS_CREDENTIALS_PROD com o valor certo pro ambiente dele)."""
    credentials = get_bd_credentials_from_env(mode="prod")
    return bigquery.Client(credentials=credentials, project=project_id)

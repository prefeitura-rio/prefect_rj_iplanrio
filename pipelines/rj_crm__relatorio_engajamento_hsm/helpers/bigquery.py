"""Cliente BigQuery autenticado com a credencial de produção do secret do work pool —
mesmo padrão de rj_crm__agentforce_classificacao_llm/utils/bigquery.py. Escopo Drive
(default de get_bd_credentials_from_env) é necessário mesmo pra BQ aqui:
disparos_ativos é uma tabela externa (Google Sheet importada)."""

from google.cloud import bigquery

from pipelines.rj_crm__relatorio_engajamento_hsm.config import PROJECT_ID
from utils.utils import load_credentials


def get_bq_client() -> bigquery.Client:
    credentials = load_credentials()
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

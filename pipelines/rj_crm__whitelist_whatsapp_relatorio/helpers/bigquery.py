"""Leitura do BigQuery.

A credencial é a do ADC (workload identity), de ``utils.utils.load_credentials``.
"""

from time import sleep

from google.cloud import bigquery

from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.log import logger_da_pipeline
from utils.utils import load_credentials

logger = logger_da_pipeline(__name__)

BILLING_PROJECT_ID = "rj-crm-registry"
"""Projeto cobrado pela consulta.

Hard coded por decisão (D28): é o padrão do repo, não é segredo e muda junto com o
código. Só este módulo precisa dele, então mora aqui e não em ``constants.py`` (§4.5).
"""


def baixar(query: str) -> list[dict[str, object]]:
    """Executa uma query no BigQuery e devolve as linhas do resultado como dicionários.

    :param query: SQL já renderizado.
    :returns: Linhas do resultado.
    :raises google.api_core.exceptions.GoogleAPIError: Se a execução falhar no BigQuery.
    """
    logger.info("Consultando o BigQuery no projeto %s", BILLING_PROJECT_ID)
    cliente = bigquery.Client(credentials=load_credentials(), project=BILLING_PROJECT_ID)
    job = cliente.query(query)
    while not job.done():
        sleep(1)
    dados = [dict(linha.items()) for linha in job.result()]
    logger.info("Query retornou %d linha(s)", len(dados))
    return dados

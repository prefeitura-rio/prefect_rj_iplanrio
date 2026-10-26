"""Read from BigQuery with the ADC credential (workload identity)."""

from time import sleep

from google.cloud import bigquery
from toolkit.telemetry import logger

from utils.utils import load_credentials

BILLING_PROJECT_ID = "rj-crm-registry"
"""Projeto cobrado pela consulta.

Hard coded por decisão (D28): é o padrão do repo, não é segredo e muda junto com o
código. Só este módulo precisa dele, então mora aqui e não em ``constants.py`` (§4.5).
"""


def baixar(query: str) -> list[dict[str, object]]:
    """Run a query in BigQuery and return the rows of the result as dictionaries.

    `query` is the rendered SQL. Raise `google.api_core.exceptions.GoogleAPIError` if
    BigQuery fails to run it.
    """
    logger.info("Consultando o BigQuery no projeto %s", BILLING_PROJECT_ID)
    cliente = bigquery.Client(credentials=load_credentials(), project=BILLING_PROJECT_ID)
    job = cliente.query(query)
    while not job.done():
        sleep(1)
    dados = [dict(linha.items()) for linha in job.result()]
    logger.info("Query retornou %d linha(s)", len(dados))
    return dados

"""
Utility tasks for CRM Wetalkie Atualiza Contato pipeline
Reutiliza funções do pipeline rj_crm__api_wetalkie
"""

from time import sleep
from typing import Dict, List, Union

import pandas as pd
from google.cloud import bigquery
from prefect import task
from prefect.logging import get_logger

from pipelines.rj_crm__wetalkie_atualiza_contato.helpers.api_handler import ApiHandler
from utils.settings import WetalkieSettings

logger = get_logger("toolkit")


@task
def access_api(login_route: str = "users/login") -> ApiHandler:
    """Return an authenticated handler of the Wetalkie API."""
    settings = WetalkieSettings()

    return ApiHandler(
        base_url=settings.WETALKIE_URL,
        username=settings.WETALKIE_USERNAME,
        password=settings.WETALKIE_PASSWORD.get_secret_value(),
        login_route=login_route,
    )


@task
def skip_flow_if_empty(
    data: Union[pd.DataFrame, List, str, Dict],
    message: str = "Data is empty. Skipping flow.",
) -> Union[pd.DataFrame, List, str, Dict, None]:
    """Skip the flow if input data is empty.
    To skip is necessary to add the following check in the flow:
    'if validated_destinations is None:
        return  # flow termina aqui, nada downstream é agendado'
    """
    if len(data) == 0:
        logger.info(message)
        return None
    return data


def download_data_from_bigquery(query: str, billing_project_id: str, bucket_name: str) -> pd.DataFrame:
    """Execute a BigQuery SQL query and return results as a pandas DataFrame."""
    logger.info("Querying data from BigQuery")
    query = str(query)
    bq_client = bigquery.Client(
        project=billing_project_id,
    )
    job = bq_client.query(query)
    while not job.done():
        sleep(1)
    logger.info("Getting result from query")
    results = job.result()
    logger.info("Converting result to pandas dataframe")
    dfr = results.to_dataframe()
    logger.info("End download data from bigquery")
    return dfr

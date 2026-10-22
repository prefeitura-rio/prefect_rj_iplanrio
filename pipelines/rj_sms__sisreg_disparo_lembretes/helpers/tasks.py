"""
Utility tasks for SISREG pipeline
"""

from time import sleep

import pandas as pd
from google.cloud import bigquery
from prefect import task
from prefect.logging import get_logger

from pipelines.rj_sms__sisreg_disparo_lembretes.helpers.api_handler import ApiHandler
from utils.settings import WetalkieSettings

logger = get_logger("toolkit")


@task
def download_data_from_bigquery(query: str, billing_project_id: str, bucket_name: str) -> pd.DataFrame:
    """
    Execute a BigQuery SQL query and return results as a pandas DataFrame.

    Args:
        query (str): SQL query to execute in BigQuery
        billing_project_id (str): GCP project ID for billing purposes
        bucket_name (str): GCS bucket name for credential loading

    Returns:
        pd.DataFrame: Query results as a pandas DataFrame

    Raises:
        Exception: If BigQuery job fails or credentials cannot be loaded
    """
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

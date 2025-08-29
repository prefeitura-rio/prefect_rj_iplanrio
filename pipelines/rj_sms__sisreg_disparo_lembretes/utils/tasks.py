# -*- coding: utf-8 -*-
"""
Utility tasks for SISREG pipeline
"""

from time import sleep

import pandas as pd
from basedosdados import Base
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task

from .api_handler import ApiHandler


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
    log("Querying data from BigQuery")
    query = str(query)
    bq_client = bigquery.Client(
        credentials=Base(bucket_name=bucket_name)._load_credentials(mode="prod"),
        project=billing_project_id,
    )
    job = bq_client.query(query)
    while not job.done():
        sleep(1)
    log("Getting result from query")
    results = job.result()
    log("Converting result to pandas dataframe")
    dfr = results.to_dataframe()
    log("End download data from bigquery")
    return dfr


@task
def access_api(login_route: str = "users/login") -> ApiHandler:
    """
    Access Wetalkie API and return authenticated API handler
    Credentials are injected via environment variables by sidecar container
    """
    # Get credentials from environment (injected by sidecar)
    url = getenv_or_action("URL")
    username = getenv_or_action("USERNAME")
    password = getenv_or_action("PASSWORD")

    api = ApiHandler(base_url=url, username=username, password=password, login_route=login_route)

    return api

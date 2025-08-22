# -*- coding: utf-8 -*-
"""
Utility tasks for CRM Wetalkie Atualiza Contato pipeline
Reutiliza funções do pipeline rj_crm__api_wetalkie
"""

from time import sleep
from typing import Dict, List, Union

import pandas as pd
from basedosdados import Base
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect.exceptions import PrefectException

from pipelines.rj_crm__wetalkie_atualiza_contato.utils.api_handler import ApiHandler


@task
def access_api(
    infisical_path: str,
    infisical_url: str,
    infisical_username: str,
    infisical_password: str,
    login_route: str = "users/login",
) -> ApiHandler:
    """
    Access API and return authenticated handler to be used in other requests.
    """
    url = getenv_or_action(infisical_url)
    username = getenv_or_action(infisical_username)
    password = getenv_or_action(infisical_password)

    api = ApiHandler(base_url=url, username=username, password=password, login_route=login_route)
    return api


@task
def skip_flow_if_empty(
    data: Union[pd.DataFrame, List, str, Dict],
    message: str = "Data is empty. Skipping flow.",
) -> Union[pd.DataFrame, List, str, Dict]:
    """Skip the flow if input data is empty."""
    if len(data) == 0:
        log(message)
        raise PrefectException(message)
    return data


def download_data_from_bigquery(query: str, billing_project_id: str, bucket_name: str) -> pd.DataFrame:
    """Execute a BigQuery SQL query and return results as a pandas DataFrame."""
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

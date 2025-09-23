# -*- coding: utf-8 -*-
"""
Utility tasks for SMAS Call Center Attendances Weekly pipeline
Adapted from rj_crm__api_wetalkie utils
"""

import os
import uuid
from datetime import datetime
from time import sleep
from typing import Dict, List, Literal, Union

import pandas as pd
from basedosdados import Base
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log
from prefect import task
from prefect.exceptions import PrefectException

from pipelines.rj_crm__api_wetalkie.utils.api_handler import ApiHandler


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
def create_date_partitions(
    dataframe,
    partition_column: str = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
):
    """Create date partitions for a DataFrame and save them to disk."""
    if partition_column is None:
        partition_column = "data_particao"
        dataframe[partition_column] = datetime.now().strftime("%Y-%m-%d")
    else:
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column], errors="coerce")
        dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")
        if dataframe["data_particao"].isnull().any():
            raise ValueError("Some dates in the partition column could not be parsed.")

    dates = dataframe["data_particao"].unique()
    dataframes = [
        (
            date,
            dataframe[dataframe["data_particao"] == date].drop(columns=["data_particao"]),
        )
        for date in dates
    ]

    for _date, _dataframe in dataframes:
        partition_folder = os.path.join(
            root_folder,
            f"ano_particao={_date[:4]}/mes_particao={_date[5:7]}/data_particao={_date}",
        )
        os.makedirs(partition_folder, exist_ok=True)

        file_folder = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")

        if file_format == "csv":
            _dataframe.to_csv(file_folder, index=False)
        elif file_format == "parquet":
            _dataframe.to_parquet(file_folder, index=False)

    log(f"Files saved on {root_folder}")
    return root_folder


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

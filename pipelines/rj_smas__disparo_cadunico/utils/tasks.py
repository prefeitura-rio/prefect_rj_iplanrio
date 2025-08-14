# -*- coding: utf-8 -*-
"""
Utility tasks for prefect_rj_iplanrio pipelines
Migrated and adapted from pipelines_rj_crm_registry
"""

import os
import uuid
from datetime import datetime
from time import sleep
from typing import Dict, List, Literal, Union

import pandas as pd
# import seaborn as sns
from basedosdados import Base  # pylint: disable=E0611, E0401
from google.cloud import bigquery  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import \
    log  # pylint: disable=E0611, E0401
from pipelines.rj_smas__disparo_cadunico.utils.api_handler import ApiHandler
from prefect import task
from prefect.exceptions import PrefectException


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
    Uses environment variables injected from Infisical secrets.

    Args:
        infisical_path: Path in Infisical (e.g., "/wetalkie")
        infisical_url: Key name for URL in Infisical
        infisical_username: Key name for username in Infisical
        infisical_password: Key name for password in Infisical
        login_route: API login endpoint path

    Returns:
        ApiHandler: Authenticated API handler instance
    """

    # Get credentials from environment variables
    url = getenv_or_action(infisical_url)
    username = getenv_or_action(infisical_username)
    password = getenv_or_action(infisical_password)

    # Create and return authenticated API handler
    api = ApiHandler(
        base_url=url, username=username, password=password, login_route=login_route
    )

    return api


@task
def safe_export_df_to_parquet(
    dfr: pd.DataFrame,
    output_path: str = "./",
    wait=None,  # pylint: disable=unused-argument
) -> str:
    """
    Safely exports a DataFrame to a Parquet file.

    Args:
        dfr (pd.DataFrame): The DataFrame to export.
        output_path (str): The path to the output Parquet file.

    Returns:
        str: The path to the output Parquet file.
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    # Generate a unique filename using uuid
    filename = f"{uuid.uuid4()}.csv"
    csv_path = os.path.join(os.path.dirname(output_path), filename)
    dfr.to_csv(csv_path, index=False)
    parquet_path = csv_path.replace(".csv", ".parquet")

    dataframe = pd.read_csv(
        csv_path,
        sep=",",
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
    )
    dataframe.to_parquet(parquet_path, index=False)

    # Delete the csv file
    os.remove(csv_path)
    return parquet_path


@task
def create_date_partitions(
    dataframe,
    partition_column: str = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
    wait=None,  # pylint: disable=unused-argument
):
    """
    Create date partitions for a DataFrame and save them to disk.
    """

    if partition_column is None:
        partition_column = "data_particao"
        dataframe[partition_column] = datetime.now().strftime("%Y-%m-%d")
    else:
        dataframe[partition_column] = pd.to_datetime(
            dataframe[partition_column], errors="coerce"
        )
        dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")
        if dataframe["data_particao"].isnull().any():
            raise ValueError("Some dates in the partition column could not be parsed.")

    dates = dataframe["data_particao"].unique()
    dataframes = [
        (
            date,
            dataframe[dataframe["data_particao"] == date].drop(
                columns=["data_particao"]
            ),
        )  # noqa
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
            safe_export_df_to_parquet(dfr=_dataframe, output_path=file_folder)

    log(f"Files saved on {root_folder}")
    return root_folder


@task
def task_download_data_from_bigquery(
    query: str,
    billing_project_id: str,
    bucket_name: str,
    wait=None,  # pylint: disable=unused-argument
) -> pd.DataFrame:
    """
    Create task to download data from bigquery
    """
    return download_data_from_bigquery(
        query=query, billing_project_id=billing_project_id, bucket_name=bucket_name
    )


@task
def skip_flow_if_empty(
    data: Union[pd.DataFrame, List, str, Dict],
    message: str = "Data is empty. Skipping flow.",
    wait=None,  # pylint: disable=unused-argument
) -> Union[pd.DataFrame, List, str, Dict]:
    """Skip the flow if input data is empty.

    Args:
        data: The data structure to check (DataFrame, list, string, or dict)
        message: Optional message to log when skipping
    """
    if len(data) == 0:
        log(message)
        # In Prefect 3, we raise an exception instead of ENDRUN
        raise PrefectException(message)
    return data


def download_data_from_bigquery(
    query: str, billing_project_id: str, bucket_name: str
) -> pd.DataFrame:
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

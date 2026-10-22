# flake8: noqa:E501
# pylint: disable='line-too-long'
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
from google.cloud import bigquery  # pylint: disable=E0611, E0401
from prefect import task  # pylint: disable=E0611, E0401
from prefect.logging import get_logger

from pipelines.rj_crm__disparo_template.helpers.api_handler import ApiHandler  # pylint: disable=E0611, E0401
from utils.utils import read_env

logger = get_logger("toolkit")


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
    url = read_env(infisical_url)
    username = read_env(infisical_username)
    password = read_env(infisical_password)

    # Create and return authenticated API handler
    api = ApiHandler(base_url=url, username=username, password=password, login_route=login_route)

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
    # Create directory if it doesn't exist (dirname returns "" for bare filenames)
    output_dir = os.path.dirname(output_path) or "."
    os.makedirs(output_dir, exist_ok=True)
    # Generate a unique filename using uuid
    filename = f"{uuid.uuid4()}.csv"
    csv_path = os.path.join(output_dir, filename)
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
    partition_column: str | None = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
):
    """Save a DataFrame in one file, with the columns `ano_particao`, `mes_particao` and `data_particao`."""
    if partition_column is None:
        partition_column = "data_particao"
        dataframe[partition_column] = datetime.now().strftime("%Y-%m-%d")
    else:
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column], errors="coerce")
        dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")
        if dataframe["data_particao"].isnull().any():
            raise ValueError("Some dates in the partition column could not be parsed.")

    dataframe["ano_particao"] = dataframe["data_particao"].str[:4]
    dataframe["mes_particao"] = dataframe["data_particao"].str[5:7]
    columns = [c for c in dataframe.columns if c not in ("ano_particao", "mes_particao", "data_particao")]
    dataframe = dataframe[[*columns, "ano_particao", "mes_particao", "data_particao"]]

    os.makedirs(root_folder, exist_ok=True)
    file_path = os.path.join(root_folder, f"{uuid.uuid4()}.{file_format}")

    if file_format == "csv":
        dataframe.to_csv(file_path, index=False)
    else:
        dataframe.to_parquet(file_path, index=False)

    logger.info("Files saved on %s", root_folder)
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
    return download_data_from_bigquery(query=query, billing_project_id=billing_project_id, bucket_name=bucket_name)


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
    if data is None or len(data) == 0:
        logger.info(message)
        return None
    return data


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
    results = job.result()
    dfr = results.to_dataframe()
    logger.info("End download data from bigquery")
    return dfr


@task
def printar(text):
    """exibe o texto passado como parâmetro"""
    logger.info(f"Printando {text}")

# -*- coding: utf-8 -*-
"""
Utility tasks for rj_smas__disparo_pic pipeline
Robust BigQuery extraction with credential management and job polling
Adapted from pipelines.rj_smas__disparo_cadunico.utils.tasks
"""

import os
import uuid
from datetime import datetime
from time import sleep
from typing import Dict, List, Literal, Union

import pandas as pd
from basedosdados import Base  # pylint: disable=E0611, E0401
from google.cloud import bigquery  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401
from prefect import task
from prefect.exceptions import PrefectException


@task
def task_download_data_from_bigquery(
    query: str,
    billing_project_id: str,
    bucket_name: str,
    wait=None,  # pylint: disable=unused-argument
) -> pd.DataFrame:
    """
    Prefect task to download data from BigQuery.

    This task wraps the download_data_from_bigquery function to make it
    executable within Prefect flows.

    Args:
        query: SQL query string to execute
        billing_project_id: GCP project ID for billing purposes
        bucket_name: GCS bucket name for credential loading
        wait: Unused parameter for Prefect task chaining (default: None)

    Returns:
        pd.DataFrame: Query results as a pandas DataFrame

    Raises:
        Exception: If BigQuery job fails or credentials cannot be loaded
    """
    return download_data_from_bigquery(query=query, billing_project_id=billing_project_id, bucket_name=bucket_name)


def download_data_from_bigquery(query: str, billing_project_id: str, bucket_name: str) -> pd.DataFrame:
    """
    Execute a BigQuery SQL query and return results as a pandas DataFrame.

    This function handles credential loading via basedosdados.Base, creates a
    BigQuery client with proper credentials, submits the query as a job,
    polls for completion, and retrieves results.

    Args:
        query: SQL query string to execute in BigQuery
        billing_project_id: GCP project ID for billing purposes
        bucket_name: GCS bucket name for credential loading

    Returns:
        pd.DataFrame: Query results as a pandas DataFrame

    Raises:
        Exception: If BigQuery job fails or credentials cannot be loaded

    Example:
        >>> df = download_data_from_bigquery(
        ...     query="SELECT * FROM dataset.table LIMIT 10",
        ...     billing_project_id="my-project",
        ...     bucket_name="my-bucket"
        ... )
    """
    log("Querying data from BigQuery")
    query = str(query)

    # Load credentials using basedosdados Base class
    credentials = Base(bucket_name=bucket_name)._load_credentials(mode="prod")

    # Create BigQuery client with credentials and billing project
    bq_client = bigquery.Client(credentials=credentials, project=billing_project_id)

    # Submit query as job
    job = bq_client.query(query)
    log(f"BigQuery job submitted: {job.job_id}")

    # Poll for job completion
    log("Waiting for BigQuery job to complete...")
    while not job.done():
        sleep(1)

    log(f"BigQuery job completed: {job.job_id}")
    log("Getting result from query")

    # Retrieve results
    results = job.result()

    log("Converting result to pandas dataframe")
    dfr = results.to_dataframe()

    log(f"End download data from bigquery. Rows returned: {len(dfr)}")
    return dfr


@task
def safe_export_df_to_parquet(
    dfr: pd.DataFrame,
    output_path: str = "./",
    wait=None,  # pylint: disable=unused-argument
) -> str:
    """
    Safely export a DataFrame to a Parquet file.

    Converts DataFrame to CSV first, then to Parquet format with proper
    encoding and data type handling.

    Args:
        dfr: The DataFrame to export
        output_path: The path to the output Parquet file
        wait: Unused parameter for Prefect task chaining (default: None)

    Returns:
        str: The path to the output Parquet file

    Raises:
        OSError: If directory creation or file operations fail
    """
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Generate a unique filename using uuid
    filename = f"{uuid.uuid4()}.csv"
    csv_path = os.path.join(os.path.dirname(output_path), filename)

    dfr.to_csv(csv_path, index=False)

    # Read CSV with proper encoding and type handling
    dataframe = pd.read_csv(
        csv_path,
        sep=",",
        dtype=str,
        keep_default_na=False,
        encoding="utf-8",
    )

    # Convert to Parquet
    parquet_path = csv_path.replace(".csv", ".parquet")
    dataframe.to_parquet(parquet_path, index=False)

    # Delete the temporary CSV file
    os.remove(csv_path)
    log(f"Parquet file saved to: {parquet_path}")

    return parquet_path


@task
def create_date_partitions(
    dataframe: pd.DataFrame,
    partition_column: str | None = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder: str = "./data/",
    wait=None,  # pylint: disable=unused-argument
) -> str:
    """
    Create date partitions for a DataFrame and save them to disk.

    Partitions data by date in the format:
    ano_particao={YYYY}/mes_particao={MM}/data_particao={YYYY-MM-DD}

    Args:
        dataframe: The DataFrame to partition
        partition_column: Column name to use for date partitioning.
                         If None, creates a column with current date.
        file_format: Output format ("csv" or "parquet")
        root_folder: Root folder where partitioned data will be saved
        wait: Unused parameter for Prefect task chaining (default: None)

    Returns:
        str: The root folder path where files are saved

    Raises:
        ValueError: If dates in partition column cannot be parsed
    """
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
    wait=None,  # pylint: disable=unused-argument
) -> Union[pd.DataFrame, List, str, Dict]:
    """
    Skip the flow if input data is empty.

    Raises PrefectException if data is empty, which will prevent further
    flow execution.

    Args:
        data: The data structure to check (DataFrame, list, string, or dict)
        message: Message to log when skipping
        wait: Unused parameter for Prefect task chaining (default: None)

    Returns:
        The input data if not empty

    Raises:
        PrefectException: If data is empty
    """
    if len(data) == 0:
        log(message)
        raise PrefectException(message)
    return data

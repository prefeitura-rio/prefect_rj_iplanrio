"""BigQuery utilities for rj_cvl__osinfo_mongo pipeline.

Functions for querying BigQuery, loading SQL queries, and refreshing
external table metadata caches.
"""

import os
import re
from string import Template

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from .log import get_logger

logger = get_logger(__name__)


def load_query(package_path: str, query_name: str) -> str:
    """Load SQL query from file.

    Args:
        package_path: Path to the package (from __file__).
        query_name: Query name (without .sql extension).

    Returns:
        SQL query as string.

    Raises:
        FileNotFoundError: If query file does not exist.
        IOError: If query file cannot be read.
    """
    # Go up one level from utils/ to pipeline root
    pipeline_dir = os.path.dirname(os.path.dirname(package_path))
    query_dir = os.path.join(pipeline_dir, "queries")
    query_file = os.path.join(query_dir, f"{query_name}.sql")

    if not os.path.exists(query_file):
        raise FileNotFoundError(
            f"Query file not found: {query_file}. "
            f"Expected location: {query_dir}/{query_name}.sql"
        )

    try:
        with open(query_file) as f:
            return f.read()
    except IOError as e:
        logger.error(f"Failed to read query file {query_file}: {e}")
        raise


def get_pendentes(meses_envio: list[str], bq_files_limit: int | None = None) -> pd.DataFrame:
    """Query BigQuery for pending PDFs (uri IS NULL) by month.

    Args:
        meses_envio: List of months in YYYY-MM-DD format (e.g., ["2021-11-01", "2021-12-01"]).
        bq_files_limit: Optional limit on number of files to retrieve.

    Returns:
        DataFrame with columns: mes_envio (DATE), filename (STRING).

    Raises:
        ValueError: If meses_envio is empty or bq_files_limit is invalid.
        KeyError: If query template is missing required variables.
        GoogleAPIError: If BigQuery query fails.
    """
    # Validate meses_envio
    if not meses_envio:
        raise ValueError("meses_envio cannot be empty. Provide at least one month in YYYY-MM-DD format.")

    if not isinstance(meses_envio, list):
        raise TypeError(f"meses_envio must be a list, got {type(meses_envio).__name__}")

    # Validate bq_files_limit
    if bq_files_limit is not None and bq_files_limit <= 0:
        raise ValueError(f"bq_files_limit must be positive, got {bq_files_limit}")

    try:
        query_template = load_query(__file__, "get_meses_pendentes")
    except FileNotFoundError as e:
        logger.error(f"Cannot load query template: {e}")
        raise

    # Format months as SQL array literal: ['2021-11-01', '2021-12-01']
    meses_sql = "[" + ", ".join(f"'{m}'" for m in meses_envio) + "]"

    # Format LIMIT clause if bq_files_limit is provided
    limit_clause = f"LIMIT {bq_files_limit}" if bq_files_limit else ""

    # Use string.Template for substitution with error handling
    template = Template(query_template)
    try:
        query = template.substitute(meses_envio=meses_sql, bq_files_limit_clause=limit_clause)
    except KeyError as e:
        logger.error(f"Query template missing required variable: {e}")
        raise KeyError(
            f"Query template 'get_meses_pendentes.sql' is missing required variable: {e}. "
            f"Expected variables: $meses_envio, $bq_files_limit_clause"
        ) from e

    # Execute BigQuery query with error handling
    try:
        client = bigquery.Client()
        df = client.query(query).result().to_pandas()
    except GoogleAPIError as e:
        logger.error(f"BigQuery error executing query: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error querying BigQuery: {e}")
        raise

    logger.info(
        f"Fetched {len(df)} pending files across {len(meses_envio)} months",
        extra={"meses_count": len(meses_envio), "files_count": len(df)},
    )
    return df


def refresh_metadata_cache(project_id: str, dataset_id: str, table_id: str) -> None:
    """Refresh BigQuery external table metadata cache.

    Executes CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE(...) to force refresh
    of external table metadata (for BigLake tables using GCS).

    Args:
        project_id: GCP project ID.
        dataset_id: BigQuery dataset ID.
        table_id: BigQuery table ID.

    Raises:
        ValueError: If any parameter is empty or contains invalid characters.
        GoogleAPIError: If BigQuery procedure call fails.
    """
    # Validate and sanitize inputs to prevent SQL injection
    # BigQuery identifiers should match: [a-zA-Z0-9_-]
    identifier_pattern = r"^[a-zA-Z0-9_-]+$"

    for param_name, param_value in [
        ("project_id", project_id),
        ("dataset_id", dataset_id),
        ("table_id", table_id),
    ]:
        if not param_value:
            raise ValueError(f"{param_name} cannot be empty")

        if not re.match(identifier_pattern, param_value):
            raise ValueError(
                f"{param_name} contains invalid characters: '{param_value}'. "
                f"Only alphanumeric, underscore, and hyphen characters are allowed."
            )

    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    query = f"CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('{table_ref}')"

    try:
        client = bigquery.Client()
        logger.info(f"Refreshing metadata cache for {table_ref}")
        client.query(query).result()
        logger.info(f"Metadata cache successfully refreshed for {table_ref}")
    except GoogleAPIError as e:
        logger.error(f"BigQuery error refreshing metadata cache for {table_ref}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error refreshing metadata cache for {table_ref}: {e}")
        raise

"""BigQuery utilities for rj_cvl__osinfo_mongo pipeline.

Functions for querying BigQuery, loading SQL queries, and refreshing
external table metadata caches.
"""

from string import Template

import pandas as pd
from google.cloud import bigquery

from .log import logger_da_pipeline

logger = logger_da_pipeline(__name__)


def load_query(package_path: str, query_name: str) -> str:
    """Load SQL query from file.

    Args:
        package_path: Path to the package (from __file__).
        query_name: Query name (without .sql extension).

    Returns:
        SQL query as string.
    """
    import os

    query_dir = os.path.join(os.path.dirname(package_path), "queries")
    query_file = os.path.join(query_dir, f"{query_name}.sql")
    with open(query_file) as f:
        return f.read()


def get_pendentes(meses_envio: list[str]) -> pd.DataFrame:
    """Query BigQuery for pending PDFs (uri IS NULL) by month.

    Args:
        meses_envio: List of months in YYYY-MM-DD format (e.g., ["2021-11-01", "2021-12-01"]).

    Returns:
        DataFrame with columns: mes_envio (DATE), filename (STRING).
    """
    query_template = load_query(__file__, "get_meses_pendentes")

    # Format months as SQL array literal: ['2021-11-01', '2021-12-01']
    meses_sql = "[" + ", ".join(f"'{m}'" for m in meses_envio) + "]"

    # Use string.Template for substitution
    template = Template(query_template)
    query = template.substitute(meses_envio=meses_sql)

    client = bigquery.Client()
    df = client.query(query).to_pandas()

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
    """
    client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    query = f"CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('{table_ref}')"

    logger.info(f"Refreshing metadata cache for {table_ref}")
    client.query(query).result()

    logger.info(f"Metadata cache refreshed for {table_ref}")

# -*- coding: utf-8 -*-
"""
Data extraction tasks for rj_smas__disparo_pic pipeline

Provides robust BigQuery extraction with optional query processor support.
Handles credential management via basedosdados.Base and includes job polling
for reliable execution.

Two extraction methods are available:
  1. extract_from_bigquery: Legacy method for backward compatibility
  2. extract_with_processors: Robust method with processor support (recommended)
"""

import pandas as pd
from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_smas__disparo_pic.processors import get_query_processor
from pipelines.rj_smas__disparo_pic.utils.tasks import task_download_data_from_bigquery


@task
def extract_with_processors(
    query: str,
    billing_project_id: str,
    bucket_name: str | None = None,
    query_processor_name: str | None = None,
) -> pd.DataFrame:
    """
    Extract data from BigQuery with optional query processor support.

    This is the recommended extraction method. It uses robust credential
    management via basedosdados.Base and includes automatic job polling.

    Processors allow dynamic query transformations before execution, enabling
    flexible extraction workflows based on business logic.

    Args:
        query: SQL query string to execute
        billing_project_id: GCP project ID for billing purposes
        bucket_name: GCS bucket name for credential loading. Defaults to
                    billing_project_id if not provided.
        query_processor_name: Optional name of query processor to apply.
                             If specified, the processor transforms the query
                             before execution. Use None to skip processing.

    Returns:
        pd.DataFrame: Query results with all columns from BigQuery

    Raises:
        ValueError: If query processor is specified but not found in registry
        Exception: If BigQuery query fails or credentials cannot be loaded

    Example:
        >>> # Simple extraction without processors
        >>> df = extract_with_processors(
        ...     query="SELECT * FROM dataset.table",
        ...     billing_project_id="my-project"
        ... )
        >>>
        >>> # With processor (if one exists)
        >>> df = extract_with_processors(
        ...     query="SELECT * FROM dataset.table WHERE date > {cutoff_date}",
        ...     billing_project_id="my-project",
        ...     query_processor_name="my_processor"
        ... )
    """
    # Default bucket_name to billing_project_id if not provided
    bucket_name = bucket_name or billing_project_id

    # Apply query processor if specified
    final_query = query
    if query_processor_name:
        processor_func = get_query_processor(query_processor_name)
        if processor_func:
            log(f"Applying query processor: {query_processor_name}")
            final_query = processor_func(query)
            log(f"Query after processing:\n{final_query}")
        else:
            log(
                f"Warning: Query processor '{query_processor_name}' not found, "
                "using original query"
            )

    # Execute query using robust BigQuery method
    log(f"Extracting data from BigQuery (project: {billing_project_id})")
    return task_download_data_from_bigquery(
        query=final_query,
        billing_project_id=billing_project_id,
        bucket_name=bucket_name,
    )


@task
def extract_from_bigquery(sql: str, params: dict | None = None) -> pd.DataFrame:
    """
    DEPRECATED: Use extract_with_processors instead.

    Simple BigQuery extraction without credential management or processor support.

    This method is kept for backward compatibility only. It does not use
    basedosdados.Base for credentials and does not include job polling.

    For new implementations, use extract_with_processors which provides:
      - Robust credential management via basedosdados.Base
      - Automatic job polling for reliable completion
      - Optional query processor support
      - Better error handling and logging

    Args:
        sql: SQL query string to execute
        params: Optional dictionary of parameters for string formatting.
               Parameters will be substituted into the query using str.format()

    Returns:
        pd.DataFrame: Query results

    Raises:
        Exception: If BigQuery query fails

    Example:
        >>> # Simple query
        >>> df = extract_from_bigquery("SELECT * FROM dataset.table")
        >>>
        >>> # With parameters
        >>> df = extract_from_bigquery(
        ...     "SELECT * FROM dataset.table WHERE id = {id}",
        ...     params={"id": 123}
        ... )
    """
    log(
        "WARNING: Using deprecated extract_from_bigquery. "
        "Consider migrating to extract_with_processors for better reliability "
        "and credential management."
    )

    client = bigquery.Client()
    if params:
        sql = sql.format(**params)
    job = client.query(sql)
    return job.to_dataframe()

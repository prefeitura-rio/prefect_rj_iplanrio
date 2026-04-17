# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
"""

from typing import Optional

from iplanrio.pipelines_templates.dump_db.tasks import (
    dump_upload_batch_task,
    format_partitioned_query_task,
    get_database_username_and_password_from_secret_task,
    parse_comma_separated_string_to_list_task,
)
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow


@flow(log_prints=True)
def rj_sma__contracheques(
    db_database: str = "P01.PCRJ",
    db_host: str = "10.70.6.21",
    db_port: str = "1526",
    db_type: str = "oracle",
    db_charset: Optional[str] = "utf8",
    execute_query: str = "execute_query",
    dataset_id: str = "brutos_contracheque",
    table_id: str = "table_id",
    infisical_secret_path: str = "/db-contracheque",
    dump_mode: str = "overwrite",
    partition_date_format: str = "%Y-%m-%d",
    partition_columns: Optional[str] = None,
    lower_bound_date: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    retry_dump_upload_attempts: int = 2,
    batch_size: int = 50000,
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
    max_concurrency: int = 1,
    only_staging_dataset: bool = False,
    add_timestamp_column: bool = True,
):
    """
    Prefect flow that extracts contracheque data from the SMA Oracle database and uploads it in batches to a BigQuery table, optionally partitioning the data and configuring BigLake-specific settings.

    Args:
        db_database (str, default "P01.PCRJ"): Oracle database/service name to connect to as the data source.
        db_host (str, default "10.70.6.21"): Hostname or IP of the Oracle database server.
        db_port (str, default "1526"): Port used to connect to the Oracle database.
        db_type (str, default "oracle"): Database type identifier passed to the dump/query tasks (e.g. "oracle").
        db_charset (Optional[str], default "utf8"): Character set used for the database connection or `None` to use driver default.
        execute_query (str): Base SQL or named query used to build the partitioned dump query.
        dataset_id (str, default "brutos_contracheque"): Target BigQuery dataset ID where data will be written.
        table_id (str): Target BigQuery table ID within `dataset_id` that will receive the dumped data.
        infisical_secret_path (str, default "/db-contracheque"): Path in Infisical where DB username/password secrets are stored.
        dump_mode (str, default "overwrite"): Dump behavior in BigQuery (e.g. "overwrite", "append"), forwarded to the dump task.
        partition_date_format (str, default "%Y-%m-%d"): Date format used when building partition filters in the query.
        partition_columns (Optional[str]): Comma-separated list of column names to use as partition columns in BigQuery; `None` for no partitioning.
        lower_bound_date (Optional[str]): Earliest date (as string in `partition_date_format`) to include in the partitioned query.
        break_query_frequency (Optional[str]): Frequency (e.g. "day", "month") used to break the query into smaller time-based chunks.
        break_query_start (Optional[str]): Start datetime/string for breaking the query into ranges.
        break_query_end (Optional[str]): End datetime/string for breaking the query into ranges.
        retry_dump_upload_attempts (int, default 2): Maximum number of retry attempts for each batch upload.
        batch_size (int, default 50000): Number of rows per batch when dumping and uploading data.
        batch_data_type (str, default "csv"): Format of batch files written before upload (e.g. "csv", "parquet").
        biglake_table (bool, default True): Whether the target BigQuery table is configured as a BigLake table.
        log_number_of_batches (int, default 100): Frequency (in number of batches) at which progress logs are printed.
        max_concurrency (int, default 1): Maximum number of concurrent dump/upload operations.
        only_staging_dataset (bool, default False): If True, writes only to a staging dataset instead of production/final dataset.
        add_timestamp_column (bool, default True): If True, adds a load timestamp column to the dumped data.

    Side effects:
        Writes batched data from the SMA Oracle database to the specified BigQuery dataset/table and renames the Prefect flow run to the target `table_id`.
    """
    rename_current_flow_run_task(new_name=table_id)
    inject_bd_credentials_task(environment="prod")
    secrets = get_database_username_and_password_from_secret_task(infisical_secret_path=infisical_secret_path)
    partition_columns_list = parse_comma_separated_string_to_list_task(text=partition_columns)

    formated_query = format_partitioned_query_task(
        query=execute_query,
        dataset_id=dataset_id,
        table_id=table_id,
        database_type=db_type,
        partition_columns=partition_columns_list,
        lower_bound_date=lower_bound_date,
        date_format=partition_date_format,
        break_query_start=break_query_start,
        break_query_end=break_query_end,
        break_query_frequency=break_query_frequency,
    )

    dump_upload_batch_task(
        queries=formated_query,
        batch_size=batch_size,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        partition_columns=partition_columns_list,
        batch_data_type=batch_data_type,
        biglake_table=biglake_table,
        log_number_of_batches=log_number_of_batches,
        retry_dump_upload_attempts=retry_dump_upload_attempts,
        database_type=db_type,
        hostname=db_host,
        port=db_port,
        user=secrets["DB_USERNAME"],
        password=secrets["DB_PASSWORD"],
        database=db_database,
        charset=db_charset,
        max_concurrency=max_concurrency,
        only_staging_dataset=only_staging_dataset,
        add_timestamp_column=add_timestamp_column,
    )

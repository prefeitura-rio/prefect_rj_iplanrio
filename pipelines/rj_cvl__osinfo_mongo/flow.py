# -*- coding: utf-8 -*-
"""
This flow is used to dump MongoDB collections from OSINFO to BigQuery.

MongoDB Filter Examples:
- Without filter: execute_query = "FILES.files"
- Single file: execute_query = "FILES.chunks|{\"files_id\": \"xxx\"}"
- Multiple files: execute_query = "FILES.chunks|{\"files_id\": {\"$in\": [\"id1\", \"id2\", \"id3\"]}}"
- Filter by field: execute_query = "FILES.chunks|{\"n\": 0}"
- From BigQuery: bq_files_ids_query = "SELECT _id FROM dataset.table"

Note: String values in *_id fields are automatically converted to ObjectId.
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

from .tasks import (
    build_batch_query,
    chunk_list,
    get_batch_dump_mode,
    get_files_ids_from_bigquery,
)


@flow(log_prints=True)
def rj_cvl__osinfo_mongo(
    db_database: str = "OSINFO_FILES",
    db_host: str = "187.111.98.189",
    db_port: str = "27017",
    db_type: str = "mongodb",
    db_charset: Optional[str] = None,
    db_auth_source: Optional[str] = "OSINFO_FILES",
    execute_query: str = "FILES.files",
    bq_files_ids_query: Optional[str] = None,
    files_id_batch_size: int = 10000,
    dataset_id: str = "brutos_osinfo_mongo",
    table_id: str = "files",
    infisical_secret_path: str = "/db-osinfo-mongo",
    dump_mode: str = "overwrite",
    partition_date_format: str = "%Y-%m-%d",
    partition_columns: Optional[str] = None,
    lower_bound_date: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    retry_dump_upload_attempts: int = 1,
    batch_size: int = 50000,
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
    max_concurrency: int = 1,
    only_staging_dataset: bool = True,
    add_timestamp_column: bool = True,
):
    rename_current_flow_run_task(new_name=table_id)
    inject_bd_credentials_task(environment="prod")
    secrets = get_database_username_and_password_from_secret_task(
        infisical_secret_path=infisical_secret_path
    )
    partition_columns_list = parse_comma_separated_string_to_list_task(
        text=partition_columns
    )

    # If BigQuery query is provided, process files_id in batches
    if bq_files_ids_query:
        files_ids = get_files_ids_from_bigquery(bq_files_ids_query)
        batches = chunk_list(files_ids, files_id_batch_size)

        for batch_idx, batch_ids in enumerate(batches):
            batch_query = build_batch_query(execute_query, batch_ids)
            batch_dump_mode = get_batch_dump_mode(batch_idx, dump_mode)

            formatted_query = format_partitioned_query_task(
                query=batch_query,
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
                queries=formatted_query,
                batch_size=batch_size,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode=batch_dump_mode,
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
                auth_source=db_auth_source,
                charset=db_charset,
                max_concurrency=max_concurrency,
                only_staging_dataset=only_staging_dataset,
                add_timestamp_column=add_timestamp_column,
            )
    else:
        # Standard processing without BigQuery integration
        formatted_query = format_partitioned_query_task(
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
            queries=formatted_query,
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
            auth_source=db_auth_source,
            charset=db_charset,
            max_concurrency=max_concurrency,
            only_staging_dataset=only_staging_dataset,
            add_timestamp_column=add_timestamp_column,
        )

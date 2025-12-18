# -*- coding: utf-8 -*-
"""
This flow is used to dump MongoDB collections from OSINFO to BigQuery

MongoDB Filter Examples:
- Without filter: execute_query = "FILES.files"
- Single file: execute_query = "FILES.chunks|{\"files_id\": \"xxx\"}"
- Multiple files: execute_query = "FILES.chunks|{\"files_id\": {\"$in\": [\"id1\", \"id2\", \"id3\"]}}"
- Filter by field: execute_query = "FILES.chunks|{\"n\": 0}"
- From BigQuery: bq_files_ids_query = "SELECT _id FROM dataset.table"

Note: String values in *_id fields are automatically converted to ObjectId.

Individual Files Mode:
- Set save_individual_files_by_id=True to save one parquet file per file_id
- Requires bq_files_ids_query to be set
- Saves to GCS path: gs://bucket/staging/dataset/table/files_id=<id>/data.parquet
- Does NOT create BigLake table (only saves files to GCS)
- More efficient for large numbers of files (processes in batches)

PDF Reconstruction Mode:
- Set reconstruct_pdfs_from_chunks=True to reconstruct PDFs from chunks
- Requires save_individual_files_by_id=True
- Uses temp files from dump process (no GCS download needed)
- Saves to: gs://bucket/staging/dataset/files_pdfs/[original_filename].pdf
- Much faster than downloading from GCS (6-12x speedup)
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
    dump_files_by_id_to_gcs,
    dump_files_by_id_to_gcs_parallel,
    get_batch_dump_mode,
    get_files_ids_from_bigquery,
    update_processing_metadata,
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
    files_id_batch_size: int = 1000,
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
    save_individual_files_by_id: bool = False,
    reconstruct_pdfs_from_chunks: bool = False,
    metadata_table_id: Optional[str] = None,
    gcs_bucket_name: str = "rj-nf-agent",
    use_parallel_processing: bool = False,
    mongo_pool_size: int = 5,
    batch_workers: int = 5,
    upload_max_workers: int = 50,
):
    rename_current_flow_run_task(new_name=table_id)
    inject_bd_credentials_task(environment="prod")
    secrets = get_database_username_and_password_from_secret_task(
        infisical_secret_path=infisical_secret_path
    )
    partition_columns_list = parse_comma_separated_string_to_list_task(
        text=partition_columns
    )

    # NEW MODE: Save individual parquet files per file_id to GCS
    if save_individual_files_by_id:
        if not bq_files_ids_query:
            raise ValueError(
                "save_individual_files_by_id=True requires bq_files_ids_query to be set"
            )

        # Get file_ids from BigQuery
        files_ids = get_files_ids_from_bigquery(bq_files_ids_query)

        # Choose parallel or sequential processing
        if use_parallel_processing:
            processing_results = dump_files_by_id_to_gcs_parallel(
                database_type=db_type,
                hostname=db_host,
                port=int(db_port),
                user=secrets["DB_USERNAME"],
                password=secrets["DB_PASSWORD"],
                database=db_database,
                collection_query=execute_query,
                file_ids=files_ids,
                bucket_name=gcs_bucket_name,
                gcs_path=f"staging/{dataset_id}/{table_id}",
                charset=db_charset,
                auth_source=db_auth_source,
                batch_size=files_id_batch_size,
                mongo_batch_size=batch_size,
                upload_max_workers=upload_max_workers,
                upload_retry_attempts=retry_dump_upload_attempts,
                reconstruct_pdfs_from_chunks=reconstruct_pdfs_from_chunks,
                mongo_pool_size=mongo_pool_size,
                batch_workers=batch_workers,
            )
        else:
            # If reconstruct_pdfs_from_chunks=True, PDFs are reconstructed during batch processing
            processing_results = dump_files_by_id_to_gcs(
                database_type=db_type,
                hostname=db_host,
                port=int(db_port),
                user=secrets["DB_USERNAME"],
                password=secrets["DB_PASSWORD"],
                database=db_database,
                collection_query=execute_query,
                file_ids=files_ids,
                bucket_name=gcs_bucket_name,
                gcs_path=f"staging/{dataset_id}/{table_id}",
                charset=db_charset,
                auth_source=db_auth_source,
                batch_size=files_id_batch_size,
                mongo_batch_size=batch_size,
                reconstruct_pdfs_from_chunks=reconstruct_pdfs_from_chunks,
            )

        # Update metadata table if requested
        if metadata_table_id:
            update_processing_metadata(
                dataset_id=dataset_id,
                metadata_table_id=metadata_table_id,
                processing_results=processing_results,
            )

        return

    # ORIGINAL MODE: If BigQuery query is provided, process files_id in batches
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

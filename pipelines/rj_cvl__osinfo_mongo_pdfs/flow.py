# -*- coding: utf-8 -*-
"""
This flow dumps MongoDB chunks to GCS and creates a metadata table in BigQuery.

Flow:
1. Fetch files_id list from BigQuery
2. Connect to MongoDB
3. For each batch of files:
   - Process files concurrently (asyncio with semaphore)
   - Fetch chunks from MongoDB
   - Save chunks as CSV to GCS: gs://bucket/prefix/{file_id}.csv
   - Record metadata: (file_id, gcs_path, updated_at)
4. Create/update BigQuery metadata table

Future: Integrate PDF reconstruction from rj_cvl__osinfo_pdf_reconstruct
"""

from iplanrio.pipelines_templates.dump_db.tasks import (
    get_database_username_and_password_from_secret_task,
)
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from .tasks import (
    chunk_list,
    create_metadata_table,
    get_files_ids_from_bigquery,
    get_mongodb_client,
    process_files_batch_async,
)


@flow(log_prints=True)
def rj_cvl__osinfo_mongo_pdfs(
    bq_files_ids_query: str,
    db_database: str = "OSINFO_FILES",
    db_collection: str = "chunks",
    db_host: str = "187.111.98.189",
    db_port: str = "27017",
    db_auth_source: str = "OSINFO_FILES",
    gcs_bucket: str = "rj-iplanrio",
    gcs_prefix: str = "staging/brutos_osinfo_mongo/chunks",
    dataset_id: str = "brutos_osinfo_mongo",
    metadata_table_id: str = "target_chunks_gcs",
    infisical_secret_path: str = "/db-osinfo-mongo",
    dump_mode: str = "overwrite",
    files_id_batch_size: int = 100,
    max_concurrency: int = 10,
):
    """
    Dump MongoDB OSINFO chunks collection to GCS and create metadata table

    Args:
        bq_files_ids_query: SQL query to fetch files_id list from BigQuery
        db_database: MongoDB database name
        db_collection: MongoDB collection name (default: chunks)
        db_host: MongoDB host
        db_port: MongoDB port
        db_auth_source: MongoDB auth source database
        gcs_bucket: GCS bucket name
        gcs_prefix: GCS prefix path for chunks
        dataset_id: BigQuery dataset ID
        metadata_table_id: BigQuery metadata table ID
        infisical_secret_path: Path to MongoDB credentials in Infisical
        dump_mode: Dump mode (overwrite or append)
        files_id_batch_size: Number of files to process per batch
        max_concurrency: Maximum concurrent file operations within batch
    """
    rename_current_flow_run_task(new_name=metadata_table_id)
    inject_bd_credentials_task(environment="prod")

    # Get MongoDB credentials
    secrets = get_database_username_and_password_from_secret_task(
        infisical_secret_path=infisical_secret_path
    )

    # Connect to MongoDB
    mongo_client = get_mongodb_client(
        host=db_host,
        port=db_port,
        username=secrets["DB_USERNAME"],
        password=secrets["DB_PASSWORD"],
        auth_source=db_auth_source,
    )

    # Fetch files_id list from BigQuery
    files_ids = get_files_ids_from_bigquery(bq_files_ids_query)
    batches = chunk_list(files_ids, files_id_batch_size)

    # Process batches sequentially, but files within batch concurrently
    all_metadata = []
    for _batch_idx, batch_ids in enumerate(batches):
        # Process all files in batch concurrently with asyncio
        batch_metadata = process_files_batch_async(
            client=mongo_client,
            database=db_database,
            collection=db_collection,
            files_ids=batch_ids,
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_prefix,
            max_concurrency=max_concurrency,
        )

        all_metadata.extend(batch_metadata)

    # Create metadata table with all records
    create_metadata_table(
        metadata_records=all_metadata,
        dataset_id=dataset_id,
        table_id=metadata_table_id,
        dump_mode=dump_mode,
    )

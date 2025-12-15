# -*- coding: utf-8 -*-
"""
Tasks for rj_cvl__osinfo_mongo_pdfs pipeline
"""

import asyncio
from datetime import datetime
from pathlib import Path

from bson import ObjectId
import basedosdados as bd
import pandas as pd
from google.cloud import bigquery
from prefect import get_run_logger, task
from pymongo import MongoClient


@task
def get_files_ids_from_bigquery(query: str) -> list[str]:
    """
    Fetch files_id list from BigQuery

    Args:
        query: SQL query to fetch files_id (e.g., "SELECT _id FROM dataset.table")

    Returns:
        List of files_id strings
    """
    logger = get_run_logger()
    client = bigquery.Client()
    logger.info("Fetching files_id from BigQuery")

    query_job = client.query(query)
    results = query_job.result()

    files_ids = [row[0] for row in results]
    logger.info(f"Retrieved {len(files_ids):,} files_id from BigQuery")

    return files_ids


@task
def chunk_list(items: list, chunk_size: int) -> list[list]:
    """
    Split a list into chunks

    Args:
        items: List to split
        chunk_size: Size of each chunk

    Returns:
        List of chunked lists

    Raises:
        ValueError: If chunk_size is less than or equal to 0
    """
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be greater than 0, got {chunk_size}")
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


@task
def get_mongodb_client(host: str, port: str, username: str, password: str, auth_source: str) -> MongoClient:
    """
    Create MongoDB client connection

    Args:
        host: MongoDB host
        port: MongoDB port
        username: MongoDB username
        password: MongoDB password
        auth_source: MongoDB auth source database

    Returns:
        MongoClient instance
    """
    logger = get_run_logger()
    logger.info(f"Connecting to MongoDB at {host}:{port}")
    connection_string = f"mongodb://{username}:{password}@{host}:{port}/{auth_source}"
    return MongoClient(connection_string)


async def dump_chunks_to_gcs_async(
    client: MongoClient,
    database: str,
    collection: str,
    file_id: str,
    gcs_bucket: str,
    gcs_prefix: str,
    logger=None,
) -> dict:
    """
    Dump chunks for a single file_id to GCS as CSV

    Args:
        client: MongoDB client instance
        database: MongoDB database name
        collection: MongoDB collection name
        file_id: File ID to fetch chunks for
        gcs_bucket: GCS bucket name
        gcs_prefix: GCS prefix path
        logger: Prefect logger instance (optional)

    Returns:
        Dict with file_id, gcs_path, and updated_at
    """
    if logger:
        logger.info(f"Fetching chunks for file_id: {file_id}")

    # Convert string file_id to ObjectId
    object_id = ObjectId(file_id)

    # Query MongoDB for chunks
    db = client[database]
    coll = db[collection]
    chunks = list(coll.find({"files_id": object_id}).sort("n", 1))

    if not chunks:
        if logger:
            logger.warning(f"No chunks found for file_id: {file_id}")
        return None

    if logger:
        logger.info(f"Found {len(chunks)} chunks for file_id: {file_id}")

    # Convert to DataFrame
    df = pd.DataFrame(chunks)

    # Convert ObjectId to string
    df["_id"] = df["_id"].astype(str)
    df["files_id"] = df["files_id"].astype(str)

    # Save to temporary CSV
    temp_dir = Path("/tmp/osinfo_chunks")
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_file = temp_dir / f"{file_id}.csv"
    df.to_csv(temp_file, index=False)

    # Upload to GCS
    st = bd.Storage(dataset_id="brutos_osinfo_mongo", table_id="chunks")
    gcs_path = f"{gcs_prefix}/{file_id}.csv"
    blob = st.bucket.blob(gcs_path)
    blob.upload_from_filename(str(temp_file))

    # Clean up temp file
    temp_file.unlink()

    full_gcs_path = f"gs://{gcs_bucket}/{gcs_path}"
    if logger:
        logger.info(f"Uploaded chunks for {file_id} to GCS")

    return {
        "file_id": file_id,
        "gcs_path": full_gcs_path,
        "updated_at": datetime.now(datetime.timezone.utc).isoformat(),
    }


@task
def create_metadata_table(
    metadata_records: list[dict],
    dataset_id: str,
    table_id: str,
    dump_mode: str,
) -> None:
    """
    Create/update metadata table in BigQuery

    Args:
        metadata_records: List of metadata dicts (file_id, gcs_path, updated_at)
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID (e.g., "target_chunks_gcs")
        dump_mode: Dump mode (overwrite or append)
    """
    logger = get_run_logger()

    # Filter out None records
    metadata_records = [r for r in metadata_records if r is not None]

    if not metadata_records:
        logger.warning("No metadata records to save")
        return

    logger.info(f"Creating metadata table with {len(metadata_records)} records")

    # Convert to DataFrame
    df = pd.DataFrame(metadata_records)

    # Save to temporary Parquet
    temp_dir = Path("/tmp/osinfo_metadata")
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_file = temp_dir / "metadata.parquet"
    df.to_parquet(temp_file, index=False)

    # Create/update table using basedosdados
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)

    if dump_mode == "overwrite":
        if_table_exists = "replace"
        if_storage_data_exists = "replace"
    else:
        if_table_exists = "pass"
        if_storage_data_exists = "pass"

    dataset_is_public = tb.client["bigquery_prod"].project == "datario"

    tb.create(
        path=temp_file,
        if_table_exists=if_table_exists,
        if_storage_data_exists=if_storage_data_exists,
        dataset_is_public=dataset_is_public,
    )

    # Clean up temp file
    temp_file.unlink()

    logger.info(f"Metadata table {dataset_id}.{table_id} created/updated successfully")


@task
async def process_files_batch_async(
    client: MongoClient,
    database: str,
    collection: str,
    files_ids: list[str],
    gcs_bucket: str,
    gcs_prefix: str,
    max_concurrency: int = 10,
) -> list[dict]:
    """
    Process multiple files concurrently using asyncio with semaphore

    Args:
        client: MongoDB client instance
        database: MongoDB database name
        collection: MongoDB collection name
        files_ids: List of file IDs to process
        gcs_bucket: GCS bucket name
        gcs_prefix: GCS prefix path
        max_concurrency: Maximum concurrent operations

    Returns:
        List of metadata records
    """
    logger = get_run_logger()
    semaphore = asyncio.Semaphore(max_concurrency)

    async def process_with_semaphore(file_id: str) -> dict:
        async with semaphore:
            try:
                return await dump_chunks_to_gcs_async(
                    client=client,
                    database=database,
                    collection=collection,
                    file_id=file_id,
                    gcs_bucket=gcs_bucket,
                    gcs_prefix=gcs_prefix,
                    logger=logger,
                )
            except Exception as e:
                logger.error(f"Error processing file_id {file_id}: {e}")
                return None

    # Process all files concurrently
    tasks = [process_with_semaphore(file_id) for file_id in files_ids]
    results = await asyncio.gather(*tasks)

    # Filter out None results
    return [r for r in results if r is not None]

# -*- coding: utf-8 -*-
"""
Tasks for rj_cvl__osinfo_mongo pipeline
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery, storage
from iplanrio.pipelines_templates.dump_db.utils import database_get_db
from iplanrio.pipelines_utils.constants import NOT_SET
from prefect import task


@task
def get_files_ids_from_bigquery(query: str) -> list[str]:
    """
    Fetch files_id list from BigQuery

    Args:
        query: SQL query to fetch files_id (e.g., "SELECT _id FROM dataset.table")

    Returns:
        List of files_id strings
    """
    client = bigquery.Client()
    print(f"Fetching files_id from BigQuery: {query}")

    query_job = client.query(query)
    results = query_job.result()

    files_ids = [row[0] for row in results]
    print(f"Retrieved {len(files_ids):,} files_id from BigQuery")

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
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


@task
def build_batch_query(execute_query: str, batch_ids: list[str]) -> str:
    """
    Build MongoDB query with $in filter for batch processing

    Args:
        execute_query: Base collection query (e.g., "FILES.chunks")
        batch_ids: List of files_id to include in batch

    Returns:
        MongoDB query string with $in filter
    """
    # Extract collection name from execute_query
    base_collection = execute_query.split("|")[0] if "|" in execute_query else execute_query

    # Build query with $in filter
    ids_json = json.dumps(batch_ids)
    return f'{base_collection}|{{"files_id": {{"$in": {ids_json}}}}}'


@task
def get_batch_dump_mode(batch_idx: int, dump_mode: str) -> str:
    """
    Determine dump mode for batch (overwrite for first, append for rest)

    Args:
        batch_idx: Index of current batch
        dump_mode: Original dump mode

    Returns:
        Dump mode to use for this batch
    """
    return dump_mode if batch_idx == 0 else "append"


@task
def dump_files_by_id_to_gcs(
    # Database connection parameters
    database_type: str,
    hostname: str,
    port: int,
    user: str,
    password: str,
    database: str,
    # Query parameters
    collection_query: str,
    file_ids: list[str],
    # GCS parameters
    bucket_name: str,
    gcs_path: str,
    # Processing parameters
    charset: str = NOT_SET,
    auth_source: str = NOT_SET,
    batch_size: int = 10000,
    mongo_batch_size: int = 50000,
):
    """
    Dump MongoDB chunks to individual parquet files per file_id in GCS

    This task processes file_ids in batches to minimize MongoDB queries while
    creating individual parquet files for each file_id in GCS.

    Args:
        database_type: Database type (should be "mongodb")
        hostname: MongoDB hostname
        port: MongoDB port
        user: MongoDB username
        password: MongoDB password
        database: MongoDB database name
        charset: Database charset (optional)
        auth_source: MongoDB authentication database (optional)
        collection_query: Collection query (e.g., "FILES.chunks")
        file_ids: List of file_ids to process
        bucket_name: GCS bucket name
        gcs_path: Base path in GCS (e.g., "staging/dataset/table")
        batch_size: Number of file_ids to process per MongoDB query
        mongo_batch_size: Number of documents to fetch per MongoDB batch

    Example:
        For file_ids ['id1', 'id2', 'id3'] with batch_size=2:
        - Batch 1: Query MongoDB for 'id1' and 'id2'
        - Batch 2: Query MongoDB for 'id3'
        - Save: gs://bucket/path/files_id=id1/data.parquet
                gs://bucket/path/files_id=id2/data.parquet
                gs://bucket/path/files_id=id3/data.parquet
    """
    print(f"Starting dump of {len(file_ids):,} file_ids to GCS")
    print(f"GCS destination: gs://{bucket_name}/{gcs_path}")
    print(f"Processing in batches of {batch_size:,} file_ids")

    # Create MongoDB connection
    db = database_get_db(
        database_type=database_type,
        hostname=hostname,
        port=port,
        user=user,
        password=password,
        database=database,
        charset=charset,
        auth_source=auth_source,
    )

    # Create GCS client
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)

    # Split file_ids into batches
    batches = [file_ids[i:i + batch_size] for i in range(0, len(file_ids), batch_size)]
    total_batches = len(batches)
    total_files_saved = 0

    print(f"Split into {total_batches} batches")

    try:
        for batch_idx, batch_file_ids in enumerate(batches, 1):
            print(f"\n--- Processing batch {batch_idx}/{total_batches} ---")
            print(f"Batch contains {len(batch_file_ids):,} file_ids")

            # Build MongoDB query with $in filter
            batch_query = build_batch_query(collection_query, batch_file_ids)
            print(f"Executing MongoDB query: {batch_query[:100]}...")

            # Execute query
            db.execute_query(batch_query)

            # Fetch all data for this batch
            all_chunks = []
            chunk_count = 0

            while True:
                batch_data = db.fetch_batch(mongo_batch_size)
                if not batch_data or len(batch_data) == 0:
                    break
                all_chunks.extend(batch_data)
                chunk_count += len(batch_data)
                if chunk_count % 100000 == 0:
                    print(f"  Fetched {chunk_count:,} chunks so far...")

            print(f"  Total chunks fetched: {len(all_chunks):,}")

            if not all_chunks:
                print(f"  No data found for this batch, skipping...")
                continue

            # Convert to DataFrame
            columns = db.get_columns()
            df = pd.DataFrame(data=all_chunks, columns=columns)

            # Group by files_id
            grouped = df.groupby('files_id')
            files_in_batch = grouped.ngroups
            print(f"  Grouped into {files_in_batch:,} unique file_ids")

            # Save each file_id as individual parquet
            for file_idx, (file_id, file_chunks_df) in enumerate(grouped, 1):
                # Create GCS path following Hive partitioning
                partition_path = f"{gcs_path}/files_id={file_id}"
                # Use timestamp for filename to track when data was captured
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                blob_path = f"{partition_path}/{timestamp}.parquet"

                # Save to temporary local file first
                temp_dir = Path(f"/tmp/osinfo_dump_{batch_idx}")
                temp_dir.mkdir(parents=True, exist_ok=True)
                temp_file = temp_dir / f"{file_id}.parquet"

                # Write parquet locally
                file_chunks_df.to_parquet(temp_file, engine='pyarrow', index=False)

                # Upload to GCS
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(str(temp_file))

                # Clean up temp file
                temp_file.unlink()

                total_files_saved += 1

                # Log progress every 100 files
                if file_idx % 100 == 0:
                    print(f"    Saved {file_idx}/{files_in_batch} files from batch...")

            print(f"  Batch {batch_idx} complete: saved {files_in_batch} files")

            # Clean up temp directory
            if temp_dir.exists():
                temp_dir.rmdir()

    finally:
        # Ensure connection is closed
        if hasattr(db, 'close'):
            db.close()

    print(f"Total files saved to GCS: {total_files_saved:,}")
    print(f"GCS location: gs://{bucket_name}/{gcs_path}/files_id=*/data.parquet")

    return total_files_saved

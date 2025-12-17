# -*- coding: utf-8 -*-
"""
Tasks for rj_cvl__osinfo_mongo pipeline
"""

import base64
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def _reconstruct_pdf_from_chunks(
    file_id: str,
    temp_file_path: Path,
    filename: str,
    bucket,
    dest_gcs_path: str,
) -> str:
    """
    Reconstruct PDF from chunks parquet file and upload to GCS

    Args:
        file_id: MongoDB file ID
        temp_file_path: Path to local parquet file with chunks
        filename: Original filename for the PDF
        bucket: GCS bucket object
        dest_gcs_path: Destination path in GCS for PDFs

    Returns:
        filename on success
    """
    # Read parquet with chunks
    chunks_df = pd.read_parquet(temp_file_path)

    # Sort by chunk order 'n'
    chunks_df = chunks_df.sort_values('n')

    # Concatenate bytes from 'data' column
    pdf_bytes = b''.join(chunks_df['data'].values)

    # Save PDF to temporary file
    pdf_temp_file = Path(f"/tmp/pdf_{file_id}.pdf")
    with open(pdf_temp_file, 'wb') as f:
        f.write(pdf_bytes)

    # Upload to GCS
    dest_blob_path = f"{dest_gcs_path}/{filename}"
    blob = bucket.blob(dest_blob_path)
    blob.upload_from_filename(str(pdf_temp_file))

    # Cleanup
    pdf_temp_file.unlink()
    temp_file_path.unlink()  # Remove parquet temp file

    return filename


def _decode_base64_data(x):
    """
    Decode base64 string to bytes (iplanrio database_sql.py encodes bytes as base64)

    Args:
        x: Value from MongoDB chunk data column

    Returns:
        Bytes object
    """
    if isinstance(x, bytes):
        return x
    elif isinstance(x, str):
        # iplanrio converts bytes to base64 strings
        return base64.b64decode(x)
    else:
        return x


def _upload_file_to_gcs(
    bucket, temp_file: Path, blob_path: str, file_id: str, keep_temp: bool = False
) -> str:
    """
    Upload a single file to GCS and optionally clean up local file

    Args:
        bucket: GCS bucket object
        temp_file: Local temporary file path
        blob_path: Destination path in GCS
        file_id: File ID for logging
        keep_temp: If True, keep temp file after upload (default: False)

    Returns:
        file_id on success
    """
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(str(temp_file))

    if not keep_temp:
        temp_file.unlink()  # Clean up only if not keeping

    return file_id


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
    batch_size: int = 500,  # Changed from 10000 to 500 for better memory management
    mongo_batch_size: int = 50000,
    upload_max_workers: int = 10,  # Number of parallel upload threads
    reconstruct_pdfs_from_chunks: bool = False,  # Keep temp files for PDF reconstruction
) -> dict[str, dict]:
    """
    Dump MongoDB chunks to individual parquet files per file_id in GCS

    This task processes file_ids in batches to minimize MongoDB queries while
    creating individual parquet files for each file_id in GCS. Uses parallel
    uploads for better performance.

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
        batch_size: Number of file_ids to process per MongoDB query (default: 500)
        mongo_batch_size: Number of documents to fetch per MongoDB batch
        upload_max_workers: Number of parallel upload threads (default: 10)
        reconstruct_pdfs_from_chunks: Keep temp files for PDF reconstruction (default: False)

    Returns:
        Dict mapping file_id to processing info:
        {
            "file_id": {
                "chunks_captured_at": datetime,
                "chunks_gcs_path": "gs://...",
                "chunks_not_found": False,
                "temp_file_path": Path(...),  # Only if reconstruct_pdfs_from_chunks=True
            },
            ...
        }

    Example:
        For file_ids ['id1', 'id2', 'id3'] with batch_size=2:
        - Batch 1: Query MongoDB for 'id1' and 'id2'
        - Batch 2: Query MongoDB for 'id3'
        - Save locally, then upload in parallel (10 threads)
        - Result: gs://bucket/path/files_id=id1/20251217_143025.parquet
                  gs://bucket/path/files_id=id2/20251217_143025.parquet
                  gs://bucket/path/files_id=id3/20251217_143025.parquet
    """
    print(f"Starting dump of {len(file_ids):,} file_ids to GCS")
    print(f"GCS destination: gs://{bucket_name}/{gcs_path}")
    print(f"Processing in batches of {batch_size:,} file_ids")
    if reconstruct_pdfs_from_chunks:
        print("Keeping temp files for PDF reconstruction")

    # Track processing results for all file_ids
    processing_results = {}

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

            # Prepare temp directory
            temp_dir = Path(f"/tmp/osinfo_dump_{batch_idx}")
            temp_dir.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # Step 1: Save all files locally first (fast)
            print(f"  Saving {files_in_batch} files locally...")
            local_files = []
            capture_time = datetime.now()

            for file_id, file_chunks_df in grouped:
                temp_file = temp_dir / f"{file_id}.parquet"

                # Ensure 'data' column is bytes type for binary data
                # Note: iplanrio database_sql.py converts bytes to base64 strings, so we need to decode
                if 'data' in file_chunks_df.columns:
                    file_chunks_df['data'] = file_chunks_df['data'].apply(_decode_base64_data)

                file_chunks_df.to_parquet(temp_file, engine='pyarrow', index=False)

                partition_path = f"{gcs_path}/files_id={file_id}"
                blob_path = f"{partition_path}/{timestamp}.parquet"
                gcs_full_path = f"gs://{bucket_name}/{blob_path}"

                local_files.append((temp_file, blob_path, file_id, gcs_full_path))

            print(f"  Local save complete. Starting parallel upload with {upload_max_workers} workers...")

            # Step 2: Upload to GCS in parallel (much faster)
            uploaded_count = 0

            with ThreadPoolExecutor(max_workers=upload_max_workers) as executor:
                # Submit all upload tasks
                futures = {
                    executor.submit(
                        _upload_file_to_gcs,
                        bucket,
                        temp_file,
                        blob_path,
                        file_id,
                        reconstruct_pdfs_from_chunks,  # Pass keep_temp parameter
                    ): (file_id, temp_file, gcs_full_path)
                    for temp_file, blob_path, file_id, gcs_full_path in local_files
                }

                # Process completed uploads
                for future in as_completed(futures):
                    try:
                        file_id = future.result()
                        uploaded_count += 1

                        # Get metadata from futures
                        file_id_key, temp_file_path, gcs_full_path_result = futures[future]

                        # Store processing results
                        processing_results[file_id] = {
                            "chunks_captured_at": capture_time,
                            "chunks_gcs_path": gcs_full_path_result,
                            "chunks_not_found": False,
                        }

                        # Add temp file path if keeping for PDF reconstruction
                        if reconstruct_pdfs_from_chunks:
                            processing_results[file_id]["temp_file_path"] = temp_file_path

                        # Log progress every 50 files
                        if uploaded_count % 50 == 0:
                            print(f"    Uploaded {uploaded_count}/{files_in_batch} files...")
                    except Exception as e:
                        file_id_key, _, _ = futures[future]
                        print(f"    Error uploading {file_id_key}: {e}")
                        raise

            # Update total counter after batch completes (avoid race condition)
            total_files_saved += uploaded_count
            print(f"  Batch {batch_idx} complete: saved {uploaded_count} files (total: {total_files_saved})")

            # Clean up temp directory only if not keeping files
            if not reconstruct_pdfs_from_chunks and temp_dir.exists():
                temp_dir.rmdir()

    finally:
        # Ensure connection is closed
        if hasattr(db, 'close'):
            db.close()

    print(f"\n=== SUMMARY ===")
    print(f"Total files saved to GCS: {total_files_saved:,}")
    print(f"GCS location: gs://{bucket_name}/{gcs_path}/files_id=*/data.parquet")

    # Check for missing file_ids
    input_file_ids_set = set(file_ids)
    saved_file_ids = set(processing_results.keys())
    missing_file_ids = input_file_ids_set - saved_file_ids

    # Mark missing file_ids in processing_results
    for missing_id in missing_file_ids:
        processing_results[missing_id] = {
            "chunks_captured_at": None,
            "chunks_gcs_path": None,
            "chunks_not_found": True,
        }

    if missing_file_ids:
        print(f"\n⚠️  WARNING: {len(missing_file_ids)} file_ids were NOT saved:")
        for fid in list(missing_file_ids)[:10]:  # Show first 10
            print(f"  - {fid}")
        if len(missing_file_ids) > 10:
            print(f"  ... and {len(missing_file_ids) - 10} more")
    else:
        print(f"✅ All {len(file_ids):,} file_ids were successfully saved!")

    if reconstruct_pdfs_from_chunks:
        temp_files_count = sum(1 for r in processing_results.values() if "temp_file_path" in r)
        print(f"\nKept {temp_files_count:,} temp files for PDF reconstruction")

    return processing_results


@task
def reconstruct_pdfs_from_temp_files(
    processing_results: dict[str, dict],
    dest_gcs_path: str,
    bucket_name: str,
    max_workers: int = 10,
) -> dict[str, dict]:
    """
    Reconstruct PDFs from temporary parquet files and upload to GCS

    Args:
        processing_results: Dict mapping file_id to processing info (must include temp_file_path)
        dest_gcs_path: Destination path in GCS for PDFs (e.g., "staging/dataset/files_pdfs")
        bucket_name: GCS bucket name
        max_workers: Number of parallel processing threads (default: 10)

    Returns:
        Updated processing_results dict with pdf_reconstructed_at and pdf_reconstruction_failed fields

    Example:
        processing_results = {
            "abc123": {
                "chunks_captured_at": datetime.now(),
                "chunks_gcs_path": "gs://...",
                "chunks_not_found": False,
                "temp_file_path": Path("/tmp/osinfo_dump_1/abc123.parquet"),
            }
        }
        reconstruct_pdfs_from_temp_files(
            processing_results=processing_results,
            dest_gcs_path="staging/brutos_osinfo_mongo/files_pdfs",
            bucket_name="datario-public",
        )
        # Result: gs://datario-public/staging/.../files_pdfs/document1.pdf
        #         gs://datario-public/staging/.../files_pdfs/document2.pdf
    """
    # Filter only entries with temp_file_path
    temp_files_map = {
        file_id: result["temp_file_path"]
        for file_id, result in processing_results.items()
        if "temp_file_path" in result
    }

    print(f"Reconstructing {len(temp_files_map):,} PDFs from temp files...")

    if not temp_files_map:
        print("No temp files to process")
        return processing_results

    # Step 1: Fetch metadata from BigQuery (1 query for all file_ids)
    file_ids = list(temp_files_map.keys())
    metadata_query = f"""
      SELECT
        files_id as file_id,
        filename,
        contenttype
      FROM `rj-nf-agent.poc_osinfo_ia.target_files`
      WHERE files_id IN UNNEST(@file_ids)
    """

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("file_ids", "STRING", file_ids)
        ]
    )

    print(f"Fetching metadata for {len(file_ids):,} files from BigQuery...")
    metadata_df = client.query(metadata_query, job_config=job_config).to_dataframe()
    metadata_dict = metadata_df.set_index('file_id').to_dict('index')

    print(f"Loaded metadata for {len(metadata_dict):,} files")

    # Step 2: Reconstruct PDFs in parallel
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)

    pdfs_created = 0
    failed_pdfs = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}

        for file_id, temp_file_path in temp_files_map.items():
            if file_id not in metadata_dict:
                print(f"  Warning: No metadata found for {file_id}, skipping...")
                # Mark as failed in processing_results
                processing_results[file_id]["pdf_reconstructed_at"] = None
                processing_results[file_id]["pdf_reconstruction_failed"] = True
                continue

            meta = metadata_dict[file_id]
            filename = meta['filename']

            future = executor.submit(
                _reconstruct_pdf_from_chunks,
                file_id,
                temp_file_path,
                filename,
                bucket,
                dest_gcs_path,
            )
            futures[future] = file_id

        # Process completed reconstructions
        for future in as_completed(futures):
            file_id = futures[future]
            try:
                filename = future.result()
                pdfs_created += 1

                # Update processing_results with success
                processing_results[file_id]["pdf_reconstructed_at"] = datetime.now()
                processing_results[file_id]["pdf_reconstruction_failed"] = False

                if pdfs_created % 50 == 0:
                    print(f"  Reconstructed {pdfs_created}/{len(temp_files_map)} PDFs...")
            except Exception as e:
                print(f"  Error reconstructing {file_id}: {e}")
                # Update processing_results with failure
                processing_results[file_id]["pdf_reconstructed_at"] = None
                processing_results[file_id]["pdf_reconstruction_failed"] = True
                failed_pdfs.append((file_id, str(e)))

    print(f"Successfully reconstructed {pdfs_created:,} PDFs")
    if failed_pdfs:
        print(f"⚠️  Failed to reconstruct {len(failed_pdfs)} PDFs:")
        for file_id, error in failed_pdfs[:5]:  # Show first 5
            print(f"  - {file_id}: {error}")
        if len(failed_pdfs) > 5:
            print(f"  ... and {len(failed_pdfs) - 5} more")
    print(f"Location: gs://{bucket_name}/{dest_gcs_path}/*.pdf")

    return processing_results


@task
def update_processing_metadata(
    dataset_id: str,
    metadata_table_id: str,
    processing_results: dict[str, dict],
) -> int:
    """
    Update BigQuery metadata table with processing status

    Args:
        dataset_id: BigQuery dataset
        metadata_table_id: Metadata table name
        processing_results: Dict mapping file_id to processing info:
            {
                "file_id_1": {
                    "chunks_captured_at": datetime or None,
                    "chunks_gcs_path": "gs://..." or None,
                    "chunks_not_found": bool,
                    "pdf_reconstructed_at": datetime or None,
                    "pdf_reconstruction_failed": bool or None,
                },
                ...
            }

    Returns:
        Number of rows written to BigQuery
    """
    print(f"Updating metadata for {len(processing_results):,} file_ids...")

    # Prepare rows from processing_results
    rows = []
    for file_id, result in processing_results.items():
        # Ensure all schema fields are present (set None for missing PDF fields)
        row = {
            "file_id": file_id,
            "chunks_captured_at": result.get("chunks_captured_at"),
            "chunks_gcs_path": result.get("chunks_gcs_path"),
            "chunks_not_found": result.get("chunks_not_found", False),
            "pdf_reconstructed_at": result.get("pdf_reconstructed_at"),
            "pdf_reconstruction_failed": result.get("pdf_reconstruction_failed"),
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Write to BigQuery
    client = bigquery.Client()
    table_ref = f"{dataset_id}_staging.{metadata_table_id}"

    schema = [
        bigquery.SchemaField("file_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("chunks_captured_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("chunks_gcs_path", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("chunks_not_found", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("pdf_reconstructed_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("pdf_reconstruction_failed", "BOOLEAN", mode="NULLABLE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    # Summary stats
    chunks_found = sum(1 for r in rows if not r["chunks_not_found"])
    pdfs_ok = sum(1 for r in rows if r["pdf_reconstructed_at"] is not None)

    print(f"✅ Written {len(rows):,} rows to {table_ref}")
    print(f"  - Chunks found: {chunks_found:,}")
    print(f"  - Chunks not found: {len(rows) - chunks_found:,}")
    if pdfs_ok > 0:
        print(f"  - PDFs reconstructed: {pdfs_ok:,}")

    return len(rows)

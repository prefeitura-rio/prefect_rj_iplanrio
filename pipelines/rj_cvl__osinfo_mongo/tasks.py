# -*- coding: utf-8 -*-
"""
Tasks for rj_cvl__osinfo_mongo pipeline
"""

import json

from google.cloud import bigquery
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
    """
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
def get_chunks_from_bigquery(
    files_ids: list[str],
    dataset_id: str = "brutos_osinfo_mongo",
    table_id: str = "chunks"
) -> dict[str, list[dict]]:
    """
    Fetch chunks from BigQuery for specific files_id and group them

    Args:
        files_ids: List of files_id to retrieve chunks for
        dataset_id: BigQuery dataset name
        table_id: BigQuery table name

    Returns:
        Dictionary mapping files_id to sorted list of chunks
        Each chunk has: {'n': int, 'data': bytes}
    """
    client = bigquery.Client()

    # Build query with files_id filter
    files_ids_str = ", ".join([f"'{fid}'" for fid in files_ids])
    query = f"""
        SELECT files_id, n, data
        FROM `{client.project}.{dataset_id}.{table_id}`
        WHERE files_id IN ({files_ids_str})
        ORDER BY files_id, n
    """

    print(f"Fetching chunks for {len(files_ids)} files from BigQuery...")
    query_job = client.query(query)
    results = query_job.result()

    # Group chunks by files_id
    chunks_by_file = {}
    for row in results:
        file_id = row.files_id
        if file_id not in chunks_by_file:
            chunks_by_file[file_id] = []

        chunks_by_file[file_id].append({
            'n': row.n,
            'data': row.data
        })

    # Sort chunks by n for each file
    for file_id in chunks_by_file:
        chunks_by_file[file_id].sort(key=lambda x: x['n'])

    print(f"Retrieved chunks for {len(chunks_by_file)} files")
    for file_id, chunks in chunks_by_file.items():
        print(f"  - {file_id}: {len(chunks)} chunks")

    return chunks_by_file


@task
def reconstruct_pdf(file_id: str, chunks: list[dict]) -> bytes:
    """
    Reconstruct PDF from ordered chunks

    Args:
        file_id: File identifier (for logging)
        chunks: List of chunks sorted by n, each with 'data' field

    Returns:
        Complete PDF as bytes
    """
    print(f"Reconstructing PDF for {file_id} from {len(chunks)} chunks...")

    # Concatenate all chunk data in order
    pdf_data = b''.join(chunk['data'] for chunk in chunks)

    print(f"  - Total size: {len(pdf_data):,} bytes")

    # Validate PDF header
    if not pdf_data.startswith(b'%PDF-'):
        print(f"  - WARNING: File does not start with PDF header")

    return pdf_data


@task
def save_pdf_to_gcs(
    file_id: str,
    pdf_data: bytes,
    bucket_name: str,
    prefix: str = "reconstructed_pdfs"
) -> str:
    """
    Save reconstructed PDF to Google Cloud Storage

    Args:
        file_id: File identifier (used as filename)
        pdf_data: PDF binary data
        bucket_name: GCS bucket name
        prefix: Folder prefix in bucket

    Returns:
        GCS URI of saved file
    """
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Create blob path
    blob_path = f"{prefix}/{file_id}.pdf"
    blob = bucket.blob(blob_path)

    # Upload
    blob.upload_from_string(pdf_data, content_type='application/pdf')

    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    print(f"Saved PDF to {gcs_uri}")

    return gcs_uri


@task
def validate_pdf(file_id: str, pdf_data: bytes) -> dict:
    """
    Validate reconstructed PDF

    Args:
        file_id: File identifier (for logging)
        pdf_data: PDF binary data

    Returns:
        Validation results dict with status and details
    """
    validation = {
        'file_id': file_id,
        'size': len(pdf_data),
        'has_pdf_header': pdf_data.startswith(b'%PDF-'),
        'has_eof_marker': b'%%EOF' in pdf_data[-1024:],  # Check last 1KB
        'is_valid': False
    }

    # Basic validation
    validation['is_valid'] = (
        validation['has_pdf_header'] and
        validation['has_eof_marker'] and
        validation['size'] > 0
    )

    if validation['is_valid']:
        print(f"✓ {file_id}: Valid PDF ({validation['size']:,} bytes)")
    else:
        print(f"✗ {file_id}: Invalid PDF")
        if not validation['has_pdf_header']:
            print(f"  - Missing PDF header")
        if not validation['has_eof_marker']:
            print(f"  - Missing EOF marker")

    return validation

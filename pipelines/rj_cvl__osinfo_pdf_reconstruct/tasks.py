# -*- coding: utf-8 -*-
"""
Tasks for rj_cvl__osinfo_pdf_reconstruct pipeline
"""

from google.cloud import bigquery, storage
from prefect import task


@task
def validate_inputs(files_ids: list[str] = None, bq_files_ids_query: str = None) -> None:
    """
    Validate input parameters

    Args:
        files_ids: List of files_id to reconstruct
        bq_files_ids_query: SQL query to get files_id list

    Raises:
        ValueError: If both or neither parameters are provided
    """
    if files_ids and bq_files_ids_query:
        raise ValueError("Provide either files_ids or bq_files_ids_query, not both")
    if not files_ids and not bq_files_ids_query:
        raise ValueError("Must provide either files_ids or bq_files_ids_query")


@task
def get_files_ids_from_bigquery(query: str, max_files: int = None) -> list[str]:
    """
    Fetch files_id list from BigQuery

    Args:
        query: SQL query to fetch files_id
        max_files: Maximum number of files to return

    Returns:
        List of files_id strings
    """
    client = bigquery.Client()
    print(f"Fetching files_id from BigQuery: {query}")

    query_job = client.query(query)
    results = query_job.result()

    files_ids = [row[0] for row in results]

    if max_files:
        print(f"Limiting to first {max_files} files")
        files_ids = files_ids[:max_files]

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
    chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
    print(f"Split {len(items)} items into {len(chunks)} chunks of size {chunk_size}")
    return chunks


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

    chunks_by_file = {}
    for row in results:
        file_id = row.files_id
        if file_id not in chunks_by_file:
            chunks_by_file[file_id] = []

        chunks_by_file[file_id].append({
            'n': row.n,
            'data': row.data
        })

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

    pdf_data = b''.join(chunk['data'] for chunk in chunks)

    print(f"  - Total size: {len(pdf_data):,} bytes")

    if not pdf_data.startswith(b'%PDF-'):
        print(f"  - WARNING: File does not start with PDF header")

    return pdf_data


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
        'has_eof_marker': b'%%EOF' in pdf_data[-1024:],
        'is_valid': False
    }

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


@task
def save_pdf_to_gcs(
    file_id: str,
    pdf_data: bytes,
    bucket_name: str = "rj-iplanrio",
    prefix: str = "staging/brutos_osinfo_mongo/files_pdfs"
) -> str:
    """
    Save reconstructed PDF to Google Cloud Storage

    Args:
        file_id: File identifier (used as filename)
        pdf_data: PDF binary data
        bucket_name: GCS bucket name (default: rj-iplanrio)
        prefix: Folder prefix in bucket (default: staging/brutos_osinfo_mongo/files_pdfs)

    Returns:
        GCS URI of saved file
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob_path = f"{prefix}/{file_id}.pdf"
    blob = bucket.blob(blob_path)

    blob.upload_from_string(pdf_data, content_type='application/pdf')

    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    print(f"Saved PDF to {gcs_uri}")

    return gcs_uri

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
def get_files_ids_from_bigquery(query: str, max_files: int = None) -> list[dict]:
    """
    Fetch files information from BigQuery

    Args:
        query: SQL query to fetch files info (must return _id and filename columns)
        max_files: Maximum number of files to return

    Returns:
        List of dicts with 'file_id' and 'filename' keys
    """
    client = bigquery.Client()
    print(f"Fetching files from BigQuery: {query}")

    query_job = client.query(query)
    results = query_job.result()

    files_info = []
    for row in results:
        # Support both formats: (file_id, filename) or just (file_id,)
        if len(row) >= 2:
            files_info.append({
                'file_id': row[0],
                'filename': row[1]
            })
        else:
            # Fallback: use file_id as filename if only one column
            files_info.append({
                'file_id': row[0],
                'filename': row[0]
            })

    if max_files:
        print(f"Limiting to first {max_files} files")
        files_info = files_info[:max_files]

    print(f"Retrieved {len(files_info):,} files from BigQuery")

    return files_info


@task
def convert_ids_to_info(files_ids: list[str]) -> list[dict]:
    """
    Convert simple file_ids list to files_info format

    Args:
        files_ids: List of file IDs

    Returns:
        List of dicts with 'file_id' and 'filename' keys
    """
    return [{'file_id': fid, 'filename': fid} for fid in files_ids]


@task
def extract_file_ids(files_info: list[dict]) -> list[str]:
    """
    Extract file_ids from files_info list

    Args:
        files_info: List of dicts with 'file_id' key

    Returns:
        List of file_id strings
    """
    return [f['file_id'] for f in files_info]


@task
def create_filename_mapping(files_info: list[dict]) -> dict[str, str]:
    """
    Create mapping of file_id to filename

    Args:
        files_info: List of dicts with 'file_id' and 'filename' keys

    Returns:
        Dict mapping file_id to filename
    """
    return {f['file_id']: f['filename'] for f in files_info}


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

    query = f"""
        SELECT files_id, n, data
        FROM `{client.project}.{dataset_id}.{table_id}`
        WHERE files_id IN UNNEST(@files_ids)
        ORDER BY files_id, n
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("files_ids", "STRING", files_ids)
        ]
    )

    print(f"Fetching chunks for {len(files_ids)} files from BigQuery...")
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()

    chunks_by_file = {}
    corrupted_files = set()

    for row in results:
        file_id = row.files_id

        # Mark file as corrupted if any chunk has null/empty data
        if row.data is None or row.data == '':
            if file_id not in corrupted_files:
                print(f"⚠️ Skipping file {file_id} - chunk {row.n} has null/empty data")
                corrupted_files.add(file_id)
            continue

        # Skip if file already marked as corrupted
        if file_id in corrupted_files:
            continue

        if file_id not in chunks_by_file:
            chunks_by_file[file_id] = []

        chunks_by_file[file_id].append({
            'n': row.n,
            'data': row.data
        })

    # Sort chunks by n for each file
    for file_id in chunks_by_file:
        chunks_by_file[file_id].sort(key=lambda x: x['n'])

    print(f"Retrieved chunks for {len(chunks_by_file)} files ({len(corrupted_files)} files skipped due to null data)")
    for file_id, chunks in chunks_by_file.items():
        chunk_numbers = [c['n'] for c in chunks]
        min_n, max_n = min(chunk_numbers), max(chunk_numbers)

        # Check for gaps in chunk sequence
        expected_chunks = max_n - min_n + 1
        actual_chunks = len(chunks)
        status = "✓" if expected_chunks == actual_chunks else f"⚠️ gaps: expected {expected_chunks}, got {actual_chunks}"

        print(f"  - {file_id}: {len(chunks)} chunks (n: {min_n} to {max_n}) {status}")

    return chunks_by_file


@task
def reconstruct_pdf(file_id: str, chunks: list[dict]) -> bytes:
    """
    Reconstruct PDF from ordered chunks

    Args:
        file_id: File identifier (for logging)
        chunks: List of chunks sorted by n, each with 'data' field (base64 strings from BigQuery)

    Returns:
        Complete PDF as bytes
    """
    import base64

    print(f"Reconstructing PDF for {file_id} from {len(chunks)} chunks...")

    # Decode base64 strings from BigQuery to bytes
    pdf_data = b''.join(base64.b64decode(chunk['data']) for chunk in chunks)

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
    prefix: str = "staging/brutos_osinfo_mongo/files_pdfs",
    filename: str = None
) -> str:
    """
    Save reconstructed PDF to Google Cloud Storage

    Args:
        file_id: File identifier (for logging)
        pdf_data: PDF binary data
        bucket_name: GCS bucket name (default: rj-iplanrio)
        prefix: Folder prefix in bucket (default: staging/brutos_osinfo_mongo/files_pdfs)
        filename: Custom filename (without .pdf extension). If None, uses file_id

    Returns:
        GCS URI of saved file
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Use filename if provided, otherwise use file_id
    pdf_filename = filename if filename else file_id
    blob_path = f"{prefix}/{pdf_filename}.pdf"
    blob = bucket.blob(blob_path)

    blob.upload_from_string(pdf_data, content_type='application/pdf')

    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    print(f"Saved PDF to {gcs_uri}")

    return gcs_uri

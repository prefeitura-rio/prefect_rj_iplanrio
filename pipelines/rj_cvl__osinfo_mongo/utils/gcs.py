"""GCS (Google Cloud Storage) utilities for rj_cvl__osinfo_mongo pipeline.

Functions for uploading and checking PDF and chunk files in GCS.
"""

from datetime import datetime

import pandas as pd
from google.cloud import storage

from .log import logger_da_pipeline

logger = logger_da_pipeline(__name__)


def save_chunks_to_gcs(
    chunks_df: pd.DataFrame, files_id: str, bucket_name: str, base_path: str = "staging/brutos_osinfo_mongo"
) -> str:
    """Save chunk data to GCS as Parquet.

    Args:
        chunks_df: DataFrame with chunk data.
        files_id: The files_id (used for path).
        bucket_name: GCS bucket name.
        base_path: Base path in bucket.

    Returns:
        GCS path (blob name) of saved file.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    blob_name = f"{base_path}/chunks/files_id={files_id}/{timestamp}.parquet"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Convert to parquet bytes and upload
    parquet_bytes = chunks_df.to_parquet(index=False)
    blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")

    logger.info(f"Saved chunks to GCS: {blob_name}")
    return blob_name


def save_pdf_to_gcs(
    pdf_bytes: bytes, filename: str, mes_envio: str, bucket_name: str, base_path: str = "staging/brutos_osinfo_mongo"
) -> str:
    """Save reconstructed PDF to GCS.

    Args:
        pdf_bytes: PDF content as bytes.
        filename: Filename (without extension).
        mes_envio: Month of sending in YYYY-MM-DD format.
        bucket_name: GCS bucket name.
        base_path: Base path in bucket.

    Returns:
        GCS path (blob name) of saved file.
    """
    blob_name = f"{base_path}/files_pdfs/mes_envio={mes_envio}/{filename}"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(pdf_bytes, content_type="application/pdf")

    logger.info(f"Saved PDF to GCS: {blob_name} ({len(pdf_bytes)} bytes)")
    return blob_name


def pdf_exists_in_gcs(
    filename: str, mes_envio: str, bucket_name: str, base_path: str = "staging/brutos_osinfo_mongo"
) -> bool:
    """Check if PDF already exists in GCS (skip-check to avoid reprocessing after crashes).

    Args:
        filename: Filename (without extension).
        mes_envio: Month of sending in YYYY-MM-DD format.
        bucket_name: GCS bucket name.
        base_path: Base path in bucket.

    Returns:
        True if PDF exists in GCS, False otherwise.
    """
    blob_name = f"{base_path}/files_pdfs/mes_envio={mes_envio}/{filename}"

    client = storage.Client()
    exists = client.bucket(bucket_name).blob(blob_name).exists()

    if exists:
        logger.debug(f"PDF already exists in GCS: {blob_name}")
    return exists

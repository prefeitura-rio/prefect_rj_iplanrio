"""Utility functions for rj_cvl__osinfo_mongo pipeline.

Pure functions for:
- Querying BigQuery for pending files
- Mapping filenames to MongoDB files_id
- Fetching and reconstructing PDF chunks
- Uploading to GCS
- Refreshing metadata cache

MongoDB access uses the iplanrio `database_get_db()` wrapper (validated in production),
NOT the native pymongo driver directly. The wrapper API is:
    db.execute_query("COLLECTION|{json filter}")  # sets up a cursor
    db.fetch_batch(n) / db.fetch_all()             # returns list[list], paginated
    db.get_columns()                                # column names matching row order
This wrapper already converts ObjectId -> str and bytes -> base64 str internally.
"""

import base64
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from string import Template
from typing import Any

import pandas as pd
from google.cloud import bigquery, storage
from iplanrio.pipelines_templates.dump_db.utils import database_get_db
from pymongo.errors import AutoReconnect, NetworkTimeout
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MongoConnectionConfig:
    """MongoDB connection configuration.

    Attributes:
        hostname: MongoDB server hostname/IP.
        port: MongoDB server port.
        user: MongoDB username.
        password: MongoDB password.
        database: MongoDB database name.
        auth_source: Authentication source database (default: "OSINFO_FILES").
    """

    hostname: str
    port: str
    user: str
    password: str
    database: str
    auth_source: str = "OSINFO_FILES"


def get_mongo_connection(mongo_config: MongoConnectionConfig) -> Any:
    """Open a MongoDB connection using the iplanrio database_get_db wrapper.

    Args:
        mongo_config: MongoDB connection configuration.

    Returns:
        A MongoDB Database wrapper instance (iplanrio.pipelines_utils.database_sql.MongoDB).
    """
    return database_get_db(
        database_type="mongodb",
        hostname=mongo_config.hostname,
        port=int(mongo_config.port),
        user=mongo_config.user,
        password=mongo_config.password,
        database=mongo_config.database,
        auth_source=mongo_config.auth_source,
    )


def close_mongo_connection(db: Any) -> None:
    """Close a MongoDB connection if the wrapper exposes a close() method.

    The iplanrio MongoDB wrapper does not currently expose a public close()
    method (validated against production commit). This is a defensive no-op
    matching the pattern used in the previously validated pipeline code.

    Args:
        db: MongoDB Database wrapper instance.
    """
    if hasattr(db, "close"):
        db.close()


def build_mongo_filter_query(collection: str, field: str, values: list[str]) -> str:
    """Build a MongoDB query string with an $in filter (iplanrio wrapper format).

    The iplanrio MongoDB wrapper's execute_query() expects a string in the form
    "COLLECTION|{json filter}". This mirrors the pattern validated in production
    (build_batch_query in the previous pipeline implementation).

    Args:
        collection: MongoDB collection name (e.g., "FILES.chunks").
        field: Field name to filter on (e.g., "files_id" or "filename").
        values: List of values for the $in filter.

    Returns:
        Query string in the format "COLLECTION|{\"field\": {\"$in\": [...]}}".
    """
    values_json = json.dumps(values)
    return f'{collection}|{{"{field}": {{"$in": {values_json}}}}}'


def load_query(package_path: str, query_name: str) -> str:
    """Load SQL query from file.

    Args:
        package_path: Path to the package (from __file__).
        query_name: Query name (without .sql extension).

    Returns:
        SQL query as string.
    """
    import os

    query_dir = os.path.join(os.path.dirname(package_path), "queries")
    query_file = os.path.join(query_dir, f"{query_name}.sql")
    with open(query_file) as f:
        return f.read()


def get_pendentes(meses_envio: list[str]) -> pd.DataFrame:
    """Query BigQuery for pending PDFs (uri IS NULL) by month.

    Args:
        meses_envio: List of months in YYYY-MM-DD format (e.g., ["2021-11-01", "2021-12-01"]).

    Returns:
        DataFrame with columns: mes_envio (DATE), filename (STRING).
    """
    query_template = load_query(__file__, "get_meses_pendentes")

    # Format months as SQL array literal: ['2021-11-01', '2021-12-01']
    meses_sql = "[" + ", ".join(f"'{m}'" for m in meses_envio) + "]"

    # Use string.Template for substitution
    template = Template(query_template)
    query = template.substitute(meses_envio=meses_sql)

    client = bigquery.Client()
    df = client.query(query).to_pandas()

    logger.info(
        f"Fetched {len(df)} pending files across {len(meses_envio)} months",
        extra={"meses_count": len(meses_envio), "files_count": len(df)},
    )
    return df


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of specified size.

    Args:
        items: List to chunk.
        chunk_size: Size of each chunk.

    Returns:
        List of chunks.
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=1, max=2),
    retry=retry_if_exception_type((AutoReconnect, NetworkTimeout)),
)
def map_filenames_to_files_ids(
    filenames: list[str], mongo_config: MongoConnectionConfig
) -> dict[str, list[str]]:
    """Map filenames to MongoDB files_id.

    Sequential lookup (no parallelism) to avoid overwhelming the MongoDB server.
    Process in batches of 2000 filenames per $in query. Uses the iplanrio
    database_get_db wrapper API (execute_query + fetch_all + get_columns),
    matching the pattern validated in production.

    Args:
        filenames: List of filenames to look up.
        mongo_config: MongoDB connection configuration.

    Returns:
        Dictionary mapping filename -> list of files_id (may have multiple IDs per filename).
    """
    if not filenames:
        logger.warning("No filenames provided for lookup")
        return {}

    logger.info(f"Mapping {len(filenames)} filenames to files_id in MongoDB (sequential, batched)")

    db = get_mongo_connection(mongo_config)
    result: dict[str, list[str]] = {}

    try:
        batches = chunk_list(filenames, 2000)

        for batch_idx, batch_filenames in enumerate(batches):
            logger.info(f"Processing filename batch {batch_idx + 1}/{len(batches)} ({len(batch_filenames)} files)")

            query = build_mongo_filter_query("FILES.files", "filename", batch_filenames)
            db.execute_query(query)

            rows = db.fetch_all()
            columns = db.get_columns()

            for row in rows:
                doc = dict(zip(columns, row))
                filename = doc.get("filename")
                files_id = doc.get("_id")  # wrapper already converts ObjectId -> str

                if filename is None or files_id is None:
                    continue

                result.setdefault(filename, []).append(files_id)

            logger.info(f"Batch {batch_idx + 1}: found {len(rows)} documents")
    finally:
        close_mongo_connection(db)

    logger.info(f"Total unique filenames mapped: {len(result)}")
    return result


def _decode_base64_data(value: Any) -> Any:
    """Decode base64 string to bytes (iplanrio wrapper encodes bytes as base64).

    Args:
        value: Value from MongoDB chunk data column (may be str, bytes, or other).

    Returns:
        Decoded bytes if value was a base64 string, otherwise the original value.
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        try:
            return base64.b64decode(value)
        except Exception:
            return value
    return value


@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=1, max=2),
    retry=retry_if_exception_type((AutoReconnect, NetworkTimeout)),
)
def fetch_chunks_batch(db: Any, files_ids: list[str], mongo_batch_size: int = 20000) -> pd.DataFrame:
    """Fetch chunk documents for a batch of files_id from MongoDB in a single query.

    Uses a single $in query for the whole batch (validated production pattern),
    not one query per file. Automatically retries up to 2 times on transient
    MongoDB connection errors.

    Args:
        db: MongoDB Database wrapper instance (already connected, reused across batch).
        files_ids: List of files_id (as strings) to fetch chunks for.
        mongo_batch_size: Page size for MongoDB cursor pagination via fetch_batch.

    Returns:
        DataFrame with all chunk rows for the given files_ids (columns include at
        least: n, data, files_id). Empty DataFrame if no chunks found.
    """
    if not files_ids:
        return pd.DataFrame(columns=["n", "data", "files_id"])

    query = build_mongo_filter_query("FILES.chunks", "files_id", files_ids)
    db.execute_query(query)

    all_rows = []
    while True:
        batch = db.fetch_batch(mongo_batch_size)
        if not batch:
            break
        all_rows.extend(batch)

    columns = db.get_columns()

    if not all_rows:
        logger.warning(f"No chunks found for {len(files_ids)} files_id in this batch")
        return pd.DataFrame(columns=["n", "data", "files_id"])

    df = pd.DataFrame(data=all_rows, columns=columns)

    if "data" in df.columns:
        df["data"] = df["data"].apply(_decode_base64_data)

    logger.info(f"Fetched {len(df)} total chunk rows for {len(files_ids)} files_id")
    return df


def reconstruct_pdf_bytes(chunks_df: pd.DataFrame) -> bytes:
    """Reconstruct PDF from chunks.

    Args:
        chunks_df: DataFrame with columns: n (int), data (bytes).

    Returns:
        Reconstructed PDF as bytes.
    """
    if chunks_df.empty:
        logger.error("Cannot reconstruct PDF from empty chunks")
        raise ValueError("No chunks provided")

    # Sort by n and concatenate data
    chunks_df = chunks_df.sort_values("n")
    pdf_bytes = b"".join(chunks_df["data"])

    logger.info(f"Reconstructed PDF: {len(pdf_bytes)} bytes from {len(chunks_df)} chunks")
    return pdf_bytes


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


def refresh_metadata_cache(project_id: str, dataset_id: str, table_id: str) -> None:
    """Refresh BigQuery external table metadata cache.

    Executes CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE(...) to force refresh
    of external table metadata (for BigLake tables using GCS).

    Args:
        project_id: GCP project ID.
        dataset_id: BigQuery dataset ID.
        table_id: BigQuery table ID.
    """
    client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    query = f"CALL BQ.REFRESH_EXTERNAL_METADATA_CACHE('{table_ref}')"

    logger.info(f"Refreshing metadata cache for {table_ref}")
    client.query(query).result()

    logger.info(f"Metadata cache refreshed for {table_ref}")

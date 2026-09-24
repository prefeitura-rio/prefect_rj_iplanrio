"""MongoDB utilities for rj_cvl__osinfo_mongo pipeline.

Functions for connecting, querying, and manipulating MongoDB collections.
Uses pymongo directly to avoid dependency conflicts with the iplanrio
database wrapper (which pins basedosdados==2.0.0b23 while other pipelines
require basedosdados==2.0.3).
"""

from dataclasses import dataclass
from typing import Any

import pandas as pd
from bson import ObjectId
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, NetworkTimeout
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .log import logger_da_pipeline

logger = logger_da_pipeline(__name__)


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


def get_mongo_connection(mongo_config: MongoConnectionConfig) -> MongoClient:
    """Open a MongoDB connection using pymongo directly.

    Args:
        mongo_config: MongoDB connection configuration.

    Returns:
        A pymongo.MongoClient instance connected to the target database.
    """
    connection_string = (
        f"mongodb://{mongo_config.user}:{mongo_config.password}@"
        f"{mongo_config.hostname}:{mongo_config.port}/{mongo_config.database}"
        f"?authSource={mongo_config.auth_source}"
    )
    client: MongoClient = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
    client.server_info()  # force connection check (fail fast on bad credentials/network)
    return client


def close_mongo_connection(client: MongoClient) -> None:
    """Close a MongoDB connection.

    Args:
        client: pymongo.MongoClient instance to close.
    """
    client.close()


def check_mongo_indexes(mongo_config: MongoConnectionConfig) -> dict[str, dict]:
    """Connect to MongoDB and return index metadata for FILES.chunks and FILES.files.

    Args:
        mongo_config: MongoDB connection configuration.

    Returns:
        Dictionary mapping collection name -> index_information() result.
    """
    client = get_mongo_connection(mongo_config)
    try:
        db = client[mongo_config.database]
        indexes = {
            "FILES.chunks": db["FILES.chunks"].index_information(),
            "FILES.files": db["FILES.files"].index_information(),
        }
    finally:
        close_mongo_connection(client)

    for collection, index_info in indexes.items():
        logger.warning(f"Indexes on {collection}: {index_info}")

    return indexes


def _chunk_list(items: list, chunk_size: int) -> list[list]:
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
def map_filenames_to_files_ids(filenames: list[str], mongo_config: MongoConnectionConfig) -> dict[str, list[str]]:
    """Map filenames to MongoDB files_id.

    Sequential lookup (no parallelism) to avoid overwhelming the MongoDB server.
    Process in batches of 2000 filenames per $in query, using pymongo directly.

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

    client = get_mongo_connection(mongo_config)
    result: dict[str, list[str]] = {}

    try:
        db = client[mongo_config.database]
        collection = db["FILES.files"]
        batches = _chunk_list(filenames, 2000)

        for batch_idx, batch_filenames in enumerate(batches):
            logger.info(f"Processing filename batch {batch_idx + 1}/{len(batches)} ({len(batch_filenames)} files)")

            query = {"filename": {"$in": batch_filenames}}
            documents = list(collection.find(query, {"_id": 1, "filename": 1}))

            for doc in documents:
                filename = doc.get("filename")
                files_id = str(doc["_id"])

                if filename is None:
                    continue

                result.setdefault(filename, []).append(files_id)

            logger.info(f"Batch {batch_idx + 1}: found {len(documents)} documents")
    finally:
        close_mongo_connection(client)

    logger.info(f"Total unique filenames mapped: {len(result)}")
    return result


def _decode_base64_data(value: Any) -> Any:
    """Decode base64 string to bytes, if needed.

    pymongo returns BSON binary fields as native bytes, so this is mostly a
    defensive no-op. Kept in case any chunk stores its "data" field as a
    base64-encoded string instead of native BSON binary.

    Args:
        value: Value from MongoDB chunk data column (may be str, bytes, or other).

    Returns:
        Decoded bytes if value was a base64 string, otherwise the original value.
    """
    import base64

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
def fetch_chunks_batch(client: MongoClient, database: str, files_ids: list[str]) -> pd.DataFrame:
    """Fetch chunk documents for a batch of files_id from MongoDB in a single query.

    Uses a single $in query for the whole batch (validated production pattern),
    not one query per file. Automatically retries up to 2 times on transient
    MongoDB connection errors.

    Args:
        client: pymongo.MongoClient instance (already connected, reused across batch).
        database: MongoDB database name.
        files_ids: List of files_id (as strings) to fetch chunks for.

    Returns:
        DataFrame with all chunk rows for the given files_ids (columns: n, data,
        files_id). Empty DataFrame if no chunks found.
    """
    if not files_ids:
        return pd.DataFrame(columns=["n", "data", "files_id"])

    db = client[database]
    collection = db["FILES.chunks"]

    files_ids_obj = [ObjectId(fid) for fid in files_ids]
    query = {"files_id": {"$in": files_ids_obj}}
    documents = list(collection.find(query))

    if not documents:
        logger.warning(f"No chunks found for {len(files_ids)} files_id in this batch")
        return pd.DataFrame(columns=["n", "data", "files_id"])

    rows = []
    for doc in documents:
        rows.append(
            {
                "n": doc.get("n"),
                "data": doc.get("data"),
                "files_id": str(doc.get("files_id")),
            }
        )

    df = pd.DataFrame(rows)
    df["data"] = df["data"].apply(_decode_base64_data)

    logger.info(f"Fetched {len(df)} total chunk rows for {len(files_ids)} files_id")
    return df

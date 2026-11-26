"""PDF reconstruction utilities for rj_cvl__osinfo_mongo pipeline.

Functions for reconstructing PDF files from MongoDB chunks.
"""

import pandas as pd

from .log import get_logger

logger = get_logger(__name__)


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of specified size.

    Args:
        items: List to chunk.
        chunk_size: Size of each chunk.

    Returns:
        List of chunks.
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def chunk_by_size(items: list[dict], max_items: int, max_bytes: int | None, default_bytes: int) -> list[list[dict]]:
    """Split items into batches bounded by both count and total bytes.

    Keeps the original order. A batch is closed when adding the next item would
    exceed ``max_items`` or ``max_bytes``; an item bigger than ``max_bytes``
    still gets a batch of its own.

    Args:
        items: Dicts with an optional ``length`` (bytes).
        max_items: Maximum number of items per batch.
        max_bytes: Maximum total bytes per batch, or None/0 to disable.
        default_bytes: Size assumed for items without a known length.

    Returns:
        List of batches.
    """
    batches: list[list[dict]] = []
    current: list[dict] = []
    current_bytes = 0
    for item in items:
        size = item.get("length") or default_bytes
        if current and (len(current) >= max_items or (max_bytes and current_bytes + size > max_bytes)):
            batches.append(current)
            current, current_bytes = [], 0
        current.append(item)
        current_bytes += size
    if current:
        batches.append(current)
    return batches


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

"""PDF reconstruction utilities for rj_cvl__osinfo_mongo pipeline.

Functions for reconstructing PDF files from MongoDB chunks.
"""

import pandas as pd

from .log import logger_da_pipeline

logger = logger_da_pipeline(__name__)


def chunk_list(items: list, chunk_size: int) -> list[list]:
    """Split a list into chunks of specified size.

    Args:
        items: List to chunk.
        chunk_size: Size of each chunk.

    Returns:
        List of chunks.
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


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

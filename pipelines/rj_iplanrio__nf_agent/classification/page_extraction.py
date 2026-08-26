"""Single-page PDF extraction helper for the Gemini classifier."""

import logging
from pathlib import Path

import fitz  # PyMuPDF

logger = logging.getLogger(__name__)


def extract_page_as_bytes(pdf_path: Path, page_num: int, as_pdf: bool = False) -> bytes:
    """
    Extract a single page from PDF as PNG or PDF bytes.

    :param pdf_path: Path to PDF file.
    :param page_num: Page number (0-indexed).
    :param as_pdf: If True, return single-page PDF bytes; if False, return PNG bytes.
    :returns: PNG or PDF bytes depending on as_pdf parameter.
    :raises ValueError: If page_num is out of range.
    :raises RuntimeError: If PDF is corrupted or cannot be processed.
    """
    doc = None
    new_doc = None

    try:
        doc = fitz.open(pdf_path)

        # Validate page number is within valid range
        if page_num < 0 or page_num >= len(doc):
            raise ValueError(
                f"Page number {page_num} is out of range. PDF has {len(doc)} pages (valid range: 0-{len(doc) - 1})"
            )

        if as_pdf:
            # Create a new PDF with just this one page
            new_doc = fitz.open()  # Create empty PDF
            try:
                new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
            except RuntimeError as e:
                # Handle PyMuPDF errors (e.g., corrupted PDFs, object number out of range)
                logger.error(
                    f"Failed to insert page {page_num} from {pdf_path.name}: {e}. PDF may be corrupted or malformed."
                )
                raise RuntimeError(f"Failed to extract page {page_num} as PDF: {e}") from e

            pdf_bytes = new_doc.tobytes()
            return pdf_bytes
        else:
            # Extract as PNG (original behavior)
            page = doc[page_num]

            # Render at 2x resolution for better quality
            zoom = 2.0
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat)

            img_bytes = pix.tobytes("png")
            return img_bytes

    except (ValueError, RuntimeError):
        # Re-raise validation and known errors
        raise
    except Exception as e:
        # Catch unexpected errors
        logger.error(f"Unexpected error extracting page {page_num} from {pdf_path.name}: {e}")
        raise RuntimeError(f"Unexpected error extracting page {page_num}: {e}") from e
    finally:
        # Always close documents to free resources
        if new_doc is not None:
            try:
                new_doc.close()
            except Exception:
                pass
        if doc is not None:
            try:
                doc.close()
            except Exception:
                pass

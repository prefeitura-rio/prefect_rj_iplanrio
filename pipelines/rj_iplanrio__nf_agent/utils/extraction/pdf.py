"""PDF slicing and page batching helpers for ``NFExtractor``."""

import io
from pathlib import Path

from pypdf import PdfReader, PdfWriter


class NFExtractorPdfMixin:
    """PDF filtering and page-batching helpers."""

    def _create_filtered_pdf(self, pdf_path: Path, pages: list[int]) -> bytes:
        """
        Create a filtered PDF containing only specified pages.

        :param pdf_path: Path to source PDF.
        :param pages: Page numbers to include (1-indexed).
        :returns: PDF as bytes.
        """
        reader = PdfReader(str(pdf_path))
        writer = PdfWriter()

        # Add specified pages to writer (convert from 1-indexed to 0-indexed)
        for page_num in pages:
            writer.add_page(reader.pages[page_num - 1])

        # Write to bytes
        pdf_bytes = io.BytesIO()
        writer.write(pdf_bytes)
        pdf_bytes.seek(0)

        return pdf_bytes.read()

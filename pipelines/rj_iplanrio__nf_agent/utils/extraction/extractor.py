"""
NF Data Extractor using the ``NFExtractor`` class.

The extractor is split into cohesive mixins to keep the file maintainable
while preserving the public API (``NFExtractor`` and ``extract_nf_data``).
"""

from pathlib import Path

from .api import NFExtractorApiMixin
from .auth import NFExtractorAuthMixin
from .coalesce import NFExtractorCoalesceMixin
from .pdf import NFExtractorPdfMixin
from .prompt import NFExtractorPromptMixin


class NFExtractor(
    NFExtractorAuthMixin,
    NFExtractorPromptMixin,
    NFExtractorPdfMixin,
    NFExtractorCoalesceMixin,
    NFExtractorApiMixin,
):
    """Extract structured NF data using Google Gemini."""


def extract_nf_data(
    pdf_path: Path,
    pages: list[int] | None = None,
    service_account_file: str | None = None,
    api_key: str | None = None,
) -> dict:
    """
    Convenience function to extract NF data from a PDF.

    :param pdf_path: Path to PDF file.
    :param pages: Specific pages to process (1-indexed).
    :param service_account_file: Path to Google service account JSON.
    :param api_key: Google API key.
    :returns: Extraction result dictionary.
    """
    extractor = NFExtractor(service_account_file=service_account_file, api_key=api_key)
    return extractor.extract_from_pdf(pdf_path, pages=pages)

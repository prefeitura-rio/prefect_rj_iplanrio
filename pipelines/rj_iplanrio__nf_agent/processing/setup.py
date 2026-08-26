"""Construction and lazy helpers for ``POCProcessor``."""

import io
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import fitz  # PyMuPDF

from ..classification.gemini_classifier import GeminiClassifier
from ..extraction import NFExtractor
from ..io.gcs_downloader import GCSDownloader
from ..io.sqlite_cache import DatabaseManager

if TYPE_CHECKING:
    from .processor import POCProcessor

logger = logging.getLogger(".".join(__name__.split(".")[:-1] + ["processor"]))


def initialize(
    processor: "POCProcessor",
    db_manager: DatabaseManager,
    gcs_downloader: GCSDownloader,
    gemini_credentials_path: Path,
    temp_dir: Path | None = None,
    quiet: bool = False,
    prompt_versions: dict[str, str] | None = None,
    extraction_batch_size: int = 5,
    min_match_score: int = 2,
    match_requires_pdf_name: bool = False,
) -> None:
    """
    Initialize ``processor``.

    :param processor: The ``POCProcessor`` instance being constructed.
    :param db_manager: Database manager for caching.
    :param gcs_downloader: GCS downloader for PDF retrieval.
    :param gemini_credentials_path: Path to Gemini service account credentials.
    :param temp_dir: Temporary directory for downloaded PDFs.
    :param quiet: Suppress debug output.
    :param prompt_versions: Dict with 'classification' and 'extraction' versions (e.g., {'classification': 'v1', 'extraction': 'v1'}).
        If None, uses latest available versions.
    :param extraction_batch_size: Maximum pages per extraction API call (default: 5).
        Set to 1 to process one page at a time and inject
        per-page classification hints into the prompt.
    :param min_match_score: Minimum fields (CNPJ + número + data) that must match for a
        declaration to be considered found (2 = legacy 2/3 fallback,
        3 = strict perfect match only). Default: 2.
    :param match_requires_pdf_name: Controls the scope of declaration matching. When
        True (legacy behaviour), each page's
        match_id_documento only considers declarations whose pdf_name
        matches the current PDF. When False (default), all declarations
        in the input are considered regardless of which PDF they point
        to — useful for cross-PDF analysis in BigQuery.
    """
    processor.db_manager = db_manager
    processor.gcs_downloader = gcs_downloader
    processor.gemini_credentials_path = gemini_credentials_path
    processor.temp_dir = Path(temp_dir) if temp_dir else Path("run_poc/temp")
    processor.temp_dir.mkdir(parents=True, exist_ok=True)
    processor.quiet = quiet

    # Load prompts from specified versions
    from ..prompts import list_available_versions, load_prompt_version

    if prompt_versions is None:
        # Use latest available versions
        classification_versions = list_available_versions("classification")
        extraction_versions = list_available_versions("extraction")
        prompt_versions = {
            "classification": classification_versions[-1] if classification_versions else "v1",
            "extraction": extraction_versions[-1] if extraction_versions else "v1",
        }

    processor.prompt_versions = prompt_versions
    processor.extraction_batch_size = extraction_batch_size
    processor.min_match_score = min_match_score
    processor.match_requires_pdf_name = match_requires_pdf_name

    # Load the actual prompt content
    processor.classification_prompt = load_prompt_version("classification", prompt_versions["classification"])
    processor.extraction_prompt = load_prompt_version("extraction", prompt_versions["extraction"])

    # Configure logger level based on quiet flag
    if quiet:
        logger.setLevel(logging.WARNING)  # Only warnings and errors
    else:
        logger.setLevel(logging.INFO)  # Info, warnings, and errors

    # Initialize core modules (lazy loaded)
    processor._classifier = None
    processor._extractor = None


def get_classifier(processor: "POCProcessor") -> GeminiClassifier:
    """Lazy load ``processor``'s classifier."""
    if processor._classifier is None:
        processor._classifier = GeminiClassifier(
            service_account_path=str(processor.gemini_credentials_path) if processor.gemini_credentials_path else None,
            classification_prompt=processor.classification_prompt,
        )
    return processor._classifier


def get_extractor(processor: "POCProcessor") -> NFExtractor:
    """Lazy load ``processor``'s extractor."""
    if processor._extractor is None:
        processor._extractor = NFExtractor(
            service_account_file=str(processor.gemini_credentials_path) if processor.gemini_credentials_path else None,
            extraction_prompt=processor.extraction_prompt,
            batch_size=processor.extraction_batch_size,
        )
    return processor._extractor


def pdf_page_to_bytes(pdf_path: Path, page_number: int) -> bytes:
    """
    Convert a single PDF page to PNG image bytes.

    :param pdf_path: Path to PDF file.
    :param page_number: Page number (1-indexed).
    :returns: PNG image bytes.
    """
    doc = fitz.open(str(pdf_path))
    page = doc[page_number - 1]  # Convert to 0-indexed

    # Render page to image (200 DPI)
    # TODO: Since we aren't using it to send to LLM, and solely for deduplication, consider lowering DPI to save time/CPU
    pix = page.get_pixmap(dpi=200)
    img_bytes = pix.pil_tobytes(format="PNG")

    doc.close()
    return img_bytes


def create_filtered_pdf_bytes(pdf_path: Path, pages: list[int]) -> bytes:
    """
    Create filtered PDF with only specified pages.

    :param pdf_path: Path to source PDF.
    :param pages: Page numbers to include (1-indexed).
    :returns: Filtered PDF as bytes.
    """
    from pypdf import PdfReader, PdfWriter

    reader = PdfReader(str(pdf_path))
    writer = PdfWriter()

    # Add specified pages
    for page_num in pages:
        writer.add_page(reader.pages[page_num - 1])

    # Write to bytes
    pdf_bytes = io.BytesIO()
    writer.write(pdf_bytes)
    pdf_bytes.seek(0)

    return pdf_bytes.read()

"""
POC Pipeline Processor - Processes database rows using core NF pipeline with caching.
Integrates GCS downloading, SQLite caching, and core NF processing modules.
"""

import hashlib
import io
import json
import logging
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF
import pandas as pd

# Core modules
from ..core.classifiers.gemini_classifier import (
    ClassificationOptions,
    GeminiClassifier,
    classify_page_with_model,
    extract_page_as_bytes,
)
from ..compliance import ComplianceValidator
from ..compliance.rules import UnmappedDocumentTypeRule
from ..compliance.utils import normalize_cnpj, normalize_number
from ..extraction import NFExtractor

# POC modules
from ..run_poc.database import DatabaseManager
from ..run_poc.gcs_downloader import GCSDownloader

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Capture all levels, filter in handler

# Create handler that writes to stdout (visible in Prefect Cloud logs)
_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setLevel(logging.INFO)  # Default to INFO level

# Format: [timestamp] [level] message
formatter = logging.Formatter("%(message)s")  # Keep it clean for now
_stream_handler.setFormatter(formatter)

logger.addHandler(_stream_handler)


class ExecutionMode(Enum):
    """Pipeline execution modes for granular control over processing steps."""

    FULL = "full"  # Complete pipeline (default)
    PREPROCESS_CLASSIFICATION = (
        "preprocess_classification"  # Step 1: Generate classification inputs
    )
    RUN_CLASSIFICATION = "run_classification"  # Step 2: Run classification API
    PREPROCESS_EXTRACTION = (
        "preprocess_extraction"  # Step 3: Generate extraction inputs
    )
    RUN_EXTRACTION = "run_extraction"  # Step 4: Run extraction API
    VALIDATE = "validate"  # Step 5: Run validation only


class POCProcessor:
    """Processes NF database rows with caching and GCS integration."""

    # Max inner threads for parallel page classification within a single PDF
    MAX_INTRA_PDF_WORKERS = 5

    def __init__(
        self,
        db_manager: DatabaseManager,
        gcs_downloader: GCSDownloader,
        gemini_credentials_path: Path,
        temp_dir: Path | None = None,
        quiet: bool = False,
        prompt_versions: dict[str, str] | None = None,
        extraction_batch_size: int = 5,
        min_match_score: int = 2,
        output_mode: str = "excel",
        match_requires_pdf_name: bool = False,
    ):
        """
        Initialize processor.

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
        :param output_mode: Output format for process_database results.
            "excel" (default) saves an .xlsx file.
            "json"  saves a per-page JSON file (no BQ/GCS writes).
        :param match_requires_pdf_name: Controls the scope of declaration matching in JSON
            output mode. When True (legacy behaviour), each page's
            match_id_documento only considers declarations whose pdf_name
            matches the current PDF. When False (default), all declarations
            in the input are considered regardless of which PDF they point
            to — useful for cross-PDF analysis in BigQuery.
        """
        self.db_manager = db_manager
        self.gcs_downloader = gcs_downloader
        self.gemini_credentials_path = gemini_credentials_path
        self.temp_dir = Path(temp_dir) if temp_dir else Path("run_poc/temp")
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.quiet = quiet

        # Load prompts from specified versions
        from ..core.prompts import list_available_versions, load_prompt_version

        if prompt_versions is None:
            # Use latest available versions
            classification_versions = list_available_versions('classification')
            extraction_versions = list_available_versions('extraction')
            prompt_versions = {
                'classification': classification_versions[-1] if classification_versions else 'v1',
                'extraction': extraction_versions[-1] if extraction_versions else 'v1'
            }

        self.prompt_versions = prompt_versions
        self.extraction_batch_size = extraction_batch_size
        self.min_match_score = min_match_score
        self.output_mode = output_mode
        self.match_requires_pdf_name = match_requires_pdf_name

        # Load the actual prompt content
        self.classification_prompt = load_prompt_version('classification', prompt_versions['classification'])
        self.extraction_prompt = load_prompt_version('extraction', prompt_versions['extraction'])

        # Configure logger level based on quiet flag
        if quiet:
            _stream_handler.setLevel(logging.WARNING)  # Only warnings and errors
        else:
            _stream_handler.setLevel(logging.INFO)  # Info, warnings, and errors

        # Initialize core modules (lazy loaded)
        self._classifier = None
        self._extractor = None

    @property
    def classifier(self) -> GeminiClassifier:
        """Lazy load classifier."""
        if self._classifier is None:
            self._classifier = GeminiClassifier(
                service_account_path=str(self.gemini_credentials_path) if self.gemini_credentials_path else None,
                save_api_responses=False,  # We manage caching ourselves
                max_workers=10,
                classification_prompt=self.classification_prompt,
            )
        return self._classifier

    @property
    def extractor(self) -> NFExtractor:
        """Lazy load extractor."""
        if self._extractor is None:
            self._extractor = NFExtractor(
                service_account_file=str(self.gemini_credentials_path) if self.gemini_credentials_path else None,
                extraction_prompt=self.extraction_prompt,
                batch_size=self.extraction_batch_size,
            )
        return self._extractor

    def _pdf_page_to_bytes(self, pdf_path: Path, page_number: int) -> bytes:
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

    def _create_filtered_pdf_bytes(self, pdf_path: Path, pages: list[int]) -> bytes:
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

    def preprocess_classification_page(
        self, pdf_path: Path, page_number: int
    ) -> tuple[int, bool]:
        """
        Preprocess a single PDF page for classification (Step 1).
        Converts page to image and saves to api_inputs table.

        :param pdf_path: Path to PDF file.
        :param page_number: Page number to preprocess (1-indexed).
        :returns: Tuple of (input_id, is_new):
            - input_id: ID of the saved input in database
            - is_new: True if newly created, False if already existed
        """
        pdf_name = pdf_path.name
        page_image_bytes = self._pdf_page_to_bytes(pdf_path, page_number)

        input_id, is_new_input, _, _ = self.db_manager.get_or_create_input(
            input_type="classification_page",
            pdf_name=pdf_name,
            content=page_image_bytes,
            page_number=page_number,
            metadata={"page_number": page_number},
        )

        return (input_id, is_new_input)

    # TODO: change name to classify_page, the cache is used, but the
    # cache logic is not the only purpose of the function, maybe
    # add a check hash logic in another function that is called here
    # also make a simpler check using the pdf name and the page
    # number that is called directly on the process_pdf function
    # only the hash check would be done here
    def classify_page_from_cache(
        self,
        pdf_path: Path,
        page_number: int,
        skip_api_call: bool = False,  # TODO: remove this parameter for simplicity
    ) -> tuple[str | None, str | None, bool, str | None, int | None]:
        """
        Classify using cached input, optionally calling API (Step 2).

        :param pdf_path: Path to PDF file.
        :param page_number: Page number to classify (1-indexed).
        :param skip_api_call: If True, only return cached results (don't call API).
        :returns: Tuple of (category, justification, from_cache, cached_pdf_name, cached_page_num):
            - category: Page category or None if no cache and skip_api_call=True
            - justification: Classification justification or empty string
            - from_cache: True if using cached output, False if new API call
            - cached_pdf_name: PDF name of cached entry (None if new API call)
            - cached_page_num: Page number of cached entry (None if new API call)
        """
        pdf_name = pdf_path.name

        # Fast pre-check: skip PDF byte preparation if we already have cached result
        # for this exact (pdf_name, page_number). Saves expensive PNG rendering on re-runs.
        cached_result = self.db_manager.get_cached_classification(pdf_name, page_number)
        if cached_result:
            logger.info(
                f"[CACHE] Early skip for {pdf_name} page {page_number} -> {cached_result['category']}"
            )

            return (
                cached_result["category"],
                cached_result["justification"],
                True,
                cached_result["cached_pdf_name"],
                cached_result["cached_page_num"],
            )

        # TODO: EFFICIENCY - Improve cache checking to avoid redundant PDF→PNG conversion.
        # Current inefficiency: This block always converts PDF→PNG to check content_hash,
        # even though get_cached_classification above already checked (pdf_name, page_number).
        #
        # Suggested fix: Extend get_cached_classification to:
        #   1. First check by (pdf_name, page_number) - fast metadata lookup
        #   2. If not found, check by content_hash for cross-PDF dedup (requires PDF→PNG)
        # This way we only convert PDF→PNG once, and only when necessary.
        #
        # Current behavior serves two purposes:
        #   - Cross-PDF deduplication (identical pages from different PDFs reuse classification)
        #   - RUN_CLASSIFICATION mode support (without preprocess)

        # Get input_id (creates input if doesn't exist) - use PNG for hashing (deduplication)
        page_image_bytes = self._pdf_page_to_bytes(pdf_path, page_number)
        input_id, is_new, cached_pdf_name, cached_page_num = (
            self.db_manager.get_or_create_input(
                input_type="classification_page",
                pdf_name=pdf_name,
                content=page_image_bytes,
                page_number=page_number,
                metadata={"page_number": page_number},
            )
        )

        # Check for cached output
        cached_output = self.db_manager.get_output(input_id)

        if cached_output:
            response_data = json.loads(cached_output["response_text"])
            category = response_data.get("categoria", "Nenhuma das Opções")
            justification = response_data.get("justificativa", "")
            return (category, justification, True, cached_pdf_name, cached_page_num)

        if skip_api_call:
            return (None, "", False, None, None)

        # DEBUG: Check classifier before calling
        thread_id = threading.current_thread().ident
        logger.debug(f"\n[DEBUG Thread {thread_id}] About to call classifier API:")
        logger.debug(f"  PDF: {pdf_path.name}, Page: {page_number}")
        logger.debug(f"  db_manager: {self.db_manager}")
        logger.debug(f"  db_manager.conn: {self.db_manager.conn}")
        logger.debug(f"  classifier type: {type(self.classifier)}")

        # Call API - extract bytes
        start_time = time.time()
        logger.debug(f"[DEBUG Thread {thread_id}] Extracting page bytes for API...")

        # Try to extract page bytes - handle corrupted PDFs gracefully
        try:
            # TODO: OPTIMIZATION - This re-extracts PNG bytes that were already extracted at line 277. However, extract_page_as_bytes has more robust error handling (validation, try/except/finally, resource cleanup) than _pdf_page_to_bytes. To optimize: refactor _pdf_page_to_bytes to match extract_page_as_bytes error handling, then reuse page_image_bytes here when use_pdf_input=False.
            # NOTE: extract_page_as_bytes expects 0-indexed page numbers, so convert from 1-indexed
            page_bytes = extract_page_as_bytes(
                pdf_path, page_number - 1, as_pdf=self.classifier.use_pdf_input
            )
        except (ValueError, RuntimeError) as e:
            # Page extraction failed (corrupted PDF, invalid page number, etc.)
            logger.warning(
                f"Failed to extract page {page_number} from {pdf_path.name}: {e}. "
                f"Marking as 'Nenhuma das Opções'."
            )
            # Return error result without calling API
            error_result = {
                "categoria": "Nenhuma das Opções",
                "justificativa": f"Erro ao extrair página: {e!s}",
                "usage_metadata": {},
            }
            response_text = json.dumps(error_result)
            elapsed = time.time() - start_time
            self.db_manager.save_output(
                input_id=input_id,
                model_name=self.classifier.model_name,
                response_text=response_text,
                usage_metadata={},
                elapsed_seconds=elapsed,
            )
            return (error_result["categoria"], error_result["justificativa"], False, None, None)

        logger.debug(
            f"[DEBUG Thread {thread_id}] Calling classify_page_with_model()..."
        )
        api_result = classify_page_with_model(
            model=self.classifier.model,
            page_bytes=page_bytes,
            page_num=page_number,  # Keep 1-indexed for display/logging
            pdf_name=pdf_path.stem,
            options=ClassificationOptions(
                model_name=self.classifier.model_name,
                save_api_response=False,
                api_response_path=None,
                input_is_pdf=self.classifier.use_pdf_input,
                classification_prompt=self.classifier.classification_prompt,
            ),
        )
        elapsed = time.time() - start_time
        logger.debug(
            f"[DEBUG Thread {thread_id}] [OK] classify_page_with_model() completed"
        )

        # Flatten result to match expected format (categoria at top level)
        if not api_result.get("success"):
            # API call failed (e.g. auth/credential error, network error, quota).
            # Do NOT cache this and do NOT treat it as "Nenhuma das Opções" —
            # that would be indistinguishable from a legitimate classification
            # and would permanently poison the cache with a fake result.
            # Let the caller (process_pdf) see this as a real processing failure.
            raise RuntimeError(
                f"Gemini classification API call failed for {pdf_path.name} "
                f"page {page_number}: {api_result.get('error_message', 'Unknown error')}"
            )

        # Extract classification data and flatten to top level
        classification_data = api_result["classification"]
        result = {
            "categoria": classification_data.get("categoria", "Nenhuma das Opções"),
            "justificativa": classification_data.get("justificativa", ""),
            "usage_metadata": {
                "input_tokens": api_result.get("input_tokens", 0),
                "output_tokens": api_result.get("output_tokens", 0),
                "total_tokens": api_result.get("total_tokens", 0),
            },
        }

        # Save output
        response_text = json.dumps(result)
        self.db_manager.save_output(
            input_id=input_id,
            model_name=self.classifier.model_name,
            response_text=response_text,
            usage_metadata=result.get("usage_metadata", {}),
            elapsed_seconds=elapsed,
        )

        category = result.get("categoria", "Nenhuma das Opções")
        justificativa = result.get("justificativa", "")
        return (category, justificativa, False, None, None)

    def preprocess_extraction_pdf(
        self, pdf_path: Path, nf_pages: list[int]
    ) -> tuple[int, bool]:
        """
        Preprocess filtered PDF for extraction (Step 3).
        Creates filtered PDF with only NF pages and saves to api_inputs table.

        :param pdf_path: Path to source PDF.
        :param nf_pages: List of NF page numbers (1-indexed).
        :returns: Tuple of (input_id, is_new):
            - input_id: ID of the saved input in database
            - is_new: True if newly created, False if already existed
        """
        pdf_name = pdf_path.name
        filtered_pdf_bytes = self._create_filtered_pdf_bytes(pdf_path, nf_pages)

        input_id, is_new_input, _, _ = self.db_manager.get_or_create_input(
            input_type="extraction_filtered_pdf",
            pdf_name=pdf_name,
            content=filtered_pdf_bytes,
            page_number=None,  # NULL for extraction
            metadata={"nf_pages": sorted(nf_pages), "page_count": len(nf_pages)},
        )

        return (input_id, is_new_input)

    # TODO: change name to extract_nf, the cache is used, but it's
    # not the main purpose of the function, the cache logic would
    # fit better in another function that is called inside the
    # process_pdf function, maybe using the check_extraction_cache
    # function
    def extract_nf_from_cache(
        self,
        pdf_path: Path,
        nf_pages: list[int],
        skip_api_call: bool = False,  # TODO: remove this parameter for simplicity
        page_classifications: dict[int, str] | None = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        """
        Extract using cached metadata, optionally calling API (Step 4).
        Uses 1-byte placeholder to save disk space instead of storing full PDF.

        :param pdf_path: Path to PDF file.
        :param nf_pages: List of NF page numbers (1-indexed).
        :param skip_api_call: If True, only return cached results (don't call API).
        :param page_classifications: Optional mapping of original page number (1-indexed) →
            document type as classified by the classifier (e.g.
            {3: "NFS-e", 7: "Fatura"}). Passed through to
            NFExtractor.extract_from_pdf to inject per-page hints
            when extraction_batch_size=1.
        :returns: Tuple of (result, from_cache):
            - result: Extraction result dict or None if no cache and skip_api_call=True
            - from_cache: True if using cached output, False if new API call
        """
        # IMPORTANT: Use stem (no extension) to match classification cache format
        pdf_name = pdf_path.stem

        # Create metadata for cache lookup (no need to create filtered PDF yet)
        nf_pages_sorted = sorted(nf_pages)
        cache_metadata = {"nf_pages": nf_pages_sorted, "page_count": len(nf_pages)}

        # Use minimal placeholder instead of full PDF bytes (saves ~30GB for 19,772 PDFs)
        # Hash is based on (pdf_name + nf_pages) for uniqueness
        # TODO: remove this logic, just checking the pdf name should be enough
        cache_key = f"{pdf_name}:{','.join(map(str, nf_pages_sorted))}"
        content_hash = hashlib.sha256(cache_key.encode()).hexdigest()

        input_id, _, _, _ = self.db_manager.get_or_create_input(
            input_type="extraction_filtered_pdf",
            pdf_name=pdf_name,  # Stored WITHOUT .pdf extension
            content=b"\x00",  # 1 byte placeholder instead of full PDF (~1-2 MB) TODO: review this during the db review
            page_number=None,
            metadata=cache_metadata,
            content_hash_override=content_hash,  # Use custom hash
        )

        # Check for cached output
        cached_output = self.db_manager.get_output(input_id)

        if cached_output:
            result = json.loads(cached_output["response_text"])
            result["cached"] = True
            return (result, True)

        if skip_api_call:
            return (None, False)

        # Call API
        start_time = time.time()
        result = self.extractor.extract_from_pdf(
            pdf_path=pdf_path,
            pages=nf_pages,
            save_api_response=False,
            page_classifications=page_classifications,
        )
        elapsed = time.time() - start_time

        if not result.get("processed_successfully", True):
            # Extraction API call failed (e.g. auth/credential error, network
            # error, quota). Do NOT cache this as if it were a legitimate
            # "0 notas fiscais" result — that would permanently poison the
            # cache. Let the caller (process_pdf) see this as a real failure.
            raise RuntimeError(
                f"Gemini extraction API call failed for {pdf_path.name}: "
                f"{result.get('error', 'Unknown error')}"
            )

        # Save output
        response_text = json.dumps(result, ensure_ascii=False)
        self.db_manager.save_output(
            input_id=input_id,
            model_name=self.extractor.model_name,
            response_text=response_text,
            usage_metadata=result.get("usage_metadata", {}),
            elapsed_seconds=elapsed,
        )

        return (result, False)

    def check_extraction_cache(
        self, pdf_path: Path
    ) -> tuple[dict | None, list[int] | None]:
        """
        Check if extraction output already exists for this PDF.

        This method queries the database for cached extraction results,
        allowing us to skip classification entirely if extraction is already done.

        :param pdf_path: Path to PDF file.
        :returns: Tuple of (extraction_result, nf_pages):
            - If cached: (result_dict, [page_numbers])
            - If not cached: (None, None)
        """
        pdf_name = pdf_path.stem  # Without .pdf extension

        # Query for any extraction inputs for this PDF
        cursor = self.db_manager.conn.execute(
            """
            SELECT id, metadata
            FROM api_inputs
            WHERE pdf_name = ? AND input_type = 'extraction_filtered_pdf'
            LIMIT 1
            """,
            (pdf_name,),
        )

        row = cursor.fetchone()
        if not row:
            return (None, None)

        input_id, metadata_str = row

        # Check if this input has output
        cached_output = self.db_manager.get_output(input_id)
        if not cached_output:
            return (None, None)

        # Parse metadata to get nf_pages
        metadata = json.loads(metadata_str) if metadata_str else {}
        nf_pages = metadata.get("nf_pages", [])

        # Parse extraction result
        result = json.loads(cached_output["response_text"])
        result["cached"] = True

        return (result, nf_pages)

    def check_classification_cache(self, pdf_path: Path, total_pages: int) -> bool:
        """
        Check if ALL pages of this PDF are already classified in cache.

        :param pdf_path: Path to PDF file.
        :param total_pages: Total number of pages in PDF.
        :returns: True if all pages are classified, False otherwise.
        """
        pdf_name = pdf_path.name

        cursor = self.db_manager.conn.execute(
            """
            SELECT COUNT(DISTINCT i.page_number)
            FROM api_inputs i
            JOIN api_outputs o ON i.id = o.input_id
            WHERE i.pdf_name = ?
            AND i.input_type = 'classification_page'
            """,
            (pdf_name,),
        )

        row = cursor.fetchone()
        cached_pages_count = row[0] if row else 0

        return cached_pages_count == total_pages

    def load_all_cached_classifications(
        self, pdf_path: Path
    ) -> tuple[dict[int, str], dict[int, str]]:
        """
        Load ALL cached page classifications with justifications in a single query (optimized).

        :param pdf_path: Path to PDF file.
        :returns: Tuple of (page_categories, page_justifications):
            - page_categories: Dictionary mapping page_number -> category
            - page_justifications: Dictionary mapping page_number -> justificativa
        """
        pdf_name = pdf_path.name
        page_categories = {}
        page_justifications = {}

        cursor = self.db_manager.conn.execute(
            """
            SELECT i.page_number, o.response_text
            FROM api_inputs i
            JOIN api_outputs o ON i.id = o.input_id
            WHERE i.pdf_name = ?
            AND i.input_type = 'classification_page'
            ORDER BY i.page_number
            """,
            (pdf_name,),
        )

        for row in cursor:
            page_num, response_text = row
            try:
                response = json.loads(response_text)
                # Handle both Portuguese 'categoria' and English 'category'
                category = response.get("categoria") or response.get(
                    "category", "Unknown"
                )
                justificativa = response.get("justificativa", "")
                page_categories[page_num] = category
                page_justifications[page_num] = justificativa
            except Exception as exc:
                # Cache entry exists but its JSON is malformed (truncated write,
                # encoding corruption, etc.). Log and fall back to "Unknown" so
                # the page is still emitted — but now visible in logs for investigation.
                logger.warning(
                    "[cache] Malformed classification cache for %s page %d "
                    "(response_text=%r…): %s — falling back to 'Unknown'",
                    pdf_name, page_num, (response_text or "")[:120], exc,
                )
                page_categories[page_num] = "Unknown"
                page_justifications[page_num] = ""

        return page_categories, page_justifications

    def load_cached_page_categories(
        self, pdf_path: Path, total_pages: int
    ) -> dict[int, str]:
        """
        Load cached page categories from database (legacy method, use load_all_cached_classifications for better performance).

        :param pdf_path: Path to PDF file.
        :param total_pages: Total number of pages in PDF.
        :returns: Dictionary mapping page_number -> category.
        """
        # Use full filename with .pdf extension (as stored in database)
        pdf_name = pdf_path.name
        page_categories = {}

        for page_num in range(1, total_pages + 1):
            cursor = self.db_manager.conn.execute(
                """
                SELECT o.response_text
                FROM api_inputs i
                JOIN api_outputs o ON i.id = o.input_id
                WHERE i.pdf_name = ?
                AND i.page_number = ?
                AND i.input_type = 'classification_page'
                LIMIT 1
                """,
                (pdf_name, page_num),
            )

            row = cursor.fetchone()
            if row:
                try:
                    response = json.loads(row[0])
                    # Handle both Portuguese 'categoria' and English 'category'
                    category = response.get("categoria") or response.get(
                        "category", "Unknown"
                    )
                    page_categories[page_num] = category
                except Exception:
                    page_categories[page_num] = "Unknown"
            else:
                page_categories[page_num] = None

        return page_categories

    def process_pdf(
        self,
        pdf_filename: str,
        expected_nfs: list[dict[str, Any]],
        mode: ExecutionMode = ExecutionMode.FULL,
        pdf_path: Path | None = None,
    ) -> dict[str, Any]:
        """
        Process a single PDF according to execution mode.

        :param pdf_filename: Name of PDF file (with or without .pdf extension).
        :param expected_nfs: List of expected NF dictionaries from database.
        :param mode: Execution mode controlling which steps to run.
        :param pdf_path: Optional pre-downloaded PDF path (if None, will download from GCS).
        :returns: Processing result (structure depends on mode).
        """
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing: {pdf_filename} [Mode: {mode.value}]")
        logger.info(f"Expected NFs: {len(expected_nfs)}")
        logger.info(f"{'='*80}")

        # Per-PDF timing accumulators (seconds); None = not measured / all cache hits
        _t_preprocess: float | None = None   # classification pages + filtered-PDF creation
        _t_validacao: float | None = None    # ComplianceValidator + validate_extraction

        # Use pre-downloaded PDF or download from GCS
        clean_temp_file = False
        if pdf_path is None:
            try:
                logger.info("  [Download] Downloading from GCS...")
                pdf_path = self.gcs_downloader.download_pdf_by_name(
                    pdf_name=pdf_filename, local_dir=self.temp_dir
                )
                logger.info(f"  [OK] Downloaded to: {pdf_path}")
                clean_temp_file = True
            except Exception as e:
                logger.error(f"  [X] Download failed: {e}")
                return {
                    "pdf_name": pdf_filename,
                    "mode": mode.value,
                    "success": False,
                    "error": f"Download failed: {e!s}",
                }
        else:
            logger.info(f"  [Using pre-downloaded PDF: {pdf_path}]")

        try:
            # Get total pages
            doc: fitz.Document = fitz.open(str(pdf_path))
            total_pages = len(doc)
            doc.close()

            # CLASSIFICATION FAST PATH: Check if all pages already classified
            # If yes, skip Steps 1-2 entirely (no loops, no hash calculation)
            if mode in [
                ExecutionMode.FULL,
                ExecutionMode.RUN_EXTRACTION,
                ExecutionMode.VALIDATE,
            ]:
                all_classified = self.check_classification_cache(pdf_path, total_pages)

                if all_classified:
                    # All pages classified! Load all classifications in 1 query
                    logger.info(
                        f"  [Classification Fast Path] All {total_pages} pages already classified"
                    )

                    page_categories, page_justifications = self.load_all_cached_classifications(pdf_path)

                    # Identify NF pages from cached classifications
                    from ..core.classifiers.gemini_classifier import NF_CATEGORIES

                    nf_pages = []
                    for page_num, category in page_categories.items():
                        if category in NF_CATEGORIES:
                            nf_pages.append(page_num)

                    logger.info(
                        f"  [OK] Found {len(nf_pages)} NF pages (from classification cache): {nf_pages}"
                    )

                    # Try extraction cache
                    if nf_pages:
                        extraction_result, cached_nf_pages = (
                            self.check_extraction_cache(pdf_path)
                        )

                        if extraction_result and cached_nf_pages is not None:
                            # Have extraction cache! Go straight to validation
                            nf_count = extraction_result.get(
                                "quantidade_notas_fiscais", 0
                            )
                            logger.info(f"  [OK] Extracted {nf_count} NFs [cached]")

                            extracted_nfs = extraction_result.get("notas_fiscais", [])

                            # For RUN_EXTRACTION mode, return immediately
                            if mode == ExecutionMode.RUN_EXTRACTION:
                                return {
                                    "pdf_name": pdf_filename,
                                    "mode": mode.value,
                                    "success": True,
                                    "total_pages": total_pages,
                                    "nf_pages": nf_pages,
                                    "extracted_nf_count": len(extracted_nfs),
                                    "extracted_nfs": extracted_nfs,
                                    "fast_path": True,
                                }

                            # For FULL/VALIDATE: go to validation
                            if mode in [ExecutionMode.FULL, ExecutionMode.VALIDATE]:
                                logger.info(
                                    "  [Step 5/5] Validating against expected NFs..."
                                )
                                # NOTE: Validation rules disabled - all validation in BigQuery
                                validator = ComplianceValidator(
                                    expected_nfs=expected_nfs,
                                    use_bigquery_deduplication=False,
                                    rules=[UnmappedDocumentTypeRule()],
                                    min_match_score=self.min_match_score,
                                )

                                page_categories_list = [
                                    page_categories.get(i + 1)
                                    for i in range(total_pages)
                                ]

                                validation_result = validator.validate_extraction(
                                    pdf_name=pdf_filename,
                                    extracted_nfs=extracted_nfs,
                                    page_categories=page_categories_list,
                                )

                                logger.info(
                                    f"  [Validation] Status: {validation_result['status']}"
                                )

                                return {
                                    "pdf_name": pdf_filename,
                                    "mode": mode.value,
                                    "success": True,
                                    "total_pages": total_pages,
                                    "nf_pages": nf_pages,
                                    "page_categories": page_categories,
                                    "page_justifications": page_justifications,  # ADDED: Include justifications
                                    "extracted_nf_count": len(extracted_nfs),
                                    "extracted_nfs": extracted_nfs,
                                    "validation": validation_result,
                                    "fast_path": True,
                                }
                        # else: No extraction cache but has NF pages → continue to Step 4
                    # No NF pages → validate with empty extraction
                    elif mode in [ExecutionMode.FULL, ExecutionMode.VALIDATE]:
                        logger.info(
                            "  [Step 5/5] Validating against expected NFs..."
                        )
                        # NOTE: Validation rules disabled - all validation in BigQuery
                        validator = ComplianceValidator(
                            expected_nfs=expected_nfs,
                            use_bigquery_deduplication=False,
                            rules=[UnmappedDocumentTypeRule()],
                            min_match_score=self.min_match_score,
                        )

                        page_categories_list = [
                            page_categories.get(i + 1) for i in range(total_pages)
                        ]

                        validation_result = validator.validate_extraction(
                            pdf_name=pdf_filename,
                            extracted_nfs=[],
                            page_categories=page_categories_list,
                        )

                        logger.info(
                            f"  [Validation] Status: {validation_result['status']}"
                        )

                        return {
                            "pdf_name": pdf_filename,
                            "mode": mode.value,
                            "success": True,
                            "total_pages": total_pages,
                            "nf_pages": [],
                            "page_categories": page_categories,
                            "page_justifications": page_justifications,  # ADDED: Include justifications
                            "extracted_nf_count": 0,
                            "extracted_nfs": [],
                            "validation": validation_result,
                            "fast_path": True,
                        }

                # EXTRACTION FAST PATH: Check extraction cache FIRST before any preprocessing
                # If extraction is already cached, skip all classification/extraction steps
                # (This handles legacy cases where classification cache might be incomplete)
                extraction_result, cached_nf_pages = self.check_extraction_cache(
                    pdf_path
                )

                if extraction_result and cached_nf_pages is not None:
                    # We have cached extraction! Skip ALL preprocessing and classification
                    logger.info(
                        "  [Fast Path] Extraction already cached, skipping all preprocessing"
                    )
                    logger.info(
                        f"  [OK] Found {len(cached_nf_pages)} NF pages (from cache): {cached_nf_pages}"
                    )

                    nf_count = extraction_result.get("quantidade_notas_fiscais", 0)
                    logger.info(f"  [OK] Extracted {nf_count} NFs [cached]")

                    extracted_nfs = extraction_result.get("notas_fiscais", [])

                    # Pós-processamento: vincula NFSTs a Faturas de telecom (cross-page merge)
                    from ..compliance.nfst_fatura_cross_page_merger import merge_nfst_with_fatura
                    extracted_nfs = merge_nfst_with_fatura(extracted_nfs)

                    # For RUN_EXTRACTION mode, return immediately
                    if mode == ExecutionMode.RUN_EXTRACTION:
                        return {
                            "pdf_name": pdf_filename,
                            "mode": mode.value,
                            "success": True,
                            "total_pages": total_pages,
                            "nf_pages": cached_nf_pages,
                            "extracted_nf_count": len(extracted_nfs),
                            "extracted_nfs": extracted_nfs,
                            "fast_path": True,  # Indicates we skipped classification
                        }

                    # For FULL/VALIDATE modes, set variables and skip to STEP 5
                    nf_pages = cached_nf_pages

                    # Load cached page categories and justifications from database
                    page_categories, page_justifications = self.load_all_cached_classifications(
                        pdf_path
                    )

                    # Convert to list format for validation (0-indexed list for pages 1..N)
                    # page_categories_list[0] = Page 1, page_categories_list[1] = Page 2, etc.
                    page_categories_list = [
                        page_categories.get(i + 1) for i in range(total_pages)
                    ]

                    # Jump to validation (STEP 5)
                    if mode in [ExecutionMode.FULL, ExecutionMode.VALIDATE]:
                        logger.info("  [Step 5/5] Validating against expected NFs...")
                        # NOTE: Validation rules disabled - all validation in BigQuery
                        validator = ComplianceValidator(
                            expected_nfs=expected_nfs,
                            use_bigquery_deduplication=False,  # TEMPORARILY DISABLED: Avoid repeated BigQuery queries
                            rules=[UnmappedDocumentTypeRule()],
                            min_match_score=self.min_match_score,
                        )

                        # Run validation
                        validation_result = validator.validate_extraction(
                            pdf_name=pdf_filename,
                            extracted_nfs=extracted_nfs,
                            page_categories=page_categories_list,
                        )

                        logger.info(
                            f"  [Validation] Status: {validation_result['status']}"
                        )

                        return {
                            "pdf_name": pdf_filename,
                            "mode": mode.value,
                            "success": True,
                            "total_pages": total_pages,
                            "nf_pages": cached_nf_pages,
                            "page_categories": page_categories,
                            "page_justifications": page_justifications,  # ADDED: Include justifications
                            "extracted_nf_count": len(extracted_nfs),
                            "extracted_nfs": extracted_nfs,
                            "validation": validation_result,
                            "fast_path": True,  # Indicates we skipped classification
                        }

            # STEP 1: Preprocess Classification Inputs
            # Only reached if fast path didn't apply
            if mode in [ExecutionMode.FULL, ExecutionMode.PREPROCESS_CLASSIFICATION]:
                logger.info("  [Step 1/5] Preprocessing classification inputs...")
                preprocessed_count = 0
                for page_num in range(1, total_pages + 1):
                    input_id, is_new = self.preprocess_classification_page(
                        pdf_path, page_num
                    )
                    status = "[NEW]" if is_new else "[CACHED]"
                    logger.info(f"    Page {page_num}: input_id={input_id} {status}")
                    if is_new:
                        preprocessed_count += 1

                logger.info(
                    f"  [OK] Preprocessed {preprocessed_count}/{total_pages} pages (rest already cached)"
                )

                if mode == ExecutionMode.PREPROCESS_CLASSIFICATION:
                    return {
                        "pdf_name": pdf_filename,
                        "mode": mode.value,
                        "success": True,
                        "total_pages": total_pages,
                        "preprocessed_pages": preprocessed_count,
                    }

            # STEP 2: Run Classification
            page_categories = {}
            page_justifications = {}  # ADDED: Store justifications for each page
            nf_pages = []

            if mode in [
                ExecutionMode.FULL,
                ExecutionMode.RUN_CLASSIFICATION,
                ExecutionMode.PREPROCESS_EXTRACTION,
                ExecutionMode.RUN_EXTRACTION,
                ExecutionMode.VALIDATE,
            ]:
                logger.info(f"  [Step 2/5] Running classification ({total_pages} pages, {self.MAX_INTRA_PDF_WORKERS} inner workers)...")

                _t_preprocess_start = time.time()
                _t_classif_start = time.time()
                from ..core.classifiers.gemini_classifier import NF_CATEGORIES

                _max_inner = min(self.MAX_INTRA_PDF_WORKERS, total_pages)
                with ThreadPoolExecutor(max_workers=_max_inner) as _exec:
                    _futures = {
                        _exec.submit(
                            self.classify_page_from_cache, pdf_path, pn, False
                        ): pn
                        for pn in range(1, total_pages + 1)
                    }
                    for _future in as_completed(_futures):
                        pn = _futures[_future]
                        category, justification, from_cache, _, _ = _future.result()
                        page_categories[pn] = category
                        page_justifications[pn] = justification
                        if category in NF_CATEGORIES:
                            nf_pages.append(pn)
                        if not from_cache:
                            logger.info(f"    Page {pn}: {category} [new]")

                _t_classif_wall = time.time() - _t_classif_start
                logger.info(f"  [OK] Found {len(nf_pages)} NF pages: {nf_pages}")

                # Close preprocess timer here for modes that skip Step 3
                if mode not in [ExecutionMode.FULL, ExecutionMode.PREPROCESS_EXTRACTION]:
                    _t_preprocess = time.time() - _t_preprocess_start

                if mode == ExecutionMode.RUN_CLASSIFICATION:
                    return {
                        "pdf_name": pdf_filename,
                        "mode": mode.value,
                        "success": True,
                        "total_pages": total_pages,
                        "page_categories": page_categories,
                        "nf_pages": nf_pages,
                    }

            # STEP 3: Preprocess Extraction Inputs
            # TODO: remove this step, it's not as useful as the preprocess classification
            # and adds a lot of complexity to the pipeline, the extraction should be able
            # to receive the classification results directly and filter the pdf and send
            # to the llm, that logic can be moved to inside the extractor class.
            if mode in [ExecutionMode.FULL, ExecutionMode.PREPROCESS_EXTRACTION]:
                if nf_pages:
                    logger.info("  [Step 3/5] Preprocessing extraction inputs...")
                    input_id, is_new = self.preprocess_extraction_pdf(
                        pdf_path, nf_pages
                    )
                    status = "[NEW]" if is_new else "[CACHED]"
                    logger.info(
                        f"    Filtered PDF ({len(nf_pages)} pages): input_id={input_id} {status}"
                    )
                else:
                    logger.info(
                        "  [Step 3/5] Skipping extraction preprocessing (no NF pages)"
                    )

                # Close preprocess timer (steps 2+3 = local CPU work, no API)
                _t_preprocess = time.time() - _t_preprocess_start

                if mode == ExecutionMode.PREPROCESS_EXTRACTION:
                    return {
                        "pdf_name": pdf_filename,
                        "mode": mode.value,
                        "success": True,
                        "total_pages": total_pages,
                        "nf_pages": nf_pages,
                        "extraction_preprocessed": len(nf_pages) > 0,
                    }

            # STEP 4: Run Extraction

            extracted_nfs = []

            if mode in [
                ExecutionMode.FULL,
                ExecutionMode.RUN_EXTRACTION,
                ExecutionMode.VALIDATE,
            ]:
                if nf_pages:
                    logger.info("  [Step 4/5] Running extraction...")
                    # Pass page_classifications so that per-page hints can be injected
                    # into the prompt when extraction_batch_size=1. page_categories may be
                    # empty here if we arrived via the extraction fast-path (no classification
                    # was run), in which case hints are simply omitted.
                    extraction_result, from_cache = self.extract_nf_from_cache(
                        pdf_path,
                        nf_pages,
                        skip_api_call=False,
                        page_classifications=page_categories if page_categories else None,
                    )

                    cache_marker = "[cached]" if from_cache else "[new]"
                    nf_count = extraction_result.get("quantidade_notas_fiscais", 0)
                    logger.info(f"  [OK] Extracted {nf_count} NFs {cache_marker}")

                    extracted_nfs = extraction_result.get("notas_fiscais", [])

                    # Pós-processamento: vincula NFSTs a Faturas de telecom (cross-page merge)
                    from ..compliance.nfst_fatura_cross_page_merger import merge_nfst_with_fatura
                    extracted_nfs = merge_nfst_with_fatura(extracted_nfs)
                else:
                    logger.info("  [Step 4/5] Skipping extraction (no NF pages)")

                if mode == ExecutionMode.RUN_EXTRACTION:
                    return {
                        "pdf_name": pdf_filename,
                        "mode": mode.value,
                        "success": True,
                        "total_pages": total_pages,
                        "nf_pages": nf_pages,
                        "extracted_nf_count": len(extracted_nfs),
                        "extracted_nfs": extracted_nfs,
                    }

            # STEP 5: Validation and Classification
            if mode in [ExecutionMode.FULL, ExecutionMode.VALIDATE]:
                logger.info("  [Step 5/5] Validating against expected NFs...")
                # NOTE: Validation rules are disabled - all validation will be done in BigQuery
                # Only keeping validation structure for NF matching logic
                _t_validacao_start = time.time()
                validator = ComplianceValidator(
                    expected_nfs=expected_nfs,
                    use_bigquery_deduplication=False,  # TEMPORARILY DISABLED: Avoid repeated BigQuery queries
                    rules=[UnmappedDocumentTypeRule()],
                    min_match_score=self.min_match_score,
                )

                # Get page categories list for validation
                page_categories_list = [
                    page_categories.get(i) for i in range(1, total_pages + 1)
                ]

                validation_result = validator.validate_extraction(
                    pdf_name=pdf_filename,
                    extracted_nfs=extracted_nfs,
                    page_categories=page_categories_list,
                )
                _t_validacao = time.time() - _t_validacao_start

                # Extract classifications for each expected NF
                classifications = []
                for expected_nf in expected_nfs:
                    cnpj = expected_nf.get("cnpj", "")
                    numero = expected_nf.get("num_documento", "")

                    classification = "Not Analyzable"  # Default

                    for entry in validation_result.get("entries", []):
                        if (
                            entry.get("expected_cnpj") == cnpj
                            and entry.get("expected_numero") == numero
                        ):
                            classification = entry.get(
                                "classification", "Not Analyzable"
                            )
                            break

                    classifications.append(
                        {
                            "cnpj": cnpj,
                            "numero_nf": numero,
                            "valor_documento": expected_nf.get("valor_documento"),
                            "valor_pago": expected_nf.get("valor_pago"),
                            "classification": classification,
                        }
                    )

                logger.info("  [OK] Validation complete")
                logger.debug(f"    Summary: {validation_result.get('summary', {})}")

                return {
                    "pdf_name": pdf_filename,
                    "mode": mode.value,
                    "success": True,
                    "total_pages": total_pages,
                    "nf_pages": nf_pages,
                    "page_categories": page_categories,
                    "page_justifications": page_justifications,  # ADDED: Include justifications
                    "extracted_nf_count": len(extracted_nfs),
                    "extracted_nfs": extracted_nfs,
                    "expected_nf_count": len(expected_nfs),
                    "validation": validation_result,
                    "classifications": classifications,
                    # Timing fields (None = not measured / all cache hits)
                    "_t_preprocess_sec":     _t_preprocess,
                    "_t_validacao_sec":      _t_validacao,
                    "_t_classif_wall_sec":   _t_classif_wall,
                }

        except Exception as e:
            # Surface the failure with whatever partial per-page state we
            # accumulated before it happened (e.g. pages 1-3 classified fine,
            # page 4 hit a Gemini auth error). This lets downstream consumers
            # (like the JSON output builder) distinguish "never reached this
            # page" from "processed and found nothing" instead of collapsing
            # the whole PDF into an opaque success.
            local_vars = locals()
            return {
                "pdf_name": pdf_filename,
                "mode": mode.value,
                "success": False,
                "error": str(e),
                "total_pages": local_vars.get("total_pages"),
                "page_categories": local_vars.get("page_categories", {}),
                "page_justifications": local_vars.get("page_justifications", {}),
                "nf_pages": local_vars.get("nf_pages", []),
                "extracted_nfs": local_vars.get("extracted_nfs", []),
                # Timing fields — partial values from whatever was measured before the error
                "_t_preprocess_sec": local_vars.get("_t_preprocess"),
                "_t_validacao_sec": local_vars.get("_t_validacao"),
            }

        finally:
            # Cleanup - delete downloaded PDF only if we downloaded it
            if clean_temp_file:
                logger.info("  [Cleanup] Deleting temporary PDF...")
                self.gcs_downloader.cleanup_local_file(pdf_path)
                logger.info("  [OK] Cleanup complete")

    def _process_single_pdf_worker(
        self,
        pdf_name: str,
        expected_nfs: list[dict[str, Any]],
        mode: ExecutionMode,
        progress_lock: threading.Lock,
        completed_count: list[int],
        total_pdfs: int,
        pdf_path: Path | None = None,
    ) -> dict[str, Any]:
        """
        Worker function to process a single PDF (for parallelization).
        Each worker creates its own classifier/extractor and database connection.

        :param pdf_name: PDF filename.
        :param expected_nfs: List of expected NF dicts for this PDF.
        :param mode: Execution mode.
        :param progress_lock: Thread lock for progress tracking.
        :param completed_count: Mutable list with single element for tracking progress.
        :param total_pdfs: Total number of PDFs being processed.
        :param pdf_path: Optional pre-downloaded PDF path (if None, will download from GCS).
        :returns: Processing result dict.
        """
        thread_db_manager = None
        thread_id = threading.current_thread().ident

        # DEBUG: Print worker startup
        logger.debug(f"\n{'='*80}")
        logger.debug(f"[DEBUG] Worker started - Thread ID: {thread_id}")
        logger.debug(f"[DEBUG] Processing PDF: {pdf_name}")
        logger.debug(f"[DEBUG] self.db_path = {getattr(self, 'db_path', 'NOT SET!')}")
        logger.debug(f"{'='*80}\n")

        try:
            # Create thread-local database connection
            # (SQLite connections must be used in the thread that created them)
            logger.debug(
                f"[DEBUG Thread {thread_id}] Creating thread-local DatabaseManager..."
            )
            logger.debug(f"[DEBUG Thread {thread_id}]   db_path: {self.db_path}")
            thread_db_manager = DatabaseManager(db_path=self.db_path)
            logger.debug(f"[DEBUG Thread {thread_id}] [OK] DatabaseManager created")
            logger.debug(
                f"[DEBUG Thread {thread_id}]   Connection object: {thread_db_manager.conn}"
            )
            logger.debug(
                f"[DEBUG Thread {thread_id}]   Connection thread: {threading.current_thread().ident}"
            )

            # TODO: REFACTOR - This is a workaround for SQLite thread-safety.
            # Instead of creating an entire new POCProcessor instance per thread,
            # we should refactor process_pdf() to accept db_manager as a parameter
            # and reuse self (classifier, extractor, etc). Current approach creates
            # unnecessary overhead by lazy-loading new classifiers/extractors per thread.

            # Create fresh processor with thread-local DB connection
            logger.debug(
                f"[DEBUG Thread {thread_id}] Creating thread-local POCProcessor..."
            )
            thread_processor = POCProcessor(
                db_manager=thread_db_manager,  # Thread-local connection
                gcs_downloader=self.gcs_downloader,  # Shared (thread-safe)
                gemini_credentials_path=self.gemini_credentials_path,
                temp_dir=self.temp_dir,
                quiet=self.quiet,
                prompt_versions=self.prompt_versions,
                extraction_batch_size=self.extraction_batch_size,
                min_match_score=self.min_match_score,
                output_mode=self.output_mode,
            )
            logger.debug(f"[DEBUG Thread {thread_id}] [OK] POCProcessor created")

            # Mark start
            _t_start = time.time()

            # Process PDF (with optional pre-downloaded path)
            logger.debug(f"[DEBUG Thread {thread_id}] Calling process_pdf()...")
            result = thread_processor.process_pdf(
                pdf_name, expected_nfs, mode=mode, pdf_path=pdf_path
            )
            logger.debug(
                f"[DEBUG Thread {thread_id}] [OK] process_pdf() completed successfully"
            )

            _elapsed = time.time() - _t_start

            # Inject per-PDF wall time into result for main-thread reporting
            result["_t_pdf_wall_sec"] = _elapsed

            # Update progress counter (thread-safe, no print — moved to main thread)
            with progress_lock:
                completed_count[0] += 1

            return result

        except Exception as e:
            # Print full error details
            logger.error(f"\n[ERROR Thread {thread_id}] Exception caught in worker:")
            logger.error(f"  Error type: {type(e).__name__}")
            logger.error(f"  Error message: {e!s}")
            import traceback

            logger.error("  Full stack trace:")
            traceback.print_exc()

            _elapsed = time.time() - _t_start if '_t_start' in dir() else 0

            # Update progress counter only — print moved to main thread
            with progress_lock:
                completed_count[0] += 1

            return {
                "pdf_name": pdf_name,
                "mode": mode.value,
                "success": False,
                "error": str(e),
                "_t_pdf_wall_sec": _elapsed,
            }

        finally:
            # Always close thread-local database connection
            if thread_db_manager:
                logger.debug(
                    f"[DEBUG Thread {thread_id}] Closing thread-local DatabaseManager..."
                )
                thread_db_manager.close()
                logger.debug(f"[DEBUG Thread {thread_id}] [OK] DatabaseManager closed")

    @staticmethod
    def _build_classification_detail(
        page_categories: dict[int, str],
        page_justifications: dict[int, str],
        nf_pages: list[int]
    ) -> dict:
        """
        Constrói detalhe estruturado da classificação por página.

        :param page_categories: Dicionário {page_num: category}.
        :param page_justifications: Dicionário {page_num: justification}.
        :param nf_pages: Lista de páginas consideradas documentos fiscais válidos.
        :returns: Dicionário estruturado com detalhes da classificação.
        """
        pages_detail = []
        for page_num in sorted(page_categories.keys()):
            category = page_categories[page_num]
            justification = page_justifications.get(page_num, "")
            is_valid = page_num in nf_pages

            pages_detail.append({
                "page": page_num,
                "category": category,
                "justification": justification,
                "is_valid_document": is_valid
            })

        return {
            "total_pages": len(page_categories),
            "classified_pages": len(page_categories),
            "valid_document_pages": sorted(nf_pages),
            "pages": pages_detail
        }

    @staticmethod
    def _build_extraction_detail(
        extracted_nfs: list[dict],
        result: dict
    ) -> dict:
        """
        Constrói detalhe estruturado da extração por documento.
        Inclui metadados completos da resposta do modelo (possui_nota_fiscal, quantidade, etc).

        :param extracted_nfs: Lista de NFs extraídas.
        :param result: Resultado completo da extração.
        :returns: Dicionário estruturado com detalhes da extração.
        """
        # Capturar resposta completa do modelo (se disponível)
        possui_nota_fiscal = result.get('possui_nota_fiscal', len(extracted_nfs) > 0)
        quantidade = result.get('quantidade_notas_fiscais', len(extracted_nfs))


        documents_detail = []
        for nf in extracted_nfs:
            doc = {
                "original_page": nf.get('pagina'),
                "tipo_documento": nf.get('tipo_documento'),
                "cnpj_emitente": nf.get('cnpj_emitente'),
                "cnpj_destinatario": nf.get('cnpj_destinatario'),
                "numero_nf": nf.get('numero_nf'),
                "valor_total": nf.get('valor_total'),
                "data_emissao": nf.get('data_emissao')
            }

            # Adicionar batch_info se presente (novo campo de rastreabilidade)
            if '_page_mapping' in nf:
                doc['batch_info'] = nf['_page_mapping']

            documents_detail.append(doc)

        extraction_method = "batch" if result.get("batching_used", False) else "single"

        extraction_detail = {
            "possui_nota_fiscal": possui_nota_fiscal,
            "quantidade_notas_fiscais": quantidade,
            "documents_extracted": len(extracted_nfs),
            "extraction_method": extraction_method,
            "documents": documents_detail
        }

        # Adicionar batch_details se batching foi usado (inclui raw responses)
        if result.get("batching_used"):
            extraction_detail["batch_details"] = result.get("batch_details", [])

        return extraction_detail

    def _build_versao_pipeline(
        self,
        mode: "ExecutionMode",
        workers: int,
        requests_per_minute: int,
        max_concurrent: int,
    ) -> dict[str, Any]:
        """
        Monta o JSON de rastreabilidade de configuração da execução.

        Inclui parâmetros operacionais e informações do repositório git para
        permitir comparar resultados apenas entre execuções com a mesma config.
        """
        info: dict[str, Any] = {
            "mode": mode.value,
            "extraction_batch_size": self.extraction_batch_size,
            "min_match_score": self.min_match_score,
            "match_requires_pdf_name": self.match_requires_pdf_name,
            "workers": workers,
            "requests_per_minute": requests_per_minute,
            "max_concurrent": max_concurrent,
        }
        info.update(self._get_git_info())
        return info

    def _build_versao_prompt(self) -> dict[str, Any]:
        """
        Monta o JSON de rastreabilidade de versões de prompt.

        Permite filtrar/comparar resultados apenas entre execuções que usaram
        exatamente os mesmos prompts e batch_size de extração.
        """
        return {
            "versao_prompt_classificacao": self.prompt_versions.get("classification"),
            "versao_prompt_extracao": self.prompt_versions.get("extraction"),
            "batch_size_extracao": self.extraction_batch_size,
        }

    @staticmethod
    def _get_git_info() -> dict[str, Any]:
        """
        Get Git repository information (commit, branch, dirty status).

        :returns: Dictionary with git info, or empty dict if not in a git repo.
        """
        try:
            # Get current commit hash
            commit = subprocess.check_output(
                ['git', 'rev-parse', '--short', 'HEAD'],
                stderr=subprocess.DEVNULL,
                text=True
            ).strip()

            # Get current branch
            branch = subprocess.check_output(
                ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                stderr=subprocess.DEVNULL,
                text=True
            ).strip()

            # Check if working directory is dirty (uncommitted changes)
            dirty_check = subprocess.check_output(
                ['git', 'status', '--porcelain'],
                stderr=subprocess.DEVNULL,
                text=True
            ).strip()
            dirty = len(dirty_check) > 0

            return {
                'commit': commit,
                'branch': branch,
                'dirty': dirty
            }
        except (subprocess.CalledProcessError, FileNotFoundError):
            # Not in a git repo or git not available
            return {}

    def _generate_metadata(
        self,
        experiment_id: str,
        run_id: str,
        timestamp_start: datetime,
        timestamp_end: datetime,
        config: dict[str, Any],
        results_df: pd.DataFrame,
        cache_stats: dict[str, int],
        api_usage: dict[str, int]
    ) -> dict[str, Any]:
        """
        Generate metadata JSON for experiment run.

        :param experiment_id: Experiment ID (e.g., 'exp001_baseline').
        :param run_id: Run ID (e.g., 'run_20260325_143022').
        :param timestamp_start: Start timestamp.
        :param timestamp_end: End timestamp.
        :param config: Pipeline configuration.
        :param results_df: Results DataFrame.
        :param cache_stats: Cache statistics.
        :param api_usage: API usage statistics.
        :returns: Metadata dictionary.
        """
        from ..core.prompts import get_active_prompt_info

        # Calculate duration
        duration_seconds = (timestamp_end - timestamp_start).total_seconds()

        # Parse experiment name from ID
        experiment_name = experiment_id.split('_', 1)[1] if '_' in experiment_id else experiment_id

        # Get prompt versions and hashes (use versions from POCProcessor if available)
        classification_info = get_active_prompt_info('classification', version=self.prompt_versions.get('classification'))
        extraction_info = get_active_prompt_info('extraction', version=self.prompt_versions.get('extraction'))

        # Aggregate results using actual DataFrame columns
        # Calculate NF encontrada vs não encontrada
        nf_encontrada_counts = results_df['indicador_nf_encontrada_modelo'].value_counts().to_dict() if not results_df.empty else {}
        classification_counts = results_df['classificacao_modelo'].value_counts().to_dict() if not results_df.empty else {}

        # Map to legacy format for compatibility
        total_nf_encontrada = nf_encontrada_counts.get(True, 0)
        total_nf_nao_encontrada = nf_encontrada_counts.get(False, 0)
        total_not_analyzable = classification_counts.get('Not Analyzable', 0)
        total_apontamento_leve = classification_counts.get('Apontamento Leve', 0)

        # Build metadata
        metadata = {
            'experiment_id': experiment_id,
            'experiment_name': experiment_name,
            'run_id': run_id,
            'timestamp_start': timestamp_start.isoformat(),
            'timestamp_end': timestamp_end.isoformat(),
            'duration_seconds': int(duration_seconds),

            'prompts': {
                'classification': {
                    'version': classification_info['version'],
                    'file': classification_info['file'],
                    'hash': classification_info['hash']
                },
                'extraction': {
                    'version': extraction_info['version'],
                    'file': extraction_info['file'],
                    'hash': extraction_info['hash']
                }
            },

            'config': config,

            'results': {
                'total_rows': len(results_df),
                'nf_encontrada': total_nf_encontrada,
                'nf_nao_encontrada': total_nf_nao_encontrada,
                'not_analyzable': total_not_analyzable,
                'apontamento_leve': total_apontamento_leve,
                'classification_breakdown': {
                    k: int(v) for k, v in classification_counts.items() if pd.notna(k)
                },
                'nf_encontrada_breakdown': {
                    str(k): int(v) for k, v in nf_encontrada_counts.items()
                }
            },

            'api_usage': api_usage,

            'cache': cache_stats,

            'git': self._get_git_info()
        }

        # NEW: Add rate limiting metrics from API metrics tracker
        from ..core.api_metrics_tracker import get_tracker
        tracker = get_tracker()
        rate_limiting_metrics = tracker.get_metrics()

        # Add workers configured to concurrency metrics
        rate_limiting_metrics['concurrency']['workers_configured'] = config.get('workers', 0)

        metadata['rate_limiting'] = rate_limiting_metrics

        return metadata

    # ------------------------------------------------------------------
    # JSON output helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _empty_json_item(
        pdf_name: str,
        page_num: int | None,
        pipeline_status: str,
        pipeline_erro: str | None = None,
        tipo_classificacao: str | None = None,
        justificativa_classif: str | None = None,
    ) -> dict:
        """Build a JSON output item with all document fields set to null."""
        return {
            "nome_arquivo":                 pdf_name,
            "pagina":                       page_num,
            "pipeline_status":              pipeline_status,
            "pipeline_erro":                pipeline_erro,
            "tipo_documento_classificacao": tipo_classificacao,
            "justificativa_classificacao":  justificativa_classif,
            "tipo_documento_extracao":      None,
            "numero_documento":             None,
            "data_emissao_documento":       None,
            "cnpj_emitente":                None,
            "match_id_documento":           [],
            "valor_documento":              None,
            "cnpj_destinatario":            None,
            "data_competencia_documento":   None,
            "data_servico_documento":       None,
            "numero_rps":                   None,
            "valores_encontrados":          None,
            "cnpjs_encontrados":            None,
            "observacao_extracao":          None,
        }

    @staticmethod
    def _build_json_output(
        pdf_tasks: list[dict],
        pdf_results: dict[str, dict],
        input_df: "pd.DataFrame",
        min_match_score: int,
        match_requires_pdf_name: bool = False,
        timestamp_geracao: datetime | None = None,
        versao_pipeline: dict | None = None,
        versao_prompt: dict | None = None,
    ) -> list[dict]:
        """
        Build the per-page JSON output list.

        Every page of every processed PDF gets exactly one item in the output
        list — regardless of whether a fiscal document was found on it or
        whether processing failed. This is what lets a consumer distinguish
        "processed successfully, nothing here" from "never got processed".

        Per PDF, pages are classified into one of three states:
        - Page was classified and (optionally) had a document extracted:
          pipeline_status="ok", with extraction fields populated if a
          document was found on that page, or null if not.
        - Page was never reached because processing aborted partway through
          (e.g. a Gemini API/credential error on an earlier page):
          pipeline_status="erro_processamento".
        - The PDF's page count itself is unknown (e.g. download failed
          before the file could even be opened): a single sentinel item
          with pagina=None is emitted, since there's nothing to enumerate.

        Erros silenciosos tratados aqui:
        - Páginas em page_categories cuja justificativa contém "Erro ao extrair
          página" (falha de bytes/PDF corrompido) são emitidas com
          pipeline_status="erro_processamento", não "ok". Isso evita que uma
          classificação fake ("Nenhuma das Opções" forçada por exceção) seja
          indistinguível de uma classificação legítima.

        Schema per item:
        {
            "nome_arquivo":              str,
            "pagina":                    int | null,
            "pipeline_status":           "ok" | "erro_processamento",
            "pipeline_erro":             str | null,
            "tipo_documento_classificacao": str | null,
            "justificativa_classificacao":  str | null,
            "tipo_documento_extracao":   str | null,
            "numero_documento":          str | null,
            "data_emissao_documento":    str | null,
            "cnpj_emitente":             str | null,
            "match_id_documento":        list[str],
            "valor_documento":           float | null,
            "cnpj_destinatario":         str | null,
            "data_competencia_documento": str | null,
            "data_servico_documento":    str | null,
            "numero_rps":                str | null,
            "valores_encontrados":       dict | null,
            "cnpjs_encontrados":         dict | null,
            "observacao_extracao":       str | null,
            "timestamp_geracao":         str (ISO-8601 UTC),
            "versao_pipeline":           dict | null,
            "versao_prompt":             dict | null,
        }

        :param pdf_tasks: list of task dicts produced in process_database.
        :param pdf_results: mapping pdf_name -> result dict from process_pdf.
        :param input_df: the full input DataFrame (used to build id_documento lookup).
        :param min_match_score: minimum match_score_3_fields threshold for match_id_documento.
        :param match_requires_pdf_name: when True, only declarations whose pdf_name matches the
            current PDF are candidates for match_id_documento (legacy
            behaviour). When False (default), every declaration in
            input_df is a candidate regardless of pdf_name, enabling
            cross-PDF match analysis in BigQuery.
        :param timestamp_geracao: UTC timestamp of this pipeline run (auto-generated if None).
        :param versao_pipeline: dict with pipeline config params for traceability.
        :param versao_prompt: dict with prompt versions and batch_size for traceability.
        :returns: List of per-page dicts ready for json.dump / NDJSON write.
        """
        from ..compliance.utils import (
            DocumentFields,
            match_score_3_fields,
        )

        # Garante que timestamp_geracao é sempre gerado automaticamente pela pipeline.
        # O campo NUNCA deve depender de input manual — gerado aqui uma única vez
        # por run e injetado em todos os itens de saída.
        if timestamp_geracao is None:
            timestamp_geracao = datetime.utcnow()
        ts_iso = timestamp_geracao.isoformat() + "Z"

        # Pre-build a lookup from pdf_name -> list of declaration dicts,
        # used to resolve match_id_documento for each extracted NF page.
        declaration_lookup: dict[str, list] = {}
        for _, row in input_df.iterrows():
            pdf_name = str(row.get("descricao_limpa", ""))
            declaration_lookup.setdefault(pdf_name, []).append({
                "id_documento":  str(row.get("id_documento", "")),
                "cnpj_norm":     normalize_cnpj(str(row.get("cnpj_cpf", ""))),
                "numero_norm":   normalize_number(str(row.get("num_documento", ""))),
                "data_emissao":  str(row.get("data_emissao", "")),
            })

        output_items: list[dict] = []

        for task in pdf_tasks:
            pdf_name = task["pdf_name"]
            result   = pdf_results.get(pdf_name, {})

            total_pages          = result.get("total_pages")
            page_categories      = result.get("page_categories") or {}
            page_justifications  = result.get("page_justifications") or {}
            extracted_nfs        = result.get("extracted_nfs") or []
            pipeline_ok          = result.get("success", True)
            error_msg            = result.get("error") if not pipeline_ok else None

            # A page can yield at most one extracted document in the current schema.
            extracted_by_page = {
                nf.get("pagina"): nf for nf in extracted_nfs if nf.get("pagina") is not None
            }
            if match_requires_pdf_name:
                # Legacy behaviour: only declarations that explicitly point to this PDF.
                declarations = declaration_lookup.get(pdf_name, [])
            else:
                # Cross-PDF mode: all declarations in the input are candidates.
                # Useful for BigQuery analysis of documents declared in one PDF
                # that match content extracted from a different PDF.
                declarations = [d for dlist in declaration_lookup.values() for d in dlist]

            if not total_pages:
                # Page count itself is unknown (e.g. download failed before the
                # PDF could be opened) — nothing to enumerate, emit one sentinel.
                item = POCProcessor._empty_json_item(
                    pdf_name, None,
                    "ok" if pipeline_ok else "erro_processamento",
                    error_msg,
                )
                item["timestamp_geracao"] = ts_iso
                item["versao_pipeline"]   = versao_pipeline
                item["versao_prompt"]     = versao_prompt
                output_items.append(item)
                continue

            for page_num in range(1, total_pages + 1):
                if page_num not in page_categories:
                    # Never classified: either the whole PDF failed before
                    # reaching this page, or processing aborted partway through.
                    item = POCProcessor._empty_json_item(
                        pdf_name, page_num, "erro_processamento",
                        error_msg or "Página não processada",
                    )
                    item["timestamp_geracao"] = ts_iso
                    item["versao_pipeline"]   = versao_pipeline
                    item["versao_prompt"]     = versao_prompt
                    output_items.append(item)
                    continue

                tipo_classificacao     = page_categories.get(page_num)
                justificativa_classif  = page_justifications.get(page_num, "")

                # Erro silencioso 1.1: página foi "classificada" apenas porque
                # extract_page_as_bytes falhou (PDF corrompido) e a exceção foi
                # capturada, salvando "Nenhuma das Opções" no cache.
                # Nesses casos a justificativa contém o prefixo "Erro ao extrair
                # página:" — emitimos como erro_processamento, não como ok.
                _is_byte_extraction_error = (
                    justificativa_classif is not None
                    and justificativa_classif.startswith("Erro ao extrair página:")
                )
                if _is_byte_extraction_error:
                    item = POCProcessor._empty_json_item(
                        pdf_name, page_num, "erro_processamento",
                        justificativa_classif,
                        tipo_classificacao, justificativa_classif,
                    )
                    item["timestamp_geracao"] = ts_iso
                    item["versao_pipeline"]   = versao_pipeline
                    item["versao_prompt"]     = versao_prompt
                    output_items.append(item)
                    continue

                nf = extracted_by_page.get(page_num)

                if nf is None:
                    item = POCProcessor._empty_json_item(
                        pdf_name, page_num, "ok", None,
                        tipo_classificacao, justificativa_classif,
                    )
                    item["timestamp_geracao"] = ts_iso
                    item["versao_pipeline"]   = versao_pipeline
                    item["versao_prompt"]     = versao_prompt
                    output_items.append(item)
                    continue

                # Find which declarations match this NF with the configured threshold.
                # match_id_documento is a list (possibly multiple declarations match same NF).
                matched_ids: list[str] = []
                ext_cnpj   = nf.get("cnpj_emitente", "") or ""
                ext_numero = nf.get("numero_nf", "") or ""
                ext_data   = nf.get("data_emissao") or ""

                for decl in declarations:
                    score = match_score_3_fields(
                        expected=DocumentFields(
                            cnpj=decl["cnpj_norm"],
                            numero=decl["numero_norm"],
                            data=decl["data_emissao"],
                        ),
                        extracted=DocumentFields(
                            cnpj=ext_cnpj,
                            numero=ext_numero,
                            data=ext_data,
                        ),
                    )
                    if score >= min_match_score:
                        matched_ids.append(decl["id_documento"])

                item = POCProcessor._empty_json_item(
                    pdf_name, page_num, "ok", None,
                    tipo_classificacao, justificativa_classif,
                )
                item.update({
                    "tipo_documento_extracao":      nf.get("tipo_documento"),
                    "numero_documento":             nf.get("numero_nf"),
                    "data_emissao_documento":       nf.get("data_emissao"),
                    "cnpj_emitente":                nf.get("cnpj_emitente"),
                    "match_id_documento":           matched_ids,
                    "valor_documento":              nf.get("valor_total"),
                    "cnpj_destinatario":            nf.get("cnpj_destinatario"),
                    "data_competencia_documento":   nf.get("data_competencia"),
                    "data_servico_documento":       nf.get("data_servico"),
                    "numero_rps":                   nf.get("numero_rps"),
                    "valores_encontrados":          nf.get("campos_de_valor_encontrados"),
                    "cnpjs_encontrados":            nf.get("campos_de_cnpj_encontrados"),
                    "observacao_extracao":          nf.get("observacao"),
                    "timestamp_geracao":            ts_iso,
                    "versao_pipeline":              versao_pipeline,
                    "versao_prompt":                versao_prompt,
                })
                output_items.append(item)

        return output_items

    def process_database(
        self,
        csv_path: Path,
        output_path: Path | None = None,
        limit: int | None = None,
        mode: ExecutionMode = ExecutionMode.FULL,
        max_workers: int = 1000,  # Batch download enables 1000 workers
        keep_pdfs: bool = False,  # Keep downloaded PDFs instead of cleaning up
        experiment_id: str | None = None,  # NEW: Experiment ID for metadata generation
        requests_per_minute: int = 0,  # Passado para versao_pipeline (rastreabilidade)
        max_concurrent: int = 0,       # Passado para versao_pipeline (rastreabilidade)
    ) -> pd.DataFrame:
        """
        Process entire database CSV with specified execution mode and parallelization.

        :param csv_path: Path to modulo-de-despesas.csv.
        :param output_path: Optional path to save results Excel.
        :param limit: Optional limit on number of PDFs to process.
        :param mode: Execution mode controlling which steps to run.
        :param max_workers: Number of concurrent workers (default: 20).
        :param keep_pdfs: Keep downloaded PDFs after processing.
        :param experiment_id: Optional experiment ID (e.g., 'exp001_baseline'). If provided, generates metadata.json.
        :param requests_per_minute: RPM configurado no rate limiter (para versao_pipeline).
        :param max_concurrent: Máximo de requisições simultâneas (para versao_pipeline).
        :returns: DataFrame with processing results.
        """
        # Track start time for metadata
        timestamp_start = datetime.now()

        # Initialize API usage counters (will be updated from cache stats)
        # These will be populated at the end from cache statistics
        api_usage_counters = {
            'classification_calls': 0,
            'extraction_calls': 0,
            'total_input_tokens': 0,
            'total_output_tokens': 0,
            'estimated_cost_usd': 0.0
        }

        print(f"\n{'#'*80}")
        print(f"# POC Pipeline - Database Processing [Mode: {mode.value}]")
        print(f"{'#'*80}\n")

        # DEBUG: Verify thread-local DB fix is loaded
        # TODO change quiet to debug logger
        # TODO remove this inspect section
        if not self.quiet:
            import inspect

            source = inspect.getsource(self._process_single_pdf_worker)
            if "thread_db_manager = DatabaseManager" in source:
                print("[DEBUG] >>> Thread-local DB fix IS LOADED <<<")
            else:
                print(
                    "[DEBUG] XXX Thread-local DB fix NOT LOADED - using old code! XXX"
                )

        # Store db_path for worker threads (each worker creates its own connection)
        self.db_path = self.db_manager.db_path
        logger.info(f"[DEBUG] Main thread DB path stored: {self.db_path}\n")

        # Read CSV
        print(f"Reading database: {csv_path}")
        df = pd.read_csv(csv_path)
        print(f"Total rows: {len(df)}")

        # Group by PDF (descricao_limpa column - normalized without .pdf extension)
        pdf_groups = df.groupby("descricao_limpa")
        print(f"Unique PDFs: {len(pdf_groups)}")

        if limit:
            print(f"Processing limit: {limit} PDFs")

        print(f"Parallel workers: {max_workers}")

        # Load available PDFs from pre-generated CSV (faster than GCS API call)
        print("Loading available PDFs from CSV...")
        # TODO rename symbol
        if "pdf_url_download" in df.columns and df["pdf_url_download"].notna().any():
            # View provides pdf_url_download — existence already guaranteed by INNER JOIN.
            # Extract GCS base_path from the URL and skip bucket listing entirely.
            # URL format: https://storage.cloud.google.com/<bucket>/<base_path>/<filename>
            sample_url = df["pdf_url_download"].dropna().iloc[0]
            # Strip query-string params (signed URLs) before parsing
            sample_url_clean = sample_url.split("?")[0]
            # Support both GCS URL formats:
            #   https://storage.cloud.google.com/<bucket>/...
            #   https://storage.googleapis.com/<bucket>/...
            for _prefix in ("https://storage.cloud.google.com/", "https://storage.googleapis.com/"):
                if sample_url_clean.startswith(_prefix):
                    sample_url_clean = sample_url_clean[len(_prefix):]
                    break
            url_parts = sample_url_clean.split("/")
            # url_parts = [bucket, ...base_path_parts..., filename]
            bucket_from_url = url_parts[0]
            gcs_base_path = "/".join(url_parts[1:-1])
            self.gcs_downloader.default_base_path = gcs_base_path
            # If no bucket was supplied via CLI/env, derive it from the URL
            if not self.gcs_downloader.bucket_name:
                self.gcs_downloader.bucket_name = bucket_from_url
                self.gcs_downloader._bucket = None  # force lazy reload with new bucket name
                print(f"  [Auto] GCS bucket set from pdf_url_download: {bucket_from_url}")
            available_pdfs = set(df["descricao_limpa"].dropna().unique())
            print(f"  Using pdf_url_download — {len(available_pdfs):,} PDFs (bucket: {self.gcs_downloader.bucket_name}, base_path: {gcs_base_path})")
        else:
            available_pdfs = self.gcs_downloader.get_available_pdf_filenames_from_csv()
            print(f"  Found {len(available_pdfs):,} PDFs in GCS")
            sample_bq = list(pdf_groups.groups.keys())[:5]
            sample_gcs = sorted(list(available_pdfs))[:5]
            print(f"  [DIAG] Primeiros pdf_names do BQ:  {sample_bq}")
            print(f"  [DIAG] Primeiros filenames do GCS: {sample_gcs}")
        print()

        # Prepare PDF tasks (limit if specified)
        # Filter CSV PDFs against available GCS PDFs using fast set lookup
        pdf_tasks = []
        checked_count = 0
        found_count = 0
        not_found_pdfs = []  # For debugging: track PDFs not found in GCS
        failed_pdfs_download = []  # For debugging: track PDFs that failed to download

        for pdf_idx, (pdf_name, group_df) in enumerate(pdf_groups):
            # Stop if we've found enough PDFs
            if limit and found_count >= limit:
                break

            checked_count += 1

            # Check if PDF exists in GCS (instant set lookup - O(1))
            if pdf_name not in available_pdfs:
                # Show first 20 skips + any while still searching for limit
                if checked_count <= 20 or (
                    limit and found_count < limit and checked_count <= found_count + 30
                ):
                    print(f"  [{checked_count}] Skipping {pdf_name} (not found in GCS)")
                not_found_pdfs.append(pdf_name)
                continue

            found_count += 1
            print(
                f"  [{found_count}/{limit if limit else '∞'}] Found {pdf_name} in GCS"
            )

            # TODO this entire expected_nfs must occupy too much memory for large datasets, reconsider building them on the fly in the worker
            # Prepare expected NFs from all rows for this PDF
            expected_nfs = []
            for _, row in group_df.iterrows():
                expected_nf = {
                    "pdf_name": pdf_name,
                    "cnpj": str(row.get("cnpj_cpf", "")),  # Updated: cnpj → cnpj_cpf
                    "numero_nf": str(
                        row.get("num_documento", "")
                    ),  # Use numero_nf for ComplianceValidator
                    "num_documento": str(
                        row.get("num_documento", "")
                    ),  # Keep for backward compatibility
                    "valor_total": row.get(
                        "valor_documento"
                    ),  # Use valor_total for ComplianceValidator
                    "valor_documento": row.get(
                        "valor_documento"
                    ),  # Keep for backward compatibility
                    # TODO: Check if this should be valor_pago instead
                    "valor_pago": row.get(
                        "valor_pago_total"
                    ),  # Updated: valor_pago → valor_pago_total
                    "tipo_documento": row.get(
                        "id_tipo_documento", None
                    ),  # Optional, defaults to None
                    "data_emissao": (
                        str(row.get("data_emissao", ""))
                        if pd.notna(row.get("data_emissao"))
                        else None
                    ),  # For date mismatch rule
                }
                expected_nfs.append(expected_nf)

            pdf_tasks.append(
                {
                    "pdf_name": pdf_name,
                    "group_df": group_df,
                    "expected_nfs": expected_nfs,
                }
            )

        total_pdfs = len(pdf_tasks)
        print(f"\n{'='*80}")
        print("GCS Search Summary:")
        print(f"  PDFs checked: {checked_count}")
        print(f"  PDFs found in GCS: {found_count}")
        print(f"  PDFs skipped (not in GCS): {checked_count - found_count}")
        print(f"{'='*80}")

        # PRE-DOWNLOAD: Download all PDFs in batches before parallel processing
        print(f"\n[Pre-download] Downloading {total_pdfs} PDFs in batches...")
        print("Using concurrent downloads (50 at a time) to optimize network usage")

        # Get PDF names
        pdf_names_to_download = [task["pdf_name"] for task in pdf_tasks]

        # Batch download all PDFs — time the whole block for avg_sec_download_gcs
        _t_download_start = time.time()
        downloaded_paths = self.gcs_downloader.download_pdfs_batch(
            pdf_names=pdf_names_to_download,
            local_dir=self.temp_dir,
            batch_size=max_workers,  # align with processing workers to avoid urllib3 pool exhaustion
        )
        _t_download_total = time.time() - _t_download_start

        print(
            f"[OK] Downloaded {len(downloaded_paths)} / {len(pdf_names_to_download)} PDFs"
        )

        # Filter out PDFs that failed to download
        pdf_tasks_filtered = []
        for task in pdf_tasks:
            if task["pdf_name"] in downloaded_paths:
                task["pdf_path"] = downloaded_paths[task["pdf_name"]]
                pdf_tasks_filtered.append(task)
            else:
                print(f"[Warning] Skipping {task['pdf_name']} (download failed)")
                failed_pdfs_download.append(task["pdf_name"])

        print(
            f"\n[Processing] Processing {len(pdf_tasks_filtered)} PDFs with {max_workers} workers...\n"
        )

        # ── Wall-clock timer for the whole processing stage ──

        # Thread-safe progress tracking
        progress_lock = threading.Lock()
        completed_count = [0]  # Mutable for thread-safe updates

        # Parallel processing (using pre-downloaded PDFs)
        results = []
        pdf_results = {}  # Map PDF name to result
        _n_total = len(pdf_tasks_filtered)

        print(f"[Progress] Processing {_n_total} PDFs with {max_workers} workers...", flush=True)

        _t_core_start = time.time()
        _submitted_at: dict[str, float] = {}
        _finished_at: dict[str, float] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all PDF processing tasks (with pre-downloaded paths)
            future_to_pdf = {}
            for task in pdf_tasks_filtered:
                _submitted_at[task["pdf_name"]] = time.time()
                future = executor.submit(
                    self._process_single_pdf_worker,
                    pdf_name=task["pdf_name"],
                    expected_nfs=task["expected_nfs"],
                    mode=mode,
                    progress_lock=progress_lock,
                    completed_count=completed_count,
                    total_pdfs=_n_total,
                    pdf_path=task.get("pdf_path"),
                )
                future_to_pdf[future] = task

            # Collect results as they complete
            _last_summary_count = 0
            for future in as_completed(future_to_pdf):
                task = future_to_pdf[future]
                result = future.result()
                pdf_results[task["pdf_name"]] = result
                _finished_at[task["pdf_name"]] = time.time()

                # Print each PDF result (now from main thread — visible in Prefect Cloud)
                _pdf_elapsed = _finished_at[task["pdf_name"]] - _submitted_at[task["pdf_name"]]
                _n_docs = len(result.get("extracted_nfs", []))
                _truncated = task["pdf_name"][:60]
                _status = "OK" if result.get("success") else "FAIL"
                print(
                    f"  {_truncated} → {_status} ({_n_docs} docs, {_pdf_elapsed:.0f}s)",
                    flush=True,
                )

                # Periodic summary every N PDFs
                # Use len(pdf_results) instead of completed_count[0] to avoid
                # phantom FAILs caused by the worker incrementing the counter
                # before the result reaches pdf_results (race condition).
                _done = len(pdf_results)
                if _done - _last_summary_count >= 10 or _done == _n_total:
                    _last_summary_count = _done
                    _elapsed = time.time() - _t_core_start
                    _n_ok = sum(1 for r in pdf_results.values() if r.get("success"))
                    _n_fail = _done - _n_ok
                    _rate = _done / _elapsed if _elapsed > 0 else 0
                    _eta = (_n_total - _done) / _rate if _rate > 0 else 0
                    print(
                        f"[Progress] {_done}/{_n_total} ({100*_done//_n_total}%) | "
                        f"{_n_ok} OK, {_n_fail} FAIL | "
                        f"elapsed={_elapsed:.0f}s rate={_rate:.1f}pdf/s "
                        f"eta={_eta:.0f}s",
                        flush=True,
                    )
                    # When rate is low, show which PDFs are still in-flight
                    if _rate < 0.1:
                        _inflight = {
                            n: time.time() - t
                            for n, t in _submitted_at.items()
                            if n not in pdf_results
                        }
                        if _inflight:
                            _slowest = sorted(_inflight.items(), key=lambda x: -x[1])[:3]
                            print(
                                "  ⏳ In-flight: "
                                + ", ".join(f"{n[:40]}…({s:.0f}s)" for n, s in _slowest),
                                flush=True,
                            )
        _t_core_wall = time.time() - _t_core_start

        _n_ok = sum(1 for r in pdf_results.values() if r.get("success"))
        _n_fail = _n_total - _n_ok
        print(
            f"[Progress] Done: {_n_ok} OK, {_n_fail} FAIL "
            f"in {_t_core_wall:.0f}s ({_t_core_wall/_n_total:.1f}s/pdf avg)",
            flush=True,
        )

        # ── Top 5 slowest PDFs ──
        _sorted_pdfs = sorted(
            (
                (n, _finished_at.get(n, 0) - _submitted_at.get(n, 0))
                for n in pdf_results
            ),
            key=lambda x: -x[1],
        )
        print("[Slowest] Top 5:")
        for _rank, (_name, _sec) in enumerate(_sorted_pdfs[:5], 1):
            _r = pdf_results[_name]
            _ok = "OK" if _r.get("success") else "FAIL"
            _docs = len(_r.get("extracted_nfs", []))
            _pages = _r.get("total_pages", "?")
            _cf = _r.get("_t_classif_wall_sec")
            _cf_str = f"classif={_cf:.0f}s" if _cf is not None else ""
            print(f"  #{_rank} {_name[:60]} → {_ok} ({_sec:.0f}s, {_pages}p, {_docs} docs {_cf_str})")

        # ── Intra-PDF parallelism indicator ──
        _classif_walls = [
            r.get("_t_classif_wall_sec")
            for r in pdf_results.values()
            if r.get("_t_classif_wall_sec") is not None
        ]
        if _classif_walls:
            _avg = sum(_classif_walls) / len(_classif_walls)
            print(
                f"[Parallelism] Média classif intra-PDF: {_avg:.1f}s wall "
                f"(quanto menor que páginas×3s, mais paralelo)",
                flush=True,
            )

        # ── Aggregate per-batch timing stats ─────────────────────────────────
        _timing_list_preprocess: list[float] = []
        _timing_list_validacao: list[float] = []
        for task in pdf_tasks_filtered:
            res = pdf_results.get(task["pdf_name"], {})
            t_pre = res.get("_t_preprocess_sec")
            t_val = res.get("_t_validacao_sec")
            if t_pre is not None:
                _timing_list_preprocess.append(t_pre)
            if t_val is not None:
                _timing_list_validacao.append(t_val)

        # Classification and extraction elapsed times come from SQLite api_outputs.
        # We only count rows inserted during THIS batch (elapsed_seconds > 0 means
        # real API call, not a cache hit — cache hits have elapsed_seconds = 0 or
        # the row simply doesn't exist for the new pdf_name).
        _pdf_stems = list({t["pdf_name"].replace(".pdf", "").replace(".PDF", "") for t in pdf_tasks_filtered})
        # api_inputs stores classification pages WITH .pdf extension; extraction inputs WITHOUT.
        # Build both variants so the IN clause matches regardless of how pdf_name was stored.
        _pdf_with_ext = [s + ".pdf" for s in _pdf_stems]
        _timing_list_classificacao: list[float] = []
        _timing_list_extracao: list[float] = []
        if _pdf_stems:
            _placeholders_stem = ",".join("?" * len(_pdf_stems))
            _placeholders_ext  = ",".join("?" * len(_pdf_with_ext))
            # Classification: stored with .pdf extension
            _cur_c = self.db_manager.conn.execute(
                f"""
                SELECT o.elapsed_seconds
                FROM api_outputs o
                JOIN api_inputs i ON i.id = o.input_id
                WHERE i.input_type = 'classification_page'
                  AND (i.pdf_name IN ({_placeholders_stem})
                       OR i.pdf_name IN ({_placeholders_ext}))
                  AND o.elapsed_seconds > 0
                """,
                _pdf_stems + _pdf_with_ext,
            )
            _timing_list_classificacao = [r[0] for r in _cur_c.fetchall()]

            # Extraction: stored without .pdf extension, but handle both variants for safety
            _cur_e = self.db_manager.conn.execute(
                f"""
                SELECT o.elapsed_seconds
                FROM api_outputs o
                JOIN api_inputs i ON i.id = o.input_id
                WHERE i.input_type = 'extraction_filtered_pdf'
                  AND (i.pdf_name IN ({_placeholders_stem})
                       OR i.pdf_name IN ({_placeholders_ext}))
                  AND o.elapsed_seconds > 0
                """,
                _pdf_stems + _pdf_with_ext,
            )
            _timing_list_extracao = [r[0] for r in _cur_e.fetchall()]

        def _safe_avg(lst: list[float]) -> float | None:
            return round(sum(lst) / len(lst), 3) if lst else None

        # ── Concrete timing metrics (no proportional estimation) ──
        timing_stats: dict[str, Any] = {
            "wall_sec_download_gcs":    round(_t_download_total, 3),
            "wall_sec_core":            round(_t_core_wall, 3),
            "wall_sec_escrita":         None,
            "avg_cpu_sec_preprocess_por_pdf":         _safe_avg(_timing_list_preprocess),
            "avg_cpu_sec_classificacao_por_pagina":   _safe_avg(_timing_list_classificacao),
            "avg_cpu_sec_extracao_por_declaracao":    _safe_avg(_timing_list_extracao),
            "avg_cpu_sec_validacao_por_pdf":           _safe_avg(_timing_list_validacao),
            # Actual batch counts (not diff-based — mirrors what was really processed)
            "_n_pdfs_total": _n_total,
            "_n_pdfs_ok":    _n_ok,
            "_n_pdfs_fail":  _n_fail,
        }

        print(
            f"[Timing] Wall: Download={_t_download_total:.1f}s | Core={_t_core_wall:.1f}s\n"
            f"[Timing] Per-PDF CPU avg: Preprocess={_safe_avg(_timing_list_preprocess)}s | "
            f"Classificação(pag)={_safe_avg(_timing_list_classificacao)}s | "
            f"Extração(doc)={_safe_avg(_timing_list_extracao)}s | "
            f"Validação={_safe_avg(_timing_list_validacao)}s"
        )
        # ─────────────────────────────────────────────────────────────────────

        # Build output rows (map results back to original CSV rows)
        # Using new report format inspired by evaluation/utils/generate_results.py

        for task in pdf_tasks:
            pdf_name = task["pdf_name"]
            group_df = task["group_df"]
            result = pdf_results.get(pdf_name, {})

            # Get validation result (contains correctly_extracted, missing_nfs, etc.)
            validation = result.get("validation", {})

            # Get page categories, justifications, and nf_pages for classification
            page_categories = result.get("page_categories", {})
            page_justifications = result.get("page_justifications", {})
            nf_pages = result.get("nf_pages", [])

            # Build lookup from validation correctly_extracted (has matched info)
            # Note: Merge logic is now handled per-declaration in validator.py via RPS matching
            matched_lookup = {}
            for item in validation.get("correctly_extracted", []):
                expected = item.get("expected", {})
                exp_cnpj = str(expected.get("cnpj", ""))
                exp_numero = str(expected.get("numero_nf", ""))
                matched_lookup[(exp_cnpj, exp_numero)] = item

            # Add result for each row (each expected NF from database)
            for row_idx, row in group_df.iterrows():
                id_documento = row.get("id_documento", "")  # Document ID from database
                cnpj = str(row.get("cnpj_cpf", ""))  # CNPJ from database
                cnpj_cpf_declaracao = cnpj  # Preserve for Excel output
                numero = str(row.get("num_documento", ""))  # NF number from database
                valor_documento = row.get("valor_documento")  # Document value
                valor_pago_total = row.get("valor_pago_total")  # Paid value (aggregated total)
                # Get individual paid value (for cases with multiple installments/rateio)
                # Falls back to valor_pago_total if valor_pago column doesn't exist in CSV
                valor_pago_individual = row.get("valor_pago", row.get("valor_pago_total"))  # CORRECTED: Individual paid value per row

                # Initialize model columns as empty/None
                pagina_nf_modelo = None
                tipo_documento_modelo = ""
                cnpj_modelo = ""
                numero_nf_modelo = ""
                valor_total_modelo = None
                data_emissao_modelo = None
                observacao_modelo = None  # Pure LLM output from extraction (if exists)
                justificativa_modelo = None  # From classification phase (only when NF found)
                categoria_modelo = None  # From classification phase (only when NF found)
                indicador_nf_encontrada = False  # Indica se esta declaração teve match com alguma NF extraída (2/3 campos: CNPJ + número + data)
                nf_extraida_pdf = len(result.get("extracted_nfs", [])) > 0  # Indica se alguma NF foi extraída do PDF (independente de match com declaração)
                classificacao_modelo = None  # Will only be set to "Not Analyzable" from classification phase

                # New structured fields replacing debug_info
                pipeline_classification_detail = None
                pipeline_extraction_detail = None
                pipeline_error = None

                # Check if PDF processing failed (download error, API timeout, etc.)
                if not result.get("success", True):
                    # Processing error - populate pipeline_error with error details
                    error_message = result.get("error", "Unknown processing error")

                    # Determine error type and stage from error message
                    error_type = "unknown_error"
                    stage = "unknown"
                    if "download" in error_message.lower():
                        error_type = "download_failed"
                        stage = "download"
                    elif "timeout" in error_message.lower():
                        error_type = "api_timeout"
                        stage = "extraction"  # Usually happens during extraction
                    elif "extraction" in error_message.lower():
                        error_type = "extraction_failed"
                        stage = "extraction"
                    elif "classification" in error_message.lower():
                        error_type = "classification_failed"
                        stage = "classification"

                    # Set classification to "Não foi possível analisar" for processing errors
                    classificacao_modelo = "Não foi possível analisar"

                    # Build structured error
                    pipeline_error = json.dumps({
                        "stage": stage,
                        "error_type": error_type,
                        "error_message": error_message
                    }, ensure_ascii=False)

                    # Classification/extraction details are null when processing failed
                    pipeline_classification_detail = None
                    pipeline_extraction_detail = None

                    # Skip to appending result (all other fields stay as initialized defaults)
                    results.append(
                        {
                            # Required output fields (19 columns total)
                            "id_documento": id_documento,
                            "nome_arquivo": pdf_name,
                            "cnpj_cpf_declaracao": cnpj_cpf_declaracao,
                            "numero_nf_declaracao": numero,
                            "pagina_nf_modelo": pagina_nf_modelo,
                            "cnpj_modelo": cnpj_modelo,
                            "numero_nf_modelo": numero_nf_modelo,
                            "valor_total_modelo": valor_total_modelo,
                            "data_emissao_modelo": data_emissao_modelo,
                            "tipo_documento_modelo": tipo_documento_modelo,
                            "nf_extraida_pdf_modelo": nf_extraida_pdf,
                            "indicador_nf_encontrada_modelo": indicador_nf_encontrada,
                            "classificacao_modelo": classificacao_modelo if classificacao_modelo in ["Not Analyzable", "Apontamento Leve"] else None,
                            "categoria_modelo": categoria_modelo,  # From classification phase (only when NF found)
                            "justificativa_modelo": justificativa_modelo,  # From classification phase (only when NF found)
                            "observacao_modelo": observacao_modelo,  # Pure LLM output (currently not generated)
                            # NEW: Structured pipeline details (replacing debug_info)
                            "pipeline_classification_detail": pipeline_classification_detail,
                            "pipeline_extraction_detail": pipeline_extraction_detail,
                            "pipeline_error": pipeline_error,
                        }
                    )
                    continue  # Skip to next row

                # Check if this expected NF was matched in validation
                match_key = (cnpj, numero)
                if match_key in matched_lookup:
                    # NF was found by model
                    match_item = matched_lookup[match_key]
                    extracted = match_item.get("extracted", {})

                    pagina_nf_modelo = extracted.get("pagina")
                    tipo_documento_modelo = extracted.get("tipo_documento", "")
                    cnpj_modelo = extracted.get("cnpj_emitente", "")
                    numero_nf_modelo = extracted.get("numero_nf", "")
                    valor_total_modelo = extracted.get("valor_total")
                    data_emissao_modelo = extracted.get("data_emissao")

                    indicador_nf_encontrada = True

                    # Build structured pipeline details (NEW)
                    # Classification detail: full page-by-page classification
                    pipeline_classification_detail = json.dumps(
                        self._build_classification_detail(page_categories, page_justifications, nf_pages),
                        ensure_ascii=False
                    ) if page_categories else None

                    # Extraction detail: all extracted documents with page mapping
                    # Always populate (even when empty) - includes metadata like possui_nota_fiscal, quantidade, etc
                    pipeline_extraction_detail = json.dumps(
                        self._build_extraction_detail(result.get("extracted_nfs", []), result),
                        ensure_ascii=False
                    )

                    # No error when NF is found
                    pipeline_error = None

                    # Get category and justification from the page where NF was found
                    if pagina_nf_modelo and pagina_nf_modelo in page_justifications:
                        page_justification = page_justifications[pagina_nf_modelo]
                        justificativa_modelo = page_justification
                        categoria_modelo = page_categories.get(pagina_nf_modelo)
                        # Check if page was classified as "Not Analyzable"
                        if "não analisável" in page_justification.lower() or "not analyzable" in page_justification.lower():
                            classificacao_modelo = "Not Analyzable"

                    # Check if validator set "Apontamento Leve" classification
                    # This happens when declaration uses Ticket number instead of NF number (reverse RPS match)
                    validator_classification = match_item.get('classification')
                    if validator_classification == 'Apontamento Leve':
                        classificacao_modelo = "Apontamento Leve"
                        justificativa_modelo = match_item.get('reason', justificativa_modelo)
                else:
                    # NF NOT found - build structured pipeline details
                    # Classification detail: full page-by-page classification
                    if page_categories:
                        pipeline_classification_detail = json.dumps(
                            self._build_classification_detail(page_categories, page_justifications, nf_pages),
                            ensure_ascii=False
                        )
                    else:
                        # No classification available - mark as error
                        pipeline_classification_detail = None
                        pipeline_error = json.dumps({
                            "stage": "classification",
                            "error_type": "no_classification_available",
                            "error_message": "Nenhuma classificação disponível para este PDF"
                        }, ensure_ascii=False)

                    # Extraction detail: all extracted documents (even though none matched)
                    # Always populate (even when empty) - includes metadata like possui_nota_fiscal, quantidade, etc
                    pipeline_extraction_detail = json.dumps(
                        self._build_extraction_detail(result.get("extracted_nfs", []), result),
                        ensure_ascii=False
                    )

                    # No error if classification/extraction worked (just no match)
                    if page_categories and pipeline_error is None:
                        pipeline_error = None

                    # Populate categoria_modelo and tipo_documento_modelo even when no match
                    # Strategy based on whether extraction found any documents

                    extracted_nfs = result.get("extracted_nfs", [])

                    if extracted_nfs:
                        # CENÁRIO B: PDF has extracted NFs but none matched
                        # Use most prioritized document
                        from ..compliance.document_prioritizer import select_prioritized_document

                        prioritized_doc = select_prioritized_document(extracted_nfs)
                        if prioritized_doc:
                            tipo_documento_modelo = prioritized_doc.get("tipo_documento", "")
                            pagina_nf_modelo = prioritized_doc.get("pagina")

                            # Get category from the page where this NF was found
                            if pagina_nf_modelo and pagina_nf_modelo in page_categories:
                                categoria_modelo = page_categories.get(pagina_nf_modelo)
                                justificativa_modelo = page_justifications.get(pagina_nf_modelo)
                    # CENÁRIO C: No documents extracted
                    # Set category but leave tipo as null (to differentiate from extraction running)
                    elif page_categories and not nf_pages:
                        # All pages classified as "Nenhuma das Opções"
                        categoria_modelo = "Nenhuma das Opções"
                        justificativa_modelo = "Nenhum documento fiscal encontrado no PDF"
                        # tipo_documento_modelo stays None
                    elif page_categories and nf_pages:
                        # Edge case: pages classified as fiscal but extraction failed/returned empty
                        # Use first NF page classification
                        first_nf_page = nf_pages[0]
                        categoria_modelo = page_categories.get(first_nf_page)
                        justificativa_modelo = page_justifications.get(first_nf_page)
                            # tipo_documento_modelo stays None (extraction failed)


                # NOTE: Simplified output - validation rules disabled, only classification phase results
                # classificacao_modelo populated for "Not Analyzable" or "Apontamento Leve"
                results.append(
                    {
                        # Required output fields (19 columns total)
                        "id_documento": id_documento,
                        "nome_arquivo": pdf_name,
                        "cnpj_cpf_declaracao": cnpj_cpf_declaracao,
                        "numero_nf_declaracao": numero,
                        "pagina_nf_modelo": pagina_nf_modelo,
                        "cnpj_modelo": cnpj_modelo,
                        "numero_nf_modelo": numero_nf_modelo,
                        "valor_total_modelo": valor_total_modelo,
                        "data_emissao_modelo": data_emissao_modelo,
                        "tipo_documento_modelo": tipo_documento_modelo,
                        "nf_extraida_pdf_modelo": nf_extraida_pdf,
                        "indicador_nf_encontrada_modelo": indicador_nf_encontrada,
                        "classificacao_modelo": classificacao_modelo if classificacao_modelo in ["Not Analyzable", "Apontamento Leve"] else None,
                        "categoria_modelo": categoria_modelo,  # From classification phase (only when NF found)
                        "justificativa_modelo": justificativa_modelo,  # From classification phase (only when NF found)
                        "observacao_modelo": observacao_modelo,  # Pure LLM output (currently not generated)
                        # NEW: Structured pipeline details (replacing debug_info)
                        "pipeline_classification_detail": pipeline_classification_detail,
                        "pipeline_extraction_detail": pipeline_extraction_detail,
                        "pipeline_error": pipeline_error,
                    }
                )

        # Validate page consistency (NEW: detect page mapping issues)
        print(f"\n{'='*80}")
        print("Validating page consistency...")
        print(f"{'='*80}")

        inconsistencies = []
        for task in pdf_tasks:
            pdf_name = task["pdf_name"]
            result = pdf_results.get(pdf_name, {})

            if not result.get("success", True):
                continue  # Skip failed PDFs

            extracted_nfs = result.get("extracted_nfs", [])
            nf_pages = result.get("nf_pages", [])

            # Validate: páginas extraídas devem estar em nf_pages
            for nf in extracted_nfs:
                page = nf.get('pagina')
                if page and page not in nf_pages:
                    inconsistencies.append({
                        "pdf_name": pdf_name,
                        "extracted_page": page,
                        "nf_pages": nf_pages,
                        "nf_numero": nf.get('numero_nf'),
                        "issue": "Página extraída não está em nf_pages (possível bug de mapeamento)"
                    })

        if inconsistencies:
            print(f"\n⚠️  WARNING: Found {len(inconsistencies)} page mapping inconsistencies:")
            for issue in inconsistencies[:10]:  # Show first 10
                print(f"  PDF: {issue['pdf_name']}")
                print(f"    Extracted page: {issue['extracted_page']} (NF: {issue['nf_numero']})")
                print(f"    Expected pages: {issue['nf_pages']}")
                print(f"    Issue: {issue['issue']}")
            if len(inconsistencies) > 10:
                print(f"  ... and {len(inconsistencies) - 10} more")
        else:
            print("✓ No page mapping inconsistencies found")

        # Create results DataFrame
        results_df = pd.DataFrame(results)

        # Print summary
        print(f"\n{'#'*80}")
        print("# Processing Complete")
        print(f"{'#'*80}")
        print(f"Total rows processed: {len(results_df)}")
        print(f"PDFs processed: {total_pdfs}")
        print("\nClassification Summary:")
        if results_df.empty:
            print("  Nenhum PDF processado.")
        else:
            print(results_df["classificacao_modelo"].value_counts())

        # Save output — format depends on output_mode
        json_items = None  # populated below when output_mode == "json"
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if self.output_mode == "json":
                # Build and save per-page JSON (no BQ/GCS writes here — caller handles GCS/BQ)
                _run_ts = datetime.utcnow()
                json_items = self._build_json_output(
                    pdf_tasks=pdf_tasks,
                    pdf_results=pdf_results,
                    input_df=df,
                    min_match_score=self.min_match_score,
                    match_requires_pdf_name=self.match_requires_pdf_name,
                    timestamp_geracao=_run_ts,
                    versao_pipeline=self._build_versao_pipeline(
                        mode=mode,
                        workers=max_workers,
                        requests_per_minute=requests_per_minute,
                        max_concurrent=max_concurrent,
                    ),
                    versao_prompt=self._build_versao_prompt(),
                )
                import json as _json
                with open(output_path, "w", encoding="utf-8") as _fh:
                    _json.dump(json_items, _fh, ensure_ascii=False, indent=2, default=str)
                ok_with_doc = sum(1 for i in json_items if i['pipeline_status'] == 'ok' and i['tipo_documento_extracao'])
                ok_without_doc = sum(1 for i in json_items if i['pipeline_status'] == 'ok' and not i['tipo_documento_extracao'])
                erro = sum(1 for i in json_items if i['pipeline_status'] == 'erro_processamento')
                print(f"\n[SUCCESS] JSON results saved to: {output_path}")
                print(f"          {len(json_items)} páginas ({ok_with_doc} com documento, "
                      f"{ok_without_doc} sem documento, {erro} com erro de processamento)")
            else:
                json_items = None
                results_df.to_excel(output_path, index=False)
                print(f"\n[SUCCESS] Results saved to: {output_path}")

        # Print cache statistics
        print("\nCache Statistics:")
        stats = self.db_manager.get_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # Cleanup pre-downloaded PDFs (optional)
        if not keep_pdfs:
            print(
                f"\n[Cleanup] Removing {len(downloaded_paths)} pre-downloaded PDFs..."
            )
            for pdf_path in downloaded_paths.values():
                try:
                    self.gcs_downloader.cleanup_local_file(pdf_path)
                except Exception:
                    pass  # Ignore cleanup errors
            print("[OK] Cleanup complete")
        else:
            print(
                f"\n[Cleanup] Skipped - Keeping {len(downloaded_paths)} PDFs (--keep-pdfs flag)"
            )
            print(f"PDFs saved in: {self.temp_dir}")

        # Generate metadata if experiment_id provided
        if experiment_id and output_path:
            timestamp_end = datetime.now()
            run_id = f"run_{timestamp_start.strftime('%Y%m%d_%H%M%S')}"

            # Build config dict from parameters
            config = {
                'mode': mode.value,
                'workers': max_workers,
                'limit': limit,
                'keep_pdfs': keep_pdfs,
                'input_csv': str(csv_path)
            }

            # Estimate API usage from cache stats
            # Note: This is approximate - actual API calls are tracked in cache DB
            # We use cache misses as proxy for API calls
            cache_stats_dict = {
                'classification_hits': stats.get('classification_cache_hits', 0),
                'classification_misses': stats.get('classification_cache_misses', 0),
                'extraction_hits': stats.get('extraction_cache_hits', 0),
                'extraction_misses': stats.get('extraction_cache_misses', 0),
                'cache_hit_rate': stats.get('overall_cache_hit_rate', 0.0)
            }

            # Approximate API usage (cache misses = API calls)
            api_usage_counters['classification_calls'] = cache_stats_dict['classification_misses']
            api_usage_counters['extraction_calls'] = cache_stats_dict['extraction_misses']

            # TODO: Get actual token counts from cache DB if available
            # For now, estimate based on typical usage:
            # - Classification: ~500 input tokens, ~50 output tokens per call
            # - Extraction: ~2000 input tokens, ~500 output tokens per call
            api_usage_counters['total_input_tokens'] = (
                cache_stats_dict['classification_misses'] * 500 +
                cache_stats_dict['extraction_misses'] * 2000
            )
            api_usage_counters['total_output_tokens'] = (
                cache_stats_dict['classification_misses'] * 50 +
                cache_stats_dict['extraction_misses'] * 500
            )

            # Estimate cost (Gemini Flash 2.0 pricing: ~$0.075/1M input, ~$0.30/1M output)
            api_usage_counters['estimated_cost_usd'] = round(
                (api_usage_counters['total_input_tokens'] / 1_000_000 * 0.075) +
                (api_usage_counters['total_output_tokens'] / 1_000_000 * 0.30),
                2
            )

            # Generate metadata
            metadata = self._generate_metadata(
                experiment_id=experiment_id,
                run_id=run_id,
                timestamp_start=timestamp_start,
                timestamp_end=timestamp_end,
                config=config,
                results_df=results_df,
                cache_stats=cache_stats_dict,
                api_usage=api_usage_counters
            )

            # Save metadata.json next to results.xlsx
            metadata_path = output_path.parent / 'metadata.json'
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            print(f"\n[Metadata] Saved to: {metadata_path}")
            print(f"  Experiment: {experiment_id}")
            print(f"  Run: {run_id}")
            print(f"  Duration: {metadata['duration_seconds']/60:.1f} min")
            print(f"  Classification prompt: v{metadata['prompts']['classification']['version']}")
            print(f"  Extraction prompt: v{metadata['prompts']['extraction']['version']}")

        # Return DataFrame, json_items, and timing_stats for this batch.
        # json_items is None when output_mode != "json" or output_path is not set.
        # timing_stats contains avg_sec_* metrics to be written to pipeline_runs.
        return results_df, json_items, timing_stats

"""
POC Pipeline Processor - Processes database rows using core NF pipeline with caching.
Integrates GCS downloading, SQLite caching, and core NF processing modules.

``POCProcessor`` composes plain-function modules (``setup``, ``cache``, ``process``,
``database``) rather than mixin classes: each module operates on an explicit
``processor`` instance passed as its first argument, and the class below just
holds state and delegates — preserving the exact method surface other modules
and tests call directly (``process_pdf``, ``classify_page_from_cache``, etc).
"""

import logging
import sys
import threading
from pathlib import Path
from typing import Any

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

from ..core.classifiers.gemini_classifier import GeminiClassifier
from ..extraction import NFExtractor
from ..run_poc.gcs_downloader import GCSDownloader
from ..run_poc.sqlite_cache_manager import DatabaseManager
from . import cache, database, process, setup
from .modes import ExecutionMode


class POCProcessor:
    """Processes database rows using the core NF pipeline with caching."""

    # Inner (intra-PDF) classification worker pool size. Restored here after
    # being dropped during the Fase 2/3 mechanical migration from
    # agent-nf-validator (was a class attribute there too) — its absence
    # meant any real call reaching process_pdf's Step 2 raised AttributeError.
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
        match_requires_pdf_name: bool = False,
    ):
        """Initialize processor. See ``setup.initialize`` for full parameter documentation."""
        setup.initialize(
            self,
            db_manager=db_manager,
            gcs_downloader=gcs_downloader,
            gemini_credentials_path=gemini_credentials_path,
            temp_dir=temp_dir,
            quiet=quiet,
            prompt_versions=prompt_versions,
            extraction_batch_size=extraction_batch_size,
            min_match_score=min_match_score,
            match_requires_pdf_name=match_requires_pdf_name,
        )

    @property
    def classifier(self) -> GeminiClassifier:
        """Lazy load classifier. See ``setup.get_classifier``."""
        return setup.get_classifier(self)

    @property
    def extractor(self) -> NFExtractor:
        """Lazy load extractor. See ``setup.get_extractor``."""
        return setup.get_extractor(self)

    def _pdf_page_to_bytes(self, pdf_path: Path, page_number: int) -> bytes:
        """See ``setup.pdf_page_to_bytes``."""
        return setup.pdf_page_to_bytes(pdf_path, page_number)

    def _create_filtered_pdf_bytes(self, pdf_path: Path, pages: list[int]) -> bytes:
        """See ``setup.create_filtered_pdf_bytes``."""
        return setup.create_filtered_pdf_bytes(pdf_path, pages)

    def preprocess_classification_page(self, pdf_path: Path, page_number: int) -> tuple[int, bool]:
        """See ``cache.preprocess_classification_page``."""
        return cache.preprocess_classification_page(self, pdf_path, page_number)

    def classify_page_from_cache(
        self, pdf_path: Path, page_number: int, skip_api_call: bool = False
    ) -> tuple[str | None, str | None, bool, str | None, int | None]:
        """See ``cache.classify_page_from_cache``."""
        return cache.classify_page_from_cache(self, pdf_path, page_number, skip_api_call)

    def preprocess_extraction_pdf(self, pdf_path: Path, nf_pages: list[int]) -> tuple[int, bool]:
        """See ``cache.preprocess_extraction_pdf``."""
        return cache.preprocess_extraction_pdf(self, pdf_path, nf_pages)

    def extract_nf_from_cache(
        self,
        pdf_path: Path,
        nf_pages: list[int],
        skip_api_call: bool = False,
        page_classifications: dict[int, str] | None = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        """See ``cache.extract_nf_from_cache``."""
        return cache.extract_nf_from_cache(self, pdf_path, nf_pages, skip_api_call, page_classifications)

    def check_extraction_cache(self, pdf_path: Path) -> tuple[dict | None, list[int] | None]:
        """See ``cache.check_extraction_cache``."""
        return cache.check_extraction_cache(self, pdf_path)

    def check_classification_cache(self, pdf_path: Path, total_pages: int) -> bool:
        """See ``cache.check_classification_cache``."""
        return cache.check_classification_cache(self, pdf_path, total_pages)

    def load_all_cached_classifications(self, pdf_path: Path) -> tuple[dict[int, str], dict[int, str]]:
        """See ``cache.load_all_cached_classifications``."""
        return cache.load_all_cached_classifications(self, pdf_path)

    def load_cached_page_categories(self, pdf_path: Path, total_pages: int) -> dict[int, str]:
        """See ``cache.load_cached_page_categories``."""
        return cache.load_cached_page_categories(self, pdf_path, total_pages)

    def process_pdf(
        self,
        pdf_filename: str,
        expected_nfs: list[dict[str, Any]],
        mode: ExecutionMode = ExecutionMode.FULL,
        pdf_path: Path | None = None,
    ) -> dict[str, Any]:
        """See ``process.process_pdf``."""
        return process.process_pdf(self, pdf_filename, expected_nfs, mode, pdf_path)

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
        """See ``process.process_single_pdf_worker``."""
        return process.process_single_pdf_worker(
            self, pdf_name, expected_nfs, mode, progress_lock, completed_count, total_pdfs, pdf_path
        )

    def process_database(
        self,
        csv_path: Path,
        output_path: Path | None = None,
        limit: int | None = None,
        mode: ExecutionMode = ExecutionMode.FULL,
        max_workers: int = 1000,
        keep_pdfs: bool = False,
        experiment_id: str | None = None,
        requests_per_minute: int = 0,
        max_concurrent: int = 0,
    ):
        """See ``database.process_database``."""
        return database.process_database(
            self,
            csv_path,
            output_path=output_path,
            limit=limit,
            mode=mode,
            max_workers=max_workers,
            keep_pdfs=keep_pdfs,
            experiment_id=experiment_id,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
        )

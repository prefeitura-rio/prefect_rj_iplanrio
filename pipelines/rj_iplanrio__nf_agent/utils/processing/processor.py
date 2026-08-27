"""
POC Pipeline Processor - Processes database rows using core NF pipeline with caching.
Integrates GCS downloading, SQLite caching, and core NF processing modules.

``POCProcessor`` composes plain-function modules (``setup``, ``classification_cache``,
``process``, ``batch``) rather than mixin classes: each module operates on an explicit
``processor`` instance passed as its first argument, and the class below just
holds state and delegates — preserving the exact method surface other modules
and tests call directly (``process_pdf``, ``classify_page_from_cache``, etc).
"""

import threading
from pathlib import Path
from typing import Any

from prefect_rj_iplanrio.logging import get_logger

from ..cache import DatabaseManager
from ..classification.gemini_classifier import GeminiClassifier
from ..extraction.extractor import NFExtractor
from ..gcs import GCSDownloader
from . import batch, classification_cache, process, setup

logger = get_logger(__name__)


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
        temp_dir: Path | None = None,
        quiet: bool = False,
        prompt_versions: dict[str, str] | None = None,
    ):
        """Initialize processor. See ``setup.initialize`` for full parameter documentation."""
        setup.initialize(
            self,
            db_manager=db_manager,
            gcs_downloader=gcs_downloader,
            temp_dir=temp_dir,
            quiet=quiet,
            prompt_versions=prompt_versions,
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
        """See ``classification_cache.preprocess_classification_page``."""
        return classification_cache.preprocess_classification_page(self, pdf_path, page_number)

    def classify_page_from_cache(
        self, pdf_path: Path, page_number: int, skip_api_call: bool = False
    ) -> tuple[str | None, str | None, bool, str | None, int | None]:
        """See ``classification_cache.classify_page_from_cache``."""
        return classification_cache.classify_page_from_cache(self, pdf_path, page_number, skip_api_call)

    def preprocess_extraction_pdf(self, pdf_path: Path, nf_pages: list[int]) -> tuple[int, bool]:
        """See ``classification_cache.preprocess_extraction_pdf``."""
        return classification_cache.preprocess_extraction_pdf(self, pdf_path, nf_pages)

    def extract_nf_from_cache(
        self,
        pdf_path: Path,
        nf_pages: list[int],
        skip_api_call: bool = False,
        page_classifications: dict[int, str] | None = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        """See ``classification_cache.extract_nf_from_cache``."""
        return classification_cache.extract_nf_from_cache(self, pdf_path, nf_pages, skip_api_call, page_classifications)

    def check_extraction_cache(self, pdf_path: Path) -> tuple[dict | None, list[int] | None]:
        """See ``classification_cache.check_extraction_cache``."""
        return classification_cache.check_extraction_cache(self, pdf_path)

    def check_classification_cache(self, pdf_path: Path, total_pages: int) -> bool:
        """See ``classification_cache.check_classification_cache``."""
        return classification_cache.check_classification_cache(self, pdf_path, total_pages)

    def load_all_cached_classifications(self, pdf_path: Path) -> tuple[dict[int, str], dict[int, str]]:
        """See ``classification_cache.load_all_cached_classifications``."""
        return classification_cache.load_all_cached_classifications(self, pdf_path)

    def process_pdf(
        self,
        pdf_filename: str,
        pdf_path: Path | None = None,
    ) -> dict[str, Any]:
        """See ``process.process_pdf``."""
        return process.process_pdf(self, pdf_filename, pdf_path)

    def _process_single_pdf_worker(
        self,
        pdf_name: str,
        progress_lock: threading.Lock,
        completed_count: list[int],
        pdf_path: Path | None = None,
    ) -> dict[str, Any]:
        """See ``process.process_single_pdf_worker``."""
        return process.process_single_pdf_worker(self, pdf_name, progress_lock, completed_count, pdf_path)

    def process_database(
        self,
        pdf_names: list[str],
        max_workers: int = 1000,
        requests_per_minute: int = 0,
        max_concurrent: int = 0,
    ):
        """See ``batch.process_database``."""
        return batch.process_database(
            self,
            pdf_names,
            max_workers=max_workers,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
        )

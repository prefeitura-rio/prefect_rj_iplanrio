"""Core NF Processing: OCR, Classification, Extraction, and Pipeline orchestration"""

from .base import BaseClassifier, BaseExtractor, BasePipeline
from .classifiers import GeminiClassifier, NFClassifier
from .config import (
    BEST_PARAMS,
    BIGQUERY_SERVICE_ACCOUNT_PATH,
    EXTRACTION_PROMPT,
    GEMINI_CONFIG,
    OCR_CONFIG,
    load_categories,
)
from .ocr import OCRConfig, OCRProcessor, PaddleOCRConfig, get_page_count, run_ocr_on_pdf
from .pipeline import NFPipeline, run_pipeline

__all__ = [
    "BEST_PARAMS",
    "BIGQUERY_SERVICE_ACCOUNT_PATH",
    "EXTRACTION_PROMPT",
    "GEMINI_CONFIG",
    "OCR_CONFIG",
    "BaseClassifier",
    "BaseExtractor",
    "BasePipeline",
    "ComplianceValidator",
    "GeminiClassifier",
    "NFClassifier",
    "NFExtractor",
    "NFPipeline",
    "OCRConfig",
    "OCRProcessor",
    "PaddleOCRConfig",
    "extract_nf_data",
    "get_page_count",
    "load_categories",
    "normalize_cnpj",
    "normalize_number",
    "normalize_value",
    "run_ocr_on_pdf",
    "run_pipeline",
    "validate_against_expected",
]


def __getattr__(name: str):
    """Lazily re-export domain names to break cross-package import cycles.

    ``utils.compliance`` and ``utils.extraction`` depend on ``utils.core``
    (config/helpers), and ``utils.core`` historically re-exported them as a
    facade. Importing them eagerly would create a cycle when the domain
    packages are the entry point. Resolve them on demand instead.
    """
    import importlib

    if name in {"ComplianceValidator", "normalize_cnpj", "normalize_number", "normalize_value", "validate_against_expected"}:
        mod = importlib.import_module("..compliance", __name__)
        return getattr(mod, name)
    if name in {"NFExtractor", "extract_nf_data"}:
        mod = importlib.import_module("..extraction", __name__)
        return getattr(mod, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

"""Core NF Processing: OCR, Classification, Extraction, and Pipeline orchestration"""

from .base import BaseClassifier, BaseExtractor, BasePipeline
from .classifiers import GeminiClassifier, NFClassifier
from ..compliance import (
    ComplianceValidator,
    normalize_cnpj,
    normalize_number,
    normalize_value,
    validate_against_expected,
)
from .config import (
    BEST_PARAMS,
    BIGQUERY_SERVICE_ACCOUNT_PATH,
    EXTRACTION_PROMPT,
    GEMINI_CONFIG,
    OCR_CONFIG,
    load_categories,
)
from ..extraction import NFExtractor, extract_nf_data
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

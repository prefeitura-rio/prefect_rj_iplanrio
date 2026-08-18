"""
Configuration for the NF processing module.
Contains optimized classifier parameters and model settings.
Prompts are now stored in core/prompts/ folder.
"""

import json
from pathlib import Path

from .prompts import EXTRACTION_PROMPT, CLASSIFICATION_PROMPT  # noqa: F401  # re-exported by core/__init__

# Import prompts from dedicated prompts module

# Best classifier parameters (optimized)
BEST_PARAMS = {
    "weight_NF-specific_high_confidence": 23,
    "weight_NF-specific_medium_confidence": 5,
    "weight_NF-specific_low_confidence": 1,
    "weight_Common": 0,
    "weight_Non-NF": -13,
    "threshold_NF": 2.5,
    "threshold_NonNF": 0
}

# Path to OCR classifier sequence patterns
CATEGORIES_FILE = Path(__file__).parent / "classifiers" / "ocr_sequence_patterns.json"

# OCR Configuration
OCR_CONFIG = {
    "languages": ["pt", "en"],
    "gpu": True,
    "dpi": 200,  # DPI for PDF to image conversion
    "engine": "easyocr",  # Options: "easyocr" or "paddleocr"
    # PaddleOCR specific settings
    "paddleocr": {
        "lang": "en",  # PaddleOCR language code
        "ocr_version": "PP-OCRv5",
        "use_doc_orientation_classify": True,
        "use_doc_unwarping": True,
        "use_textline_orientation": True,
        "textline_orientation_batch_size": 1,
        "text_recognition_batch_size": 1
    }
}

# Note: EXTRACTION_PROMPT is imported from .prompts module
# Prompts are versioned in core/prompts/versions/{classification,extraction}/v*.txt
# See core/prompts/versions/ to view/edit prompt versions

# Gemini model configuration
GEMINI_CONFIG = {
    "model_name": "gemini-3.1-flash-lite",
    "temperature": 0.1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192
}

# Service account paths (default)
# Put your Gemini service account file at: organized_repo_module/credentials/gemini-service-account.json
SERVICE_ACCOUNT_PATH = Path(__file__).parent.parent / "credentials" / "gemini-service-account.json"

# BigQuery service account path
# Put your BigQuery service account file at: organized_repo_module/credentials/bigquery-service-account.json
BIGQUERY_SERVICE_ACCOUNT_PATH = Path(__file__).parent.parent / "credentials" / "bigquery-service-account.json"


def load_categories() -> dict:
    """Load sequence categories from JSON file."""
    if CATEGORIES_FILE.exists():
        with open(CATEGORIES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    else:
        raise FileNotFoundError(f"Categories file not found: {CATEGORIES_FILE}")

"""Authentication and model setup for ``NFExtractor``."""

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from ..classification.config import EXTRACTION_PROMPT, GEMINI_CONFIG, SERVICE_ACCOUNT_PATH

if TYPE_CHECKING:
    from .extractor import NFExtractor

logger = logging.getLogger(__name__)


def initialize(
    extractor: "NFExtractor",
    model_name: str | None = None,
    service_account_file: str | None = None,
    api_key: str | None = None,
    extraction_prompt: str | None = None,
    batch_size: int = 5,
) -> None:
    """
    Initialize ``extractor`` with Gemini model settings.

    Supports authentication in priority order:

    1. Service account file path
    2. API key
    3. Application Default Credentials (ADC) — covers Infisical-injected creds,
       GCP VM/pod service accounts, and ``gcloud auth application-default login``.

    :param extractor: The ``NFExtractor`` instance being constructed.
    :param model_name: Gemini model name (default from config).
    :param service_account_file: Path to service account JSON file.
    :param api_key: Google API key (alternative to service account).
    :param extraction_prompt: Custom prompt text to use (default: EXTRACTION_PROMPT from config).
    :param batch_size: Maximum number of pages per extraction API call (default: 5).
        Set to 1 to process one page at a time (useful for testing and when passing
        per-page classification hints via page_classifications).
    """
    extractor.model_name = model_name or GEMINI_CONFIG["model_name"]
    extractor._model = None
    extractor._service_account_file = service_account_file
    extractor._api_key = api_key
    extractor.extraction_prompt = extraction_prompt or EXTRACTION_PROMPT
    extractor.batch_size = batch_size


def configure_genai(extractor: "NFExtractor"):
    """
    Configure google.generativeai with credentials for ``extractor``.

    Tries authentication in order:
    1. Service account file (if provided and exists)
    2. API key (if provided)
    3. Application Default Credentials (ADC) - fallback for GCP environments
    """
    import google.generativeai as genai

    # 1. Try service account file
    service_account_path = extractor._service_account_file or os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")

    # If no explicit path, check default location
    if service_account_path is None:
        service_account_path = SERVICE_ACCOUNT_PATH

    if service_account_path and Path(service_account_path).exists():
        try:
            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_file(
                service_account_path,
                scopes=["https://www.googleapis.com/auth/generative-language"],
            )
            genai.configure(credentials=credentials)
            return genai
        except Exception as e:
            logger.warning(
                "Failed to load service account from %s: %s. Falling back to other authentication methods.",
                service_account_path,
                e,
            )

    # 2. Try API key
    api_key = extractor._api_key or os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
    if api_key:
        genai.configure(api_key=api_key)
        return genai

    # 3. Try Application Default Credentials (ADC) - for GCP environments
    try:
        import google.auth

        credentials, project = google.auth.default(scopes=["https://www.googleapis.com/auth/generative-language"])
        genai.configure(credentials=credentials)
        logger.info("Using Application Default Credentials (ADC)")
        if project:
            logger.info("GCP Project: %s", project)
        return genai
    except Exception as adc_error:
        # ADC failed - no credentials available
        raise ValueError(
            "No Gemini credentials found. Provide one of:\n"
            "1. service_account_file parameter or GOOGLE_SERVICE_ACCOUNT_FILE env var\n"
            "2. api_key parameter or GOOGLE_API_KEY/GEMINI_API_KEY env var\n"
            "3. Application Default Credentials (run 'gcloud auth application-default login')\n"
            f"\nADC Error: {adc_error}"
        )


def get_model(extractor: "NFExtractor"):
    """Lazy load the Gemini model for ``extractor``."""
    if extractor._model is None:
        genai = configure_genai(extractor)
        extractor._model = genai.GenerativeModel(extractor.model_name)
    return extractor._model

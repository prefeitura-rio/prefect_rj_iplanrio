"""
Gemini Vision-based NF Classifier - Uses Gemini Flash API for page classification.
Does NOT require OCR preprocessing - works directly with PDF images.

Split into sibling modules to keep this file focused on ``GeminiClassifier`` itself:
``categories.py`` (category constants/normalization), ``page_extraction.py``
(single-page PDF->bytes), and ``page_classification.py`` (the Gemini API call).
Everything previously public here is re-exported below to keep existing
``from .gemini_classifier import X`` imports working unchanged.

``GeminiClassifier`` itself is a thin config holder + lazy model loader:
``processing/cache.py`` (the only real consumer) reads its attributes
(``.model``, ``.model_name``, ``.use_pdf_input``, ``.classification_prompt``)
and calls ``classify_page_with_model`` directly rather than going through
instance methods.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ..credentials import SERVICE_ACCOUNT_PATH
from ..prompts import CLASSIFICATION_PROMPT
from .categories import (  # noqa: F401  (re-exported; public API)
    CATEGORY_ALIASES,
    NF_CATEGORIES,
    PAGE_CATEGORIES,
    is_nf_category,
    normalize_category,
    similarity_score,
)
from .page_classification import (
    DEFAULT_GENERATION_CONFIG,
    DEFAULT_MODEL_NAME,
    ClassificationOptions,  # noqa: F401  (re-exported; public API)
    classify_page_with_model,  # noqa: F401  (re-exported; public API)
)
from .page_extraction import extract_page_as_bytes  # noqa: F401  (re-exported; public API)

logger = logging.getLogger(__name__)

# Note: CLASSIFICATION_PROMPT is imported from the pipeline-root prompts/ package
# Prompts are versioned in prompts/versions/classification/v*.txt
# See prompts/versions/ to view/edit prompt versions


class GeminiClassifier:
    """
    Vision-based classifier using Gemini Flash API.
    Does NOT require OCR - works directly with PDF/image input.
    """

    def __init__(
        self,
        service_account_path: str | None = None,
        model_name: str | None = None,
        generation_config: dict | None = None,
        use_pdf_input: bool = True,
        classification_prompt: str | None = None,
    ):
        """
        Initialize Gemini classifier.

        :param service_account_path: Path to service account JSON (None = use ADC).
        :param model_name: Gemini model name (default: gemini-2.0-flash-exp).
        :param generation_config: Generation config dict (default: optimized for
            classification).
        :param use_pdf_input: If True, send single-page PDFs to Gemini; if False,
            send PNG images (default: True).
        :param classification_prompt: Custom classification prompt to use
            (default: CLASSIFICATION_PROMPT).
        """
        # If no explicit path, check default location (but allow None)
        if service_account_path is None:
            # Only use default path if it exists
            if Path(SERVICE_ACCOUNT_PATH).exists():
                self.service_account_path = SERVICE_ACCOUNT_PATH
            else:
                # No service account - will use ADC
                self.service_account_path = None
        else:
            self.service_account_path = service_account_path

        self.model_name = model_name or DEFAULT_MODEL_NAME
        self.generation_config = generation_config or DEFAULT_GENERATION_CONFIG
        self.use_pdf_input = use_pdf_input
        self.classification_prompt = classification_prompt or CLASSIFICATION_PROMPT

        self._model = None

    @property
    def model(self):
        """
        Lazy initialization of Gemini model.

        Tries authentication in order:
        1. Service account file (if provided and exists)
        2. Application Default Credentials (ADC) - fallback for GCP environments
        """
        if self._model is None:
            import google.generativeai as genai
            from google.oauth2 import service_account

            # 1. Try service account file
            if self.service_account_path and Path(self.service_account_path).exists():
                try:
                    credentials = service_account.Credentials.from_service_account_file(
                        self.service_account_path,
                        scopes=["https://www.googleapis.com/auth/generative-language.retriever"],
                    )
                    genai.configure(credentials=credentials)
                    self._model = genai.GenerativeModel(
                        model_name=self.model_name, generation_config=self.generation_config
                    )
                    return self._model
                except Exception as e:
                    logger.warning("Failed to load service account from %s: %s", self.service_account_path, e)
                    logger.warning("Falling back to Application Default Credentials (ADC)")

            # 3. Try Application Default Credentials (ADC)
            try:
                import google.auth

                credentials, project = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/generative-language.retriever"]
                )
                genai.configure(credentials=credentials)
                self._model = genai.GenerativeModel(
                    model_name=self.model_name, generation_config=self.generation_config
                )
                logger.info("GeminiClassifier using Application Default Credentials (ADC)")
                if project:
                    logger.info("GCP Project: %s", project)
                return self._model
            except Exception as adc_error:
                raise ValueError(
                    "No Gemini credentials found. Provide one of:\n"
                    "1. service_account_path parameter with valid JSON file\n"
                    "2. Application Default Credentials (run 'gcloud auth application-default login')\n"
                    f"\nADC Error: {adc_error}"
                )

        return self._model

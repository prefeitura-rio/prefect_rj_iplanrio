"""
Gemini Vision-based NF Classifier - Uses Gemini Flash API for page classification.
Does NOT require OCR preprocessing - works directly with PDF images.

Split into sibling modules to keep this file focused on ``GeminiClassifier`` itself:
``categories.py`` (category constants/normalization), ``page_extraction.py``
(single-page PDF->bytes), and ``page_classification.py`` (the Gemini API call).
Everything previously public here is re-exported below to keep existing
``from .gemini_classifier import X`` imports working unchanged.

``GeminiClassifier`` itself is a thin config holder + lazy model loader:
``processing/classification_cache.py`` (the only real consumer) reads its attributes
(``.model``, ``.model_name``, ``.use_pdf_input``, ``.classification_prompt``)
and calls ``classify_page_with_model`` directly rather than going through
instance methods.
"""

from __future__ import annotations

from prefect_rj_iplanrio.logging import get_logger

from ..llm import build_gemini_model
from ..prompts import CLASSIFICATION_PROMPT
from .categories import (  # noqa: F401  (re-exported; public API)
    CATEGORY_ALIASES,
    NF_CATEGORIES,
    PAGE_CATEGORIES,
    is_nf_category,
    normalize_category,
    similarity_score,
)
from .config import DEFAULT_GENERATION_CONFIG, DEFAULT_MODEL_NAME
from .page_classification import (
    ClassificationOptions,  # noqa: F401  (re-exported; public API)
    classify_page_with_model,  # noqa: F401  (re-exported; public API)
)
from .page_extraction import extract_page_as_bytes  # noqa: F401  (re-exported; public API)

logger = get_logger(__name__)

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
        model_name: str | None = None,
        generation_config: dict | None = None,
        use_pdf_input: bool = True,
        classification_prompt: str | None = None,
    ):
        """
        Initialize Gemini classifier.

        :param model_name: Gemini model name (default: ``DEFAULT_MODEL_NAME``).
        :param generation_config: Generation config dict (default: optimized for
            classification).
        :param use_pdf_input: If True, send single-page PDFs to Gemini; if False,
            send PNG images (default: True).
        :param classification_prompt: Custom classification prompt to use
            (default: CLASSIFICATION_PROMPT).
        """
        self.model_name = model_name or DEFAULT_MODEL_NAME
        self.generation_config = generation_config or DEFAULT_GENERATION_CONFIG
        self.use_pdf_input = use_pdf_input
        self.classification_prompt = classification_prompt or CLASSIFICATION_PROMPT

        self._model = None

    @property
    def model(self):
        """Lazy-load the Bifrost-routed Gemini model.

        :returns: A ``google.generativeai.GenerativeModel`` for ``self.model_name``.
        """
        if self._model is None:
            self._model = build_gemini_model(self.model_name, self.generation_config)
        return self._model

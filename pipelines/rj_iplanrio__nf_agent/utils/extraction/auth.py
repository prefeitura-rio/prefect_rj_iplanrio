"""Model setup for ``NFExtractor`` — LLM calls are routed through Bifrost."""

from typing import TYPE_CHECKING

from prefect_rj_iplanrio.logging import get_logger

from ..llm import build_gemini_model
from ..prompts import EXTRACTION_PROMPT
from .config import GEMINI_CONFIG

if TYPE_CHECKING:
    from .extractor import NFExtractor

logger = get_logger(__name__)


def initialize(
    extractor: "NFExtractor",
    model_name: str | None = None,
    extraction_prompt: str | None = None,
) -> None:
    """Initialize ``extractor`` with Gemini model settings.

    Authentication for the model itself is handled by the Bifrost gateway
    (see :func:`..llm.build_gemini_model`) — there's no service-account file
    or API key to configure here.

    :param extractor: The ``NFExtractor`` instance being constructed.
    :param model_name: Gemini model name (default from config).
    :param extraction_prompt: Custom prompt text (default: ``EXTRACTION_PROMPT``).
    """
    extractor.model_name = model_name or GEMINI_CONFIG["model_name"]
    extractor._model = None
    extractor.extraction_prompt = extraction_prompt or EXTRACTION_PROMPT
    # Always 1 page per extraction API call — enables per-page classification-hint
    # injection into the prompt (see extraction/prompt.py); see also
    # utils/processing/setup.py::get_extractor.
    extractor.batch_size = 1


def get_model(extractor: "NFExtractor"):
    """Lazy-load the Bifrost-routed Gemini model for ``extractor``.

    :param extractor: The ``NFExtractor`` instance.
    :returns: A ``google.generativeai.GenerativeModel`` bound to ``extractor.model_name``.
    """
    if extractor._model is None:
        extractor._model = build_gemini_model(extractor.model_name)
    return extractor._model

"""Prompt construction and response parsing for ``NFExtractor``."""

from typing import TYPE_CHECKING

from iplanrio_agent_toolkit.gemini.response_parsing import parse_json_response

from prefect_rj_iplanrio.logging import get_logger

if TYPE_CHECKING:
    from .extractor import NFExtractor

logger = get_logger(__name__)


def build_prompt_with_hint(extractor: "NFExtractor", classification_hint: str | None = None) -> str:
    """
    Build the extraction prompt, substituting the ``{classification_hint}`` placeholder.

    If the prompt contains a ``{classification_hint}`` placeholder, it is replaced with
    a formatted hint block when a classification is provided, or with an empty string
    when no classification is available.

    The hint is wrapped in a delimited block (``<<<...>>>``) so the model clearly
    understands it is an automatically injected pre-classification note, separate from
    the main extraction instructions that follow.

    :param extractor: The ``NFExtractor`` instance (supplies ``extraction_prompt``).
    :param classification_hint: The document type identified by the classifier (e.g.
        ``"NFS-e"``), or None if no classification is available.
    :returns: Prompt text with the placeholder resolved.
    """
    if classification_hint:
        hint_text = (
            f"<<<\n"
            f"NOTA DE PRÉ-CLASSIFICAÇÃO (inserida automaticamente pelo pipeline):\n"
            f"O classificador automático identificou esta página como um possível "
            f"documento do tipo **{classification_hint}**. Use como referência inicial, "
            f"mas confirme visualmente antes de extrair — a classificação pode estar incorreta.\n"
            f">>>\n\n"
        )
    else:
        hint_text = ""

    return extractor.extraction_prompt.replace("{classification_hint}", hint_text)


def parse_response(response_text: str) -> dict:
    """
    Parse Gemini response text to JSON.

    :param response_text: Raw response from Gemini.
    :returns: Parsed JSON dictionary.
    """
    return parse_json_response(response_text)

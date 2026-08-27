"""Shared LLM client: routes ``google-generativeai`` through the Bifrost gateway.

Single construction point for the Gemini model used by both the classification
and extraction code paths. Replaces the former per-path service-account / API-key
/ ADC cascade with one Bifrost virtual key; the gateway then fans out to the
underlying Gemini/Vertex provider.
"""

import os

from prefect_rj_iplanrio.logging import get_logger

from .. import constants

logger = get_logger(__name__)


def build_gemini_model(model_name: str, generation_config: dict[str, object] | None = None):
    """Return a ``google.generativeai`` model routed through the Bifrost gateway.

    :param model_name: Gemini model id, e.g. ``gemini-3.1-flash-lite``. Bifrost's
        default provider is Vertex/Gemini, so no provider prefix is needed.
    :param generation_config: Optional ``generation_config`` dict forwarded to
        ``GenerativeModel``.
    :returns: A configured ``google.generativeai.GenerativeModel`` whose requests
        go through the Bifrost gateway.
    :raises RuntimeError: If the Bifrost virtual key or base URL env var is not set.
    """
    api_key = os.environ.get(constants.BIFROST_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(
            f"{constants.BIFROST_API_KEY_ENV} is not set — required to reach the Bifrost LLM gateway"
        )

    base_url = os.environ.get(constants.BIFROST_BASE_URL_ENV)
    if not base_url:
        raise RuntimeError(
            f"{constants.BIFROST_BASE_URL_ENV} is not set — required to reach the Bifrost LLM gateway"
        )

    import google.generativeai as genai  # noqa: PLC0415  (SDK installed only in the Docker image)

    genai.configure(
        api_key=api_key,
        transport="rest",
        client_options={"api_endpoint": base_url},
    )
    # INFO
    logger.warning("Gemini model %s configured via Bifrost", model_name)
    return genai.GenerativeModel(model_name=model_name, generation_config=generation_config)

"""Shared LLM client: routes calls through the Bifrost gateway.

Single construction point for the client used by both the classification
and extraction code paths. Bifrost exposes Google models (Gemini) through
an **OpenAI-compatible** endpoint (``/openai/v1``), not the native Gemini
``generateContent`` protocol — confirmed against
https://docs.dados.rio/ferramentas/opencode-usuario, whose own Bifrost
config for Google models is ``"npm": "@ai-sdk/openai-compatible"`` with
``baseURL`` ending in ``/openai/v1``, and model ids like
``vertex/gemini-3.5-flash`` (not the bare Gemini model name — that routes
straight to Vertex AI and previously hit a real
``constraints/vertexai.allowedModels`` Org Policy block on the Bifrost
project). The ``openai`` SDK has no protobuf/grpc conflict with the
workspace's dependencies, so — unlike the ``google-generativeai`` SDK this
replaces — it's a normal ``uv`` dependency, no Docker-isolated install
needed.
"""

import os

from openai import OpenAI

from prefect_rj_iplanrio.logging import get_logger

from .. import constants

logger = get_logger(__name__)


def build_llm_client() -> OpenAI:
    """Return an ``openai.OpenAI`` client routed through the Bifrost gateway.

    Unlike the old ``google-generativeai`` model object, this client isn't
    bound to a specific model — the model id is passed per-call (see
    ``classification/page_classification.py`` / ``extraction/api.py``),
    since that's how the OpenAI chat-completions protocol works.

    :returns: An ``OpenAI`` client whose requests go through the Bifrost gateway.
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

    logger.warning("OpenAI-compatible client configured via Bifrost (%s)", base_url)
    return OpenAI(api_key=api_key, base_url=base_url)

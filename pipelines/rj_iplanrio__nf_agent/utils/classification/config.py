"""LLM generation config for ``GeminiClassifier`` (classification-only; extraction has its own)."""

# "vertex/" prefix: this is the literal model id Bifrost expects on its
# OpenAI-compatible endpoint for Google models (confirmed against
# https://docs.dados.rio/ferramentas/opencode-usuario's own Bifrost config —
# `"models": {"gemini-3.5-flash": {"id": "vertex/gemini-3.5-flash"}}`). The
# bare model name (no prefix) routes to the native Gemini `generateContent`
# protocol instead, which both (a) isn't what Bifrost speaks for Google
# models here, and (b) previously hit a real
# `constraints/vertexai.allowedModels` Org Policy block when tried directly.
DEFAULT_MODEL_NAME = "vertex/gemini-3.5-flash"

DEFAULT_GENERATION_CONFIG = {
    "temperature": 0.1,
    "top_p": 0.95,
    # "top_k" has no equivalent in the OpenAI chat-completions protocol —
    # kept here as a record of the previous Gemini-native tuning, but
    # page_classification.py does not forward it to the API call.
    "top_k": 40,
    "max_output_tokens": 8192,
    # OpenAI-protocol JSON mode is requested per-call via
    # `response_format={"type": "json_object"}`, not a generation-config field —
    # see page_classification.py.
}

"""Model names and generation config for the Vertex AI Batch Prediction path.

Deliberately separate from ``utils/classification/config.py`` /
``utils/extraction/config.py``: those hold the ``"vertex/"``-prefixed model
id required by *Bifrost's* OpenAI-compatible endpoint for the synchronous
path (see their docstrings) — a prefix that is Bifrost-specific routing
syntax, not a real Vertex AI model id. Batch jobs talk to Vertex AI directly
via the ``google-genai`` SDK, which expects the bare model name instead (see
the Vertex AI Batch Prediction docs' Python sample:
``model="gemini-3.5-flash"``, no prefix).
"""

# Bare model id, no "vertex/" prefix — see module docstring. Kept in sync
# manually with utils/extraction/config.py::GEMINI_CONFIG["model_name"] and
# utils/classification/config.py::DEFAULT_MODEL_NAME (both currently
# "vertex/gemini-3.1-flash-lite" once the prefix is stripped), so the batch
# path uses the same model as the synchronous path.
BATCH_MODEL_NAME = "gemini-3.1-flash-lite"

# Mirrors utils/classification/config.py::DEFAULT_GENERATION_CONFIG, minus
# "top_k" (dropped by the synchronous path too — no equivalent forwarded to
# the API; kept only there as a historical record).
CLASSIFICATION_GENERATION_CONFIG: dict[str, float | int | str] = {
    "temperature": 0.1,
    "top_p": 0.95,
    "max_output_tokens": 8192,
    "response_mime_type": "application/json",
}

# Mirrors utils/extraction/config.py::GEMINI_CONFIG, same fields kept as
# CLASSIFICATION_GENERATION_CONFIG above.
EXTRACTION_GENERATION_CONFIG: dict[str, float | int | str] = {
    "temperature": 0.1,
    "top_p": 0.95,
    "max_output_tokens": 8192,
    "response_mime_type": "application/json",
}

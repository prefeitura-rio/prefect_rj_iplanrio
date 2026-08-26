"""Gemini generation config for ``GeminiClassifier`` (classification-only; extraction has its own)."""

DEFAULT_MODEL_NAME = "gemini-3.1-flash-lite"

DEFAULT_GENERATION_CONFIG = {
    "temperature": 0.1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192,
    "response_mime_type": "application/json",
}

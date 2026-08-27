"""Gemini generation config for ``NFExtractor`` (extraction-only; classification has its own)."""

GEMINI_CONFIG = {
    "model_name": "gemini-3.1-flash-lite",
    "temperature": 0.1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192,
}

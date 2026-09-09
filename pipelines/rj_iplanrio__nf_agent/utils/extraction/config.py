"""LLM generation config for ``NFExtractor`` (extraction-only; classification has its own)."""

# See utils/classification/config.py's comment on DEFAULT_MODEL_NAME — same
# "vertex/" model-id reasoning applies here. "top_k" is likewise kept only as
# a record of the previous Gemini-native tuning — extraction/api.py does not
# forward it (no equivalent in the OpenAI chat-completions protocol).
GEMINI_CONFIG = {
    "model_name": "vertex/gemini-3.1-flash-lite",
    "temperature": 0.1,
    "top_p": 0.95,
    "top_k": 40,
    "max_output_tokens": 8192,
}

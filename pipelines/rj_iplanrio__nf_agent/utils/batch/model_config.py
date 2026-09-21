"""Model name, provider hint, and generation config for the Bifrost-routed batch path.

Reuses the exact same model id sync mode already calls successfully through
Bifrost (``utils/classification/config.py::DEFAULT_MODEL_NAME`` /
``utils/extraction/config.py::GEMINI_CONFIG["model_name"]``) — no separate
"batch model id" exists.

``BIFROST_BATCH_PROVIDER`` is this deployment's provider name for the
Files/Batch API's ``extra_body={"provider": ...}`` hint — confirmed live
against staging on 2026-09-19 (a real batch job was created end-to-end
with ``provider="vertex"``); Bifrost's public docs
(https://docs.getbifrost.ai/integrations/openai-sdk/files-and-batch) don't
even list ``vertex`` in their provider table for Files/Batch, since
Bifrost's ``vertex`` provider maps to real Vertex AI Batch Prediction
under the hood rather than one of Bifrost's own documented Files/Batch
providers — see ``bifrost_batch.py``'s module docstring for the GCS
storage_config/output_folder consequences of that.

Two DIFFERENT model-id shapes are needed because of that same Vertex
passthrough, confirmed empirically against staging:

- ``BATCH_MODEL_NAME`` (``"vertex/gemini-3.1-flash-lite"``, prefixed): goes
  in each JSONL row's ``body.model`` — same ``provider/model`` shape as a
  live chat-completions call, since each row is itself routed through
  Bifrost's normal per-request path once the batch runs.
- ``BATCH_CREATE_MODEL_NAME`` (``"gemini-3.1-flash-lite"``, bare): goes in
  ``batches.create``'s top-level ``extra_body["model"]``, which Bifrost
  passes straight through to Vertex's native ``BatchPredictionJob.model``
  field — Vertex's own API rejects the ``vertex/``-prefixed form there
  with "Invalid Model resource name" (confirmed against staging).
"""

BIFROST_BATCH_PROVIDER = "vertex"

# Same id as utils/classification/config.py::DEFAULT_MODEL_NAME /
# utils/extraction/config.py::GEMINI_CONFIG["model_name"] — kept in sync
# manually, see those modules' docstrings for the "vertex/" prefix reasoning.
# Used in each JSONL row's body.model (see module docstring).
BATCH_MODEL_NAME = "vertex/gemini-3.1-flash-lite"

# Bare (unprefixed) form of BATCH_MODEL_NAME, kept in sync with it manually
# — used only in batches.create's extra_body["model"] (see module
# docstring for why this one specific field needs the unprefixed id).
BATCH_CREATE_MODEL_NAME = "gemini-3.1-flash-lite"

# Mirrors utils/classification/config.py::DEFAULT_GENERATION_CONFIG minus
# "top_k" (no OpenAI chat-completions equivalent — see that module). Field
# names match the OpenAI chat-completions request shape used by the JSONL
# batch input rows (classification_submit.py/extraction_submit.py), not the
# camelCase Vertex-native shape the old direct-Vertex implementation used.
CLASSIFICATION_GENERATION_CONFIG: dict[str, float | int | str] = {
    "temperature": 0.1,
    "top_p": 0.95,
    "max_tokens": 8192,
}

# Mirrors utils/extraction/config.py::GEMINI_CONFIG, same fields as above.
EXTRACTION_GENERATION_CONFIG: dict[str, float | int | str] = {
    "temperature": 0.1,
    "top_p": 0.95,
    "max_tokens": 8192,
}

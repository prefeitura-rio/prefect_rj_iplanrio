"""Model name, provider hint, and generation config for the Bifrost-routed batch path.

Reuses the exact same model id sync mode already calls successfully through
Bifrost (``utils/classification/config.py::DEFAULT_MODEL_NAME`` /
``utils/extraction/config.py::GEMINI_CONFIG["model_name"]``) — no separate
"batch model id" exists; Bifrost's Batch API's ``model`` field takes the
same ``provider/model`` string as a live chat-completions call.

``BIFROST_BATCH_PROVIDER`` is this deployment's provider name for the
Files/Batch API's ``extra_body={"provider": ...}`` hint — inferred from the
``vertex/`` prefix already confirmed live in ``utils/llm.py``'s model ids
(there is no per-call provider hint on the sync path since the model id
prefix alone is enough to route there; the Batch/Files endpoints need it
explicitly since ``files.create`` has no ``model`` argument to infer from).
**Not yet validated against a real Bifrost batch call** — Bifrost's public
docs (https://docs.getbifrost.ai) show a generic ``"gemini"`` provider in
their own example, not ``"vertex"``; this repo's Bifrost deployment names
Google's virtual provider ``vertex`` (confirmed for the synchronous
endpoint against https://docs.dados.rio/ferramentas/opencode-usuario — see
``utils/llm.py``'s docstring), so ``"vertex"`` is the working assumption
here until proven otherwise by an actual batch submission.
"""

BIFROST_BATCH_PROVIDER = "vertex"

# Same id as utils/classification/config.py::DEFAULT_MODEL_NAME /
# utils/extraction/config.py::GEMINI_CONFIG["model_name"] — kept in sync
# manually, see those modules' docstrings for the "vertex/" prefix reasoning.
BATCH_MODEL_NAME = "vertex/gemini-3.1-flash-lite"

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

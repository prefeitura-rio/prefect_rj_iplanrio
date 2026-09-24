"""Shared constants for the NF Agent pipeline.

Holds the Bifrost LLM-gateway configuration used by both the
``classification`` and ``extraction`` code paths.
"""

# --- Bifrost LLM gateway ----------------------------------------------------
# Every LLM call (page classification + NF extraction) is routed through the
# Bifrost gateway instead of talking to the Gemini API directly. Bifrost exposes
# Google models through an OpenAI-compatible endpoint under ``/openai/v1`` (not
# the native Gemini ``generateContent`` protocol) — the ``openai`` SDK is pointed
# at it via ``OpenAI(api_key=..., base_url=BIFROST_BASE_URL)``. See utils/llm.py.
#
# Both of these are just the env var *names* — no values, no defaults. The
# actual values are Infisical secrets, mounted as env vars into the job pod
# via the k8s secret (``prefect-jobs-secrets`` / ``-staging``, see
# prefect.yaml). Resolved at call time in utils/llm.py::build_gemini_model,
# which raises if either is missing — no fallback, nothing hardcoded here.
# GCP ADC is still used for GCS and BigQuery — not Bifrost.
BIFROST_API_KEY_ENV = "BIFROST_API_KEY"
BIFROST_BASE_URL_ENV = "BIFROST_BASE_URL"

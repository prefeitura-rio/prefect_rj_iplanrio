"""Shared constants for the NF Agent pipeline.

Holds the Bifrost LLM-gateway configuration used by both the
``classification`` and ``extraction`` code paths.
"""

# --- Bifrost LLM gateway ----------------------------------------------------
# Every LLM call (page classification + NF extraction) is routed through the
# Bifrost gateway instead of talking to the Gemini API directly. Bifrost exposes
# a Google-GenAI-compatible endpoint under ``/genai``; the ``google-generativeai``
# SDK is pointed at it via ``client_options={"api_endpoint": BIFROST_BASE_URL}``.
#
# Both of these are just the env var *names* — no values, no defaults. The
# actual values are Infisical secrets, mounted as env vars into the job pod
# via the k8s secret (``prefect-jobs-secrets`` / ``-staging``, see
# prefect.yaml). Resolved at call time in utils/llm.py::build_gemini_model,
# which raises if either is missing — no fallback, nothing hardcoded here.
# GCP ADC is still used for GCS and BigQuery — not Bifrost.
BIFROST_API_KEY_ENV = "BIFROST_API_KEY"
BIFROST_BASE_URL_ENV = "BIFROST_BASE_URL"

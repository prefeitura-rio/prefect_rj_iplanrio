"""Shared constants for the NF Agent pipeline.

Holds the Gemini service-account path (still used for GCS/BigQuery credential
discovery) and the Bifrost LLM-gateway configuration used by both the
``classification`` and ``extraction`` code paths.
"""

import os
from pathlib import Path

SERVICE_ACCOUNT_PATH = Path(__file__).parent / "credentials" / "gemini-service-account.json"

# --- Bifrost LLM gateway ----------------------------------------------------
# Every LLM call (page classification + NF extraction) is routed through the
# Bifrost gateway instead of talking to the Gemini API directly. Bifrost exposes
# a Google-GenAI-compatible endpoint under ``/genai``; the ``google-generativeai``
# SDK is pointed at it via ``client_options={"api_endpoint": BIFROST_BASE_URL}``.
#
# Auth is a single Bifrost virtual key, injected as an env var from the
# k8s/Infisical secret (``prefect-jobs-secrets`` / ``-staging``). GCP ADC is
# still used for GCS and BigQuery — not here.
BIFROST_API_KEY_ENV = "BIFROST_API_KEY"

# Fixed per environment; override with the ``BIFROST_BASE_URL`` env var.
# TODO: confirm the production Bifrost host with the platform team.
BIFROST_BASE_URL = os.environ.get("BIFROST_BASE_URL", "https://bifrost.dados.rio/genai")

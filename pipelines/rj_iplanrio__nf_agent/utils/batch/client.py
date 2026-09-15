"""Vertex AI ``google-genai`` client construction for Batch Prediction.

Unlike ``utils/llm.py::build_llm_client`` (Bifrost, OpenAI-compatible
protocol, used by sync mode's classification/extraction calls), this talks
to Vertex AI *directly* — there is no batch-prediction route through
Bifrost. See ``utils/batch/__init__.py`` for why, and for the accepted risk
that this may hit the same ``constraints/vertexai.allowedModels`` Org Policy
block Bifrost's OpenAI-compatible route was built to avoid.

Authentication/target project are resolved by the ``google-genai`` SDK
itself from environment variables when ``project``/``location`` aren't
passed explicitly — ``GOOGLE_CLOUD_PROJECT`` and ``GOOGLE_CLOUD_LOCATION``
(confirmed against ``google.genai._api_client``, which reads exactly those
two names). GCP credentials come from Application Default Credentials (ADC)
— same as ``utils/bigquery.py``/``utils/gcs.py`` — populated by
``inject_credentials_from_env`` in this pipeline's ``flow.py``.
"""

import os

from google import genai

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

# Env var names read directly by google.genai._api_client when project/location
# aren't passed explicitly to genai.Client(...) — documented here (not just
# relied on implicitly) so a missing value fails with a clear message instead
# of a confusing downstream Vertex AI 400/403.
GOOGLE_CLOUD_PROJECT_ENV = "GOOGLE_CLOUD_PROJECT"
GOOGLE_CLOUD_LOCATION_ENV = "GOOGLE_CLOUD_LOCATION"


def build_vertex_batch_client() -> genai.Client:
    """Return a ``google.genai.Client`` targeting Vertex AI (Batch Prediction).

    :returns: A ``genai.Client`` with ``enterprise=True`` (Vertex AI /
        Gemini Enterprise Agent Platform endpoints, not the Gemini
        Developer API).
    :raises RuntimeError: If ``GOOGLE_CLOUD_PROJECT`` or
        ``GOOGLE_CLOUD_LOCATION`` is not set.
    """
    project = os.environ.get(GOOGLE_CLOUD_PROJECT_ENV)
    if not project:
        raise RuntimeError(f"{GOOGLE_CLOUD_PROJECT_ENV} is not set — required to submit Vertex AI batch jobs")

    location = os.environ.get(GOOGLE_CLOUD_LOCATION_ENV)
    if not location:
        raise RuntimeError(f"{GOOGLE_CLOUD_LOCATION_ENV} is not set — required to submit Vertex AI batch jobs")

    logger.warning("Vertex AI batch client configured — project=%s, location=%s", project, location)
    return genai.Client(enterprise=True, project=project, location=location)

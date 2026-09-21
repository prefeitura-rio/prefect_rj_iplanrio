"""Shared JSONL-build/upload/create-batch mechanics for the Bifrost Batch API.

Both ``classification_submit.py`` and ``extraction_submit.py`` build the
same JSONL-line shape (``custom_id`` + an OpenAI chat-completions ``body``,
mirroring the request each already makes synchronously — see
``utils/extraction/api.py``) and go through the same
upload-file-then-create-batch sequence, factored here so neither module
repeats it. Uses the standard ``openai`` Python SDK's ``files``/``batches``
resources (not a bespoke Bifrost client) — see ``utils/llm.py`` for why
Bifrost is reached this way already on the synchronous path.

GCS is required in TWO different shapes across the two calls, both
consequences of Bifrost's ``vertex`` provider mapping to real Vertex AI
Batch Prediction under the hood — a Vertex AI characteristic, not a
Bifrost or pipeline design choice (confirmed against
https://docs.getbifrost.ai/integrations/openai-sdk/files-and-batch, whose
provider table doesn't even list ``vertex``). Both shapes were discovered
empirically by bisecting against staging on 2026-09-19:

- ``files.create``: needs ``extra_body["storage_config"] = {"gcs": {"bucket":
  ..., "prefix": ...}}`` — without it, fails with "gcs_bucket is required
  for Vertex FileUpload".
- ``batches.create``: needs ``extra_body["output_folder"] = {"url": "gs://..."}``
  (a full ``gs://`` URI, not the structured ``storage_config`` shape) —
  without it, fails with "output_folder.url (gs:// prefix) is required for
  Vertex batch API". ``storage_config`` is NOT accepted/needed here.

Both point at a dedicated bucket (``BIFROST_GCS_BUCKET``), separate from
``GCS_BUCKET`` (the pipeline's own PDFs/results bucket) — deliberately, so
the Bifrost gateway's own GCP service account
(``bifrost@<project>.iam.gserviceaccount.com``, distinct from this
pipeline's own credentials) only ever needs GCS write access to this
transport-only bucket, never to the bucket holding actual PDF/NF data.
"""

import json
import os
from dataclasses import dataclass

from openai import OpenAI

from prefect_rj_iplanrio.logging import get_logger

from .model_config import BATCH_CREATE_MODEL_NAME, BIFROST_BATCH_PROVIDER

logger = get_logger(__name__)

# Bifrost's own docs don't state a row/file-size limit for batch input
# (unlike Vertex AI's documented 200k-request cap the old direct-Vertex
# implementation sized against — see row_counting.py). Empirically confirmed
# to be ~100MB regardless of row count — see row_counting.py's module
# docstring — which is why session sizing tracks estimated byte size too.
BATCH_COMPLETION_WINDOW = "24h"

# Prefix under BIFROST_GCS_BUCKET for the vertex provider's transport files
# (JSONL input/output for Batch Prediction). Bifrost manages the contents of
# this prefix itself (uploads, and presumably cleans up); the pipeline never
# reads from or writes to it directly — it's Bifrost's own scratch space,
# not pipeline input/output data.
_VERTEX_STORAGE_PREFIX = "bifrost-batch-io"

# Env var for the dedicated GCS bucket Bifrost's vertex provider writes
# batch transport files to. Deliberately NOT the same as GCS_BUCKET (the
# pipeline's own PDFs/results bucket) — see module docstring for why: it
# keeps the Bifrost gateway's GCP service account's write access scoped to
# a bucket holding no actual PDF/NF data.
BIFROST_GCS_BUCKET_ENV = "BIFROST_GCS_BUCKET"


def _bifrost_gcs_bucket() -> str:
    """Return the dedicated GCS bucket for Bifrost's vertex-provider transport files.

    :raises RuntimeError: If ``BIFROST_GCS_BUCKET`` isn't set.
    """
    gcs_bucket = os.environ.get(BIFROST_GCS_BUCKET_ENV)
    if not gcs_bucket:
        raise RuntimeError(
            f"{BIFROST_GCS_BUCKET_ENV} is not set — required for Bifrost's vertex-provider batch file storage"
        )
    return gcs_bucket


def _vertex_storage_config(gcs_bucket: str) -> dict:
    """Build the ``storage_config`` Bifrost's ``vertex`` provider requires for file uploads.

    :param gcs_bucket: See :func:`_bifrost_gcs_bucket`.
    :returns: ``{"gcs": {"bucket": ..., "prefix": ...}}`` shape — see module
        docstring for why this is required (``files.create`` only) and how
        the shape was confirmed.
    """
    return {"gcs": {"bucket": gcs_bucket, "prefix": _VERTEX_STORAGE_PREFIX}}


def _vertex_output_folder(gcs_bucket: str) -> dict:
    """Build the ``output_folder`` Bifrost's ``vertex`` provider requires for batch creation.

    :param gcs_bucket: See :func:`_bifrost_gcs_bucket`.
    :returns: ``{"url": "gs://..."}`` shape — see module docstring for why
        this (not ``storage_config``) is required on ``batches.create``.
    """
    return {"url": f"gs://{gcs_bucket}/{_VERTEX_STORAGE_PREFIX}/output"}


@dataclass(frozen=True)
class BatchSubmitResult:
    """Outcome of submitting one phase's batch job."""

    bifrost_batch_id: str
    input_file_id: str
    row_count: int


def submit_jsonl_batch(client: OpenAI, rows: list[dict], session_id: str, phase: str) -> BatchSubmitResult:
    """Upload a JSONL input file and create the batch job that processes it.

    :param client: ``openai.OpenAI`` client routed through Bifrost (see
        ``utils/llm.py::build_llm_client``).
    :param rows: One dict per JSONL line — each already shaped as
        ``{"custom_id": ..., "method": "POST", "url": "/v1/chat/completions", "body": {...}}``.
    :param session_id: Current batch session UUID (only used for the
        uploaded file's name and log lines).
    :param phase: ``"classification"`` or ``"extraction"`` (same).
    :returns: The created batch job's identifying info.
    """
    jsonl_bytes = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows).encode("utf-8")
    gcs_bucket = _bifrost_gcs_bucket()

    uploaded_file = client.files.create(
        file=(f"nf-batch-{phase}-{session_id}.jsonl", jsonl_bytes, "application/jsonl"),
        purpose="batch",
        extra_body={"provider": BIFROST_BATCH_PROVIDER, "storage_config": _vertex_storage_config(gcs_bucket)},
    )

    batch = client.batches.create(
        input_file_id=uploaded_file.id,
        endpoint="/v1/chat/completions",
        completion_window=BATCH_COMPLETION_WINDOW,
        extra_body={
            "provider": BIFROST_BATCH_PROVIDER,
            "model": BATCH_CREATE_MODEL_NAME,
            "output_folder": _vertex_output_folder(gcs_bucket),
        },
    )

    logger.warning(
        "Submitted %s batch job %s (%d rows, input_file=%s, session=%s)",
        phase,
        batch.id,
        len(rows),
        uploaded_file.id,
        session_id,
    )

    return BatchSubmitResult(bifrost_batch_id=batch.id, input_file_id=uploaded_file.id, row_count=len(rows))

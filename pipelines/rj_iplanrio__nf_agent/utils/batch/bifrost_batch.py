"""Shared JSONL-build/upload/create-batch mechanics for the Bifrost Batch API.

Both ``classification_submit.py`` and ``extraction_submit.py`` build the
same JSONL-line shape (``custom_id`` + an OpenAI chat-completions ``body``,
mirroring the request each already makes synchronously — see
``utils/extraction/api.py``) and go through the same
upload-file-then-create-batch sequence, factored here so neither module
repeats it. Uses the standard ``openai`` Python SDK's ``files``/``batches``
resources (not a bespoke Bifrost client) — see ``utils/llm.py`` for why
Bifrost is reached this way already on the synchronous path.
"""

import json
from dataclasses import dataclass

from openai import OpenAI

from prefect_rj_iplanrio.logging import get_logger

from .model_config import BATCH_MODEL_NAME, BIFROST_BATCH_PROVIDER

logger = get_logger(__name__)

# Bifrost's own docs don't state a row/file-size limit for batch input
# (unlike Vertex AI's documented 200k-request cap the old direct-Vertex
# implementation sized against — see row_counting.py). Not yet validated
# against a real submission; the first live run in staging is what actually
# proves this.
BATCH_COMPLETION_WINDOW = "24h"


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

    uploaded_file = client.files.create(
        file=(f"nf-batch-{phase}-{session_id}.jsonl", jsonl_bytes, "application/jsonl"),
        purpose="batch",
        extra_body={"provider": BIFROST_BATCH_PROVIDER},
    )

    batch = client.batches.create(
        input_file_id=uploaded_file.id,
        endpoint="/v1/chat/completions",
        completion_window=BATCH_COMPLETION_WINDOW,
        extra_body={"provider": BIFROST_BATCH_PROVIDER, "model": BATCH_MODEL_NAME},
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

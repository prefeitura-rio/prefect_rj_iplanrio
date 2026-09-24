"""Build and submit a Bifrost Batch API job for page classification.

Each JSONL row carries the page's prompt + PDF in Vertex AI's *native*
``generateContent`` request shape (``{"request": {"contents": ...,
"generationConfig": ...}}``) — NOT the OpenAI chat-completions shape the
synchronous path uses. This is required, not stylistic: Bifrost's
``vertex`` provider maps to real Vertex AI Batch Prediction under the hood,
whose native API rejects the OpenAI-style ``{"method": ..., "url": ...,
"body": ...}`` row shape with 'The lines in the specified input JSONL file
must contain the "request" property'. Confirmed empirically against
staging on 2026-09-19: a real batch job with the native shape ran to
``completed`` and returned a valid classification of an inline PDF page.

Identity plumbing (``custom_id``) is preserved as an EXTRA top-level field
on each row — Vertex ignores (and echoes back) fields it doesn't
recognize, and ``result_adapter.py`` keys parsing on exactly that echo, so
page<->response correlation keeps working unchanged. See
``utils/batch/__init__.py`` for the overall architecture and
``custom_id.py`` for the encoding.

``CLASSIFICATION_PROMPT`` is read lazily (function-body, not module-level
import) — ``from ..prompts import CLASSIFICATION_PROMPT`` at module scope
would trigger ``prompts.py``'s ``__getattr__`` immediately at import time,
reading the ``PROMPT_CLASSIFICATION_V*`` Infisical env var before it exists
(only present once the flow actually runs — not during ``prefect deploy`` in
CI). See ``flow.py``'s module docstring for the identical trap this avoids
for ``gemini_classifier.py``.
"""

import base64
from dataclasses import dataclass
from pathlib import Path

import fitz  # PyMuPDF

from prefect_rj_iplanrio.logging import get_logger

from .. import prompts
from ..classification.page_extraction import extract_page_as_bytes
from .bifrost_batch import BatchSubmitResult, submit_jsonl_batch
from .custom_id import encode_custom_id
from .job_tracking import PHASE_CLASSIFICATION, BatchJobEvent, append_job_event
from .model_config import CLASSIFICATION_GENERATION_CONFIG
from .row_counting import BatchSessionSelection

logger = get_logger(__name__)


def _build_classification_request(page_pdf_bytes: bytes) -> dict:
    """Build the Vertex-native ``request`` payload for one page.

    Mirrors the synchronous path's classification call
    (``utils/classification/page_classification.py::_call_gemini_for_classification``)
    content-for-content — same prompt text, same PDF inlined base64 — but in
    Gemini's native ``generateContent`` shape (``contents``/``parts`` with
    ``inlineData``, ``generationConfig`` with camelCase keys), not the
    OpenAI chat-completions shape (``messages``/``content`` with
    ``{"type": "file", ...}``) the sync path uses. See module docstring for
    why the native shape is required here.

    :param page_pdf_bytes: Single-page PDF bytes (already extracted — see
        :func:`build_classification_rows`).
    :returns: A JSON-serializable dict for the JSONL row's ``request``
        field.
    """
    page_b64 = base64.b64encode(page_pdf_bytes).decode("utf-8")
    return {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": prompts.CLASSIFICATION_PROMPT},
                    {"inlineData": {"mimeType": "application/pdf", "data": page_b64}},
                ],
            }
        ],
        "generationConfig": {
            "temperature": CLASSIFICATION_GENERATION_CONFIG["temperature"],
            "topP": CLASSIFICATION_GENERATION_CONFIG["top_p"],
            "maxOutputTokens": CLASSIFICATION_GENERATION_CONFIG["max_tokens"],
        },
    }


@dataclass(frozen=True)
class ClassificationSubmitResult:
    """Outcome of submitting a classification batch job for one session."""

    bifrost_batch_id: str
    input_file_id: str
    row_count: int
    selection: BatchSessionSelection


def build_classification_rows(pdf_paths: dict[str, Path], selection: BatchSessionSelection) -> list[dict]:
    """Render every page of every selected PDF and build classification JSONL rows.

    :param pdf_paths: Mapping of pdf_name -> local downloaded path (superset
        of ``selection.selected_pdf_names`` is fine; extras are ignored).
    :param selection: Output of :func:`row_counting.select_pdfs_within_row_budget`
        — determines which PDFs (and therefore which pages) are included.
    :returns: List of JSONL row dicts ready for :func:`bifrost_batch.submit_jsonl_batch`.
    """
    rows: list[dict] = []
    for pdf_name in selection.selected_pdf_names:
        pdf_path = pdf_paths[pdf_name]
        doc = fitz.open(str(pdf_path))
        total_pages = doc.page_count
        doc.close()

        for page_number in range(1, total_pages + 1):
            page_pdf_bytes = extract_page_as_bytes(pdf_path, page_number - 1, as_pdf=True)
            custom_id = encode_custom_id(pdf_name, page_number)
            rows.append(
                {
                    # Vertex-native row shape — see module docstring.
                    # custom_id is extra (not part of Vertex's contract),
                    # echoed back untouched on each output row so
                    # result_adapter can correlate responses to pages.
                    "custom_id": custom_id,
                    "request": _build_classification_request(page_pdf_bytes),
                }
            )

    return rows


def submit_classification_job(
    client,
    nf_batch_jobs_table: str,
    pdf_paths: dict[str, Path],
    selection: BatchSessionSelection,
    session_id: str,
) -> ClassificationSubmitResult:
    """Build the classification JSONL input and submit the Bifrost batch job.

    :param client: ``openai.OpenAI`` client routed through Bifrost (see
        ``utils/llm.py::build_llm_client``).
    :param nf_batch_jobs_table: Fully-qualified ``nf_batch_jobs`` tracking table.
    :param pdf_paths: Mapping of pdf_name -> local downloaded path for every
        candidate PDF (see ``selection`` for which subset is actually used).
    :param selection: Output of ``row_counting.select_pdfs_within_row_budget``.
    :param session_id: Current batch session UUID.
    :returns: The submitted job's identifying info, recorded in
        ``nf_batch_jobs`` before returning.
    """
    rows = build_classification_rows(pdf_paths, selection)
    result: BatchSubmitResult = submit_jsonl_batch(client, rows, session_id, PHASE_CLASSIFICATION)

    append_job_event(
        nf_batch_jobs_table,
        BatchJobEvent(
            session_id=session_id,
            phase=PHASE_CLASSIFICATION,
            bifrost_batch_id=result.bifrost_batch_id,
            state="validating",
            input_file_id=result.input_file_id,
            output_file_id=None,
            row_count=result.row_count,
        ),
    )

    return ClassificationSubmitResult(
        bifrost_batch_id=result.bifrost_batch_id,
        input_file_id=result.input_file_id,
        row_count=result.row_count,
        selection=selection,
    )

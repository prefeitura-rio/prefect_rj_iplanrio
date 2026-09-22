"""Build and submit a Bifrost Batch API job for NF extraction.

Mirrors the synchronous path's single-page extraction call
(``utils/extraction/api.py::extract_from_pdf_bytes`` with
``extractor.batch_size == 1`` — always true in production, see
``extraction/auth.py``): one row per NF-classified page, with the
classification hint injected into the prompt exactly as
``utils/extraction/prompt.py::build_prompt_with_hint`` does. Only reachable
after the corresponding classification batch job has succeeded — see
``poll.py`` for the phase transition that calls this module.

``EXTRACTION_PROMPT`` is read lazily (function-body, not module-level
import) for the same reason ``classification_submit.py`` reads
``CLASSIFICATION_PROMPT`` lazily — see that module's docstring.
"""

import base64
from dataclasses import dataclass
from pathlib import Path

from prefect_rj_iplanrio.logging import get_logger

from .. import prompts
from ..classification.page_extraction import extract_page_as_bytes
from ..extraction.prompt import build_prompt_with_hint
from .bifrost_batch import BatchSubmitResult, submit_jsonl_batch
from .custom_id import encode_custom_id
from .job_tracking import PHASE_EXTRACTION, BatchJobEvent, append_job_event
from .model_config import EXTRACTION_GENERATION_CONFIG

logger = get_logger(__name__)


@dataclass(frozen=True)
class ExtractionCandidate:
    """One page selected for extraction, with its classification hint."""

    pdf_name: str
    page_number: int
    classification_hint: str | None  # e.g. "NFS-e", or None if unknown


def _resolved_extraction_prompt(classification_hint: str | None) -> str:
    """Build the extraction prompt text with the classification hint substituted.

    Delegates to the same ``build_prompt_with_hint`` helper the synchronous
    path uses, via a throwaway object exposing just the one attribute that
    function reads (``extraction_prompt``) — avoids constructing a full
    ``NFExtractor`` (which would build a Bifrost client this path doesn't
    need one instance of) just to reach this pure string-formatting helper.

    :param classification_hint: Document type identified by the
        classification batch job for this page, or ``None``.
    :returns: Prompt text with the ``{classification_hint}`` placeholder resolved.
    """

    class _PromptHolder:
        extraction_prompt = prompts.EXTRACTION_PROMPT

    return build_prompt_with_hint(_PromptHolder(), classification_hint)


def _build_extraction_request(page_pdf_bytes: bytes, classification_hint: str | None) -> dict:
    """Build the Vertex-native ``request`` payload for one extraction candidate page.

    Same content as the synchronous path's extraction call — same resolved
    prompt (with classification hint), same PDF inlined base64 — but in
    Gemini's native ``generateContent`` shape, not the OpenAI
    chat-completions shape. See ``classification_submit.py``'s module
    docstring for why the native shape is required on this path.

    :param page_pdf_bytes: Single-page PDF bytes.
    :param classification_hint: Document type identified during
        classification (injected into the prompt), or ``None``.
    :returns: A JSON-serializable dict for the JSONL row's ``request``
        field.
    """
    page_b64 = base64.b64encode(page_pdf_bytes).decode("utf-8")
    return {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": _resolved_extraction_prompt(classification_hint)},
                    {"inlineData": {"mimeType": "application/pdf", "data": page_b64}},
                ],
            }
        ],
        "generationConfig": {
            "temperature": EXTRACTION_GENERATION_CONFIG["temperature"],
            "topP": EXTRACTION_GENERATION_CONFIG["top_p"],
            "maxOutputTokens": EXTRACTION_GENERATION_CONFIG["max_tokens"],
        },
    }


@dataclass(frozen=True)
class ExtractionSubmitResult:
    """Outcome of submitting an extraction batch job for one session."""

    bifrost_batch_id: str
    input_file_id: str
    row_count: int


def build_extraction_rows(
    pdf_paths: dict[str, Path],
    candidates: list[ExtractionCandidate],
    session_id: str,
) -> list[dict]:
    """Render every NF-classified page and build extraction JSONL rows.

    :param pdf_paths: Mapping of pdf_name -> local downloaded path for every
        PDF referenced by ``candidates``.
    :param candidates: Pages to extract, one row each — see
        :class:`ExtractionCandidate`.
    :param session_id: Current batch session UUID (encoded into every row's
        ``custom_id``).
    :returns: List of JSONL row dicts ready for :func:`bifrost_batch.submit_jsonl_batch`.
    """
    rows: list[dict] = []
    for candidate in candidates:
        pdf_path = pdf_paths[candidate.pdf_name]
        page_pdf_bytes = extract_page_as_bytes(pdf_path, candidate.page_number - 1, as_pdf=True)
        custom_id = encode_custom_id(PHASE_EXTRACTION, session_id, candidate.pdf_name, candidate.page_number)
        rows.append(
            {
                # Vertex-native row shape — see classification_submit.py's
                # module docstring; custom_id is extra, echoed back untouched.
                "custom_id": custom_id,
                "request": _build_extraction_request(page_pdf_bytes, candidate.classification_hint),
            }
        )

    return rows


def submit_extraction_job(
    client,
    nf_batch_jobs_table: str,
    pdf_paths: dict[str, Path],
    candidates: list[ExtractionCandidate],
    session_id: str,
) -> ExtractionSubmitResult:
    """Build the extraction JSONL input and submit the Bifrost batch job.

    :param client: ``openai.OpenAI`` client routed through Bifrost (see
        ``utils/llm.py::build_llm_client``).
    :param nf_batch_jobs_table: Fully-qualified ``nf_batch_jobs`` tracking table.
    :param pdf_paths: Mapping of pdf_name -> local downloaded path.
    :param candidates: NF-classified pages to extract (from the
        classification job's output — see ``poll.py``).
    :param session_id: Current batch session UUID.
    :returns: The submitted job's identifying info, recorded in
        ``nf_batch_jobs`` before returning.
    """
    rows = build_extraction_rows(pdf_paths, candidates, session_id)
    result: BatchSubmitResult = submit_jsonl_batch(client, rows, session_id, PHASE_EXTRACTION)

    append_job_event(
        nf_batch_jobs_table,
        BatchJobEvent(
            session_id=session_id,
            phase=PHASE_EXTRACTION,
            bifrost_batch_id=result.bifrost_batch_id,
            state="validating",
            input_file_id=result.input_file_id,
            output_file_id=None,
            row_count=result.row_count,
        ),
    )

    return ExtractionSubmitResult(
        bifrost_batch_id=result.bifrost_batch_id,
        input_file_id=result.input_file_id,
        row_count=result.row_count,
    )

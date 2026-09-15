"""Build and submit a Vertex AI Batch Prediction job for NF extraction.

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

from dataclasses import dataclass
from pathlib import Path

from google.cloud import bigquery

from prefect_rj_iplanrio.logging import get_logger

from .. import prompts
from ..extraction.prompt import build_prompt_with_hint
from ..gcs import GCSDownloader
from .client import build_vertex_batch_client
from .job_tracking import PHASE_EXTRACTION, BatchJobEvent, append_job_event
from .model_config import BATCH_MODEL_NAME, EXTRACTION_GENERATION_CONFIG
from .scratch_gcs import upload_page_pdf

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
    ``NFExtractor`` (which would build a Bifrost client this path never
    uses) just to reach this pure string-formatting helper.

    :param classification_hint: Document type identified by the
        classification batch job for this page, or ``None``.
    :returns: Prompt text with the ``{classification_hint}`` placeholder resolved.
    """

    class _PromptHolder:
        extraction_prompt = prompts.EXTRACTION_PROMPT

    return build_prompt_with_hint(_PromptHolder(), classification_hint)


def _build_extraction_request(page_uri: str, classification_hint: str | None) -> dict:
    """Build the ``request`` column payload for one extraction candidate page.

    :param page_uri: ``gs://...`` URI of the single-page PDF to extract from.
    :param classification_hint: Document type identified during
        classification (injected into the prompt), or ``None``.
    :returns: A JSON-serializable dict matching the batch input schema's
        ``request`` column.
    """
    return {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": _resolved_extraction_prompt(classification_hint)},
                    {"fileData": {"fileUri": page_uri, "mimeType": "application/pdf"}},
                ],
            }
        ],
        "generationConfig": {
            "temperature": EXTRACTION_GENERATION_CONFIG["temperature"],
            "topP": EXTRACTION_GENERATION_CONFIG["top_p"],
            "maxOutputTokens": EXTRACTION_GENERATION_CONFIG["max_output_tokens"],
            "responseMimeType": EXTRACTION_GENERATION_CONFIG["response_mime_type"],
        },
    }


@dataclass(frozen=True)
class ExtractionSubmitResult:
    """Outcome of submitting an extraction batch job for one session."""

    vertex_job_name: str
    input_table: str
    output_table: str
    row_count: int


def _load_extraction_input_table(bq_client: bigquery.Client, table_ref: str, rows: list[dict]) -> None:
    """Load extraction input rows into ``table_ref`` (create/replace).

    :param bq_client: Authenticated BigQuery client.
    :param table_ref: Fully-qualified destination table.
    :param rows: Rows to load — see :func:`build_extraction_rows`.
    """
    schema = [
        bigquery.SchemaField("pdf_name", "STRING"),
        bigquery.SchemaField("page_number", "INT64"),
        bigquery.SchemaField("session_id", "STRING"),
        bigquery.SchemaField("request", "JSON"),
    ]
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    load_job = bq_client.load_table_from_json(rows, table_ref, job_config=job_config)
    load_job.result()
    logger.warning("Loaded %d extraction input rows into %s", len(rows), table_ref)


def build_extraction_rows(
    gcs_downloader: GCSDownloader,
    pdf_paths: dict[str, Path],
    candidates: list[ExtractionCandidate],
    session_id: str,
) -> list[dict]:
    """Render every NF-classified page and build extraction input rows.

    :param gcs_downloader: Used only for its already-resolved ``.bucket``.
    :param pdf_paths: Mapping of pdf_name -> local downloaded path for every
        PDF referenced by ``candidates``.
    :param candidates: Pages to extract, one row each — see
        :class:`ExtractionCandidate`.
    :param session_id: Current batch session UUID (scopes the scratch prefix).
    :returns: List of row dicts ready for :func:`_load_extraction_input_table`.
    """
    rows: list[dict] = []
    for candidate in candidates:
        pdf_path = pdf_paths[candidate.pdf_name]
        page_uri = upload_page_pdf(
            bucket=gcs_downloader.bucket,
            pdf_path=pdf_path,
            page_number=candidate.page_number,
            session_id=session_id,
            phase=PHASE_EXTRACTION,
        )
        rows.append(
            {
                "pdf_name": candidate.pdf_name,
                "page_number": candidate.page_number,
                "session_id": session_id,
                "request": _build_extraction_request(page_uri, candidate.classification_hint),
            }
        )

    return rows


def submit_extraction_job(
    bq_project: str,
    bq_dataset: str,
    nf_batch_jobs_table: str,
    gcs_downloader: GCSDownloader,
    pdf_paths: dict[str, Path],
    candidates: list[ExtractionCandidate],
    session_id: str,
) -> ExtractionSubmitResult:
    """Build the extraction input table and submit the Vertex AI batch job.

    :param bq_project: GCP project hosting the batch input/output tables.
    :param bq_dataset: BigQuery dataset hosting the batch input/output tables.
    :param nf_batch_jobs_table: Fully-qualified ``nf_batch_jobs`` tracking table.
    :param gcs_downloader: Used to resolve the scratch bucket.
    :param pdf_paths: Mapping of pdf_name -> local downloaded path.
    :param candidates: NF-classified pages to extract (from the
        classification job's output — see ``poll.py``).
    :param session_id: Current batch session UUID.
    :returns: The submitted job's identifying info, recorded in
        ``nf_batch_jobs`` before returning.
    """
    bq_client = bigquery.Client(project=bq_project)
    input_table = f"{bq_project}.{bq_dataset}.nf_batch_extraction_input_{session_id}"
    output_table = f"{bq_project}.{bq_dataset}.nf_batch_extraction_output_{session_id}"

    rows = build_extraction_rows(gcs_downloader, pdf_paths, candidates, session_id)
    _load_extraction_input_table(bq_client, input_table, rows)

    client = build_vertex_batch_client()
    job = client.batches.create(
        model=BATCH_MODEL_NAME,
        src=f"bq://{input_table}",
        config={"dest": f"bq://{output_table}"},
    )
    logger.warning(
        "Submitted extraction batch job %s (%d rows, input=%s, output=%s)",
        job.name,
        len(rows),
        input_table,
        output_table,
    )

    append_job_event(
        nf_batch_jobs_table,
        BatchJobEvent(
            session_id=session_id,
            phase=PHASE_EXTRACTION,
            vertex_job_name=job.name,
            state=str(job.state),
            input_table=f"bq://{input_table}",
            output_table=f"bq://{output_table}",
            row_count=len(rows),
        ),
    )

    return ExtractionSubmitResult(
        vertex_job_name=job.name,
        input_table=input_table,
        output_table=output_table,
        row_count=len(rows),
    )

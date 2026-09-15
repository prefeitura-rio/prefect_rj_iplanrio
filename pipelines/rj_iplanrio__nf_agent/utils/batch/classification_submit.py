"""Build and submit a Vertex AI Batch Prediction job for page classification.

Mirrors the synchronous path's classification call
(``utils/classification/page_classification.py::_call_gemini_for_classification``)
in prompt/generation-config terms, but every page is a row in a BigQuery
input table instead of an individual Bifrost/OpenAI-protocol API call. See
``utils/batch/__init__.py`` for the overall architecture.

``CLASSIFICATION_PROMPT`` is read lazily (function-body, not module-level
import) — ``from ..prompts import CLASSIFICATION_PROMPT`` at module scope
would trigger ``prompts.py``'s ``__getattr__`` immediately at import time,
reading the ``PROMPT_CLASSIFICATION_V*`` Infisical env var before it exists
(only present once the flow actually runs — not during ``prefect deploy`` in
CI). See ``flow.py``'s module docstring for the identical trap this avoids
for ``gemini_classifier.py``.
"""

from dataclasses import dataclass
from pathlib import Path

import fitz  # PyMuPDF
from google.cloud import bigquery

from prefect_rj_iplanrio.logging import get_logger

from .. import prompts
from ..gcs import GCSDownloader
from .client import build_vertex_batch_client
from .job_tracking import PHASE_CLASSIFICATION, BatchJobEvent, append_job_event
from .model_config import BATCH_MODEL_NAME, CLASSIFICATION_GENERATION_CONFIG
from .row_counting import BatchSessionSelection
from .scratch_gcs import upload_page_pdf

logger = get_logger(__name__)


def _build_classification_request(page_uri: str) -> dict:
    """Build the ``request`` column payload (a ``GenerateContentRequest``-shaped dict).

    :param page_uri: ``gs://...`` URI of the single-page PDF to classify
        (already uploaded to the scratch prefix — see ``scratch_gcs.py``).
    :returns: A JSON-serializable dict matching the batch input schema's
        ``request`` column (see the Vertex AI Batch Prediction for BigQuery
        docs — ``contents``/``role``/``parts``/``generationConfig``, camelCase).
    """
    return {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": prompts.CLASSIFICATION_PROMPT},
                    {"fileData": {"fileUri": page_uri, "mimeType": "application/pdf"}},
                ],
            }
        ],
        "generationConfig": {
            "temperature": CLASSIFICATION_GENERATION_CONFIG["temperature"],
            "topP": CLASSIFICATION_GENERATION_CONFIG["top_p"],
            "maxOutputTokens": CLASSIFICATION_GENERATION_CONFIG["max_output_tokens"],
            "responseMimeType": CLASSIFICATION_GENERATION_CONFIG["response_mime_type"],
        },
    }


@dataclass(frozen=True)
class ClassificationSubmitResult:
    """Outcome of submitting a classification batch job for one session."""

    vertex_job_name: str
    input_table: str
    output_table: str
    row_count: int
    selection: BatchSessionSelection


def _load_classification_input_table(
    bq_client: bigquery.Client,
    table_ref: str,
    rows: list[dict],
) -> None:
    """Load classification input rows into ``table_ref`` (create/replace).

    :param bq_client: Authenticated BigQuery client.
    :param table_ref: Fully-qualified destination table, e.g.
        ``'project.dataset.nf_batch_classification_input_<session_id>'``.
    :param rows: Rows to load — see :func:`build_classification_rows`.
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
    logger.warning("Loaded %d classification input rows into %s", len(rows), table_ref)


def build_classification_rows(
    gcs_downloader: GCSDownloader,
    pdf_paths: dict[str, Path],
    selection: BatchSessionSelection,
    session_id: str,
) -> list[dict]:
    """Render every page of every selected PDF and build classification input rows.

    :param gcs_downloader: Used only for its already-resolved ``.bucket`` —
        page uploads go through :func:`scratch_gcs.upload_page_pdf`.
    :param pdf_paths: Mapping of pdf_name -> local downloaded path (superset
        of ``selection.selected_pdf_names`` is fine; extras are ignored).
    :param selection: Output of :func:`row_counting.select_pdfs_within_row_budget`
        — determines which PDFs (and therefore which pages) are included.
    :param session_id: Current batch session UUID (scopes the scratch prefix).
    :returns: List of row dicts ready for :func:`_load_classification_input_table`.
    """
    rows: list[dict] = []
    for pdf_name in selection.selected_pdf_names:
        pdf_path = pdf_paths[pdf_name]
        doc = fitz.open(str(pdf_path))
        total_pages = doc.page_count
        doc.close()

        for page_number in range(1, total_pages + 1):
            page_uri = upload_page_pdf(
                bucket=gcs_downloader.bucket,
                pdf_path=pdf_path,
                page_number=page_number,
                session_id=session_id,
                phase=PHASE_CLASSIFICATION,
            )
            rows.append(
                {
                    "pdf_name": pdf_name,
                    "page_number": page_number,
                    "session_id": session_id,
                    "request": _build_classification_request(page_uri),
                }
            )

    return rows


def submit_classification_job(
    bq_project: str,
    bq_dataset: str,
    nf_batch_jobs_table: str,
    gcs_downloader: GCSDownloader,
    pdf_paths: dict[str, Path],
    selection: BatchSessionSelection,
    session_id: str,
) -> ClassificationSubmitResult:
    """Build the classification input table and submit the Vertex AI batch job.

    :param bq_project: GCP project hosting the batch input/output tables.
    :param bq_dataset: BigQuery dataset hosting the batch input/output tables.
    :param nf_batch_jobs_table: Fully-qualified ``nf_batch_jobs`` tracking table.
    :param gcs_downloader: Used to resolve the scratch bucket.
    :param pdf_paths: Mapping of pdf_name -> local downloaded path for every
        candidate PDF (see ``selection`` for which subset is actually used).
    :param selection: Output of ``row_counting.select_pdfs_within_row_budget``.
    :param session_id: Current batch session UUID.
    :returns: The submitted job's identifying info, recorded in
        ``nf_batch_jobs`` before returning.
    """
    bq_client = bigquery.Client(project=bq_project)
    input_table = f"{bq_project}.{bq_dataset}.nf_batch_classification_input_{session_id}"
    output_table = f"{bq_project}.{bq_dataset}.nf_batch_classification_output_{session_id}"

    rows = build_classification_rows(gcs_downloader, pdf_paths, selection, session_id)
    _load_classification_input_table(bq_client, input_table, rows)

    client = build_vertex_batch_client()
    job = client.batches.create(
        model=BATCH_MODEL_NAME,
        src=f"bq://{input_table}",
        config={"dest": f"bq://{output_table}"},
    )
    logger.warning(
        "Submitted classification batch job %s (%d rows, input=%s, output=%s)",
        job.name,
        len(rows),
        input_table,
        output_table,
    )

    append_job_event(
        nf_batch_jobs_table,
        BatchJobEvent(
            session_id=session_id,
            phase=PHASE_CLASSIFICATION,
            vertex_job_name=job.name,
            state=str(job.state),
            input_table=f"bq://{input_table}",
            output_table=f"bq://{output_table}",
            row_count=len(rows),
        ),
    )

    return ClassificationSubmitResult(
        vertex_job_name=job.name,
        input_table=input_table,
        output_table=output_table,
        row_count=len(rows),
        selection=selection,
    )

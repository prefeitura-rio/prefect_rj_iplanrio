"""Scratch-space GCS uploads for Vertex AI Batch Prediction multimodal input.

Vertex AI Batch Prediction (BigQuery-sourced) requires ``fileData.fileUri``
references to point at a Cloud Storage object — inline base64 (what the
synchronous/Bifrost path sends per-request, see ``utils/extraction/api.py``)
is not an option here, since the whole point of a ``request`` column is to
stay small enough for BigQuery. Each page that will be classified or
extracted is therefore rendered once and uploaded to a scratch prefix before
its batch-input row is built.

Layout: ``gs://{bucket}/nf-batch-scratch/{session_id}/{phase}/{pdf_stem}__p{page:04d}.pdf``.
Cleanup is intentionally NOT done by this module — recommended approach is a
bucket lifecycle rule expiring the ``nf-batch-scratch/`` prefix after a few
days, since submit and poll (which would need to coordinate a delete) run in
separate flow-runs, possibly hours apart, and a completed session should not
block on cleanup succeeding.
"""

from pathlib import Path

from google.cloud import storage

from prefect_rj_iplanrio.logging import get_logger

from ..classification.page_extraction import extract_page_as_bytes

logger = get_logger(__name__)

SCRATCH_PREFIX = "nf-batch-scratch"


def scratch_blob_path(session_id: str, phase: str, pdf_stem: str, page_number: int) -> str:
    """Build the scratch GCS blob path for a single rendered page.

    :param session_id: Batch session UUID (keeps concurrent/repeated
        sessions from colliding on the same scratch objects).
    :param phase: ``"classification"`` or ``"extraction"``.
    :param pdf_stem: Source PDF filename without extension.
    :param page_number: 1-indexed page number within the source PDF.
    :returns: Blob path (no ``gs://bucket/`` prefix — see
        :func:`upload_page_pdf` for the full URI).
    """
    return f"{SCRATCH_PREFIX}/{session_id}/{phase}/{pdf_stem}__p{page_number:04d}.pdf"


def upload_page_pdf(
    bucket: storage.Bucket,
    pdf_path: Path,
    page_number: int,
    session_id: str,
    phase: str,
) -> str:
    """Render one page as a single-page PDF and upload it to the scratch prefix.

    :param bucket: Target GCS bucket (already resolved — see
        ``utils.gcs.GCSDownloader.bucket`` for how the pipeline normally
        gets one).
    :param pdf_path: Path to the local (already downloaded) source PDF.
    :param page_number: 1-indexed page number to render.
    :param session_id: Current batch session UUID.
    :param phase: ``"classification"`` or ``"extraction"`` — determines the
        scratch subprefix (see :func:`scratch_blob_path`).
    :returns: The uploaded object's ``gs://bucket/...`` URI, ready to use as
        a ``fileData.fileUri`` value.
    """
    # extract_page_as_bytes takes a 0-indexed page number; this module's
    # callers (and the rest of the pipeline) use 1-indexed page numbers.
    page_bytes = extract_page_as_bytes(pdf_path, page_number - 1, as_pdf=True)

    blob_path = scratch_blob_path(session_id, phase, pdf_path.stem, page_number)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(page_bytes, content_type="application/pdf")

    return f"gs://{bucket.name}/{blob_path}"

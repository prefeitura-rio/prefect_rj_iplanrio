"""
Utility helpers for the NF Agent Prefect pipeline.

All imports of ``run_poc`` and other agent-nf-validator modules are deferred
to the function body so this module stays importable during ``prefect deploy``
in CI, which only synchronises prefect/prefect-docker — not the full
agent-nf-validator install.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------


def new_or_continued_session(session_id: str | None) -> str:
    """
    Return an existing session ID or create a fresh one.

    :param session_id: Existing session UUID, or ``None`` to start a new session.
    :returns: Session UUID string (existing or newly generated).
    """
    import uuid

    if session_id:
        logger.info("Continuing session: %s", session_id)
        return session_id

    session_id = str(uuid.uuid4())
    logger.info("New session started: %s", session_id)
    return session_id


def pending_in_session(
    max_pdfs: int | None, total_in_session: int
) -> tuple[int | None, int | None]:
    """
    Compute how many PDFs/docs are still allowed before ``max_pdfs`` is reached.

    :param max_pdfs: Session cap on the total number of PDFs, or ``None`` for uncapped.
    :param total_in_session: PDFs already processed in the current session.
    :returns: ``(pending_pdfs, pending_docs)`` tuple, or ``(None, None)`` when uncapped.
    """
    if max_pdfs is None:
        return None, None
    pending_pdfs = max(0, max_pdfs - total_in_session)
    pending_docs = pending_pdfs  # estimate — exact doc count per PDF is unknown upfront
    return pending_pdfs, pending_docs


# ---------------------------------------------------------------------------
# Logging / reporting
# ---------------------------------------------------------------------------


def log_batch_summary(
    *,
    session_id: str,
    pdfs_processed: int,
    pdfs_failed: int,
    docs_processed: int,
    docs_failed: int,
    duration_seconds: float,
    avg_sec_per_pdf: float,
    avg_sec_per_doc: float,
    pending_pdfs: int | None,
    pending_docs: int | None,
    total_in_session: int,
    max_pdfs: int | None,
) -> None:
    """
    Emit a structured batch-summary log entry at INFO level.

    :param session_id: Current session UUID.
    :param pdfs_processed: Number of PDFs successfully processed.
    :param pdfs_failed: Number of PDFs that failed processing.
    :param docs_processed: Number of documents successfully extracted.
    :param docs_failed: Number of documents that failed extraction.
    :param duration_seconds: Wall-clock duration of the batch in seconds.
    :param avg_sec_per_pdf: Average seconds spent per PDF.
    :param avg_sec_per_doc: Average seconds spent per document.
    :param pending_pdfs: PDFs still pending in the session, or ``None`` if uncapped.
    :param pending_docs: Docs still pending in the session, or ``None`` if uncapped.
    :param total_in_session: Cumulative PDFs processed across the whole session.
    :param max_pdfs: Session cap on total PDFs, or ``None`` for uncapped.
    """
    lines = [
        "── Batch summary ──────────────────────",
        f"  Session:        {session_id}",
        f"  Processed:      {pdfs_processed} PDFs / {docs_processed} docs",
        f"  Failed:         {pdfs_failed} PDFs / {docs_failed} docs",
        f"  Duration:       {duration_seconds / 60:.1f} min",
        f"  Avg / PDF:      {avg_sec_per_pdf:.1f} sec",
        f"  Avg / doc:      {avg_sec_per_doc:.1f} sec",
    ]
    if pending_pdfs is not None:
        lines.append(f"  Pending in session: {pending_pdfs} PDFs / {pending_docs} docs")
        if avg_sec_per_pdf > 0 and pending_pdfs > 0:
            est_remaining_min = round(pending_pdfs * avg_sec_per_pdf / 60, 1)
            lines.append(f"  Est. remaining: ~{est_remaining_min} min")
    lines.append(f"  Cumulative:     {total_in_session} / {max_pdfs if max_pdfs else '∞'} PDFs")
    lines.append("──────────────────────────────────────")
    logger.info("\n".join(lines))


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------


def parse_project_and_dataset(bq_table_ref: str | None) -> tuple[str | None, str | None]:
    """
    Split a ``project.dataset.table`` reference into its components.

    :param bq_table_ref: Fully-qualified BigQuery table reference, or ``None``.
    :returns: ``(project, dataset)`` tuple, or ``(None, None)`` if the reference
        is absent or malformed (fewer than three dot-separated parts).
    """
    parts = (bq_table_ref or "").split(".")
    if len(parts) < 3:
        return None, None
    return parts[0], parts[1]


def write_run_summary(
    *,
    pipeline_runs_table: str,
    bq_status_table: str | None,
    session_id: str,
    started_at: Any,
    finished_at: Any,
    duration_seconds: float,
    pdfs_processed: int,
    docs_processed: int,
    pdfs_failed: int,
    docs_failed: int,
    pending_pdfs: int | None,
    pending_docs: int | None,
    avg_sec_per_pdf: float,
    avg_sec_per_doc: float,
    batch_size: int,
    workers: int,
    requests_per_minute: int,
    max_concurrent: int,
    force_reprocess: bool,
    timing_stats: dict[str, Any],
) -> None:
    """
    Write a run-summary row to BigQuery via ``BigQueryWriter``.

    :param pipeline_runs_table: Fully-qualified BQ table for run summaries.
    :param bq_status_table: Fully-qualified BQ status table (used to derive project/dataset).
    :param session_id: Current session UUID.
    :param started_at: Flow start timestamp (``datetime``).
    :param finished_at: Flow finish timestamp (``datetime``).
    :param duration_seconds: Total wall-clock duration in seconds.
    :param pdfs_processed: Successfully processed PDF count.
    :param docs_processed: Successfully extracted document count.
    :param pdfs_failed: Failed PDF count.
    :param docs_failed: Failed document count.
    :param pending_pdfs: Remaining PDFs in session, or ``None`` if uncapped.
    :param pending_docs: Remaining docs in session, or ``None`` if uncapped.
    :param avg_sec_per_pdf: Average seconds per PDF.
    :param avg_sec_per_doc: Average seconds per document.
    :param batch_size: Configured batch size.
    :param workers: Configured worker count.
    :param requests_per_minute: Configured API rate limit.
    :param max_concurrent: Configured max concurrent requests.
    :param force_reprocess: Whether force-reprocess mode was active.
    :param timing_stats: Detailed per-stage timing statistics from the pipeline.
    """
    from run_poc.bigquery_writer import BigQueryWriter  # noqa: PLC0415

    bq_project, bq_dataset = parse_project_and_dataset(bq_status_table)
    if not (bq_project and bq_dataset):
        return

    BigQueryWriter(project_id=bq_project, dataset_id=bq_dataset).write_run_summary(
        pipeline_runs_table=pipeline_runs_table,
        row={
            "session_id": session_id,
            "flow_run_id": str(get_flow_run_id()),
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": duration_seconds,
            "pdfs_processed": pdfs_processed,
            "docs_processed": docs_processed,
            "pdfs_failed": pdfs_failed,
            "docs_failed": docs_failed,
            "pending_pdfs": pending_pdfs or 0,
            "pending_docs": pending_docs or 0,
            "avg_sec_per_pdf": avg_sec_per_pdf,
            "avg_sec_per_doc": avg_sec_per_doc,
            "batch_size": batch_size,
            "workers": workers,
            "requests_per_minute": requests_per_minute,
            "max_concurrent": max_concurrent,
            # Wall-clock totals (real elapsed time per stage)
            "wall_sec_download_gcs": timing_stats.get("wall_sec_download_gcs"),
            "wall_sec_core": timing_stats.get("wall_sec_core"),
            "wall_sec_escrita": timing_stats.get("wall_sec_escrita"),
            # Per-PDF/Per-page/Per-doc CPU average (from individual timers)
            "avg_cpu_sec_preprocess_por_pdf": timing_stats.get("avg_cpu_sec_preprocess_por_pdf"),
            "avg_cpu_sec_classificacao_por_pagina": timing_stats.get("avg_cpu_sec_classificacao_por_pagina"),
            "avg_cpu_sec_extracao_por_declaracao": timing_stats.get("avg_cpu_sec_extracao_por_declaracao"),
            "avg_cpu_sec_validacao_por_pdf": timing_stats.get("avg_cpu_sec_validacao_por_pdf"),
            "force_reprocess": force_reprocess,
        },
    )


# ---------------------------------------------------------------------------
# Prefect runtime helpers
# ---------------------------------------------------------------------------


def get_flow_run_id() -> str | None:
    """
    Return the current Prefect flow run ID, or ``None`` when called outside a flow context.

    :returns: Flow run UUID string, or ``None`` if not inside a Prefect flow run.
    """
    try:
        from prefect.runtime import flow_run as current_run  # noqa: PLC0415

        return current_run.id
    except Exception:
        return None


def get_current_deployment_id(flow_run_id: str) -> str | None:
    """
    Fetch the deployment ID associated with the given flow run.

    :param flow_run_id: UUID of the Prefect flow run to look up.
    :returns: Deployment UUID string, or ``None`` if the flow run has no deployment.
    """
    from prefect import get_client  # noqa: PLC0415
    from prefect.utilities.asyncutils import run_coro_as_sync  # noqa: PLC0415

    async def _fetch() -> str | None:
        async with get_client() as client:
            flow_run = await client.read_flow_run(flow_run_id)
            return flow_run.deployment_id

    return run_coro_as_sync(_fetch())


def trigger_next_batch_if_pending(
    *,
    bq_input_table: str | None,
    bq_status_table: str | None,
    pipeline_runs_table: str | None,
    batch_size: int,
    gcs_output_base_path: str,
    db_path: str,
    gcs_bucket: str | None,
    workers: int,
    mode: str,
    requests_per_minute: int,
    max_concurrent: int,
    max_retries: int,
    session_id: str,
    match_requires_pdf_name: bool,
    max_pdfs: int | None,
    total_in_session: int,
    batch_did_work: bool,
) -> None:
    """
    Trigger the next batch flow run when pending documents remain in the queue.

    Checks Prefect runtime context and BigQuery pending count before triggering.
    No-ops when not inside a Prefect flow run, when the run has no deployment,
    when the queue is exhausted, or when the session ``max_pdfs`` cap has been reached.

    :param bq_input_table: Fully-qualified BQ table with input documents.
    :param bq_status_table: Fully-qualified BQ table with processing status.
    :param pipeline_runs_table: Fully-qualified BQ table for run summaries.
    :param batch_size: Number of documents per batch.
    :param gcs_output_base_path: GCS base path for output files.
    :param db_path: Local SQLite cache path inside the container.
    :param gcs_bucket: GCS bucket name.
    :param workers: Number of parallel workers.
    :param mode: Pipeline execution mode (e.g. ``"full"``).
    :param requests_per_minute: API rate limit.
    :param max_concurrent: Maximum concurrent API requests.
    :param max_retries: Maximum retries per document before giving up.
    :param session_id: Current session UUID.
    :param match_requires_pdf_name: Whether PDF-name matching is required.
    :param max_pdfs: Session cap on total PDFs, or ``None`` for uncapped.
    :param total_in_session: Cumulative PDFs processed in the current session.
    :param batch_did_work: ``True`` if the current batch processed at least one PDF.
    """
    from run_poc.bq_input_reader import BQInputReader  # noqa: PLC0415

    flow_run_id = get_flow_run_id()
    if not flow_run_id:
        logger.info("Not running as a flow run — skipping self-trigger check")
        return

    deployment_id = get_current_deployment_id(flow_run_id)
    if not deployment_id:
        logger.info("Flow run has no deployment — skipping self-trigger check")
        return

    pending = BQInputReader().count_pending(bq_input_table, bq_status_table, max_retries=max_retries)
    logger.info("%d documents still pending after this batch", pending)

    if max_pdfs is not None and total_in_session >= max_pdfs:
        logger.info("max_pdfs=%d atingido na sessão (%d PDFs) — encerrando cadeia", max_pdfs, total_in_session)
        return

    if pending == 0:
        logger.info("Queue exhausted — no more batches to trigger")
        return

    if not batch_did_work:
        logger.warning(
            "Batch processed nothing despite pending docs — all remaining may be at max retries. Stopping chain."
        )
        return

    from prefect.deployments import run_deployment  # noqa: PLC0415

    logger.info("Triggering next batch (deployment_id=%s)", deployment_id)
    run_deployment(
        name=deployment_id,
        parameters={
            "bq_input_table": bq_input_table,
            "bq_status_table": bq_status_table,
            "pipeline_runs_table": pipeline_runs_table,
            "batch_size": batch_size,
            "gcs_output_base_path": gcs_output_base_path,
            "db_path": db_path,
            "gcs_bucket": gcs_bucket,
            "workers": workers,
            "mode": mode,
            "requests_per_minute": requests_per_minute,
            "max_concurrent": max_concurrent,
            "max_retries": max_retries,
            "session_id": session_id,
            "match_requires_pdf_name": match_requires_pdf_name,
            "max_pdfs": max_pdfs,
            "session_pdfs_done": total_in_session,
        },
        timeout=0,
    )

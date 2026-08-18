"""
Utility helpers for the NF Agent Prefect pipeline.

All imports of ``utils.run_poc`` and other migrated agent modules are deferred
to the function body so this module stays importable during ``prefect deploy``
in CI, which only synchronises prefect/prefect-docker — not the ``gemini``
extra that powers the extraction/classification agents.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


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
# Batch parameters and summary
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchRunParams:
    """Parameters shared by a pipeline batch run and its self-triggered continuation."""

    bq_input_table: str | None
    bq_status_table: str | None
    pipeline_runs_table: str | None
    batch_size: int
    gcs_output_base_path: str
    db_path: str
    gcs_bucket: str | None
    workers: int
    mode: str
    requests_per_minute: int
    max_concurrent: int
    max_retries: int
    match_requires_pdf_name: bool
    max_pdfs: int | None


@dataclass(frozen=True)
class BatchSummary:
    """Per-batch and per-session counters derived from a pipeline run's timing stats."""

    pdfs_processed: int
    pdfs_failed: int
    docs_processed: int
    docs_failed: int
    duration_seconds: float
    total_in_session: int
    pending_pdfs: int | None
    pending_docs: int | None
    avg_sec_per_pdf: float
    avg_sec_per_doc: float
    batch_did_work: bool


def summarize_batch(
    timing_stats: dict[str, Any],
    session_pdfs_done: int,
    max_pdfs: int | None,
    duration_seconds: float,
) -> BatchSummary:
    """
    Derive per-batch and per-session counters from the raw pipeline timing stats.

    :param timing_stats: Timing/count stats returned by the agent-nf-validator run.
    :param session_pdfs_done: PDFs already processed in prior batches of this session.
    :param max_pdfs: Session cap on total PDFs, or ``None`` for uncapped.
    :param duration_seconds: Wall-clock duration of the batch in seconds.
    :returns: A :class:`BatchSummary` with processed/failed counts and averages.
    """
    pdfs_processed = timing_stats.get("_n_pdfs_ok", 0) or 0
    pdfs_failed = timing_stats.get("_n_pdfs_fail", 0) or 0
    docs_processed = timing_stats.get("_n_docs_ok", 0) or 0
    docs_failed = timing_stats.get("_n_docs_fail", 0) or 0

    total_in_session = session_pdfs_done + pdfs_processed + pdfs_failed
    pending_pdfs, pending_docs = pending_in_session(max_pdfs, total_in_session)

    avg_sec_per_pdf = round(duration_seconds / pdfs_processed, 2) if pdfs_processed > 0 else 0.0
    avg_sec_per_doc = round(duration_seconds / docs_processed, 2) if docs_processed > 0 else 0.0

    return BatchSummary(
        pdfs_processed=pdfs_processed,
        pdfs_failed=pdfs_failed,
        docs_processed=docs_processed,
        docs_failed=docs_failed,
        duration_seconds=duration_seconds,
        total_in_session=total_in_session,
        pending_pdfs=pending_pdfs,
        pending_docs=pending_docs,
        avg_sec_per_pdf=avg_sec_per_pdf,
        avg_sec_per_doc=avg_sec_per_doc,
        batch_did_work=(pdfs_processed + pdfs_failed) > 0,
    )


# ---------------------------------------------------------------------------
# Logging / reporting
# ---------------------------------------------------------------------------


def log_batch_summary(session_id: str, summary: BatchSummary, max_pdfs: int | None) -> None:
    """
    Emit a structured batch-summary log entry at INFO level.

    :param session_id: Current session UUID.
    :param summary: Counters and averages for the batch just processed.
    :param max_pdfs: Session cap on total PDFs, or ``None`` for uncapped.
    """
    lines = [
        "── Batch summary ──────────────────────",
        f"  Session:        {session_id}",
        f"  Processed:      {summary.pdfs_processed} PDFs / {summary.docs_processed} docs",
        f"  Failed:         {summary.pdfs_failed} PDFs / {summary.docs_failed} docs",
        f"  Duration:       {summary.duration_seconds / 60:.1f} min",
        f"  Avg / PDF:      {summary.avg_sec_per_pdf:.1f} sec",
        f"  Avg / doc:      {summary.avg_sec_per_doc:.1f} sec",
    ]
    if summary.pending_pdfs is not None:
        lines.append(f"  Pending in session: {summary.pending_pdfs} PDFs / {summary.pending_docs} docs")
        if summary.avg_sec_per_pdf > 0 and summary.pending_pdfs > 0:
            est_remaining_min = round(summary.pending_pdfs * summary.avg_sec_per_pdf / 60, 1)
            lines.append(f"  Est. remaining: ~{est_remaining_min} min")
    lines.append(f"  Cumulative:     {summary.total_in_session} / {max_pdfs if max_pdfs else '∞'} PDFs")
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


@dataclass(frozen=True)
class RunContext:
    """Identity and timing of a single flow run, for the run-summary row."""

    pipeline_runs_table: str
    bq_status_table: str | None
    session_id: str
    started_at: datetime
    finished_at: datetime


@dataclass(frozen=True)
class PipelineRunConfig:
    """Configuration knobs recorded alongside a run-summary row."""

    batch_size: int
    workers: int
    requests_per_minute: int
    max_concurrent: int
    force_reprocess: bool


def write_run_summary(
    context: RunContext,
    summary: BatchSummary,
    config: PipelineRunConfig,
    timing_stats: dict[str, Any],
) -> None:
    """
    Write a run-summary row to BigQuery via ``BigQueryWriter``.

    :param context: Run identity and timing (session, table refs, start/finish).
    :param summary: Counters and averages for the batch just processed.
    :param config: Batch/concurrency configuration active during the run.
    :param timing_stats: Detailed per-stage timing statistics from the pipeline.
    """
    from .run_poc.bigquery_writer import BigQueryWriter  # noqa: PLC0415

    bq_project, bq_dataset = parse_project_and_dataset(context.bq_status_table)
    if not (bq_project and bq_dataset):
        return

    BigQueryWriter(project_id=bq_project, dataset_id=bq_dataset).write_run_summary(
        pipeline_runs_table=context.pipeline_runs_table,
        row={
            "session_id": context.session_id,
            "flow_run_id": str(get_flow_run_id()),
            "started_at": context.started_at,
            "finished_at": context.finished_at,
            "duration_seconds": summary.duration_seconds,
            "pdfs_processed": summary.pdfs_processed,
            "docs_processed": summary.docs_processed,
            "pdfs_failed": summary.pdfs_failed,
            "docs_failed": summary.docs_failed,
            "pending_pdfs": summary.pending_pdfs or 0,
            "pending_docs": summary.pending_docs or 0,
            "avg_sec_per_pdf": summary.avg_sec_per_pdf,
            "avg_sec_per_doc": summary.avg_sec_per_doc,
            "batch_size": config.batch_size,
            "workers": config.workers,
            "requests_per_minute": config.requests_per_minute,
            "max_concurrent": config.max_concurrent,
            # Wall-clock totals (real elapsed time per stage)
            "wall_sec_download_gcs": timing_stats.get("wall_sec_download_gcs"),
            "wall_sec_core": timing_stats.get("wall_sec_core"),
            "wall_sec_escrita": timing_stats.get("wall_sec_escrita"),
            # Per-PDF/Per-page/Per-doc CPU average (from individual timers)
            "avg_cpu_sec_preprocess_por_pdf": timing_stats.get("avg_cpu_sec_preprocess_por_pdf"),
            "avg_cpu_sec_classificacao_por_pagina": timing_stats.get("avg_cpu_sec_classificacao_por_pagina"),
            "avg_cpu_sec_extracao_por_declaracao": timing_stats.get("avg_cpu_sec_extracao_por_declaracao"),
            "avg_cpu_sec_validacao_por_pdf": timing_stats.get("avg_cpu_sec_validacao_por_pdf"),
            "force_reprocess": config.force_reprocess,
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
    params: BatchRunParams,
    session_id: str,
    total_in_session: int,
    batch_did_work: bool,
) -> None:
    """
    Trigger the next batch flow run when pending documents remain in the queue.

    Checks Prefect runtime context and BigQuery pending count before triggering.
    No-ops when not inside a Prefect flow run, when the run has no deployment,
    when the queue is exhausted, or when the session ``max_pdfs`` cap has been reached.

    :param params: Batch parameters to forward to the next flow run.
    :param session_id: Current session UUID.
    :param total_in_session: Cumulative PDFs processed in the current session.
    :param batch_did_work: ``True`` if the current batch processed at least one PDF.
    """
    from .run_poc.bq_input_reader import BQInputReader  # noqa: PLC0415

    flow_run_id = get_flow_run_id()
    if not flow_run_id:
        logger.info("Not running as a flow run — skipping self-trigger check")
        return

    deployment_id = get_current_deployment_id(flow_run_id)
    if not deployment_id:
        logger.info("Flow run has no deployment — skipping self-trigger check")
        return

    pending = BQInputReader().count_pending(
        params.bq_input_table, params.bq_status_table, max_retries=params.max_retries
    )
    logger.info("%d documents still pending after this batch", pending)

    if params.max_pdfs is not None and total_in_session >= params.max_pdfs:
        logger.info(
            "max_pdfs=%d atingido na sessão (%d PDFs) — encerrando cadeia",
            params.max_pdfs,
            total_in_session,
        )
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
            "bq_input_table": params.bq_input_table,
            "bq_status_table": params.bq_status_table,
            "pipeline_runs_table": params.pipeline_runs_table,
            "batch_size": params.batch_size,
            "gcs_output_base_path": params.gcs_output_base_path,
            "db_path": params.db_path,
            "gcs_bucket": params.gcs_bucket,
            "workers": params.workers,
            "mode": params.mode,
            "requests_per_minute": params.requests_per_minute,
            "max_concurrent": params.max_concurrent,
            "max_retries": params.max_retries,
            "session_id": session_id,
            "match_requires_pdf_name": params.match_requires_pdf_name,
            "max_pdfs": params.max_pdfs,
            "session_pdfs_done": total_in_session,
        },
        timeout=0,
    )


def run_nf_pipeline(params: BatchRunParams, force_reprocess: bool) -> dict[str, Any]:
    """
    Run one batch of the agent-nf-validator extraction/validation pipeline.

    :param params: Batch parameters controlling input/output tables, paths and concurrency.
    :param force_reprocess: Whether to reprocess documents already marked as done.
    :returns: Timing/count stats emitted by the pipeline run, or ``{}`` if none.
    """
    from .run_poc.run_pipeline import nf_processing_flow as _run_pipeline  # noqa: PLC0415

    return (
        _run_pipeline(
            bq_input_table=params.bq_input_table,
            bq_status_table=params.bq_status_table,
            batch_size=params.batch_size,
            gcs_output_base_path=params.gcs_output_base_path,
            db_path=params.db_path,
            gcs_bucket=params.gcs_bucket,
            workers=params.workers,
            mode=params.mode,
            requests_per_minute=params.requests_per_minute,
            max_concurrent=params.max_concurrent,
            max_retries=params.max_retries,
            match_requires_pdf_name=params.match_requires_pdf_name,
            max_pdfs=params.max_pdfs,
            force_reprocess=force_reprocess,
        )
        or {}
    )

"""Prefect task wrappers for the NF Agent pipeline.

Covers both execution modes of the single ``rj_iplanrio__nf_agent`` flow —
sync (Bifrost/OpenAI-protocol, per-request) and batch (Vertex AI Batch
Prediction). Each task is still a thin wrapper delegating to a plain
function in ``utils.orchestration`` (sync) or ``utils.batch`` (batch), so
the pattern in STYLEGUIDE.md §4.2 holds for both.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from prefect import task

from .utils import orchestration
from .utils.batch import job_tracking, row_counting
from .utils.batch.classification_submit import ClassificationSubmitResult, submit_classification_job
from .utils.batch.poll import PollConfig, poll_once
from .utils.batch.row_counting import BatchSessionSelection
from .utils.gcs import GCSDownloader
from .utils.orchestration import BatchRunParams, BatchSummary, PipelineRunConfig, RunContext
from .utils.pipeline import discover_pending_files


@task
def new_or_continued_session_task(session_id: str | None) -> str:
    """Return an existing session ID or create a fresh one."""
    return orchestration.new_or_continued_session(session_id)


@task
def run_nf_pipeline_task(params: BatchRunParams) -> dict[str, Any]:
    """Run one batch of the agent-nf-validator pipeline.

    :param params: Batch parameters controlling input/output tables, paths and concurrency.
    :return: Dictionary of timing stats and counters for the batch run
    """
    return orchestration.run_nf_pipeline(params=params)


@task
def summarize_batch_task(
    timing_stats: dict[str, Any],
    session_pdfs_done: int,
    max_pdfs: int | None,
    duration_seconds: float,
) -> BatchSummary:
    """Derive per-batch and per-session counters from the raw pipeline timing stats."""
    return orchestration.summarize_batch(
        timing_stats=timing_stats,
        session_pdfs_done=session_pdfs_done,
        max_pdfs=max_pdfs,
        duration_seconds=duration_seconds,
    )


@task
def log_batch_summary_task(session_id: str, summary: BatchSummary, max_pdfs: int | None) -> None:
    """Log a structured summary of the batch just processed."""
    orchestration.log_batch_summary(session_id=session_id, summary=summary, max_pdfs=max_pdfs)


@task
def write_run_summary_task(
    pipeline_runs_table: str,
    session_id: str,
    started_at: datetime,
    finished_at: datetime,
    summary: BatchSummary,
    batch_size: int,
    workers: int,
    requests_per_minute: int,
    max_concurrent: int,
    timing_stats: dict[str, Any],
) -> None:
    """Write a run-summary row to BigQuery."""
    orchestration.write_run_summary(
        context=RunContext(
            pipeline_runs_table=pipeline_runs_table,
            session_id=session_id,
            started_at=started_at,
            finished_at=finished_at,
        ),
        summary=summary,
        config=PipelineRunConfig(
            batch_size=batch_size,
            workers=workers,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
        ),
        timing_stats=timing_stats,
    )


@task
def trigger_next_batch_if_pending_task(
    params: BatchRunParams,
    session_id: str,
    total_in_session: int,
    batch_did_work: bool,
) -> None:
    """Self-trigger the next batch flow run when pending documents remain."""
    orchestration.trigger_next_batch_if_pending(
        params=params,
        session_id=session_id,
        total_in_session=total_in_session,
        batch_did_work=batch_did_work,
    )


# ── Batch mode (Vertex AI Batch Prediction) ─────────────────────────────────


@task
def discover_pending_files_task(bq_extracao_pagina_table: str, gcs_downloader: GCSDownloader) -> tuple[set[str], str]:
    """Return pending PDF filenames and the current pipeline version (git commit)."""
    return discover_pending_files(gcs_downloader, bq_extracao_pagina_table)


@task
def has_active_session_task(nf_batch_jobs_table: str) -> bool:
    """Return whether a batch session is already in flight."""
    return job_tracking.has_active_session(nf_batch_jobs_table)


@task
def select_session_pdfs_task(pdf_paths: dict[str, Path], max_rows: int) -> BatchSessionSelection:
    """Select the page-count-bounded subset of downloaded PDFs for this session."""
    return row_counting.select_pdfs_within_row_budget(pdf_paths, max_rows=max_rows)


@task
def submit_classification_job_task(
    bq_project: str,
    bq_dataset: str,
    nf_batch_jobs_table: str,
    gcs_downloader: GCSDownloader,
    pdf_paths: dict[str, Path],
    selection: BatchSessionSelection,
    session_id: str,
) -> ClassificationSubmitResult:
    """Build the classification input table and submit the Vertex AI batch job."""
    return submit_classification_job(
        bq_project=bq_project,
        bq_dataset=bq_dataset,
        nf_batch_jobs_table=nf_batch_jobs_table,
        gcs_downloader=gcs_downloader,
        pdf_paths=pdf_paths,
        selection=selection,
        session_id=session_id,
    )


@task
def poll_active_sessions_task(config: PollConfig) -> list[str]:
    """Check every active session's Vertex job state and advance/finish it."""
    return poll_once(config)

"""Prefect task wrappers for the NF Agent pipeline."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from prefect import task

from .utils import orchestration
from .utils.orchestration import BatchRunParams, BatchSummary, PipelineRunConfig, RunContext


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
    bq_status_table: str | None,
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
            bq_status_table=bq_status_table,
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

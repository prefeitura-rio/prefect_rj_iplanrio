"""
Prefect entrypoint for the NF (Nota Fiscal) validation pipeline.

The NF business logic lives in this package (migrated from agent-nf-validator by
mechanical move). Some imports are deferred to the function
body so this module stays importable during `prefect deploy` in CI, which only
needs this pipeline's own dependencies (prefect, prefect-docker) synced — not
the `gemini` extra that powers the extraction/classification agents.
"""

from __future__ import annotations

from datetime import datetime, timezone

from iplanrio_agent_toolkit.credentials import inject_credentials_from_env
from prefect import flow

from .tasks import (
    log_batch_summary_task,
    new_or_continued_session_task,
    run_nf_pipeline_task,
    summarize_batch_task,
    trigger_next_batch_if_pending_task,
    write_run_summary_task,
)
from .utils.orchestration import BatchRunParams


@flow(log_prints=True)
def rj_iplanrio__nf_agent(
    bq_input_table: str | None = None,
    bq_status_table: str | None = None,
    pipeline_runs_table: str | None = None,
    batch_size: int = 1000,
    gcs_output_base_path: str = "staging/brutos_poc_osinfo_ia/resultado_extracao_modelo",
    db_path: str = "/tmp/nf_pipeline_cache.db",
    gcs_bucket: str | None = None,
    workers: int = 200,
    mode: str = "full",
    requests_per_minute: int = 600,
    max_concurrent: int = 50,
    max_retries: int = 3,
    session_id: str | None = None,
    max_pdfs: int | None = None,
    force_reprocess: bool = False,
    session_pdfs_done: int = 0,
) -> None:
    """Run one batch of the NF extraction/validation pipeline and self-trigger the next one."""

    # Inject GCP credentials from Infisical before any GCP client is created
    inject_credentials_from_env("RJ_NF_AGENT_CREDENTIALS")
    inject_credentials_from_env("GCS_BUCKET")

    session_id = new_or_continued_session_task(session_id)
    params = BatchRunParams(
        bq_input_table=bq_input_table,
        bq_status_table=bq_status_table,
        pipeline_runs_table=pipeline_runs_table,
        batch_size=batch_size,
        gcs_output_base_path=gcs_output_base_path,
        db_path=db_path,
        gcs_bucket=gcs_bucket,
        workers=workers,
        mode=mode,
        requests_per_minute=requests_per_minute,
        max_concurrent=max_concurrent,
        max_retries=max_retries,
        max_pdfs=max_pdfs,
    )

    started_at = datetime.now(timezone.utc)
    timing_stats = run_nf_pipeline_task(params=params, force_reprocess=force_reprocess)
    finished_at = datetime.now(timezone.utc)

    summary = summarize_batch_task(
        timing_stats=timing_stats,
        session_pdfs_done=session_pdfs_done,
        max_pdfs=max_pdfs,
        duration_seconds=(finished_at - started_at).total_seconds(),
    )

    log_batch_summary_task(session_id=session_id, summary=summary, max_pdfs=max_pdfs)

    if pipeline_runs_table:
        write_run_summary_task(
            pipeline_runs_table=pipeline_runs_table,
            bq_status_table=bq_status_table,
            session_id=session_id,
            started_at=started_at,
            finished_at=finished_at,
            summary=summary,
            batch_size=batch_size,
            workers=workers,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
            force_reprocess=force_reprocess,
            timing_stats=timing_stats,
        )

    trigger_next_batch_if_pending_task(
        params=params,
        session_id=session_id,
        total_in_session=summary.total_in_session,
        batch_did_work=summary.batch_did_work,
    )

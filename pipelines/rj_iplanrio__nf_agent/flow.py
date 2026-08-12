"""
Prefect entrypoint for the NF (Nota Fiscal) validation pipeline.

agent-nf-validator is installed as a pinned git dependency (see
[tool.uv.sources] in pyproject.toml) at image build time. All run_poc imports
are deferred to the function body so this module stays importable during
`prefect deploy` in CI, which only needs this pipeline's own dependencies
(prefect, prefect-docker) synced — not the full agent-nf-validator install.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from prefect import flow

from .utils import (
    log_batch_summary,
    new_or_continued_session,
    pending_in_session,
    trigger_next_batch_if_pending,
    write_run_summary,
)

if TYPE_CHECKING:
    pass


@flow(log_prints=True)
def nf_processing_flow(
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
    match_requires_pdf_name: bool = False,
    max_pdfs: int | None = None,
    force_reprocess: bool = False,
    session_pdfs_done: int = 0,
) -> None:
    from datetime import datetime

    from run_poc.credentials_helper import inject_credentials_from_env
    from run_poc.run_pipeline import nf_processing_flow as _run_pipeline

    # Inject GCP credentials from Infisical before any GCP client is created
    inject_credentials_from_env("RJ_NF_AGENT_CREDENTIALS")

    session_id = new_or_continued_session(session_id)

    started_at = datetime.utcnow()
    timing_stats = (
        _run_pipeline(
            bq_input_table=bq_input_table,
            bq_status_table=bq_status_table,
            batch_size=batch_size,
            gcs_output_base_path=gcs_output_base_path,
            db_path=db_path,
            gcs_bucket=gcs_bucket,
            workers=workers,
            mode=mode,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
            max_retries=max_retries,
            match_requires_pdf_name=match_requires_pdf_name,
            max_pdfs=max_pdfs,
            force_reprocess=force_reprocess,
        )
        or {}
    )
    finished_at = datetime.utcnow()
    duration_seconds = (finished_at - started_at).total_seconds()

    pdfs_processed = timing_stats.get("_n_pdfs_ok", 0) or 0
    pdfs_failed = timing_stats.get("_n_pdfs_fail", 0) or 0
    docs_processed = timing_stats.get("_n_docs_ok", 0) or 0
    docs_failed = timing_stats.get("_n_docs_fail", 0) or 0

    total_in_session = session_pdfs_done + pdfs_processed + pdfs_failed
    pending_pdfs, pending_docs = pending_in_session(max_pdfs, total_in_session)

    avg_sec_per_pdf = round(duration_seconds / pdfs_processed, 2) if pdfs_processed > 0 else 0.0
    avg_sec_per_doc = round(duration_seconds / docs_processed, 2) if docs_processed > 0 else 0.0

    log_batch_summary(
        session_id=session_id,
        pdfs_processed=pdfs_processed,
        pdfs_failed=pdfs_failed,
        docs_processed=docs_processed,
        docs_failed=docs_failed,
        duration_seconds=duration_seconds,
        avg_sec_per_pdf=avg_sec_per_pdf,
        avg_sec_per_doc=avg_sec_per_doc,
        pending_pdfs=pending_pdfs,
        pending_docs=pending_docs,
        total_in_session=total_in_session,
        max_pdfs=max_pdfs,
    )

    if pipeline_runs_table:
        write_run_summary(
            pipeline_runs_table=pipeline_runs_table,
            bq_status_table=bq_status_table,
            session_id=session_id,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_seconds,
            pdfs_processed=pdfs_processed,
            docs_processed=docs_processed,
            pdfs_failed=pdfs_failed,
            docs_failed=docs_failed,
            pending_pdfs=pending_pdfs,
            pending_docs=pending_docs,
            avg_sec_per_pdf=avg_sec_per_pdf,
            avg_sec_per_doc=avg_sec_per_doc,
            batch_size=batch_size,
            workers=workers,
            requests_per_minute=requests_per_minute,
            max_concurrent=max_concurrent,
            force_reprocess=force_reprocess,
            timing_stats=timing_stats,
        )

    trigger_next_batch_if_pending(
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
        session_id=session_id,
        match_requires_pdf_name=match_requires_pdf_name,
        max_pdfs=max_pdfs,
        total_in_session=total_in_session,
        batch_did_work=(pdfs_processed + pdfs_failed) > 0,
    )

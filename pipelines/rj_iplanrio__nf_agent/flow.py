"""
Prefect entrypoint for the NF (Nota Fiscal) validation pipeline.

agent-nf-validator is installed as a pinned git dependency (see
[tool.uv.sources] in pyproject.toml) at image build time. All run_poc imports
are deferred to the function body so this module stays importable during
`prefect deploy` in CI, which only needs this pipeline's own dependencies
(prefect, prefect-docker) synced — not the full agent-nf-validator install.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from prefect import flow

if TYPE_CHECKING:
    from datetime import datetime


def _new_or_continued_session(session_id: str | None) -> str:
    import uuid

    if session_id:
        print(f"[Flow] Continuing session: {session_id}")
        return session_id

    session_id = str(uuid.uuid4())
    print(f"[Flow] New session started: {session_id}")
    return session_id


def _pending_in_session(max_pdfs: int | None, total_in_session: int) -> tuple[int | None, int | None]:
    """PDFs/docs still allowed before `max_pdfs` is hit, or (None, None) if uncapped."""
    if max_pdfs is None:
        return None, None
    pending_pdfs = max(0, max_pdfs - total_in_session)
    pending_docs = pending_pdfs  # estimate — exact doc count per PDF is unknown upfront
    return pending_pdfs, pending_docs


def _print_batch_summary(
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
    lines = [
        "[Flow] ── Batch summary ──────────────────────",
        f"[Flow]   Session:        {session_id}",
        f"[Flow]   Processed:      {pdfs_processed} PDFs / {docs_processed} docs",
        f"[Flow]   Failed:         {pdfs_failed} PDFs / {docs_failed} docs",
        f"[Flow]   Duration:       {duration_seconds / 60:.1f} min",
        f"[Flow]   Avg / PDF:      {avg_sec_per_pdf:.1f} sec",
        f"[Flow]   Avg / doc:      {avg_sec_per_doc:.1f} sec",
    ]
    if pending_pdfs is not None:
        lines.append(f"[Flow]   Pending in session: {pending_pdfs} PDFs / {pending_docs} docs")
        if avg_sec_per_pdf > 0 and pending_pdfs > 0:
            est_remaining_min = round(pending_pdfs * avg_sec_per_pdf / 60, 1)
            lines.append(f"[Flow]   Est. remaining: ~{est_remaining_min} min")
    lines.append(f"[Flow]   Cumulative:     {total_in_session} / {max_pdfs if max_pdfs else '∞'} PDFs")
    lines.append("[Flow] ──────────────────────────────────────")
    print("\n".join(lines))


def _parse_project_and_dataset(bq_table_ref: str | None) -> tuple[str | None, str | None]:
    """Split a `project.dataset.table` reference; returns (None, None) if malformed."""
    parts = (bq_table_ref or "").split(".")
    if len(parts) < 3:
        return None, None
    return parts[0], parts[1]


def _write_run_summary(
    *,
    pipeline_runs_table: str,
    bq_status_table: str | None,
    session_id: str,
    started_at: datetime,
    finished_at: datetime,
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
    from run_poc.bigquery_writer import BigQueryWriter

    bq_project, bq_dataset = _parse_project_and_dataset(bq_status_table)
    if not (bq_project and bq_dataset):
        return

    BigQueryWriter(project_id=bq_project, dataset_id=bq_dataset).write_run_summary(
        pipeline_runs_table=pipeline_runs_table,
        row={
            "session_id": session_id,
            "flow_run_id": str(_get_flow_run_id()),
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


def _trigger_next_batch_if_pending(
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
    from run_poc.bq_input_reader import BQInputReader

    flow_run_id = _get_flow_run_id()
    if not flow_run_id:
        print("[Flow] Not running as a flow run — skipping self-trigger check")
        return

    deployment_id = _get_current_deployment_id(flow_run_id)
    if not deployment_id:
        print("[Flow] Flow run has no deployment — skipping self-trigger check")
        return

    pending = BQInputReader().count_pending(bq_input_table, bq_status_table, max_retries=max_retries)
    print(f"[Flow] {pending:,} documents still pending after this batch")

    if max_pdfs is not None and total_in_session >= max_pdfs:
        print(f"[Flow] max_pdfs={max_pdfs} atingido na sessão ({total_in_session} PDFs) — encerrando cadeia")
        return

    if pending == 0:
        print("[Flow] Queue exhausted — no more batches to trigger")
        return

    if not batch_did_work:
        print(
            "[Flow] Batch processed nothing despite pending docs — all remaining may be at max retries. Stopping chain."
        )
        return

    from prefect.deployments import run_deployment

    print(f"[Flow] Triggering next batch (deployment_id={deployment_id})")
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

    session_id = _new_or_continued_session(session_id)

    started_at = datetime.utcnow()
    timing_stats = (
        _run_pipeline.fn(
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
    pending_pdfs, pending_docs = _pending_in_session(max_pdfs, total_in_session)

    avg_sec_per_pdf = round(duration_seconds / pdfs_processed, 2) if pdfs_processed > 0 else 0.0
    avg_sec_per_doc = round(duration_seconds / docs_processed, 2) if docs_processed > 0 else 0.0

    _print_batch_summary(
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
        _write_run_summary(
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

    _trigger_next_batch_if_pending(
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


def _get_flow_run_id() -> str | None:
    try:
        from prefect.runtime import flow_run as current_run

        return current_run.id
    except Exception:
        return None


def _get_current_deployment_id(flow_run_id: str) -> str | None:
    from prefect import get_client
    from prefect.utilities.asyncutils import run_coro_as_sync

    async def _fetch() -> str | None:
        async with get_client() as client:
            flow_run = await client.read_flow_run(flow_run_id)
            return flow_run.deployment_id

    return run_coro_as_sync(_fetch())

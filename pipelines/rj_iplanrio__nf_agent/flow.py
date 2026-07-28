"""
Prefect entrypoint for the NF (Nota Fiscal) validation pipeline.

agent-nf-validator is cloned to /opt/agent-nf-validator at image build time.
All run_poc imports are deferred to the function body so this module is importable
at deploy time (CI) without /opt/agent-nf-validator being present.
"""

from prefect import flow


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
    _session_pdfs_done: int = 0,
) -> None:
    import sys
    import uuid
    from datetime import datetime

    sys.path.insert(0, "/opt/agent-nf-validator/run_poc")
    from run_poc.run_pipeline import nf_processing_flow as _impl  # noqa: PLC0415
    from run_poc.bq_input_reader import BQInputReader  # noqa: PLC0415
    from run_poc.credentials_helper import inject_credentials_from_env  # noqa: PLC0415

    # Inject GCP credentials from Infisical before any GCP client is created
    inject_credentials_from_env("RJ_NF_AGENT_CREDENTIALS")

    # Generate session_id if this is the first flow in the chain
    if not session_id:
        session_id = str(uuid.uuid4())
        print(f"[Flow] New session started: {session_id}")
    else:
        print(f"[Flow] Continuing session: {session_id}")

    started_at = datetime.utcnow()

    timing_stats = _impl.fn(
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
    ) or {}

    finished_at = datetime.utcnow()
    duration_seconds = (finished_at - started_at).total_seconds()

    # ── Actual batch counts from the pipeline (not BQ table diff) ──
    pdfs_processed = timing_stats.get("_n_pdfs_ok", 0) or 0
    pdfs_failed    = timing_stats.get("_n_pdfs_fail", 0) or 0
    docs_processed = timing_stats.get("_n_docs_ok", 0) or 0
    docs_failed    = timing_stats.get("_n_docs_fail", 0) or 0

    # ── Session-scoped pending (only when max_pdfs defines a session limit) ──
    total_in_session = _session_pdfs_done + pdfs_processed + pdfs_failed
    if max_pdfs is not None:
        pending_in_session_pdfs = max(0, max_pdfs - total_in_session)
        pending_in_session_docs = pending_in_session_pdfs  # estimate (exact docs unknown)
    else:
        pending_in_session_pdfs = None
        pending_in_session_docs = None

    avg_sec_per_pdf  = round(duration_seconds / pdfs_processed, 2) if pdfs_processed > 0 else 0.0
    avg_sec_per_doc  = round(duration_seconds / docs_processed, 2) if docs_processed > 0 else 0.0
    est_remaining_min = round(pending_in_session_pdfs * avg_sec_per_pdf / 60, 1) if (avg_sec_per_pdf > 0 and pending_in_session_pdfs and pending_in_session_pdfs > 0) else None

    _pending_line = ""
    if pending_in_session_pdfs is not None:
        _pending_line = (
            f"[Flow]   Pending in session: {pending_in_session_pdfs} PDFs / {pending_in_session_docs} docs\n"
            + (f"[Flow]   Est. remaining: ~{est_remaining_min} min\n" if est_remaining_min else "")
        )
    print(
        f"[Flow] ── Batch summary ──────────────────────\n"
        f"[Flow]   Session:        {session_id}\n"
        f"[Flow]   Processed:      {pdfs_processed} PDFs / {docs_processed} docs\n"
        f"[Flow]   Failed:         {pdfs_failed} PDFs / {docs_failed} docs\n"
        f"[Flow]   Duration:       {duration_seconds / 60:.1f} min\n"
        f"[Flow]   Avg / PDF:      {avg_sec_per_pdf:.1f} sec\n"
        f"[Flow]   Avg / doc:      {avg_sec_per_doc:.1f} sec\n"
        + _pending_line
        + f"[Flow]   Cumulative:     {total_in_session} / {max_pdfs if max_pdfs else '∞'} PDFs\n"
        + f"[Flow] ──────────────────────────────────────"
    )

    # Write run summary to BQ (if table configured)
    if pipeline_runs_table:
        from run_poc.bigquery_writer import BigQueryWriter  # noqa: PLC0415

        _ref = bq_status_table or ""
        _parts = _ref.split(".")
        bq_project = _parts[0] if len(_parts) >= 3 else None
        bq_dataset = _parts[1] if len(_parts) >= 3 else None

        if bq_project and bq_dataset:
            bq_writer = BigQueryWriter(project_id=bq_project, dataset_id=bq_dataset)
            bq_writer.write_run_summary(
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
                    "pending_pdfs": pending_in_session_pdfs or 0,
                    "pending_docs": pending_in_session_docs or 0,
                    "avg_sec_per_pdf": avg_sec_per_pdf,
                    "avg_sec_per_doc": avg_sec_per_doc,
                    "batch_size": batch_size,
                    "workers": workers,
                    "requests_per_minute": requests_per_minute,
                    "max_concurrent": max_concurrent,
                    # Wall-clock totals (real elapsed time per stage)
                    "wall_sec_download_gcs":   timing_stats.get("wall_sec_download_gcs"),
                    "wall_sec_core":           timing_stats.get("wall_sec_core"),
                    "wall_sec_escrita":        timing_stats.get("wall_sec_escrita"),
                    # Per-PDF/Per-Page/Per-Doc CPU average (concrete, from individual timers)
                    "avg_cpu_sec_preprocess_por_pdf":         timing_stats.get("avg_cpu_sec_preprocess_por_pdf"),
                    "avg_cpu_sec_classificacao_por_pagina":   timing_stats.get("avg_cpu_sec_classificacao_por_pagina"),
                    "avg_cpu_sec_extracao_por_declaracao":    timing_stats.get("avg_cpu_sec_extracao_por_declaracao"),
                    "avg_cpu_sec_validacao_por_pdf":           timing_stats.get("avg_cpu_sec_validacao_por_pdf"),
                    # Reprocess control
                    "force_reprocess":          force_reprocess,
                },
            )

    # Self-trigger next batch if work remains
    from prefect import get_client  # noqa: PLC0415
    from prefect.utilities.asyncutils import run_coro_as_sync  # noqa: PLC0415

    flow_run_id = _get_flow_run_id()
    if not flow_run_id:
        print("[Flow] Not running as a flow run — skipping self-trigger check")
        return

    async def _get_deployment_id():
        async with get_client() as client:
            fr = await client.read_flow_run(flow_run_id)
            return fr.deployment_id

    deployment_id = run_coro_as_sync(_get_deployment_id())
    if not deployment_id:
        print("[Flow] Flow run has no deployment — skipping self-trigger check")
        return

    pending = BQInputReader().count_pending(bq_input_table, bq_status_table, max_retries=max_retries)
    print(f"[Flow] {pending:,} documents still pending after this batch")

    total_in_session = _session_pdfs_done + pdfs_processed + pdfs_failed
    if max_pdfs is not None and total_in_session >= max_pdfs:
        print(f"[Flow] max_pdfs={max_pdfs} atingido na sessão ({total_in_session} PDFs) — encerrando cadeia")
        return

    batch_did_work = (pdfs_processed + pdfs_failed) > 0
    if pending > 0 and batch_did_work:
        from prefect.deployments import run_deployment  # noqa: PLC0415

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
                "_session_pdfs_done": _session_pdfs_done + pdfs_processed + pdfs_failed,
            },
            timeout=0,
        )
    elif pending > 0:
        print("[Flow] Batch processed nothing despite pending docs — all remaining may be at max retries. Stopping chain.")
    else:
        print("[Flow] Queue exhausted — no more batches to trigger")


def _get_flow_run_id():
    try:
        from prefect.runtime import flow_run as current_run  # noqa: PLC0415
        return current_run.id
    except Exception:
        return None

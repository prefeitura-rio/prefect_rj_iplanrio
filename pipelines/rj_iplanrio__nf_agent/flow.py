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

    reader = BQInputReader()

    # Snapshot status counts before the batch
    counts_before = reader.count_by_status(bq_input_table, bq_status_table)
    proc_before = counts_before.get("processado", {"docs": 0, "pdfs": 0})
    erro_before = counts_before.get("erro", {"docs": 0, "pdfs": 0})
    pend_before = counts_before.get("pendente", {"docs": 0, "pdfs": 0})
    print(f"[Flow] Before: processado={proc_before['docs']} docs/{proc_before['pdfs']} PDFs, "
          f"erro={erro_before['docs']} docs/{erro_before['pdfs']} PDFs, "
          f"pendente={pend_before['docs']} docs/{pend_before['pdfs']} PDFs")

    started_at = datetime.utcnow()

    _impl.fn(
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
    )

    finished_at = datetime.utcnow()
    duration_seconds = (finished_at - started_at).total_seconds()

    # Snapshot status counts after the batch
    counts_after = reader.count_by_status(bq_input_table, bq_status_table)
    proc_after = counts_after.get("processado", {"docs": 0, "pdfs": 0})
    erro_after = counts_after.get("erro", {"docs": 0, "pdfs": 0})
    pend_after = counts_after.get("pendente", {"docs": 0, "pdfs": 0})

    docs_processed = max(0, proc_after["docs"] - proc_before["docs"])
    pdfs_processed = max(0, proc_after["pdfs"] - proc_before["pdfs"])
    docs_failed    = max(0, erro_after["docs"] - erro_before["docs"])
    pdfs_failed    = max(0, erro_after["pdfs"] - erro_before["pdfs"])

    # pending = still needs processing (pendente + erro)
    pending_docs = pend_after["docs"] + erro_after["docs"]
    pending_pdfs = pend_after["pdfs"] + erro_after["pdfs"]

    avg_sec_per_pdf  = round(duration_seconds / pdfs_processed,  2) if pdfs_processed  > 0 else 0.0
    avg_sec_per_doc  = round(duration_seconds / docs_processed,  2) if docs_processed  > 0 else 0.0
    est_remaining_min = round(pending_pdfs * avg_sec_per_pdf / 60, 1) if avg_sec_per_pdf > 0 else None

    print(
        f"\n[Flow] ── Batch summary ──────────────────────\n"
        f"[Flow]   Session:        {session_id}\n"
        f"[Flow]   Processed:      {pdfs_processed} PDFs / {docs_processed} docs\n"
        f"[Flow]   Failed:         {pdfs_failed} PDFs / {docs_failed} docs\n"
        f"[Flow]   Duration:       {duration_seconds / 60:.1f} min\n"
        f"[Flow]   Avg / PDF:      {avg_sec_per_pdf:.1f} sec\n"
        f"[Flow]   Avg / doc:      {avg_sec_per_doc:.1f} sec\n"
        f"[Flow]   Pending:        {pending_pdfs} PDFs / {pending_docs} docs\n"
        + (f"[Flow]   Est. remaining: ~{est_remaining_min} min\n" if est_remaining_min else "")
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
                    "pending_pdfs": pending_pdfs,
                    "pending_docs": pending_docs,
                    "avg_sec_per_pdf": avg_sec_per_pdf,
                    "avg_sec_per_doc": avg_sec_per_doc,
                    "batch_size": batch_size,
                    "workers": workers,
                    "requests_per_minute": requests_per_minute,
                    "max_concurrent": max_concurrent,
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

    pending = reader.count_pending(bq_input_table, bq_status_table, max_retries=max_retries)
    print(f"[Flow] {pending:,} documents still pending after this batch")

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

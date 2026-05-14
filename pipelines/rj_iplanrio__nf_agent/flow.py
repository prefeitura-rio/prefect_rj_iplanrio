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
    session_id: str | None = None,
) -> None:
    import sys
    import uuid
    from datetime import datetime

    sys.path.insert(0, "/opt/agent-nf-validator/run_poc")
    from run_poc.run_pipeline import nf_processing_flow as _impl  # noqa: PLC0415
    from run_poc.bq_input_reader import BQInputReader  # noqa: PLC0415

    # Generate session_id if this is the first flow in the chain
    if not session_id:
        session_id = str(uuid.uuid4())
        print(f"[Flow] New session started: {session_id}")
    else:
        print(f"[Flow] Continuing session: {session_id}")

    reader = BQInputReader()

    # Snapshot status counts before the batch
    counts_before = reader.count_by_status(bq_input_table, bq_status_table)
    processado_before = counts_before.get("processado", 0)
    erro_before = counts_before.get("erro", 0)
    print(f"[Flow] Before: processado={processado_before}, erro={erro_before}, "
          f"pendente={counts_before.get('pendente', 0)}")

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
    )

    finished_at = datetime.utcnow()
    duration_seconds = (finished_at - started_at).total_seconds()

    # Snapshot status counts after the batch
    counts_after = reader.count_by_status(bq_input_table, bq_status_table)
    processado_after = counts_after.get("processado", 0)
    erro_after = counts_after.get("erro", 0)
    pending_after = counts_after.get("pendente", 0)

    pdfs_processed = max(0, processado_after - processado_before)
    pdfs_failed = max(0, erro_after - erro_before)
    avg_sec_per_pdf = round(duration_seconds / pdfs_processed, 2) if pdfs_processed > 0 else 0.0
    pending_total = pending_after + erro_after  # pendente + erro = still needs processing
    est_remaining_min = round(pending_total * avg_sec_per_pdf / 60, 1) if avg_sec_per_pdf > 0 else None

    print(
        f"\n[Flow] ── Batch summary ──────────────────────\n"
        f"[Flow]   Session:        {session_id}\n"
        f"[Flow]   Processed:      {pdfs_processed} PDFs\n"
        f"[Flow]   Failed:         {pdfs_failed} PDFs\n"
        f"[Flow]   Duration:       {duration_seconds / 60:.1f} min\n"
        f"[Flow]   Avg / PDF:      {avg_sec_per_pdf:.1f} sec\n"
        f"[Flow]   Pending:        {pending_total} PDFs\n"
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
                    "pdfs_failed": pdfs_failed,
                    "pending_after": pending_total,
                    "avg_sec_per_pdf": avg_sec_per_pdf,
                    "batch_size": batch_size,
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

    pending = reader.count_pending(bq_input_table, bq_status_table)
    print(f"[Flow] {pending:,} documents still pending after this batch")

    if pending > 0:
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
                "session_id": session_id,
            },
            timeout=0,
        )
    else:
        print("[Flow] Queue exhausted — no more batches to trigger")


def _get_flow_run_id():
    try:
        from prefect.runtime import flow_run as current_run  # noqa: PLC0415
        return current_run.id
    except Exception:
        return None

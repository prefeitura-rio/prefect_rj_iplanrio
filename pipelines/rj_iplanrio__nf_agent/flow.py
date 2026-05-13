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
    batch_size: int = 1000,
    gcs_output_base_path: str = "staging/brutos_poc_osinfo_ia/resultado_extracao_modelo",
    db_path: str = "/tmp/nf_pipeline_cache.db",
    gcs_bucket: str | None = None,
    workers: int = 200,
    mode: str = "full",
    requests_per_minute: int = 600,
    max_concurrent: int = 50,
) -> None:
    import sys

    sys.path.insert(0, "/opt/agent-nf-validator/run_poc")
    from run_poc.run_pipeline import nf_processing_flow as _impl  # noqa: PLC0415

    # Call .fn to run the implementation inside this flow's context without
    # creating a nested subflow run.
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

    # Use the runtime deployment ID to self-trigger — avoids hardcoding the name.
    from prefect.runtime import flow_run as current_run  # noqa: PLC0415

    deployment_id = current_run.deployment_id
    if not deployment_id:
        print("[Flow] Not running as a deployment — skipping self-trigger check")
        return

    from run_poc.bq_input_reader import BQInputReader  # noqa: PLC0415
    from prefect.deployments import run_deployment  # noqa: PLC0415

    reader = BQInputReader()
    pending = reader.count_pending(bq_input_table, bq_status_table)
    print(f"[Flow] {pending:,} documents still pending after this batch")

    if pending > 0:
        print(f"[Flow] Triggering next batch (deployment_id={deployment_id})")
        run_deployment(
            name=deployment_id,  # UUID accepted directly, no name lookup needed
            parameters={
                "bq_input_table": bq_input_table,
                "bq_status_table": bq_status_table,
                "batch_size": batch_size,
                "gcs_output_base_path": gcs_output_base_path,
                "db_path": db_path,
                "gcs_bucket": gcs_bucket,
                "workers": workers,
                "mode": mode,
                "requests_per_minute": requests_per_minute,
                "max_concurrent": max_concurrent,
            },
            timeout=0,  # fire-and-forget, don't block waiting for next run
        )
    else:
        print("[Flow] Queue exhausted — no more batches to trigger")

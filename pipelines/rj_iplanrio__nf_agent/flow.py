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
    )

"""Flow for rj_smfp__nota_carioca_oracle_to_bq."""

from prefect import flow

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import DEFAULT_TABLES
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.tasks import (
    cleanup_task,
    extract_table_task,
    load_table_task,
    plan_table_task,
    publish_tables_task,
    take_snapshot_task,
    validate_table_task,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions


@flow(log_prints=True)
def rj_smfp__nota_carioca_oracle_to_bq(  # noqa: PLR0913
    project: str = "rj-iplanrio-dia",
    dataset_id: str = "brutos_nota_fiscal_staging",
    table_ids: list[str] | None = None,
    source_schema: str | None = None,
    gcs_bucket: str = "rj-iplanrio-dia-bq-to-oracle",
    infisical_secret_path: str = "/db-oracle-nota-fiscal",
    workers: int = 8,
    chunk_size_blocks: int = 32768,
    batch_rows: int = 50_000,
    progress_interval_seconds: int = 30,
) -> None:
    rename_current_flow_run_task(new_name=dataset_id)
    inject_bd_credentials_task(environment="prod")
    options = ExtractOptions(
        workers=workers,
        chunk_size_blocks=chunk_size_blocks,
        batch_rows=batch_rows,
        progress_interval_seconds=progress_interval_seconds,
    )
    snapshot = take_snapshot_task(infisical_secret_path=infisical_secret_path)
    plans = [
        plan_table_task(
            infisical_secret_path=infisical_secret_path,
            source_schema=source_schema,
            project=project,
            dataset_id=dataset_id,
            table_id=table_id,
            snapshot=snapshot,
        )
        for table_id in table_ids or DEFAULT_TABLES
    ]
    try:
        validated = []
        for table_plan in plans:
            extracted = extract_table_task(
                infisical_secret_path=infisical_secret_path,
                project=project,
                bucket=gcs_bucket,
                table_plan=table_plan,
                snapshot=snapshot,
                options=options,
            )
            loaded_rows = load_table_task(
                project=project, dataset_id=dataset_id, bucket=gcs_bucket, table_plan=table_plan, extracted=extracted
            )
            validated.append(
                validate_table_task(
                    infisical_secret_path=infisical_secret_path,
                    project=project,
                    dataset_id=dataset_id,
                    bucket=gcs_bucket,
                    table_plan=table_plan,
                    extracted=extracted,
                    snapshot=snapshot,
                    loaded_rows=loaded_rows,
                )
            )
        publish_tables_task(project=project, dataset_id=dataset_id, bucket=gcs_bucket, validated=validated)
    finally:
        cleanup_task(project=project, dataset_id=dataset_id, bucket=gcs_bucket, plans=plans)

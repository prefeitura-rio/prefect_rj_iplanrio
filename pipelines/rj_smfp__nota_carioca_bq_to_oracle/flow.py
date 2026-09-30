"""Flow for rj_smfp__nota_carioca_bq_to_oracle."""

from prefect import flow

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.tasks import (
    delete_gcs_files_task,
    ensure_oracle_table_task,
    extract_table_to_gcs_task,
    get_table_schema_task,
    list_tables_task,
    load_into_oracle_task,
    plan_load_task,
    truncate_oracle_table_task,
    validate_row_count_task,
)


@flow(log_prints=True)
def rj_smfp__nota_carioca_bq_to_oracle(  # noqa: PLR0913
    project: str = "rj-iplanrio-dia",
    dataset_id: str = "nota_carioca_staging",
    table_ids: list[str] | None = None,
    gcs_bucket: str = "rj-iplanrio-dia-bq-to-oracle",
    infisical_secret_path: str = "/db-oracle-nota-fiscal",
    sqlldr_sessions: int = 2,
    template_schema: str | None = None,
) -> None:
    rename_current_flow_run_task(new_name=dataset_id)
    inject_bd_credentials_task(environment="prod")
    tables = list_tables_task(project=project, dataset_id=dataset_id, table_ids=table_ids)

    for table_id in tables:
        table_schema = get_table_schema_task(project=project, dataset_id=dataset_id, table_id=table_id)
        plan = plan_load_task(
            infisical_secret_path=infisical_secret_path,
            template_schema=template_schema,
            table_id=table_id,
            table_schema=table_schema,
        )
        oracle_table = ensure_oracle_table_task(
            infisical_secret_path=infisical_secret_path,
            project=project,
            dataset_id=dataset_id,
            table_id=table_id,
            plan=plan,
        )
        blob_names = extract_table_to_gcs_task(
            project=project, dataset_id=dataset_id, table_id=table_id, bucket=gcs_bucket
        )
        empty_table = truncate_oracle_table_task(infisical_secret_path=infisical_secret_path, table=oracle_table)
        loaded_rows = load_into_oracle_task(
            infisical_secret_path=infisical_secret_path,
            table=empty_table,
            plan=plan,
            bucket=gcs_bucket,
            blob_names=blob_names,
            sessions=sqlldr_sessions,
        )
        validated_rows = validate_row_count_task(
            infisical_secret_path=infisical_secret_path,
            table=empty_table,
            table_schema=table_schema,
            loaded_rows=loaded_rows,
        )
        delete_gcs_files_task(project=project, bucket=gcs_bucket, blob_names=blob_names, wait_for=[validated_rows])

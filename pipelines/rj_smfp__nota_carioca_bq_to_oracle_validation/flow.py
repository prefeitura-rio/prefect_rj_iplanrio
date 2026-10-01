"""Flow for rj_smfp__nota_carioca_bq_to_oracle_validation."""

from prefect import flow

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.tasks import (
    list_tables_task,
    log_session_task,
    summarize_task,
    validate_table_task,
)


@flow(log_prints=True)
def rj_smfp__nota_carioca_bq_to_oracle_validation(  # noqa: PLR0913
    project: str = "rj-iplanrio-dia",
    dataset_id: str = "nota_carioca_staging",
    table_ids: list[str] | None = None,
    infisical_secret_path: str = "/db-oracle-nota-fiscal",
    compute_column_metrics: bool = True,
    fail_on_divergence: bool = True,
    template_schema: str | None = None,
    excluded_template_columns: list[str] | None = None,
) -> None:
    rename_current_flow_run_task(new_name=f"validacao-{dataset_id}")
    inject_bd_credentials_task(environment="prod")
    log_session_task(infisical_secret_path=infisical_secret_path)
    tables = list_tables_task(project=project, dataset_id=dataset_id, table_ids=table_ids)

    results = [
        validate_table_task(
            infisical_secret_path=infisical_secret_path,
            project=project,
            dataset_id=dataset_id,
            table_id=table_id,
            template_schema=template_schema,
            excluded_template_columns=excluded_template_columns,
            compute_column_metrics=compute_column_metrics,
        )
        for table_id in tables
    ]
    summarize_task(results=results, fail_on_divergence=fail_on_divergence)

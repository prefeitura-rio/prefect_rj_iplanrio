"""Flow for rj_smfp__nota_carioca_bq_to_oracle_validation."""

from typing import Literal

from prefect import flow

from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.tasks import (
    list_tables_task,
    log_session_task,
    summarize_task,
    validate_table_task,
)


@flow(log_prints=True, flow_run_name="validacao-{dataset_id}")
def rj_smfp__nota_carioca_bq_to_oracle_validation(  # noqa: PLR0913
    project: str = "rj-iplanrio-dia",
    dataset_id: str = "nota_carioca_staging",
    table_ids: list[str] | None = None,
    compute_column_metrics: bool = True,
    fail_on_divergence: bool = True,
    template_schema: str | None = None,
    excluded_template_columns: list[str] | None = None,
    raw_text_encoding: Literal["base64", "hex"] = "base64",
) -> None:
    log_session_task()
    tables = list_tables_task(project=project, dataset_id=dataset_id, table_ids=table_ids)

    results = [
        validate_table_task(
            project=project,
            dataset_id=dataset_id,
            table_id=table_id,
            template_schema=template_schema,
            excluded_template_columns=excluded_template_columns,
            compute_column_metrics=compute_column_metrics,
            raw_text_encoding=raw_text_encoding,
        )
        for table_id in tables
    ]
    summarize_task(results=results, fail_on_divergence=fail_on_divergence)

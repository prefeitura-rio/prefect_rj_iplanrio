# -*- coding: utf-8 -*-
"""
Flow para gerar o relatório de sessões e clientes únicos.
"""
from datetime import date, timedelta
import pendulum

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.env import getenv_or_action, inject_bd_credentials_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task  # pylint: disable=E0611, E0401
from prefect import flow  # pylint: disable=E0611, E0401
from prefect.client.schemas.objects import Flow, FlowRun, State  # pylint

from pipelines.rj_crm__relatorio_cvl.constants import PipelineConstants
from pipelines.rj_crm__relatorio_cvl.tasks import calculate_24h_sessions, get_first_and_last_day_of_previous_month
from pipelines.rj_crm__disparo_template.utils.tasks import create_date_partitions, task_download_data_from_bigquery

@flow(log_prints=True, name="rj_crm__relatorio_cvl")
def rj_crm__relatorio_cvl_flow(
    # Parâmetros opcionais para override manual na UI.
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    query: str | None = None,
    start_date: date = None,
    end_date: date = None,
):
    dataset_id = dataset_id or PipelineConstants.DATASET_ID.value
    table_id = table_id or PipelineConstants.TABLE_ID.value
    dump_mode = dump_mode or PipelineConstants.DUMP_MODE.value
    # query = query or PipelineConstants.QUERY.value
    billing_project_id =  PipelineConstants.BILLING_PROJECT_ID.value

    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")  # pylint: disable=unused-variable
    crd = inject_bd_credentials_task(environment="prod")  # noqa  # pylint: disable=unused-variable

    # Default date parameters
    if start_date is None:
        start_date, end_date = get_first_and_last_day_of_previous_month()
    
    report_month = start_date[:7] 

    # Read the query from query.sql
    with open("pipelines/rj_crm__relatorio_cvl/query.sql", "r", encoding="utf-8") as f:
        query_template = f.read()

    query = query_template.replace("{START_DATE_PLACEHOLDER}", start_date)
    query = query.replace("{END_DATE_PLACEHOLDER}", end_date)

    print(f"Formatted Query: {query}")

    df_raw = task_download_data_from_bigquery(
        query=query, billing_project_id=billing_project_id, bucket_name=billing_project_id
    )

    if df_raw.empty:
        print("No data returned from BigQuery query. Skipping further processing.")
        return

    df_estatistica_mes, df_sessions = calculate_24h_sessions(df_raw, report_month)

    data_path = create_date_partitions(df_sessions, partition_column="inicio_datetime", file_format="parquet")
    
    create_table_and_upload_to_gcs_task(
            data_path=data_path,
            dataset_id=PipelineConstants.DATASET_ID.value,
            table_id=PipelineConstants.TABLE_ID.value,
            dump_mode=PipelineConstants.DUMP_MODE.value,
        )
    # print("force deploy")
    print("Flow completed successfully!")

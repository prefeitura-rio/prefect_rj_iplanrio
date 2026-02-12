# -*- coding: utf-8 -*-
"""
Flow para gerar o relatório de sessões e clientes únicos.
"""
from datetime import date, timedelta
import pendulum

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.env import inject_bd_credentials_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task  # pylint: disable=E0611, E0401
from prefect import flow  # pylint: disable=E0611, E0401

from pipelines.rj_crm__relatorio_cvl.constants import PipelineConstants
from pipelines.rj_crm__relatorio_cvl.tasks import calculate_24h_sessions, get_first_and_last_day_of_previous_month
from pipelines.rj_crm__disparo_template.utils.tasks import create_date_partitions, task_download_data_from_bigquery

@flow(log_prints=True, name="rj_crm__relatorio_cvl")
def rj_crm__relatorio_cvl(
    # Parâmetros opcionais para override manual na UI.
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    query: str | None = None,
    start_date: str = None,
    end_date: str = None,
):
    """
    Flow para gerar o relatório de sessões e clientes únicos.

    Args:
        dataset_id (str, optional): ID do dataset no BigQuery. Se None, usa o valor de PipelineConstants.DATASET_ID.
        table_id (str, optional): ID da tabela no BigQuery. Se None, usa o valor de PipelineConstants.TABLE_ID.
        dump_mode (str, optional): Modo de despejo de dados (append, overwrite, etc.). Se None, usa o valor de PipelineConstants.DUMP_MODE.
        query (str, optional): Query SQL a ser executada. Se None, usa o valor do arquivo query.sql.
        start_date (str, optional): Data de início para a query (formato 'YYYY-MM-DD'). Se None, calcula o primeiro dia do mês anterior.
        end_date (str, optional): Data de fim para a query (formato 'YYYY-MM-DD'). Se None, calcula o último dia do mês anterior.
    """

    dataset_id = dataset_id or PipelineConstants.DATASET_ID.value
    table_id = table_id or PipelineConstants.TABLE_ID.value
    dump_mode = dump_mode or PipelineConstants.DUMP_MODE.value
    billing_project_id =  PipelineConstants.BILLING_PROJECT_ID.value

    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")  # pylint: disable=unused-variable
    crd = inject_bd_credentials_task(environment="prod")  # noqa  # pylint: disable=unused-variable

    # Default date parameters
    if start_date is None or end_date is None:
        start_date, end_date = get_first_and_last_day_of_previous_month()
    
    report_month = start_date[:7] 

    # Read the query from query.sql
    if not query:
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

    df_estatistica_mes_, df_sessions = calculate_24h_sessions(df_raw, report_month)

    file_format="csv"

    data_path = create_date_partitions(
        df_sessions,
        partition_column="inicio_datetime",
        file_format=file_format,
        filename=f"Sessoes_receptivo_{start_date}_to_{end_date}",
    )
    
    create_table_and_upload_to_gcs_task(
            data_path=data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            # source_format=file_format,
    )
    print("force deploy")
    print("Flow completed successfully!")

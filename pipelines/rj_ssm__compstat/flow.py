# -*- coding: utf-8 -*-
"""
This flow is used to dump the database to the BIGQUERY
"""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_ssm__compstat.task import json_compstat_to_dataframe


@flow(log_prints=True)
def rj_ssm__compstat(
    # Parâmetros opcionais para override manual na UI
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    result: str | None = None,
    endpoint: str | None = None,
):
    """
    Flow para extrair dados da API Compstat e carregar no BigQuery.

    Este flow coleta da API Compstat

    Args:
        dataset_id: ID do dataset no BigQuery (default: brutos_Compstat)
        table_id: ID da tabela no BigQuery
        dump_mode: Modo de dump (default: overwrite)
    """

    # Renomear flow run para melhor identificação
    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")

    output_path = json_compstat_to_dataframe(
        table_id=table_id,
        result=result,
        endpoint=endpoint
    )

    # Upload para GCS e BigQuery
    create_table_and_upload_to_gcs_task(
        data_path=output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )
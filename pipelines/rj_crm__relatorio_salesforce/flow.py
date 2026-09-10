# -*- coding: utf-8 -*-
"""
Flow para o relatorio mensal de sessoes Agentforce/WhatsApp (Salesforce Data
Cloud) -> rj-crm-registry. Gera 3 tabelas: sessoes brutas, sessoes mescladas
em janela de 24h e sessoes mescladas em janela de 2h.
"""

from typing import Optional

import pendulum
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__relatorio_salesforce.constants import PipelineConstants
from pipelines.rj_crm__relatorio_salesforce.tasks import (
    authenticate_salesforce,
    check_month_not_loaded,
    create_date_partitions,
    enrich_sessions_with_messaging_data,
    fetch_raw_agentforce_sessions,
    get_first_and_last_day_of_previous_month,
    merge_agentforce_sessions,
)


@flow(log_prints=True, name="rj_crm__relatorio_salesforce")
def rj_crm__relatorio_salesforce(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    dump_mode: Optional[str] = None,
    fail_on_duplicate_month: bool = False,
):
    """
    Args:
        start_date: Primeiro dia do periodo (YYYY-MM-DD). Se None, usa o
            primeiro dia do mes anterior. Nunca busca antes de
            PipelineConstants.DATA_INICIO_HISTORICO, mesmo se informado.
        end_date: Ultimo dia do periodo, inclusive (YYYY-MM-DD). Se None,
            usa o ultimo dia do mes anterior.
        dump_mode: Modo de despejo no BigQuery ("append"/"overwrite"). Se
            None, usa PipelineConstants.DUMP_MODE.
        fail_on_duplicate_month: Se True, aborta o flow quando o mes-alvo ja
            tiver dados na tabela de destino, em vez de apenas avisar.
    """
    dump_mode = dump_mode or PipelineConstants.DUMP_MODE.value

    rename_current_flow_run_task(new_name="relatorio_salesforce")
    inject_bd_credentials_task(environment="prod")

    if start_date is None or end_date is None:
        start_date, end_date = get_first_and_last_day_of_previous_month()

    report_month = start_date[:7]
    end_date_exclusive = pendulum.parse(end_date).add(days=1).to_date_string()

    access_token, instance_url, dataspace = authenticate_salesforce()

    df_sessions = fetch_raw_agentforce_sessions(
        access_token, instance_url, dataspace, start_date, end_date_exclusive
    )
    if df_sessions.empty:
        print("Nenhuma sessao encontrada no periodo. Encerrando.")
        return

    df_raw = enrich_sessions_with_messaging_data(access_token, instance_url, df_sessions)
    if df_raw.empty:
        print("Nenhuma sessao com MessagingSession valida encontrada. Encerrando.")
        return

    check_month_not_loaded(
        PipelineConstants.RAW_DATASET_ID.value,
        PipelineConstants.RAW_TABLE_ID.value,
        report_month,
        fail_on_duplicate_month,
    )
    raw_path = create_date_partitions(
        df_raw, partition_column="inicio_sessao", file_format="csv", root_folder="./data_sessoes/"
    )
    create_table_and_upload_to_gcs_task(
        data_path=raw_path,
        dataset_id=PipelineConstants.RAW_DATASET_ID.value,
        table_id=PipelineConstants.RAW_TABLE_ID.value,
        dump_mode=dump_mode,
    )

    thresholds = (
        (PipelineConstants.THRESHOLD_SECONDS_24H.value, PipelineConstants.MERGED_TABLE_24H_ID.value),
        (PipelineConstants.THRESHOLD_SECONDS_2H.value, PipelineConstants.MERGED_TABLE_2H_ID.value),
    )
    for threshold_seconds, table_id in thresholds:
        df_merged = merge_agentforce_sessions(df_raw, threshold_seconds=threshold_seconds)
        check_month_not_loaded(
            PipelineConstants.MERGED_DATASET_ID.value, table_id, report_month, fail_on_duplicate_month
        )
        # root_folder dedicado por tabela: cada chamada a create_date_partitions
        # precisa de uma pasta propria, senao a segunda/terceira chamada mistura
        # arquivos de schemas diferentes na mesma estrutura ano/mes/dia.
        merged_path = create_date_partitions(
            df_merged, partition_column="inicio_sessao", file_format="csv", root_folder=f"./data_{table_id}/"
        )
        create_table_and_upload_to_gcs_task(
            data_path=merged_path,
            dataset_id=PipelineConstants.MERGED_DATASET_ID.value,
            table_id=table_id,
            dump_mode=dump_mode,
        )

    print("Flow concluido com sucesso!")

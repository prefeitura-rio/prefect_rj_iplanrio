# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Dispatch Flow for pic lembrete using a wrapper flow.
"""
from prefect import flow
from pipelines.rj_smas__disparo_pic.flow import rj_smas__disparo_pic as default_flow
from pipelines.rj_smas__disparo_pic_lembrete.constants import PicLembreteConstants

@flow(
    name="SMAS: Disparo PIC Lembrete DP",
    description="Flow para disparo de lembretes do PIC (versão DP)",
    log_prints=True
)
def rj_smas__disparo_pic_lembrete_dp(
    # Parâmetros do fluxo com defaults de PicLembreteConstants
    id_hsm: int | None = PicLembreteConstants.PIC_LEMBRETE_ID_HSM.value,
    campaign_name: str | None = PicLembreteConstants.PIC_LEMBRETE_CAMPAIGN_NAME.value,
    cost_center_id: int | None = PicLembreteConstants.PIC_LEMBRETE_COST_CENTER_ID.value,
    chunk_size: int | None = PicLembreteConstants.PIC_LEMBRETE_CHUNK_SIZE.value,
    dataset_id: str | None = PicLembreteConstants.PIC_LEMBRETE_DATASET_ID.value,
    table_id: str | None = PicLembreteConstants.PIC_LEMBRETE_TABLE_ID.value,
    dump_mode: str | None = PicLembreteConstants.PIC_LEMBRETE_DUMP_MODE.value,
    query: str | None = PicLembreteConstants.PIC_QUERY.value,
    query_dispatch_approved: str | None = PicLembreteConstants.PIC_QUERY_DISPATCH_APPROVED.value,
    query_processor_name: str | None = PicLembreteConstants.PIC_LEMBRETE_QUERY_PROCESSOR_NAME.value,
    test_mode: bool | None = PicLembreteConstants.PIC_LEMBRETE_TEST_MODE.value,
    sleep_minutes: int | None = 5,
    dispatch_approved_col: str | None = PicLembreteConstants.DISPATCH_APPROVED_COL.value,
    dispatch_date_col: str | None = PicLembreteConstants.DISPATCH_DATE_COL.value,
    event_date_col: str | None = PicLembreteConstants.EVENT_DATE_COL.value,
    infisical_secret_path: str = "/wetalkie",
    whitelist_percentage: int = 30,
    whitelist_environment: str = "staging",
):
    """
    Este é um fluxo wrapper que chama o fluxo de disparo PIC padrão
    com parâmetros específicos para o envio de lembretes.
    """
    print("Iniciando fluxo wrapper para disparo de lembrete PIC.")

    # Chama o fluxo original passando todos os parâmetros
    default_flow(
        id_hsm=id_hsm,
        campaign_name=campaign_name,
        cost_center_id=cost_center_id,
        chunk_size=chunk_size,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        query=query,
        query_dispatch_approved=query_dispatch_approved,
        query_processor_name=query_processor_name,
        test_mode=test_mode,
        sleep_minutes=sleep_minutes,
        dispatch_approved_col=dispatch_approved_col,
        dispatch_date_col=dispatch_date_col,
        event_date_col=event_date_col,
        infisical_secret_path=infisical_secret_path,
        whitelist_percentage=whitelist_percentage,
        whitelist_environment=whitelist_environment,
    )
    print("Fluxo wrapper para disparo de lembrete PIC finalizado.")
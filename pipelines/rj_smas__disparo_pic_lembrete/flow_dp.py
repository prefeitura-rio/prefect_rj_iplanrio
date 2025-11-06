# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Dispatch Flow for pic lembrete using deepcopy
"""
from copy import deepcopy

from prefect import flow  # pylint: disable=E0611, E0401

from pipelines.rj_smas__disparo_pic.flow import rj_smas__disparo_pic as default_flow  # pylint: disable=E0611, E0401
from pipelines.rj_smas__disparo_pic_lembrete.constants import PicLembreteConstants  # pylint: disable=E0611, E0401


rj_smas__disparo_pic_lembrete_dp = deepcopy(default_flow)
rj_smas__disparo_pic_lembrete_dp.name = "SMAS: Disparo PIC Lembrete deepbopy"

# Override parameters with values from PicLembreteConstants
rj_smas__disparo_pic_lembrete_dp.parameters = {
    "id_hsm": PicLembreteConstants.PIC_LEMBRETE_ID_HSM.value,
    "campaign_name": PicLembreteConstants.PIC_LEMBRETE_CAMPAIGN_NAME.value,
    "cost_center_id": PicLembreteConstants.PIC_LEMBRETE_COST_CENTER_ID.value,
    "chunk_size": PicLembreteConstants.PIC_LEMBRETE_CHUNK_SIZE.value,
    "dataset_id": PicLembreteConstants.PIC_LEMBRETE_DATASET_ID.value,
    "table_id": PicLembreteConstants.PIC_LEMBRETE_TABLE_ID.value,
    "dump_mode": PicLembreteConstants.PIC_LEMBRETE_DUMP_MODE.value,
    "query": PicLembreteConstants.PIC_LEMBRETE_QUERY.value,
    "query_dispatch_approved": PicLembreteConstants.PIC_LEMBRETE_QUERY_DISPATCH_APPROVED.value,
    "query_processor_name": PicLembreteConstants.PIC_LEMBRETE_QUERY_PROCESSOR_NAME.value,
    "test_mode": PicLembreteConstants.PIC_LEMBRETE_TEST_MODE.value,
    "sleep_minutes": PicLembreteConstants.PIC_LEMBRETE_SLEEP_MINUTES.value,
    "dispatch_approved_col": PicLembreteConstants.DISPATCH_APPROVED_COL.value,
    "dispatch_date_col": PicLembreteConstants.DISPATCH_DATE_COL.value,
    "event_date_col": PicLembreteConstants.EVENT_DATE_COL.value,
    "infisical_secret_path": "/wetalkie",
}

rj_smas__disparo_pic_lembrete_dp.description = "Flow para disparo de lembretes do PIC"


# This wrapper flow is needed to allow overriding parameters from the UI
@flow(log_prints=True)
def rj_smas__disparo_pic_lembrete_dp_wrapper(
    id_hsm: int | None = None,
    campaign_name: str | None = None,
    cost_center_id: int | None = None,
    chunk_size: int | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    query: str | None = None,
    query_dispatch_approved: str | None = None,
    query_processor_name: str | None = None,
    test_mode: bool | None = True,
    sleep_minutes: int | None = 5,
    dispatch_approved_col: str | None = "APROVACAO_DISPARO_LEMBRETE",
    dispatch_date_col: str | None = "DATA_DISPARO_LEMBRETE",
    event_date_col: str | None = "DATA_ENTREGA",
    infisical_secret_path: str = "/wetalkie",
):
    rj_smas__disparo_pic_lembrete_dp(
        id_hsm=id_hsm or PicLembreteConstants.PIC_LEMBRETE_ID_HSM.value,
        campaign_name=campaign_name or PicLembreteConstants.PIC_LEMBRETE_CAMPAIGN_NAME.value,
        cost_center_id=cost_center_id or PicLembreteConstants.PIC_LEMBRETE_COST_CENTER_ID.value,
        chunk_size=chunk_size or PicLembreteConstants.PIC_LEMBRETE_CHUNK_SIZE.value,
        dataset_id=dataset_id or PicLembreteConstants.PIC_LEMBRETE_DATASET_ID.value,
        table_id=table_id or PicLembreteConstants.PIC_LEMBRETE_TABLE_ID.value,
        dump_mode=dump_mode or PicLembreteConstants.PIC_LEMBRETE_DUMP_MODE.value,
        query=query or PicLembreteConstants.PIC_LEMBRETE_QUERY.value,
        query_dispatch_approved=query_dispatch_approved or PicLembreteConstants.PIC_LEMBRETE_QUERY_DISPATCH_APPROVED.value,
        query_processor_name=query_processor_name or PicLembreteConstants.PIC_LEMBRETE_QUERY_PROCESSOR_NAME.value,
        test_mode=test_mode if test_mode is not None else PicLembreteConstants.PIC_LEMBRETE_TEST_MODE.value,
        sleep_minutes=sleep_minutes or PicLembreteConstants.PIC_LEMBRETE_SLEEP_MINUTES.value,
        dispatch_approved_col=dispatch_approved_col or PicLembreteConstants.DISPATCH_APPROVED_COL.value,
        dispatch_date_col=dispatch_date_col or PicLembreteConstants.DISPATCH_DATE_COL.value,
        event_date_col=event_date_col or PicLembreteConstants.EVENT_DATE_COL.value,
        infisical_secret_path=infisical_secret_path,
    )
# force deploy
# force deploy


# -*- coding: utf-8 -*-
"""
Flow de captura de dados do SERPRO
"""
from prefect import flow

from pipelines.capture__serpro.constants import AUTUACAO_TABLE_ID, SOURCES
from pipelines.capture__serpro.tasks import create_serpro_extractor
from pipelines.common.capture.default_capture.flow import create_capture_flows_default_tasks
from pipelines.common.capture.default_capture.utils import rename_capture_flow_run


@flow(log_prints=True, flow_run_name=rename_capture_flow_run)
def capture__serpro(
    env=None,
    timestamp=None,
    recapture=False,
    recapture_days=2,
    recapture_timestamps=None,
) -> list[str]:
    create_capture_flows_default_tasks(
        env=env,
        sources=SOURCES,
        source_table_ids=(AUTUACAO_TABLE_ID,),
        timestamp=timestamp,
        create_extractor_task=create_serpro_extractor,
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
    )

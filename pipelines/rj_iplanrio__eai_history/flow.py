# -*- coding: utf-8 -*-
"""
Upload eai messages history to bq.
"""

from typing import Optional

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__eai_history.tasks import fetch_history_data


@flow(log_prints=True)
def rj_iplanrio__eai_history(
    last_update: str = "2025-08-29",
    session_timeout_seconds: Optional[int] = 3600,
    use_whatsapp_format: bool = False,
    dataset_id: str = "brutos_eai_logs",
    table_id: str = "history",
):
    name = rename_current_flow_run_task(new_name=last_update)
    cred = inject_bd_credentials_task()
    data_path = fetch_history_data(
        last_update=last_update,
        session_timeout_seconds=session_timeout_seconds,
        use_whatsapp_format=use_whatsapp_format,
    )
    create_table_and_upload_to_gcs_task(data_path=data_path, dataset_id=dataset_id, table_id=table_id)

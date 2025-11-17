# -*- coding: utf-8 -*-
"""
Upload eai messages history to BQ.
"""

from typing import Optional

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.dbt import execute_dbt_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__eai_history.tasks import fetch_history_data, get_last_update


@flow(log_prints=True)
def rj_iplanrio__eai_history(  # noqa
    last_update: Optional[str] = None,
    last_checkpoint_id: Optional[str] = None,
    session_timeout_seconds: Optional[int] = 3600,
    use_whatsapp_format: bool = False,
    dataset_id: str = "brutos_eai_logs",
    table_id: str = "history",
    max_user_save_limit: int = 100,
    environment: str = "staging",
    dbt_select: str = "--select raw_eai_logs_history",
):
    rename_current_flow_run_task(new_name=environment)
    inject_bd_credentials_task()

    last_update_task, last_checkpoint_id_task = get_last_update(
        dataset_id=dataset_id,
        table_id=table_id,
        last_update=last_update,
        last_checkpoint_id=last_checkpoint_id,
        environment=environment,
    )

    data_path = fetch_history_data(
        last_update=last_update_task,
        last_checkpoint_id_task=last_checkpoint_id_task,
        session_timeout_seconds=session_timeout_seconds,
        use_whatsapp_format=use_whatsapp_format,
        max_user_save_limit=max_user_save_limit,
        environment=environment,
    )
    create_table_and_upload_to_gcs_task(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        biglake_table=True,
        dump_mode="append",
    )
    execute_dbt_task(select=dbt_select, target="prod")

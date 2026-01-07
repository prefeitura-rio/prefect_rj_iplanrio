# -*- coding: utf-8 -*-
import os
from prefect import flow
from iplanrio.pipelines_utils.bd import (
    create_table_and_upload_to_gcs_task,
)
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task

from pipelines.rj_iplanrio__betterstack_api.constants import BetterStackConstants
from pipelines.rj_iplanrio__betterstack_api.tasks import (
    get_betterstack_credentials,
    get_betterstack_monitor_id,
    calculate_date_range,
    fetch_incidents,
    transform_incidents,
)

from pipelines.rj_iplanrio__betterstack_api.utils.tasks import create_date_partitions


@flow(log_prints=True)
def rj_iplanrio__betterstack_api(
    from_date: str | None = None,
    to_date: str | None = None,
    dataset_id: str | None = None,
    billing_project_id: str | None = None,
):
    """
    Flow para extrair dados da BetterStack API (Incidents)
    e carregar no BigQuery.
    """

    # 0. Setup
    billing_project_id = billing_project_id or BetterStackConstants.BILLING_PROJECT_ID.value
    dataset_id = dataset_id or BetterStackConstants.DATASET_ID.value
    rename_current_flow_run_task(new_name=f"BetterStack_Incidents_{from_date or 'D-1'}")

    # 0.1 Inject BD Credentials
    inject_bd_credentials_task(environment="prod")

    # 1. Credentials
    token = get_betterstack_credentials()
    monitor_id = get_betterstack_monitor_id()


    # 2. Date Logic
    # For high-availability, we always fetch the last 24h.
    # The 'date' parameters remain for manual backfills if needed.
    date_range = calculate_date_range(from_date=from_date, to_date=to_date)
    
    # --- TABLE: Incidents ---
    raw_incidents = fetch_incidents(token=token, monitor_id=monitor_id, date_range=date_range)

    df_incidents = transform_incidents(raw_incidents)

    if not df_incidents.empty:
        path_incidents = create_date_partitions(
            dataframe=df_incidents,
            partition_column=BetterStackConstants.PARTITION_COLUMN.value,
            file_format=BetterStackConstants.FILE_FORMAT.value,
            root_folder=f"{BetterStackConstants.ROOT_FOLDER.value}/{BetterStackConstants.TABLE_ID_INCIDENTS.value}"
        )

        create_table_and_upload_to_gcs_task(
            data_path=path_incidents,
            dataset_id=dataset_id,
            table_id=BetterStackConstants.TABLE_ID_INCIDENTS.value,
            dump_mode=BetterStackConstants.DUMP_MODE.value,
            biglake_table=BetterStackConstants.BIGLAKE_TABLE.value,
            source_format=BetterStackConstants.FILE_FORMAT.value,
        )
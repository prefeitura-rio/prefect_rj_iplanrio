# -*- coding: utf-8 -*-
"""
Flow for rj_crm__wetalkie_api_hsm_info pipeline
"""

import os
from typing import Optional

from prefect import flow
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from iplanrio.pipelines_utils.logging import log

from pipelines.rj_crm__wetalkie_api_hsm_info.constants import Constants
from pipelines.rj_crm__wetalkie_api_hsm_info.tasks import (
    get_wetalkie_token,
    fetch_hsm_templates,
    transform_hsm_templates,
)
from pipelines.rj_crm__wetalkie_api_hsm_info.utils.tasks import create_date_partitions


@flow(log_prints=True)
def rj_crm__wetalkie_api_hsm_info(
    dataset_id: Optional[str] = None,
    table_id: Optional[str] = None,
    dump_mode: Optional[str] = None,
    overwrite_flow_run_name: Optional[str] = None,
):
    """
    Flow to collect HSM templates from WeTalkie API and load into BigQuery.
    """
    # 0. Setup
    dataset_id = dataset_id or Constants.DATASET_ID.value
    table_id = table_id or Constants.TABLE_ID.value
    dump_mode = dump_mode or Constants.DUMP_MODE.value

    # Rename flow run
    flow_run_name = overwrite_flow_run_name or f"hsm_templates_info_{dataset_id}_{table_id}"
    rename_current_flow_run_task(new_name=flow_run_name)

    # 1. Inject BD Credentials
    inject_bd_credentials_task(environment="prod")
    
    # FORCE PROJECT ID (Override environment to ensure correct billing/destination)
    os.environ["GOOGLE_CLOUD_PROJECT"] = Constants.BILLING_PROJECT_ID.value

    # 2. Extract
    log("Starting extraction...")
    token = get_wetalkie_token(infisical_secret_path=Constants.INFISICAL_SECRET_PATH.value)
    raw_data = fetch_hsm_templates(token=token)

    if not raw_data:
        log("No data found from API. Finishing flow.")
        return

    # 3. Transform
    log("Starting transformation...")
    df_transformed = transform_hsm_templates(data=raw_data)

    if df_transformed.empty:
        log("Transformed DataFrame is empty. Finishing flow.")
        return

    # 4. Partition
    log("Partitioning data...")
    data_path = create_date_partitions(
        dataframe=df_transformed,
        partition_column=Constants.PARTITION_COLUMN.value,
        file_format=Constants.FILE_FORMAT.value,
        root_folder=Constants.ROOT_FOLDER.value,
    )

    # 5. Load to BigQuery
    log(f"Loading data to BigQuery table {dataset_id}.{table_id}...")
    create_table_and_upload_to_gcs_task(
        data_path=data_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=Constants.BIGLAKE_TABLE.value,
        source_format=Constants.FILE_FORMAT.value,
    )
    
    log("Flow finished successfully.")

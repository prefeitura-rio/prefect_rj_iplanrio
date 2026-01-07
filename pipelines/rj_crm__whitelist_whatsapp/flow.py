# -*- coding: utf-8 -*-
"""
Flow for rj_crm__whitelist_whatsapp pipeline.
"""
from prefect import flow
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task

from pipelines.rj_crm__whitelist_whatsapp.constants import WhitelistConstants
from pipelines.rj_crm__whitelist_whatsapp.tasks import (
    get_whitelist_credentials,
    fetch_whitelist_data,
    transform_whitelist_data,
    generate_backfill_data,
)
from pipelines.rj_crm__whitelist_whatsapp.utils.tasks import create_date_partitions


@flow(log_prints=True)
def rj_crm__whitelist_whatsapp(
    dataset_id: str | None = None,
    billing_project_id: str | None = None,
    table_id: str | None = None,
    backfill: bool = False,
    environment: str = "prod",
):
    """
    Flow to extract the WhatsApp Whitelist and load it into BigQuery.
    """
    # 0. Setup
    billing_project_id = billing_project_id or WhitelistConstants.BILLING_PROJECT_ID.value
    dataset_id = dataset_id or WhitelistConstants.DATASET_ID.value
    table_id = table_id or WhitelistConstants.TABLE_ID.value
    
    flow_name = f"Whitelist_WhatsApp_{dataset_id}"
    if backfill:
        flow_name += "_BACKFILL"
    
    rename_current_flow_run_task(new_name=flow_name)

    # 0.1 Inject BD Credentials
    inject_bd_credentials_task(environment=environment)

    # 1. Credentials
    creds = get_whitelist_credentials()

    # 2. Fetch Data
    raw_data = fetch_whitelist_data(creds=creds)

    # 3. Transform Data (Standard or Backfill)
    if backfill:
        df_whitelist = generate_backfill_data(raw_data=raw_data)
    else:
        df_whitelist = transform_whitelist_data(raw_data=raw_data)

    # 4. Ingest into BigQuery
    if not df_whitelist.empty:
        log_path = create_date_partitions(
            dataframe=df_whitelist,
            partition_column=WhitelistConstants.PARTITION_COLUMN.value,
            file_format=WhitelistConstants.FILE_FORMAT.value,
            root_folder=f"{WhitelistConstants.ROOT_FOLDER.value}/{table_id}"
        )

        create_table_and_upload_to_gcs_task(
            data_path=log_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=WhitelistConstants.DUMP_MODE.value,
            biglake_table=WhitelistConstants.BIGLAKE_TABLE.value,
            source_format=WhitelistConstants.FILE_FORMAT.value,
        )
    else:
        print("No data found to ingest.")


if __name__ == "__main__":
    rj_crm__whitelist_whatsapp()

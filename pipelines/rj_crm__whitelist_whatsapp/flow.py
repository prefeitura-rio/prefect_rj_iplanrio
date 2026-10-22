"""
Flow for rj_crm__whitelist_whatsapp pipeline.
"""
from pathlib import Path

from prefect import flow

from pipelines.rj_crm__whitelist_whatsapp.constants import WhitelistConstants
from pipelines.rj_crm__whitelist_whatsapp.helpers.tasks import create_date_partitions
from pipelines.rj_crm__whitelist_whatsapp.tasks import (
    fetch_whitelist_data,
    generate_backfill_data,
    get_whitelist_credentials,
    transform_whitelist_data,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True)
async def rj_crm__whitelist_whatsapp(
    *,
    cloud: Cloud,
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


    # 0.1 Inject BD Credentials

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

        table = Table(dataset_id=dataset_id, table_id=table_id)
        await upload_files_to_gcs(
            Path(log_path),
            table,
            bucket=cloud["bucket"],
            dump_mode=WhitelistConstants.DUMP_MODE.value,
            source_format=WhitelistConstants.FILE_FORMAT.value,
            options=capture_options(WhitelistConstants.DUMP_MODE.value),
        )
        await publish_table_in_bq(table, cloud)
    else:
        print("No data found to ingest.")


if __name__ == "__main__":
    rj_crm__whitelist_whatsapp()

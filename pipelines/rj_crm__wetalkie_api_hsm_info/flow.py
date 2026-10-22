"""
Flow for rj_crm__wetalkie_api_hsm_info pipeline
"""

from pathlib import Path
from typing import Optional

from prefect import flow
from prefect.logging import get_logger

from pipelines.rj_crm__wetalkie_api_hsm_info.constants import Constants
from pipelines.rj_crm__wetalkie_api_hsm_info.helpers.tasks import create_date_partitions
from pipelines.rj_crm__wetalkie_api_hsm_info.tasks import (
    fetch_hsm_templates,
    get_wetalkie_token,
    transform_hsm_templates,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options

logger = get_logger("toolkit")


@flow(log_prints=True)
async def rj_crm__wetalkie_api_hsm_info(
    *,
    cloud: Cloud,
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

    # 1. Inject BD Credentials

    # 2. Extract
    logger.info("Starting extraction...")
    token = get_wetalkie_token(infisical_secret_path=Constants.INFISICAL_SECRET_PATH.value)
    raw_data = fetch_hsm_templates(token=token)

    if not raw_data:
        logger.info("No data found from API. Finishing flow.")
        return

    # 3. Transform
    logger.info("Starting transformation...")
    df_transformed = transform_hsm_templates(data=raw_data)

    if df_transformed.empty:
        logger.info("Transformed DataFrame is empty. Finishing flow.")
        return

    # 4. Partition
    logger.info("Partitioning data...")
    data_path = create_date_partitions(
        dataframe=df_transformed,
        partition_column=Constants.PARTITION_COLUMN.value,
        file_format=Constants.FILE_FORMAT.value,
        root_folder=Constants.ROOT_FOLDER.value,
    )

    # 5. Load to BigQuery
    logger.info(f"Loading data to BigQuery table {dataset_id}.{table_id}...")
    table = Table(dataset_id=dataset_id, table_id=table_id)
    await upload_files_to_gcs(
        Path(data_path),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="csv",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)
    
    logger.info("Flow finished successfully.")

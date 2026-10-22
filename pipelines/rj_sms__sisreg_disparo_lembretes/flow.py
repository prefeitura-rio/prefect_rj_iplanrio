"""
SISREG SMS Dispatch Pipeline
Sends SMS reminders for next day appointments via Wetalkie API
"""

from pathlib import Path

from prefect import flow
from prefect.logging import get_logger

from pipelines.rj_sms__sisreg_disparo_lembretes.constants import SisregConstants
from pipelines.rj_sms__sisreg_disparo_lembretes.helpers.tasks import access_api
from pipelines.rj_sms__sisreg_disparo_lembretes.tasks import (
    check_api_status,
    create_date_partitions,
    create_dispatch_dfr,
    create_dispatch_payload,
    dispatch,
    get_destinations,
    remove_duplicate_phones,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options

logger = get_logger("toolkit")


@flow(log_prints=True, flow_run_name="sisreg_dispatch_{destination_table_id}_{destination_dataset_id}")
async def rj_sms__sisreg_disparo_lembretes(
    *,
    cloud: Cloud,
    # Parameters with defaults from constants
    dataset_id: str | None = None,
    table_id: str | None = None,
    destination_dataset_id: str | None = None,
    destination_table_id: str | None = None,
    billing_project_id: str | None = None,
    dump_mode: str | None = None,
    query: str | None = None,
    destinations: list | None = None,
    id_hsm: int | None = None,
    campaign_name: str | None = None,
    cost_center_id: str | None = None,
    chunk_size: int | None = None,
    infisical_secret_path: str = "/wetalkie",
):
    """
    SISREG SMS Dispatch Pipeline

    Sends SMS reminders to patients with scheduled appointments for the next day.
    Queries BigQuery for SISREG appointments, formats messages, and dispatches via Wetalkie API.
    """

    # Use values from constants as defaults
    dataset_id = dataset_id or SisregConstants.DATASET_ID.value
    table_id = table_id or SisregConstants.TABLE_ID.value
    destination_dataset_id = destination_dataset_id or SisregConstants.DATASET_ID.value
    destination_table_id = destination_table_id or SisregConstants.TABLE_ID.value
    billing_project_id = billing_project_id or SisregConstants.BILLING_PROJECT_ID.value
    dump_mode = dump_mode or SisregConstants.DUMP_MODE.value
    query = query or SisregConstants.QUERY.value
    id_hsm = id_hsm or SisregConstants.HSM_ID.value
    campaign_name = campaign_name or SisregConstants.CAMPAIGN_NAME.value
    cost_center_id = cost_center_id or SisregConstants.COST_CENTER_ID.value
    chunk_size = chunk_size or SisregConstants.CHUNK_SIZE.value

    # Rename flow run for better identification

    # Inject BigQuery credentials

    # Access Wetalkie API
    api = access_api(login_route="users/login")

    # Check if API is accessible
    api_status = check_api_status(api)

    # Get destinations from BigQuery query
    destinations_data = get_destinations(destinations=destinations, query=query, billing_project_id=billing_project_id)

    # Remove duplicate phone numbers
    unique_destinations = remove_duplicate_phones(destinations_data)

    # Only proceed if API is working and we have destinations
    if api_status and unique_destinations:
        # Create dispatch payload
        dispatch_payload = create_dispatch_payload(
            campaign_name=campaign_name, cost_center_id=int(cost_center_id), destinations=unique_destinations
        )

        # Dispatch messages in chunks
        dispatch_date = dispatch(api=api, id_hsm=id_hsm, dispatch_payload=dispatch_payload, chunk=chunk_size)

        # Create DataFrame with dispatch results
        dfr = create_dispatch_dfr(id_hsm=id_hsm, dispatch_payload=dispatch_payload, dispatch_date=dispatch_date)

        # Create date partitions for storage
        partitions_path = create_date_partitions(
            dataframe=dfr, partition_column="dispatch_date", file_format="csv", root_folder="./data_dispatch/"
        )

        # Upload to BigQuery
        table = Table(dataset_id=destination_dataset_id, table_id=destination_table_id)
        await upload_files_to_gcs(
            Path(partitions_path),
            table,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format="csv",
            options=capture_options(dump_mode),
        )
        await publish_table_in_bq(table, cloud)
    elif not api_status:
        raise Exception("Wetalkie API is not accessible")
    else:

        logger.info("No destinations found for dispatch. Skipping flow execution.")

# -*- coding: utf-8 -*-
"""
SISREG SMS Dispatch Pipeline
Sends SMS reminders for next day appointments via Wetalkie API
"""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from .constants import SisregConstants
from .tasks import (
    check_api_status,
    create_date_partitions,
    create_dispatch_dfr,
    create_dispatch_payload,
    dispatch,
    get_destinations,
    remove_duplicate_phones,
)
from .utils.tasks import access_api


@flow(log_prints=True)
def rj_sms__sisreg_disparo_lembretes(
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
    rename_flow_run = rename_current_flow_run_task(
        new_name=f"sisreg_dispatch_{destination_table_id}_{destination_dataset_id}"
    )

    # Inject BigQuery credentials
    credentials = inject_bd_credentials_task(environment="prod")

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
        create_table = create_table_and_upload_to_gcs_task(
            data_path=partitions_path,
            dataset_id=destination_dataset_id,
            table_id=destination_table_id,
            dump_mode=dump_mode,
            biglake_table=False,
        )
    elif not api_status:
        raise Exception("Wetalkie API is not accessible")
    else:
        from iplanrio.pipelines_utils.logging import log

        log("No destinations found for dispatch. Skipping flow execution.")

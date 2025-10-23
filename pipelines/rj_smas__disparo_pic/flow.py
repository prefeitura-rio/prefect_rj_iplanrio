# -*- coding: utf-8 -*-

import os
import time

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import getenv_or_action, inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_smas__disparo_pic.constants import PicLembreteConstants
from pipelines.rj_smas__disparo_pic.tasks import (
    check_api_status,
    create_dispatch_dfr,
    create_dispatch_payload,
    dispatch,
    get_destinations,
    printar,
    remove_duplicate_phones,
)
from pipelines.rj_smas__disparo_pic.utils.discord import (
    send_dispatch_result_notification,
    send_dispatch_success_notification,
)
from pipelines.rj_smas__disparo_pic.utils.tasks import (
    access_api,
    create_date_partitions,
    skip_flow_if_empty,
)


@flow(log_prints=True)
def rj_smas__disparo_pic(
    # Parâmetros opcionais para override manual na UI.
    id_hsm: int | None = 185,
    campaign_name: str | None = None,
    cost_center_id: int | None = 38,
    chunk_size: int | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    query: str | None = None,
    query_processor_name: str | None = None,
    test_mode: bool | None = None,
    infisical_secret_path: str = "/wetalkie",
):
    dataset_id = dataset_id or PicLembreteConstants.PIC_DATASET_ID.value
    table_id = table_id or PicLembreteConstants.PIC_TABLE_ID.value
    dump_mode = dump_mode or PicLembreteConstants.PIC_DUMP_MODE.value
    id_hsm = id_hsm or PicLembreteConstants.PIC_ID_HSM.value
    campaign_name = campaign_name or PicLembreteConstants.PIC_CAMPAIGN_NAME.value
    cost_center_id = cost_center_id or PicLembreteConstants.PIC_COST_CENTER_ID.value
    chunk_size = chunk_size or PicLembreteConstants.PIC_CHUNK_SIZE.value
    query = query or PicLembreteConstants.PIC_QUERY.value
    query_processor_name = (
        query_processor_name or PicLembreteConstants.PIC_QUERY_PROCESSOR_NAME.value
    )
    test_mode = (
        test_mode if test_mode is not None else PicLembreteConstants.PIC_TEST_MODE.value
    )

    # Se test_mode ativado, usar query mock ao invés da query real
    if test_mode:
        query = PicLembreteConstants.PIC_QUERY_MOCK.value
        print("⚠️  MODO DE TESTE ATIVADO - Disparos para números de teste apenas")

    billing_project_id = PicLembreteConstants.PIC_BILLING_PROJECT_ID.value

    destinations = getenv_or_action("PIC__DESTINATIONS", action="ignore")

    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")
    crd = inject_bd_credentials_task(environment="prod")  # noqa

    api = access_api(
        infisical_secret_path,
        "wetalkie_url",
        "wetalkie_user",
        "wetalkie_pass",
        login_route="users/login",
    )

    api_status = check_api_status(api)

    destinations_result = get_destinations(
        destinations=destinations,
        query=query,
        billing_project_id=billing_project_id,
        query_processor_name=query_processor_name,
    )

    validated_destinations = skip_flow_if_empty(
        data=destinations_result,
        message="No destinations found from query. Skipping flow execution.",
    )

    unique_destinations = remove_duplicate_phones(validated_destinations)

    # Log destination counts for tracking
    print(f"Total unique destinations to dispatch: {len(unique_destinations)}")

    if api_status:
        dispatch_payload = create_dispatch_payload(
            campaign_name=campaign_name,
            cost_center_id=cost_center_id,
            destinations=unique_destinations,
        )

        printar(id_hsm)

        dispatch_date = dispatch(
            api=api,
            id_hsm=id_hsm,
            dispatch_payload=dispatch_payload,
            chunk=chunk_size,
        )

        print(
            f"Dispatch completed successfully for {len(unique_destinations)} destinations"
        )

        # Calculate total batches
        from math import ceil

        total_batches = ceil(len(unique_destinations) / chunk_size)

        # Send Discord notification
        send_dispatch_success_notification(
            total_dispatches=len(unique_destinations),
            dispatch_date=dispatch_date,
            id_hsm=id_hsm,
            campaign_name=campaign_name,
            cost_center_id=cost_center_id,
            total_batches=total_batches,
            sample_destination=unique_destinations[0] if unique_destinations else None,
            test_mode=test_mode,
        )

        dfr = create_dispatch_dfr(
            id_hsm=id_hsm,
            original_destinations=unique_destinations,
            campaign_name=campaign_name,
            cost_center_id=cost_center_id,
            dispatch_date=dispatch_date,
        )

        print(f"DataFrame created with {len(dfr)} records for BigQuery upload")

        partitions_path = create_date_partitions(
            dataframe=dfr,
            partition_column="dispatch_date",
            file_format="csv",
            root_folder="./data_dispatch/",
        )

        if not partitions_path:
            raise ValueError("partitions_path is None - partition creation failed")

        if not os.path.exists(partitions_path):
            raise ValueError(f"partitions_path does not exist: {partitions_path}")

        print(f"Generated partitions_path: {partitions_path}")
        if os.path.exists(partitions_path):
            files_in_path = []
            for root, dirs, files in os.walk(partitions_path):
                files_in_path.extend([os.path.join(root, f) for f in files])
            print(f"Files in partitions path: {files_in_path}")

        create_table = create_table_and_upload_to_gcs_task(
            data_path=partitions_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            biglake_table=False,
        )

        # Wait 15 minutes before querying results
        print("Waiting 15 minutes before checking dispatch results...")
        time.sleep(15 * 60)  # 15 minutes in seconds

        # Send results notification with BigQuery data
        send_dispatch_result_notification(
            total_dispatches=len(unique_destinations),
            dispatch_date=dispatch_date,
            id_hsm=id_hsm,
            campaign_name=campaign_name,
            cost_center_id=cost_center_id,
            total_batches=total_batches,
            test_mode=test_mode,
        )

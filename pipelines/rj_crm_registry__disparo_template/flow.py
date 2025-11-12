# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'

# TODO: rodar teste no prefect no cadunico, pic e template
# adicionar whitelist aqui, no cadunico e no pic

import os
import time
from math import ceil
import pendulum

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.env import getenv_or_action, inject_bd_credentials_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task  # pylint: disable=E0611, E0401
from prefect import flow  # pylint: disable=E0611, E0401

from pipelines.rj_crm_registry__disparo_template.constants import TemplateConstants  # pylint: disable=E0611, E0401
# pylint: disable=E0611, E0401
from pipelines.rj_crm_registry__disparo_template.utils.discord import (
    send_dispatch_result_notification,
    send_dispatch_success_notification,
)
# pylint: disable=E0611, E0401
from pipelines.rj_crm_registry__disparo_template.utils.dispatch import (
    add_contacts_to_whitelist,
    check_api_status,
    create_dispatch_dfr,
    create_dispatch_payload,
    dispatch,
    format_query,
    get_destinations,
    remove_duplicate_phones,
)
# pylint: disable=E0611, E0401
from pipelines.rj_crm_registry__disparo_template.utils.tasks import (
    access_api,
    create_date_partitions,
    printar,
    skip_flow_if_empty,
)
# force deploy


@flow(log_prints=True)
def rj_crm_registry__disparo_template(
    # Parâmetros opcionais para override manual na UI.
    id_hsm: int | None = None,
    campaign_name: str | None = None,
    cost_center_id: int | None = None,
    chunk_size: int | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    test_mode: bool | None = True,
    query: str | None = None,
    query_processor_name: str | None = None,
    query_replacements: dict | None = None,
    sleep_minutes: int | None = 5,
    infisical_secret_path: str = "/wetalkie",
    whitelist_percentage: int = 30,
    whitelist_environment: str = "staging",
):
    dataset_id = dataset_id or TemplateConstants.DATASET_ID.value
    table_id = table_id or TemplateConstants.TABLE_ID.value
    dump_mode = dump_mode or TemplateConstants.DUMP_MODE.value
    id_hsm = id_hsm or TemplateConstants.ID_HSM.value
    campaign_name = campaign_name or TemplateConstants.CAMPAIGN_NAME.value
    cost_center_id = cost_center_id or TemplateConstants.COST_CENTER_ID.value
    chunk_size = chunk_size or TemplateConstants.CHUNK_SIZE.value
    query = query or TemplateConstants.QUERY.value
    query_processor_name = query_processor_name or TemplateConstants.QUERY_PROCESSOR_NAME.value

    billing_project_id = TemplateConstants.BILLING_PROJECT_ID.value

    destinations = getenv_or_action("TEMPLATE__DESTINATIONS", action="ignore")

    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")  # pylint: disable=unused-variable
    crd = inject_bd_credentials_task(environment="prod")  # noqa  # pylint: disable=unused-variable

    if test_mode:
        campaign_name = "teste-"+campaign_name
        print("⚠️  MODO DE TESTE ATIVADO - Disparos para números de teste apenas")

    api = access_api(
        infisical_secret_path,
        "wetalkie_url",
        "wetalkie_user",
        "wetalkie_pass",
        login_route="users/login",
    )

    api_status = check_api_status(api)

    if query_replacements:
        query_complete = format_query(
            raw_query=query,
            replacements=query_replacements,
            query_processor_name=query_processor_name,
        )
    else:
        query_complete = query
    print(f"\n⚠️  Query dispatch:\n{query_complete}")

    destinations_result = get_destinations(
        destinations=destinations,
        query=query_complete,
        billing_project_id=billing_project_id,
    )

    validated_destinations = skip_flow_if_empty(
        data=destinations_result,
        message="No destinations found from query. Skipping flow execution.",
    )

    unique_destinations = remove_duplicate_phones(validated_destinations)

    # Log destination counts for tracking
    print(f"Total unique destinations to dispatch: {len(unique_destinations)}")

    # Add contacts to whitelist if percentage is set
    if whitelist_percentage > 0:
        whitelist_group_name = f"citizen-hsm-{campaign_name}-{pendulum.now('America/Sao_Paulo').to_date_string()}"
        add_contacts_to_whitelist(
            destinations=unique_destinations,
            percentage_to_insert=whitelist_percentage,
            group_name=whitelist_group_name,
            environment=whitelist_environment,
        )

    if api_status:
        dispatch_payload = create_dispatch_payload(
            campaign_name=campaign_name,
            cost_center_id=cost_center_id,
            destinations=unique_destinations,
        )

        printar(id_hsm)
        print(
            f"\nStarting dispatch for id_hsm={id_hsm}, campaign_name={campaign_name}, example data {unique_destinations[:5]}\n"
        )
        # TODO: adicionar print da hsm
        print(f"⚠️  Sleep {sleep_minutes} minutes before dispatch. Check if event date and id_hsm is correct!!")
        time.sleep(sleep_minutes * 60)

        dispatch_date = dispatch(
            api=api,
            id_hsm=id_hsm,
            dispatch_payload=dispatch_payload,
            chunk=chunk_size,
        )

        print(f"Dispatch completed successfully for {len(unique_destinations)} destinations")

        total_batches = ceil(len(unique_destinations) / chunk_size)

        # Send Discord notification
        send_dispatch_success_notification(
            total_dispatches=len(unique_destinations),
            dispatch_date=dispatch_date,
            id_hsm=id_hsm,
            campaign_name=campaign_name,
            cost_center_id=cost_center_id,
            total_batches=total_batches,
            sample_destination=(unique_destinations[0] if unique_destinations else None),
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

        if not test_mode:
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
                for root, dirs, files in os.walk(partitions_path):  # pylint: disable=unused-variable
                    files_in_path.extend([os.path.join(root, f) for f in files])
                print(f"Files in partitions path: {files_in_path}")

            create_table_and_upload_to_gcs_task(
                data_path=partitions_path,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode=dump_mode,
                biglake_table=False,
            )

        # Wait 15 minutes before querying results
        print("⚠️  Waiting 15 minutes before checking dispatch results...")
        time.sleep(15 * 60)

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
#force deploy

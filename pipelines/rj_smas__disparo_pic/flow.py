# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Flow para o disparo do cartão pic
"""
import os
import time
from math import ceil
import pendulum

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.env import getenv_or_action, inject_bd_credentials_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task  # pylint: disable=E0611, E0401
from prefect import flow

# pylint: disable=E0611, E0401
from pipelines.rj_smas__disparo_pic.constants import PicLembreteConstants
# pylint: disable=E0611, E0401
from pipelines.rj_smas__disparo_template.utils.dispatch import (
    add_contacts_to_whitelist,
    check_api_status,
    check_if_dispatch_approved,
    create_dispatch_dfr,
    create_dispatch_payload,
    dispatch,
    format_query,
    get_destinations,
    remove_duplicate_phones,
)
# pylint: disable=E0611, E0401
from pipelines.rj_smas__disparo_template.utils.discord import (
    send_dispatch_result_notification,
    send_dispatch_success_notification,
)
# pylint: disable=E0611, E0401
from pipelines.rj_smas__disparo_template.utils.tasks import (
    access_api,
    create_date_partitions,
    printar,
    skip_flow_if_empty,
    task_download_data_from_bigquery,
)


# forçando deploy do flow
@flow(log_prints=True)
def rj_smas__disparo_pic(
    # Parâmetros opcionais para override manual na UI.
    id_hsm: int | None = 184,
    campaign_name: str | None = None,
    cost_center_id: int | None = 71,
    chunk_size: int | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    query: str | None = None,
    query_dispatch_approved: str | None = None,
    query_processor_name: str | None = None,
    test_mode: bool | None = True,
    sleep_minutes: int | None = 5,
    dispatch_approved_col: str | None = "APROVACAO_DISPARO_AVISO",
    dispatch_date_col: str | None = "DATA_DISPARO_AVISO",
    event_date_col: str | None = "DATA_ENTREGA",
    infisical_secret_path: str = "/wetalkie",
    whitelist_percentage: int = 30,
    whitelist_environment: str = "staging",
):
    dataset_id = dataset_id or PicLembreteConstants.PIC_DATASET_ID.value
    table_id = table_id or PicLembreteConstants.PIC_TABLE_ID.value
    dump_mode = dump_mode or PicLembreteConstants.PIC_DUMP_MODE.value
    id_hsm = id_hsm or PicLembreteConstants.PIC_ID_HSM.value
    campaign_name = campaign_name or PicLembreteConstants.PIC_CAMPAIGN_NAME.value
    cost_center_id = cost_center_id or PicLembreteConstants.PIC_COST_CENTER_ID.value
    chunk_size = chunk_size or PicLembreteConstants.PIC_CHUNK_SIZE.value
    query = query or PicLembreteConstants.PIC_QUERY.value
    query_dispatch_approved = query_dispatch_approved or PicLembreteConstants.PIC_QUERY_DISPATCH_APPROVED.value
    query_processor_name = query_processor_name or PicLembreteConstants.PIC_QUERY_PROCESSOR_NAME.value
    test_mode = test_mode if test_mode is not None else PicLembreteConstants.PIC_TEST_MODE.value
    dispatch_approved_col = dispatch_approved_col or PicLembreteConstants.DISPATCH_APPROVED_COL.value
    dispatch_date_col = dispatch_date_col or PicLembreteConstants.DISPATCH_DATE_COL.value
    event_date_col = event_date_col or PicLembreteConstants.EVENT_DATE_COL.value

    billing_project_id = PicLembreteConstants.PIC_BILLING_PROJECT_ID.value

    # Se test_mode ativado, usar query mock ao invés da query real
    if test_mode:
        campaign_name = "teste-"+campaign_name
        query = PicLembreteConstants.PIC_QUERY_MOCK.value
        query_dispatch_approved = PicLembreteConstants.PIC_QUERY_MOCK_DISPATCH_APPROVED.value
        query_create_mock_tables = PicLembreteConstants.CREATE_MOCK_TABLES.value
        print("⚠️  MODO DE TESTE ATIVADO - Disparos para números de teste apenas")
        task_download_data_from_bigquery(
            query=query_create_mock_tables,
            billing_project_id=billing_project_id,
            bucket_name=billing_project_id,
        )
        print("Mock tables were created")

    print(f"\nQuery citizen:\n{query}")
    print(f"\nQuery dispatch approval:\n{query_dispatch_approved}")

    destinations = getenv_or_action("PIC__DESTINATIONS", action="ignore")

    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")
    crd = inject_bd_credentials_task(environment="prod")  # noqa

    df_dispatch_approved = task_download_data_from_bigquery(
        query=query_dispatch_approved,
        billing_project_id=billing_project_id,
        bucket_name=billing_project_id,
    )

    event_date, dispatch_approved = check_if_dispatch_approved(
        df_dispatch_approved, dispatch_approved_col, event_date_col
    )

    if dispatch_approved:
        query_replacements = {"event_date_placeholder": event_date, "id_hsm_placeholder": id_hsm}
        query_complete = format_query(raw_query=query, replacements=query_replacements)
        print(f"\nQuery dispatch approval:\n{query_complete}")
        print(f"Sleep {sleep_minutes} minutes to check")
        time.sleep(sleep_minutes * 60)

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
            query=query_complete,
            billing_project_id=billing_project_id,
        )

        validated_destinations = skip_flow_if_empty(
            data=destinations_result,
            message="No destinations found from query. Skipping flow execution.",
        )

        unique_destinations = remove_duplicate_phones(validated_destinations)

        # Log destination counts for tracking!!
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
            print(f"{whitelist_percentage}% ({len(unique_destinations)*whitelist_percentage/100}) \
                of numbers where add to whitelist on group {whitelist_group_name} inside \
                environment {whitelist_environment}.")

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

            print(f"✅  Dispatch completed successfully for {len(unique_destinations)} destinations")

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
            print("⚠️  Waiting 15 minutes before checking dispatch results...")
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

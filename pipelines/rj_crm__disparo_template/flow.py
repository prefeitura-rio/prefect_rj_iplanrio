# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'

"""
Flow to dispatch templated messages via Wetalkie API
"""
import os
import time
from math import ceil
import pendulum

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.dbt import execute_dbt_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.env import getenv_or_action, inject_bd_credentials_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task  # pylint: disable=E0611, E0401
from prefect import flow  # pylint: disable=E0611, E0401
from prefect.client.schemas.objects import Flow, FlowRun, State  # pylint: disable=E0611, E0401

from pipelines.rj_crm__disparo_template.constants import TemplateConstants  # pylint: disable=E0611, E0401
# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.discord import (
    send_dispatch_no_destinations_found,
    # send_retry_dispatch_result_notification,
    send_dispatch_success_notification,
    send_discord_notification,
)
# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.dispatch import (
    add_contacts_to_whitelist,
    check_api_status,
    check_flow_status,
    create_dispatch_dfr,
    create_dispatch_payload,
    create_log_df,
    dispatch,
    filter_already_dispatched_phones_or_cpfs,
    format_query,
    get_already_dispatched_data,
    get_destinations,
    get_failed_cpfs,
    get_failed_phones,
    get_retry_destinations,
    remove_contacts_from_whitelist,
    remove_duplicate_cpfs,
    remove_duplicate_phones,
    remove_failed_phones,
    save_csv_for_sftp,
    send_to_sftp,
)
# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.validators import validate_sf_dataframe
# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.tasks import (
    access_api,
    create_date_partitions,
    task_download_data_from_bigquery,
)


def send_discord_notification_on_failure(flow: Flow, flow_run: FlowRun, state: State):
    """
    Sends a Discord notification when a flow run fails.
    """
    # Only send notification if flow_environment is production
    flow_environment = flow_run.parameters.get("flow_environment", "staging")
    if flow_environment != "production":
        print(f"Flow failed in {flow_environment} environment. Skipping Discord notification.")
        return

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_ERRORS")
    if not webhook_url:
        print("DISCORD_WEBHOOK_URL_ERRORS environment variable not set on Infisical. Cannot send notification.")
        return

    campaign_name = flow_run.parameters.get("campaign_name", "N/A")
    id_hsm = flow_run.parameters.get("id_hsm", "N/A")
    cost_center_id = flow_run.parameters.get("cost_center_id", "N/A")

    message = f"""
    Prefect flow run failed in PRODUCTION! 🚨
    📋 **Campanha:** {campaign_name}
    🆔 **Template ID:** {id_hsm}
    💰 **Centro de Custo:** {cost_center_id}
    ⚠️ **Mensagem:** {state.message}
    """
    send_discord_notification(webhook_url, message)

# force deploy
@flow(log_prints=True, on_failure=[send_discord_notification_on_failure])
def rj_crm__disparo_template_sf(
    # Parâmetros opcionais para override manual na UI.
    id_hsm: int | None = None,
    campaign_name: str | None = None,
    cost_center_id: int | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    test_mode: bool | None = True,
    query: str | None = None,
    query_processor_name: str | None = None,
    query_replacements: dict | None = None,
    filter_dispatched_phones_or_cpfs: str | None = "cpf",
    filter_duplicated_phones: bool = True,
    filter_duplicated_cpfs: bool = True,
    filter_failed_phones: bool = False,
    sleep_minutes: int | None = 5,
    max_dispatch_retries: int = 0,
    infisical_secret_path: str = "/crm_disparo_template",
    data_extension_filename: str | None = None,
    whitelist_percentage: int = 0,
    whitelist_environment: str = "production",
    flow_environment: str = "staging",
    force_add_on_whitelist_group: bool = False,
    whitelist_replace_contacts: bool = False,
    materialize_after_sftp: bool = True,
):
    """
    Orchestrates the dispatch of templated messages via Salesforce SFTP.

    Fetches destinations from BigQuery, applies filters, generates a CSV with
    a 'telefone' plus all query fields, saves it to
    disk, and uploads it to the configured SFTP server.
    Dispatch results are also logged to BigQuery.

    SFTP credentials (sf_sftp_host, sf_sftp_user, sf_sftp_password) must be available
    as environment variables injected from Infisical at infisical_secret_path.

    Args:
        id_hsm (int, optional): The ID of the HSM template.
        campaign_name (str, optional): The name of the dispatch campaign.
        cost_center_id (int, optional): The ID of the cost center.
        chunk_size (int, optional): Kept for interface compatibility; not used for SFTP batching.
        dataset_id (str, optional): BigQuery dataset ID for dispatch logs.
        table_id (str, optional): BigQuery table ID for dispatch logs.
        dump_mode (str, optional): BigQuery dump mode (e.g., "append").
        test_mode (bool, optional): If True, runs in test mode. Defaults to True.
        query (str, optional): SQL query to retrieve destinations.
        query_processor_name (str, optional): Name of the query processor.
        query_replacements (dict, optional): Replacements for query placeholders.
        filter_dispatched_phones_or_cpfs (str, optional): None, "cpf" or "phone_number". Defaults to "cpf".
        filter_duplicated_phones (bool, optional): Remove duplicate phones. Defaults to True.
        filter_duplicated_cpfs (bool, optional): Remove duplicate CPFs. Defaults to True.
        filter_failed_phones (bool, optional): Remove phones that failed in last dispatch. Defaults to False.
        sleep_minutes (int, optional): Minutes to sleep before dispatch. Defaults to 5.
        max_dispatch_retries (int): Maximum retry attempts with alternative phones. Defaults to 0.
        infisical_secret_path (str, optional): Infisical path for SFTP credentials. Defaults to "/sftp".
        sftp_remote_path (str, optional): Remote directory on the SFTP server. Defaults to "/".
        whitelist_percentage (int, optional): Percentage of contacts to whitelist. Defaults to 0.
        whitelist_environment (str, optional): Whitelist environment. Defaults to "production".
        flow_environment (str, optional): Flow environment ("staging" or "production"). Defaults to "staging".
        force_add_on_whitelist_group (bool, optional): Force add to whitelist group. Defaults to False.
        whitelist_replace_contacts (bool, optional): Remove contacts before adding to whitelist. Defaults to False.
    """

    dataset_id = dataset_id or TemplateConstants.DATASET_ID.value
    table_id = table_id or TemplateConstants.TABLE_ID.value
    dump_mode = dump_mode or TemplateConstants.DUMP_MODE.value
    id_hsm = id_hsm or TemplateConstants.ID_HSM.value
    campaign_name = campaign_name or TemplateConstants.CAMPAIGN_NAME.value
    cost_center_id = cost_center_id or TemplateConstants.COST_CENTER_ID.value
    query = query or TemplateConstants.QUERY.value
    query_processor_name = query_processor_name or TemplateConstants.QUERY_PROCESSOR_NAME.value
    billing_project_id = TemplateConstants.BILLING_PROJECT_ID.value

    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")  # pylint: disable=unused-variable
    crd = inject_bd_credentials_task(environment="prod")  # noqa  # pylint: disable=unused-variable

    flow_status = check_flow_status(
        flow_environment=flow_environment,
        billing_project_id=billing_project_id,
        bucket_name=billing_project_id,
        id_hsm=id_hsm,
        campaign_name=campaign_name,
    )
    if flow_status is None:
        print("Ending flow due to inactive status.")
        return

    if test_mode:
        print("⚠️  MODO DE TESTE ATIVADO - Disparos para números de teste apenas")

    if query_replacements:
        query_complete = format_query(
            raw_query=query,
            replacements=query_replacements,
            query_processor_name=query_processor_name,
        )
    else:
        query_complete = query
    print(f"\n⚠️  Query dispatch:\n{query_complete}")

    # O disparo SF trabalha com o DataFrame plano retornado pela query.
    # A query deve retornar as colunas: telefone, others (lista), e demais campos do disparo.
    df = task_download_data_from_bigquery(
        query=query_complete,
        billing_project_id=billing_project_id,
        bucket_name=billing_project_id,
    )

    if df is None or df.empty:
        send_dispatch_no_destinations_found(id_hsm, campaign_name, cost_center_id, test_mode)
        return

    # Valida colunas obrigatórias para o log SF: SubscriberKey e telefone.
    # Lança ValueError imediatamente se alguma estiver ausente ou com dados inválidos.
    validate_sf_dataframe(df, campaign_name) ## TODO: validar

    print(f"Query retornou {len(df)} linhas. Colunas: {list(df.columns)}")

    # Dedup por CPF — base para o loop de retentativas
    if filter_duplicated_cpfs and "SubscriberKey" in df.columns:
        n_before = len(df)
        df = df.drop_duplicates(subset=["SubscriberKey"])
        print(f"Removed {n_before - len(df)} duplicate CPFs. Remaining: {len(df)}")

    if df.empty:
        print("No destinations found after filtering duplicate CPFs. Exiting flow execution.")
        return

    # Remove telefones que falharam no último disparo
    if filter_failed_phones:
        # TODO: get_failed_phones queries fluxo_atendimento (Wetalkie). Substituir por tabela SF quando disponível.
        # TODO: trocar externalId por SubscriberKey
        failed_phones = get_failed_phones(billing_project_id=billing_project_id)
        if failed_phones:
            if max_dispatch_retries > 0:
                # Marca como None para que o loop de retry use o próximo número de 'others'
                df.loc[df["telefone"].isin(failed_phones), "telefone"] = None
            else:
                df = df[~df["telefone"].isin(failed_phones)]

        if df.empty:
            send_dispatch_no_destinations_found(id_hsm, campaign_name, cost_center_id, test_mode)
            return

    print(f"Total unique destinations to dispatch: {len(df)}")
    base_df = df.copy()

    # RETRY LOOP
    for i in range(0, max_dispatch_retries + 1):

        if i == 0:
            current_df = base_df.copy()
            # No primeiro disparo, exclui linhas com telefone None (marcadas por filter_failed_phones)
            current_df = current_df.dropna(subset=["telefone"])
        else:
            if "others" in base_df.columns and not base_df["others"].apply(
                lambda x: isinstance(x, list) and len(x) >= i
            ).any():
                print(f"✅ No others available for retry attempt {i}. Ending retry loop.")
                break

            print(f"⚠️  Sleep 3 minutes before retry dispatch.")
            time.sleep(3 * 60)
            print(f"\n⚠️  Starting retry attempt {i} for id_hsm={id_hsm}...")

            # TODO: get_failed_cpfs queries fluxo_atendimento (Wetalkie). Substituir por tabela SF quando disponível.
            failed_cpfs = get_failed_cpfs(billing_project_id=billing_project_id, id_hsm=id_hsm)

            if not failed_cpfs:
                print(f"✅ No failed CPFs found for retry attempt {i}. Ending retry loop.")
                break

            # Seleciona as linhas dos CPFs que falharam e aplica o próximo número de 'others'
            retry_df = base_df[base_df["SubscriberKey"].isin(failed_cpfs)].copy()
            if "others" not in retry_df.columns or retry_df.empty:
                print(f"No others column or no rows for retry attempt {i}.")
                break

            retry_df["telefone"] = retry_df["others"].apply(
                lambda x: x[i - 1] if isinstance(x, list) and len(x) >= i else None
            )
            current_df = retry_df.dropna(subset=["telefone"])

            if current_df.empty:
                print(f"✅ No retry phones available for attempt {i}. Ending retry loop.")
                break

            print(f"🚀 Found {len(current_df)} destinations for retry attempt {i}.")

        # Filtro de já disparados
        # No retry por CPF, desliga o filtro de CPF (o mesmo CPF deve tentar outro número)
        current_filter = filter_dispatched_phones_or_cpfs
        if i > 0 and current_filter == "cpf":
            current_filter = None

        if current_filter:
            # TODO: fluxo_atendimento é tabela Wetalkie. Substituir por tabela de controle SF quando disponível.
            print(f"🔍 Checking if phones were already dispatched today...")
            already_dispatched_df = get_already_dispatched_data(billing_project_id=billing_project_id)
            already_dispatched_df = already_dispatched_df.rename(columns={"celular_disparo": "telefone"})

            n_before = len(current_df)
            if current_filter == "cpf" and "externalId" in already_dispatched_df.columns:
                dispatched_set = set(already_dispatched_df["externalId"].dropna().tolist())
                current_df = current_df[~current_df["externalId"].isin(dispatched_set)]
            elif current_filter == "phone_number" and "telefone" in already_dispatched_df.columns:
                dispatched_set = set(already_dispatched_df["telefone"].dropna().tolist())
                current_df = current_df[~current_df["telefone"].isin(dispatched_set)]
            print(f"Removed {n_before - len(current_df)} already dispatched. Remaining: {len(current_df)}")

        # Dedup por telefone (retries podem introduzir duplicatas)
        if filter_duplicated_phones and "telefone" in current_df.columns:
            n_before = len(current_df)
            current_df = current_df.drop_duplicates(subset=["telefone"])
            print(f"Removed {n_before - len(current_df)} duplicate phones.")

        if current_df.empty:
            print("No destinations found. Exiting flow execution.")
            return

        print(f"Total destinations for attempt {i}: {len(current_df)}")

        # Whitelist (funções esperam List[Dict] com chave 'to')
        if whitelist_percentage > 0:
            whitelist_group_name = f"citizen-hsm-{campaign_name}"
            whitelist_dests = [{"to": phone} for phone in current_df["telefone"].tolist()]
            if whitelist_replace_contacts:
                remove_contacts_from_whitelist(destinations=whitelist_dests, environment=whitelist_environment)
            add_contacts_to_whitelist(
                destinations=whitelist_dests,
                percentage_to_insert=whitelist_percentage,
                group_name=whitelist_group_name,
                environment=whitelist_environment,
                force_add_on_whitelist_group=force_add_on_whitelist_group,
            )

        print(f"\nStarting SF dispatch for id_hsm={id_hsm}, attempt={i}, campaign_name={campaign_name}")
        print(f"Sample data:\n{current_df.head(2).to_dict('records')}")
        print(f"⚠️  Sleep {sleep_minutes} minutes before dispatch. Check if event date and id_hsm is correct!!")
        time.sleep(sleep_minutes * 60)

        # Salva CSV (dropa 'others') e registra data do disparo
        csv_path, dispatch_date = save_csv_for_sftp(
            df=current_df,
            data_extension_filename=data_extension_filename or campaign_name,
        )

        send_to_sftp(
            csv_path=csv_path,
            infisical_secret_path=infisical_secret_path,
        )

        print(f"CSV enviado para SFTP com {len(current_df)} destinatários na tentativa {i+1}.")

        if materialize_after_sftp:
            print("⏳ Aguardando 20 minutos antes de materializar o modelo dbt int_crm_status_disparo...")
            time.sleep(20 * 60) if not test_mode else time.sleep(1)
            github_token = getenv_or_action("GITHUB_TOKEN")
            git_repository_path = f"https://{github_token}@github.com/prefeitura-rio/queries-rj-crm-registry.git"
            execute_dbt_task(
                select="+int_crm_status_disparo",
                target="prod",
                git_repository_path=git_repository_path,
            )

        if not test_mode:
            send_dispatch_success_notification(
                total_dispatches=len(current_df),
                dispatch_date=dispatch_date,
                id_hsm=id_hsm,
                campaign_name=campaign_name,
                cost_center_id=cost_center_id,
                total_batches=1,
                sample_destination=(current_df.iloc[0].to_dict() if not current_df.empty else None),
                test_mode=test_mode,
                attempt_number=i + 1,
                total_attempt_number=max_dispatch_retries + 1,
            )

            # Log para BQ: schema fixo com coluna `data` em JSON para camada bronze
            log_df = create_log_df(
                df=current_df,
                dispatch_date=dispatch_date,
                campaign_name=campaign_name,
            )

            partitions_path = create_date_partitions(
                dataframe=log_df,
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



@flow(log_prints=True, on_failure=[send_discord_notification_on_failure])
def rj_crm__disparo_template(
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
    filter_dispatched_phones_or_cpfs: str | None = "cpf",
    filter_duplicated_phones: bool = True,
    filter_duplicated_cpfs: bool = True,
    filter_failed_phones: bool = False,
    sleep_minutes: int | None = 5,
    max_dispatch_retries: int = 0,
    infisical_secret_path: str = "/wetalkie",
    whitelist_percentage: int = 0,
    whitelist_environment: str = "production",
    flow_environment: str = "staging",
    force_add_on_whitelist_group: bool = False,
    whitelist_replace_contacts: bool = False,
):
    """
    Orchestrates the dispatch of templated messages via Wetalkie API.

    This flow handles fetching destinations, preparing dispatch payloads,
    sending messages, and logging dispatch results to BigQuery.

    Args:
        id_hsm (int, optional): The ID of the HSM (Highly Structured Message) template to be used.
        campaign_name (str, optional): The name of the dispatch campaign.
        cost_center_id (int, optional): The ID of the cost center associated with the dispatch.
        chunk_size (int, optional): The number of destinations to include in each dispatch batch.
        dataset_id (str, optional): The BigQuery dataset ID where dispatch results will be stored.
        table_id (str, optional): The BigQuery table ID where dispatch results will be stored.
        dump_mode (str, optional): The mode for dumping data to BigQuery (e.g., "append", "overwrite").
        test_mode (bool, optional): If True, the flow runs in test mode, dispatching only to test numbers. Defaults to True.
        query (str, optional): The SQL query used to retrieve the list of destinations for dispatch.
        query_processor_name (str, optional): The name of the processor to format the query.
        query_replacements (dict, optional): A dictionary of key-value pairs to replace placeholders in the `query`. Defaults to None.
        filter_dispatched_phones_or_cpfs (str, optional): If True, filters out phone numbers that have already been dispatched today. This parameter must be None, "cpf" or "phone_number". Defaults to "cpf".
        filter_duplicated_phones (bool, optional): If True, removes duplicate phone numbers from the destination list. Defaults to True.
        filter_duplicated_cpfs (bool, optional): If True, removes duplicate CPFs from the destination list. Defaults to True.
        filter_failed_phones (bool, optional): If True, removes phone numbers that have already failed in last dispatch. Defaults to False.
        sleep_minutes (int, optional): The number of minutes to wait before initiating the dispatch. Defaults to 5.
        max_dispatch_retries (int): Maximum number of retry attempts using alternative phone numbers. Defaults to 0.
        infisical_secret_path (str, optional): The path in Infisical where Wetalkie API secrets are stored. Defaults to "/wetalkie".
        whitelist_percentage (int, optional): The percentage of contacts to add to a whitelist group. Defaults to 0.
        whitelist_environment (str, optional): The environment for the whitelist (e.g., "staging", "production"). Defaults to "staging".
        flow_environment (str, optional): The environment where the flow is running (e.g., "staging", "production"). Defaults to "staging".
        force_add_on_whitelist_group (bool, optional): If True, forces adding contacts to the whitelist group. Defaults to False.
        whitelist_replace_contacts (bool, optional): If True, removes contacts from the whitelist before adding them. Defaults to True.
    """
    # force deploy ##

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

    flow_status = check_flow_status(
        flow_environment=flow_environment,
        billing_project_id=billing_project_id,
        bucket_name=billing_project_id,
        id_hsm=id_hsm,
        campaign_name=campaign_name,
    )
    if flow_status is None:
        print("Ending flow due to inactive status.")
        return  # flow termina aqui, nada downstream é agendado

    if test_mode:
        campaign_name = "teste-"+campaign_name
        print("⚠️  MODO DE TESTE ATIVADO - Disparos para números de teste apenas")
        # force deploy

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

    if destinations_result is None or len(destinations_result) == 0:
        send_dispatch_no_destinations_found(
            id_hsm,
            campaign_name,
            cost_center_id,
            test_mode
        )
        return  # flow termina aqui, nada downstream é agendado

    # Remove duplicate CPFs if flag is set - This is our BASE list for retries
    remove_duplicate_destinations = remove_duplicate_cpfs(destinations_result) if filter_duplicated_cpfs else destinations_result
    if not remove_duplicate_destinations or len(remove_duplicate_destinations) == 0:
        print("No destinations found after filtering duplicate CPFs. Exiting flow execution.")
        return

    if filter_failed_phones:
        base_destinations = remove_failed_phones(
            original_destinations=remove_duplicate_destinations,
            billing_project_id=billing_project_id,
            max_dispatch_retries=max_dispatch_retries,
        )
    else:
        base_destinations = remove_duplicate_destinations

    if not base_destinations or len(base_destinations) == 0:
        print("No destinations found after filtering failed phones. Exiting flow execution.")
        send_dispatch_no_destinations_found(
            id_hsm,
            campaign_name,
            cost_center_id,
            test_mode,
        )
        return

    print(f"Total unique destinations to dispatch: {len(base_destinations)}")

    if not api_status:
        print("API is not accessible. Ending flow execution.")
        return
    
    # Destinos que serão processados na iteração atual do loop
    current_attempt_destinations = base_destinations

    # RETRY LOOP
    for i in range(0, max_dispatch_retries + 1):
        
        if (i > 0 or filter_failed_phones) and max_dispatch_retries > 0:
            if i > 0:
                print(f"⚠️  Sleep 3 minutes before retry dispatch.")
                time.sleep(3 * 60)

                print(f"\n⚠️  Starting retry attempt {i} for id_hsm={id_hsm}. Checking for remaining failures...")
            else:
                print(f"\n⚠️  Fill with others phones for previously failed phones.")
            retry_destinations = get_retry_destinations(
                id_hsm=id_hsm,
                original_destinations=base_destinations,
                billing_project_id=billing_project_id,
                attempt_number=i
            )

            if not retry_destinations or len(retry_destinations) == 0:
                print(f"✅ No remaining phones found for retry attempt {i}. Ending retry loop.")
                break

            print(f"🚀 Found {len(retry_destinations)} destinations for retry attempt {i}.")
            current_attempt_destinations = retry_destinations

            filter_dispatched_phones_or_cpfs = None if filter_dispatched_phones_or_cpfs == "cpf" else filter_dispatched_phones_or_cpfs

        if filter_dispatched_phones_or_cpfs:
            # 1. Primeiro Disparo (i=1): O filtro original (seja por CPF ou por Telefone) é aplicado normalmente, garantindo que ninguém que
            # já tenha recebido a mensagem hoje seja processado.
            # 2. Repescagem (i > 1):
            #     * Se o filtro era por CPF, ele é desativado (None), pois o CPF já foi "tentado" no passo anterior e agora o objetivo é
            #       justamente tentar outro número para esse mesmo CPF.
            #     * Se o filtro era por Telefone, ele é mantido, garantindo que o novo número escolhido da lista others seja verificado no
            #       BigQuery. Se esse número específico já tiver recebido um disparo (por outra campanha, por exemplo), ele será filtrado.

            print(f"🔍 Checking if phone numbers were already dispatched today...")
            already_dispatched_data = get_already_dispatched_data(billing_project_id=billing_project_id)
            current_attempt_destinations = filter_already_dispatched_phones_or_cpfs(
                destinations=current_attempt_destinations,
                already_dispatched_df=already_dispatched_data,
                field=filter_dispatched_phones_or_cpfs
            )

        # Filter duplicates (important as 'others' inside retries might have repetitions)
        final_destinations = remove_duplicate_phones(current_attempt_destinations) if filter_duplicated_phones else current_attempt_destinations

        if not final_destinations or len(final_destinations) == 0:
            print("No destinations found. Exiting flow execution.")
            return

        # Add contacts to whitelist if percentage is set
        if whitelist_percentage > 0:
            whitelist_group_name = f"citizen-hsm-{campaign_name}"
            if whitelist_replace_contacts:
                remove_contacts_from_whitelist(
                    destinations=final_destinations,
                    environment=whitelist_environment,
                )
            add_contacts_to_whitelist(
                destinations=final_destinations,
                percentage_to_insert=whitelist_percentage,
                group_name=whitelist_group_name,
                environment=whitelist_environment,
                force_add_on_whitelist_group=force_add_on_whitelist_group,
            )
            
        dispatch_payload = create_dispatch_payload(
            campaign_name=campaign_name,
            cost_center_id=cost_center_id,
            destinations=final_destinations,
        )

        print(
            f"\nStarting dispatch for id_hsm={id_hsm}, attempt={i}, campaign_name={campaign_name}, example data {final_destinations[:5]}\n"
        )
        print(f"⚠️  Sleep {sleep_minutes} minutes before dispatch. Check if event date and id_hsm is correct!!")
        time.sleep(sleep_minutes * 60)

        dispatch_date = dispatch(
            api=api,
            id_hsm=id_hsm,
            dispatch_payload=dispatch_payload,
            chunk=chunk_size,
        )

        print(f"Dispatch completed successfully for {len(final_destinations)} destinations on attempt {i+1}.")

        total_batches = ceil(len(final_destinations) / chunk_size)

        dfr = create_dispatch_dfr(
            id_hsm=id_hsm,
            original_destinations=final_destinations,
            campaign_name=campaign_name,
            cost_center_id=cost_center_id,
            dispatch_date=dispatch_date,
        )

        print(f"DataFrame created with {len(dfr)} records for BigQuery upload")

        if not test_mode:
            # Send Discord notification for attempt
            send_dispatch_success_notification(
                total_dispatches=len(final_destinations),
                dispatch_date=dispatch_date,
                id_hsm=id_hsm,
                campaign_name=campaign_name,
                cost_center_id=cost_center_id,
                total_batches=total_batches,
                sample_destination=(final_destinations[0] if final_destinations else None),
                test_mode=test_mode,
                # whitelist_percentage=whitelist_percentage,
                attempt_number=i + 1,  # Exibe 1 para o primeiro disparo, 2 para o retry...
                total_attempt_number=max_dispatch_retries + 1,
            )

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

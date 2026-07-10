# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'

"""
Flow to dispatch templated messages via Salesforce SFTP
"""
import os
import time
from math import ceil
from pathlib import Path
import pandas as pd
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
    send_dispatch_success_notification,
    send_discord_notification,
)
# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.dispatch import (
    add_contacts_to_whitelist,
    check_flow_status,
    create_log_df,
    filter_already_dispatched_phones_or_cpfs,
    filter_duplicated,
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
from pipelines.rj_crm__disparo_template.utils.validators import (  # pylint: disable=E0611, E0401
    validate_campaign_name,
    validate_sf_dataframe,
)
# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.tasks import (
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

    message = f"""
    Prefect flow run failed in PRODUCTION! 🚨
    📋 **Campanha:** {campaign_name}
    ⚠️ **Mensagem:** {state.message}
    """
    send_discord_notification(webhook_url, message)

# force deployy#
@flow(log_prints=True, on_failure=[send_discord_notification_on_failure])
def rj_crm__disparo_template_sf(
    # Parâmetros opcionais para override manual na UI.
    campaign_name: str | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    test_mode: bool | None = True,
    query: str | None = None,
    query_file: str | None = None,
    query_processor_name: str | None = None,
    query_replacements: dict | None = None,
    filter_dispatched_phones_or_cpfs: str | None = "cpf",
    filter_duplicated_phones: bool = True,
    filter_duplicated_cpfs: bool = True,
    filter_failed_phones: bool = False,
    dispatch_interval_days: int = 1,
    sleep_minutes: int | None = 5,
    materialization_sleep_minutes: int | None = 20,
    max_dispatch_retries: int = 0,
    infisical_secret_path: str = "/crm_disparo_template",
    data_extension_filename: str | None = None,
    de_columns: list[str] | None = None,
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
        campaign_name (str, optional): The name of the dispatch campaign.
        chunk_size (int, optional): Kept for interface compatibility; not used for SFTP batching.
        dataset_id (str, optional): BigQuery dataset ID for dispatch logs.
        table_id (str, optional): BigQuery table ID for dispatch logs.
        dump_mode (str, optional): BigQuery dump mode (e.g., "append").
        test_mode (bool, optional): If True, runs in test mode. Defaults to True.
        query (str, optional): SQL query to retrieve destinations.
        query_file (str, optional): Path (relative to this file) to a .sql file to use as the
            query instead of passing the raw SQL in `query`. Takes precedence over `query`.
        query_processor_name (str, optional): Name of the query processor.
        query_replacements (dict, optional): Replacements for query placeholders.
        filter_dispatched_phones_or_cpfs (str, optional): None, "cpf" or "telefone". Defaults to "cpf".
        filter_duplicated_phones (bool, optional): Remove duplicate phones. Defaults to True.
        filter_duplicated_cpfs (bool, optional): Remove duplicate CPFs. Defaults to True.
        filter_failed_phones (bool, optional): Remove phones that failed in last dispatch. Defaults to False.
        sleep_minutes (int, optional): Minutes to sleep before dispatch. Defaults to 5.
        max_dispatch_retries (int): Maximum retry attempts with alternative phones. Defaults to 0.
        infisical_secret_path (str, optional): Infisical path for SFTP credentials. Defaults to "/sftp".
        de_columns (list[str], optional): Lista de campos (além de telefone/SubscriberKey) que a
            Data Extension espera. Quando informada, o CSV enviado ao SFTP é restrito a essas
            colunas — qualquer coluna de controle interno da query (ex.: 'others', 'externalId')
            é descartada antes do envio.
        sftp_remote_path (str, optional): Remote directory on the SFTP server. Defaults to "/".
        whitelist_percentage (int, optional): Percentage of contacts to whitelist. Defaults to 0.
        whitelist_environment (str, optional): Whitelist environment. Defaults to "production".
        flow_environment (str, optional): Flow environment ("staging" or "production"). Defaults to "staging".
        force_add_on_whitelist_group (bool, optional): Force add to whitelist group. Defaults to False.
        whitelist_replace_contacts (bool, optional): Remove contacts before adding to whitelist. Defaults to False.
    """

    # force deploy
    dataset_id = dataset_id or TemplateConstants.DATASET_ID.value
    table_id = table_id or TemplateConstants.TABLE_ID.value
    dump_mode = dump_mode or TemplateConstants.DUMP_MODE.value
    campaign_name = campaign_name or TemplateConstants.CAMPAIGN_NAME.value
    if query_file:
        _query_path = Path(__file__).parent / query_file
        if test_mode and "queries_test" not in str(query_file) and "queries_dev" not in str(query_file):
            _mock_path = Path(__file__).parent / "queries_test" / Path(query_file).name
            if _mock_path.exists():
                print(f"⚠️  test_mode=True: usando mock query {_mock_path.name} em vez de {query_file}")
                _query_path = _mock_path
            else:
                print(f"⚠️  test_mode=True: mock query não encontrada em {_mock_path}, usando parâmetro query")
                query = query or TemplateConstants.QUERY.value
    else:
        query = query or TemplateConstants.QUERY.value
    query_processor_name = query_processor_name or TemplateConstants.QUERY_PROCESSOR_NAME.value
    billing_project_id = TemplateConstants.BILLING_PROJECT_ID.value

    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")  # pylint: disable=unused-variable
    crd = inject_bd_credentials_task(environment="prod")  # noqa  # pylint: disable=unused-variable

    validated_campaign = validate_campaign_name(
        campaign_name=campaign_name,
        billing_project_id=billing_project_id,
        bucket_name=billing_project_id,
    )

    flow_status = check_flow_status(
        flow_environment=flow_environment,
        billing_project_id=billing_project_id,
        bucket_name=billing_project_id,
        campaign_name=campaign_name,
    )
    if flow_status is None:
        print("Ending flow due to inactive status.")
        return
    if validated_campaign is None:
        print(f"Ending flow due to invalid campaign name: {campaign_name} does not exist in table rj-crm-registry.brutos_salesforce.jornada")
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
    if query_complete is None:
        print("Query retornou None (ex: processador de fim de semana). Encerrando flow.")
        return

    print(f"\n⚠️  Query dispatch:\n{query_complete}")

    # O disparo SF trabalha com o DataFrame plano retornado pela query.
    # A query deve retornar as colunas: telefone, others (lista), e demais campos do disparo.
    df = task_download_data_from_bigquery(
        query=query_complete,
        billing_project_id=billing_project_id,
        bucket_name=billing_project_id,
    )

    if df is None or df.empty:
        send_dispatch_no_destinations_found(campaign_name, test_mode)
        return

    print(f"[DEBUG] Colunas antes do rename: {list(df.columns)}")

    # Padroniza a coluna de CPF para 'cpf' caso as queries antigas ainda retornem 'SubscriberKey'
    if "SubscriberKey" in df.columns and "cpf" not in df.columns:
        df = df.rename(columns={"SubscriberKey": "cpf"})
        print("RENAME APLICADO: SubscriberKey → cpf") 

    print(f"[DEBUG] Colunas antes da validação: {list(df.columns)}")

    # Valida colunas obrigatórias para o log SF: cpf e telefone.
    # Lança ValueError imediatamente se alguma estiver ausente ou com dados inválidos.
    validate_sf_dataframe(df, campaign_name)

    print(f"Query retornou {len(df)} linhas. Colunas: {list(df.columns)}")

    # Dedup por CPF — base para o loop de retentativas
    df = filter_duplicated(
        df=df,
        column="cpf",
        filter_indicator=filter_duplicated_cpfs,
        label="CPFs",
    )

    if df.empty:
        print("No destinations found after filtering duplicate CPFs. Exiting flow execution.")
        return

    # Remove telefones cujo último disparo falhou e estão em quarentena
    if filter_failed_phones:
        print("Filtrando telefones em quarentena")
        failed_phones = get_failed_phones(billing_project_id=billing_project_id)
        if failed_phones:
            if max_dispatch_retries > 0:
                # Marca como None para que o loop de retry use o próximo número de 'others'
                df.loc[df["telefone"].isin(failed_phones), "telefone"] = None
            else:
                df = df[~df["telefone"].isin(failed_phones)]

        if df.empty:
            send_dispatch_no_destinations_found(campaign_name, test_mode)
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
            print(f"\n⚠️  Starting retry attempt {i} for campaign_name={campaign_name}...")

            failed_cpfs = get_failed_cpfs(billing_project_id=billing_project_id, campaign_name=campaign_name)

            if not failed_cpfs:
                print(f"✅ No failed CPFs found for retry attempt {i}. Ending retry loop.")
                break

            # Seleciona as linhas dos CPFs que falharam e aplica o próximo número de 'others'
            retry_df = base_df[base_df["cpf"].isin(failed_cpfs)].copy()
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
        # No retry por CPF (i>0) eu não posso mais filtrar por cpfs já disparados
        # e desligo o filtro para o mesmo CPF tentar outro número de telefone
        current_filter = filter_dispatched_phones_or_cpfs
        if i > 0 and current_filter == "cpf":
            current_filter = None

        # Buscamos o histórico de disparos realizados no intervalo de dias configurado
        already_dispatched_df = get_already_dispatched_data(
            billing_project_id=billing_project_id, 
            dispatch_interval_days=dispatch_interval_days
        )

        # --------------------------------------------------------------------------------------
        # PASSO 1: FILTRO DE SEGURANÇA DA MESMA CAMPANHA (ALWAYS CPF/SUBSCRIBERKEY)
        # --------------------------------------------------------------------------------------
        # Esse filtro impede que o mesmo CPF (SubscriberKey) receba a MESMA campanha mais de uma
        # vez dentro da janela configurada (ex: 7 dias). Ele é um limite de segurança rígido
        # e roda sempre usando "SubscriberKey" (CPF) como chave identificadora no current_df.
        # --------------------------------------------------------------------------------------
        if not already_dispatched_df.empty:
            print(f"🔍 Aplicando filtro de mesma campanha ({campaign_name}) sobre o CPF/SubscriberKey...")
            df_same_campaign = already_dispatched_df[already_dispatched_df["nome_campanha"] == campaign_name]
            
            current_df = filter_already_dispatched_phones_or_cpfs(
                df=current_df,
                already_dispatched_df=df_same_campaign,
                current_filter="cpf",
            )

        # --------------------------------------------------------------------------------------
        # PASSO 2: FILTRO DIÁRIO CRUZADO DE CANAL (CPF ou Telefone)
        # --------------------------------------------------------------------------------------
        # Esse filtro impede que o mesmo cidadão receba QUALQUER outra campanha no dia de hoje
        # para evitar fadiga ou duplicidade de comunicação no mesmo canal.
        # - Só roda se 'current_filter' não for None (ou seja, se o usuário ativou o filtro diário
        #   e não estamos em uma retentativa de CPF i > 0).
        # - Respeita a coluna ativa ('SubscriberKey' ou 'telefone') para a comparação.
        # --------------------------------------------------------------------------------------
        if current_filter and not already_dispatched_df.empty:
            # Se o filtro ativo no envio for "cpf", nós mapeamos para "SubscriberKey" para comparar
            # com a coluna de controle (que no get_already_dispatched_data retorna como "cpf").
            print(f"🔍 [Passo 2] Aplicando filtro diário cruzado sobre a coluna '{current_filter}'...")
            
            hoje = pd.Timestamp.now('America/Sao_Paulo').date()
            datas_disparadas = pd.to_datetime(already_dispatched_df["data_particao"]).dt.date
            
            # Selecionamos apenas as linhas de disparos que ocorreram no dia de hoje
            df_dispatched_today = already_dispatched_df[datas_disparadas == hoje]
            
            current_df = filter_already_dispatched_phones_or_cpfs(
                df=current_df,
                already_dispatched_df=df_dispatched_today,
                current_filter=current_filter,
            )

        # Dedup por telefone (retries podem introduzir duplicatas)
        current_df = filter_duplicated(
            df=current_df,
            column="telefone",
            filter_indicator=filter_duplicated_phones,
            label="phones",
        )

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

        print(f"\nStarting SF dispatch for campaign_name={campaign_name}, attempt={i}")
        print(f"Sample data:\n{current_df.head(5).to_dict('records')}")
        print(f"⚠️  Sleep {sleep_minutes} minutes before dispatch. Check if event date and campaign_name is correct!!")
        time.sleep(sleep_minutes * 60)

        # Salva CSV (restrito a telefone/SubscriberKey/de_columns) e registra data do disparo
        csv_path, dispatch_date = save_csv_for_sftp(
            df=current_df.rename(columns={"cpf": "SubscriberKey"}),
            data_extension_filename=data_extension_filename or campaign_name,
            de_columns=de_columns,
        )

        send_to_sftp(
            csv_path=csv_path,
            infisical_secret_path=infisical_secret_path,
        )

        print(f"CSV enviado para SFTP com {len(current_df)} destinatários na tentativa {i+1}.")

        if materialize_after_sftp:
            print(f"⏳ Aguardando {materialization_sleep_minutes if not test_mode else 10} minutos antes de materializar o modelo dbt int_crm_status_disparo...")
            time.sleep(materialization_sleep_minutes * 60) if not test_mode else time.sleep(10*60)
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
                campaign_name=campaign_name,
                total_batches=1,
                sample_destination=(current_df.iloc[0].to_dict() if not current_df.empty else None),
                test_mode=test_mode,
                attempt_number=i + 1,
                total_attempt_number=max_dispatch_retries + 1,
            )

            # Log para BQ: schema fixo com coluna `data` em JSON para camada bronze
            log_df = create_log_df(
                df=current_df.rename(columns={"cpf": "SubscriberKey"}),
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

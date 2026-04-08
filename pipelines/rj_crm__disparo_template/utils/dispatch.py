# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Tasks migradas do template disparo do Prefect 1.4 para 3.0
Baseado em pipelines_rj_crm_registry/pipelines/templates/disparo/tasks.py
"""

import json
import os
import random
from datetime import datetime
from math import ceil
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401
from prefect import task  # pylint: disable=E0611, E0401
from prefect.exceptions import PrefectException  # pylint: disable=E0611, E0401
from pytz import timezone

from pipelines.rj_crm__disparo_template.utils.discord import send_discord_notification  # pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.processors import get_query_processor  # pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.tasks import download_data_from_bigquery  # pylint: disable=E0611, E0401
# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.validators import (
    log_validation_summary,
    validate_destinations,
    validate_dispatch_payload,
)
from pipelines.rj_crm__disparo_template.utils.whitelist import (
    BetaGroupManager,
    get_environment_config,
    normalize_numbers,
    validate_environment_config,
)


@task
def add_contacts_to_whitelist(
    destinations: List[Dict],
    percentage_to_insert: int,
    group_name: str,
    environment: str,
    force_add_on_whitelist_group: bool = False,
) -> None:
    """
    Adds a random percentage of contacts to a whitelist group.

    Args:
        destinations (List[str]): List of destination data as JSON strings.
        percentage_to_insert (int): The percentage of contacts to insert (0-100).
        group_name (str): The name of the group to add contacts to.
        environment (str): The environment to run on ('staging' or 'production').
    """
    if not destinations:
        print("\n⚠️  No destinations to add on whitelist.")
        return

    phone_numbers = []
    for dest_json in destinations:
        try:
            phone = dest_json.get("to")
            if phone:
                phone_numbers.append(phone)
        except Exception as err:
            print(f"\n⚠️  Warning: Could not process destination: {dest_json}, error: {err}")

    if not phone_numbers:
        print("\n⚠️  No valid phone numbers found in destinations to add on whitelist.")
        return

    # Remove duplicates
    unique_phone_numbers = list(set(phone_numbers))

    # Calculate the number of contacts to select
    number_to_select = int(len(unique_phone_numbers) * (percentage_to_insert / 100))

    if number_to_select == 0:
        print(f"\n⚠️  Percentage {percentage_to_insert}% results in 0 contacts to insert on whitelist. Skipping.")
        return

    # Select a random sample
    if number_to_select < len(unique_phone_numbers):
        selected_numbers = random.sample(unique_phone_numbers, number_to_select)
    else:
        selected_numbers = unique_phone_numbers  # Insert all if percentage is 100 or more

    print(f"Selected {len(selected_numbers)} contacts to add to group '{group_name}'.")

    try:
        config = get_environment_config(environment)
        validate_environment_config(config)
        print(f"Whitelist config {config}")
    except ValueError as err:
        print(f"\n⚠️  Configuration error: {err}")
        return

    manager = BetaGroupManager(
        config["issuer"],
        config["client_id"],
        config["client_secret"],
        config["api_base_url"],
    )

    if not manager.authenticate():
        message = "\n⚠️  Authentication failed. Cannot add contacts to whitelist."
        print(message)
        raise PrefectException(message)

    # Find or create the group
    group = manager.find_group_by_name(group_name)
    if not group:
        group = manager.create_group(group_name)

    if not group:
        message = f"\n⚠️  Could not find or create group '{group_name}'. Aborting."
        print(message)
        raise PrefectException(message)

    group_id = group["id"]

    # Get existing numbers to avoid duplicates
    existing_numbers_set = manager.get_existing_numbers_set(force_add_on_whitelist_group=force_add_on_whitelist_group)
    new_numbers_to_add = [num for num in selected_numbers if num not in existing_numbers_set]

    if not new_numbers_to_add:
        print(f"\n✅  All selected numbers are already in the whitelist for group '{group_name}'.")
        return

    print(f"Adding {len(new_numbers_to_add)} new contacts to group '{group_name}' (ID: {group_id}).")

    normalized_numbers = []
    for num in new_numbers_to_add:
        normalized_numbers.extend(normalize_numbers(num))
    print(f"New numbers to add: {new_numbers_to_add}")
    print(f"Normalized numbers to add: {normalized_numbers}")
    
    # Remove duplicates to avoid redundant API calls
    unique_normalized_numbers = list(set(normalized_numbers))
    print(f"Unique Normalized numbers to add: {unique_normalized_numbers}")

    if manager.add_numbers_to_group(group_id, unique_normalized_numbers):
        print("\n✅  Successfully added contacts to the whitelist.")
    else:
        message = "\n⚠️  Failed to add contacts to the whitelist."
        print(message)
        raise PrefectException(message)


@task
def remove_contacts_from_whitelist(
    destinations: List[Dict],
    environment: str,
) -> None:
    """
    Removes all contacts from the destinations list from the whitelist.

    Args:
        destinations (List[Dict]): List of destination data.
        environment (str): The environment to run on ('staging' or 'production').
    """
    if not destinations:
        print("\n⚠️  No destinations to remove from whitelist.")
        return

    phone_numbers = []
    for dest in destinations:
        try:
            phone = dest.get("to")
            print(f"DEBUG: Processing destinations for removal, found phone: {phone} in destination: {dest}")
            if phone:
                phone_numbers.append(phone)
        except Exception as err:
            print(f"\n⚠️  Warning: Could not process destination for removal: {dest}, error: {err}")

    if not phone_numbers:
        print("\n⚠️  No valid phone numbers found in destinations to remove from whitelist.")
        return

    # Remove duplicates to get the final list of numbers to remove
    selected_numbers = list(set(phone_numbers))
    print(f"DEBUG: All phone_numbers {phone_numbers} \nUnique phone numbers identified for removal: {selected_numbers}")

    print(f"Selected {len(selected_numbers)} contacts to remove from whitelist.")

    try:
        config = get_environment_config(environment)
        validate_environment_config(config)
    except ValueError as err:
        print(f"\n⚠️  Configuration error: {err}")
        return

    manager = BetaGroupManager(
        config["issuer"],
        config["client_id"],
        config["client_secret"],
        config["api_base_url"],
    )

    if not manager.authenticate():
        print("\n⚠️  Authentication failed. Cannot remove contacts from whitelist.")
        return

    # Remove in bulk directly (the API endpoint is global, no group needed)
    if manager.remove_numbers_bulk(selected_numbers):
        print(f"\n✅  Successfully removed {len(selected_numbers)} contacts from whitelist.")
    else:
        print(f"\n⚠️  Failed to remove contacts from whitelist.")


@task
def create_dispatch_payload(campaign_name: str, cost_center_id: int, destinations: Union[List, pd.DataFrame]) -> Dict:
    """
    Cria o payload para o dispatch com validação rigorosa

    Args:
        campaign_name: Nome da campanha
        cost_center_id: ID do centro de custo
        destinations: Lista de destinatários ou DataFrame

    Returns:
        Dict com payload validado para WeTalkie API

    Raises:
        ValueError: Se algum campo for inválido
    """
    # Convert DataFrame to list if needed
    if isinstance(destinations, pd.DataFrame):
        destinations = destinations.to_dict("records")
    
    # TODO: quando filtramos os telefones com failed e temos retentativa, o dado chega aqui com 
    # to: None e others: [prox_num1, prox_num2...], mas o schema exige que to seja string.
    # Poderia aqui remover esses casos do payload
    # Exemplo do dado aqui: [{'to': None, 'externalId': '00000000011', 'vars': {'NOME': 'Rodolpho ', 'CC_WT_NOME': 'Rodolpho ', 'CC_WT_CPF_CIDADAO': '00000000011'}, 'others': ['5511984677798']}, {'to': None, 'externalId': '00000000011', 'vars': {'NOME': 'Patricia ', 'CC_WT_NOME': 'Patricia ', 'CC_WT_CPF_CIDADAO': '00000000011'}, 'others': ['5511984677798']}, {'to': '5592984212629', 'externalId': '00000000055', 'vars': {'NOME': 'Patrick ', 'CC_WT_NOME': 'Patrick ', 'CC_WT_CPF_CIDADAO': '00000000055'}, 'others': ['5592984212629']}]
    # como não tem failed ele não entra no retry loop

    # Validate destinations first
    validated_destinations, validation_stats = validate_destinations(destinations)
    log_validation_summary(validation_stats, "create_dispatch_payload")

    # Validate complete payload
    payload = validate_dispatch_payload(
        campaign_name=campaign_name, cost_center_id=cost_center_id, destinations=validated_destinations
    )

    log(f"Payload created successfully for {len(validated_destinations)} validated destinations")

    # Retorna o dicionário EXCLUINDO o campo 'others' de todos os destinatários na lista
    # Isso garante que a API não receba um campo que ela não conhece
    return payload.dict(exclude={"destinations": {"__all__": {"others"}}})


@task
def dispatch(api: object, id_hsm: int, dispatch_payload: dict, chunk: int) -> str:
    """
    Do a dispatch in chunks (função do template disparo)
    Fixed to not mutate original payload
    """
    destinations = dispatch_payload["destinations"]
    total = len(destinations)
    original_campaign_name = dispatch_payload["campaignName"]

    dispatch_date = datetime.now(timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")

    if total == 0:
        log("Total de números é igual a zero. Nenhum disparo será feito.")
        raise Exception("No destinations to dispatch")

    total_batches = ceil(total / chunk)
    log(f"Starting dispatch of {total} destinations in {total_batches} batches of size {chunk}")

    for i, start in enumerate(range(0, total, chunk), 1):
        end = start + chunk
        batch = destinations[start:end]

        # Create a copy of payload for each batch to avoid mutation
        batch_payload = dispatch_payload.copy()
        batch_payload["destinations"] = batch
        batch_payload["campaignName"] = f"{original_campaign_name}-{dispatch_date[:10]}-lote{i}"

        log(f"Disparando lote {i} de {total_batches} com {len(batch)} destinos")
        response = api.post(path=f"/callcenter/hsm/send/{id_hsm}", json=batch_payload)

        if response.status_code != 201:
            log(f"Falha no disparo do lote {i}: {response.text}")
            response.raise_for_status()
            raise Exception(f"Dispatch failed: {response.text}")

        log(f"Disparo do lote {i} realizado com sucesso!")

    log(f"Disparo realizado com sucesso! Total de {total} destinations processadas em {total_batches} lotes")
    return dispatch_date


@task
def create_dispatch_dfr(
    id_hsm: int,
    original_destinations: List[Dict],
    campaign_name: str,
    cost_center_id: int,
    dispatch_date: str,
) -> pd.DataFrame:
    """
    Salva o disparo no banco de dados usando todas as destinations originais
    Agora inclui validação para garantir integridade dos dados salvos
    """
    # Validate destinations before creating DataFrame
    validated_destinations, validation_stats = validate_destinations(original_destinations)
    log_validation_summary(validation_stats, "create_dispatch_dfr")

    if not validated_destinations:
        raise ValueError("Nenhum destinatário válido para criar DataFrame de dispatch")

    data = []
    for destination in validated_destinations:
        # Use Pydantic model attributes
        row = {
            "id_hsm": id_hsm,
            "dispatch_date": dispatch_date,
            "campaignName": campaign_name,
            "costCenterId": cost_center_id,
            "to": destination.to,
            "externalId": destination.externalId,  # Now mandatory
            "vars": destination.vars,
        }
        data.append(row)

    dfr = pd.DataFrame(data)
    dfr = dfr[
        [
            "id_hsm",
            "dispatch_date",
            "campaignName",
            "costCenterId",
            "to",
            "externalId",
            "vars",
        ]
    ]

    log(f"DataFrame created with {len(dfr)} validated records")
    log("All records have mandatory externalId field populated")

    # Validate that no externalId is None (should not happen with our validation)
    null_external_ids = dfr["externalId"].isnull().sum()
    if null_external_ids > 0:
        log(f"WARNING: Found {null_external_ids} records with null externalId after validation")

    return dfr


@task
def check_api_status(api: object) -> bool:
    """Verifica se a API está funcionando retornando status 200"""
    try:
        response = api.get("/")
        if response.status_code == 200:
            print("API está funcionando corretamente.")
            return True

        print(f"API retornou status {response.status_code}.")
        return False
    except Exception as error:
        print(f"Erro ao acessar a API: {error}")
        return False


@task
def get_already_dispatched_data(billing_project_id: str) -> pd.DataFrame:
    """
    Busca no BigQuery a lista de CPFs ou telefones que já tiveram um disparo
    bem-sucedido ou em processamento hoje.
    """
    query = """
        SELECT DISTINCT targetExternalId as externalId, flatTarget as celular_disparo, status
        FROM `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
        WHERE DATE(createDate) = CURRENT_DATE("America/Sao_Paulo")
          AND status IN ("PROCESSING", "SENT", "DELIVERED", "READ")
    """
    log(f"Buscando disparos já realizados hoje para evitar duplicidade:\n{query}")
    try:
        df = download_data_from_bigquery(
            query=query, billing_project_id=billing_project_id, bucket_name=billing_project_id
        )
        return df
    except Exception as err:
        log(f"Erro ao buscar disparos realizados: {err}. Retornando DataFrame vazio.", level="warning")
        return pd.DataFrame(columns=["externalId", "celular_disparo", "status"])


@task
def filter_already_dispatched_phones_or_cpfs(
    destinations: List[Dict], 
    already_dispatched_df: pd.DataFrame, 
    field: str = "cpf"
) -> List[Dict]:
    """
    Filters out CPFs that have already been dispatched today.

    Args:
        destinations (List[Dict]): A list of destination dictionaries.
        already_dispatched_df (pd.DataFrame): A DataFrame containing already dispatched destinations.
        field (str): The field in the destination dict that contains the phone number.
            Field must be "cpf" or "phone_number"

    Returns:
        List[Dict]: A list of destination dictionaries that have not yet been dispatched.
    """
    if not destinations or already_dispatched_df.empty:
        return destinations

    if field not in ["cpf", "phone_number"]:
        log(f"\n⚠️  Invalid field '{field}' for filtering dispatched phones. Must be 'cpf' or 'phone_number'")
        return destinations

    already_dispatched_df.rename(columns={"celular_disparo": "to"}, inplace=True)

    destinations_field = "externalId" if field == "cpf" else "to"
    already_dispatched_field = "externalId" if field == "cpf" else "to"

    if already_dispatched_field not in already_dispatched_df.columns:
        log(f"Coluna {already_dispatched_field} não encontrada nos dados de controle. Ignorando filtro.", level="warning")
        return destinations

    dispatched_set = set(already_dispatched_df[already_dispatched_field].tolist())
    
    filtered_destinations = [
        dest for dest in destinations if dest.get(destinations_field) not in dispatched_set
    ]
    
    removed_count = len(destinations) - len(filtered_destinations)
    log(f"Filtro '{field}' aplicado: {removed_count} registros removidos por já terem disparos realizados hoje.")
    
    return filtered_destinations


def normalize_keys(d: Dict) -> Dict:
        """Helper para normalizar chaves do dicionário para o padrão esperado pelo schema."""
        if not isinstance(d, dict):
            return d
        
        mapping = {
            "celular_disparo": "to",
            "to": "to",
            "externalid": "externalId",
            "external_id": "externalId",
            "vars": "vars",
            "others": "others"
        }
        
        normalized = {}
        for k, v in d.items():
            k_lower = k.lower()
            if k_lower in mapping:
                normalized[mapping[k_lower]] = v
            else:
                normalized[k] = v
        return normalized


@task
def get_destinations(
    destinations: Union[None, List[Dict], str],
    query: str,
    billing_project_id: str = "rj-crm-registry",
) -> List[Dict]:
    """
    Get destinations from the query or from the parameter with validation.
    Normaliza chaves de forma insensível a maiúsculas/minúsculas.
    """
    if query:
        log("\nQuery was found")
        destinations_df = download_data_from_bigquery(
            query=query,
            billing_project_id=billing_project_id,
            bucket_name=billing_project_id,
        )
        if destinations_df is None or destinations_df.empty or destinations_df.shape[1] == 0:
            log("No destinations found from query. Returning empty list.")
            return []

        log(f"Resposta da query: {destinations_df.iloc[0]}")
        
        # Pega a primeira coluna (que deve ser o JSON STRING)
        destinations_list = destinations_df.iloc[:, 0].tolist()
        destinations = [json.loads(str(item)) for item in destinations_list]

    else:
        if isinstance(destinations, str):
            destinations = json.loads(destinations)
        else:
            return []

    if destinations:
        print(f"Exemplo de destino antes da normalização: {destinations[0]}")
        # Normaliza as chaves (ex: EXTERNALID -> externalId, celular_disparo -> to)
        destinations = [normalize_keys(d) for d in destinations]
        print(f"Exemplo de destino após a normalização: {destinations[0]}")

        validated_destinations, validation_stats = validate_destinations(destinations)
        log_validation_summary(validation_stats, "get_destinations")
        return [dest.dict() for dest in validated_destinations]

    return []


@task
def remove_duplicate_phones(destinations: List[Dict]) -> List[Dict]:
    """
    Remove duplicate phone numbers from destinations list with validation.
    Keeps only the first occurrence of each phone number.
    Validates each destination before processing.
    """
    if not destinations:
        log("No destinations to process")
        return destinations

    # Re-validate destinations to ensure data integrity
    validated_destinations, validation_stats = validate_destinations(destinations)
    log_validation_summary(validation_stats, "remove_duplicate_phones")

    # Process only validated destinations
    seen_phones = set()
    unique_destinations = []
    duplicates_count = 0

    for destination in validated_destinations:
        phone = destination.to  # Pydantic model attribute
        external_id = destination.externalId

        if phone not in seen_phones:
            seen_phones.add(phone)
            unique_destinations.append(destination.dict())  # Convert back to dict
        else:
            duplicates_count += 1
            log(f"Duplicate phone removed: {phone[:8]}**** (externalId: {external_id})")

    log(f"Removed {duplicates_count} duplicate phone numbers")
    log(f"Total unique destinations: {len(unique_destinations)}")

    return unique_destinations


@task
def remove_duplicate_cpfs(destinations: List[Dict]) -> List[Dict]:
    """
    Remove duplicate CPFs (externalId) from destinations list with validation.
    Keeps only the first occurrence of each CPF.
    Validates each destination before processing.
    """
    if not destinations:
        log("No destinations to process")
        return destinations

    # Re-validate destinations to ensure data integrity
    validated_destinations, validation_stats = validate_destinations(destinations)
    log_validation_summary(validation_stats, "remove_duplicate_cpfs")

    # Process only validated destinations
    seen_cpfs = set()
    unique_destinations = []
    duplicates_count = 0

    for destination in validated_destinations:
        cpf = destination.externalId  # Pydantic model attribute

        if cpf not in seen_cpfs:
            seen_cpfs.add(cpf)
            unique_destinations.append(destination.dict())  # Convert back to dict
        else:
            duplicates_count += 1
            log(f"Duplicate CPF removed: {cpf[:4]}**** (phone: {destination.to})")

    log(f"Removed {duplicates_count} duplicate CPFs")
    log(f"Total unique destinations: {len(unique_destinations)}")

    return unique_destinations


@task
def check_if_dispatch_approved(
    dfr: pd.DataFrame,
    dispatch_approved_col: str,
    event_date_col: str,
) -> Tuple[str, bool]:
    """
    Check if dispatch was approved using a specific table in BQ
    """
    if dfr.empty:
        log("\n⚠️  Approval dataframe is empty.")
        return None, False

    log(f"Dataframe for today: {dfr.iloc[0]}")

    normalized_status_col = (
        dfr[dispatch_approved_col]
        .dropna()
        .astype(str)
        .str.strip()
        .str.lower()
    )

    if normalized_status_col.empty:
        log("\n⚠️  No valid values found in dispatch approval column.")
        return None, False

    dispatch_status = normalized_status_col.sort_values().iloc[0]

    log(f"\nChecking dispatch approval for today: Status='{dispatch_status}'")

    if dispatch_status == "aprovado":
        event_date = dfr[event_date_col].astype(str).iloc[0]
        log(f"\n✅  Dispatch approved for event day: {event_date}.")
        return event_date, True

    log("\n⚠️  Dispatch was not approved for today.")
    return None, False


@task
def format_query(raw_query: str, replacements: dict, query_processor_name: str = None) -> Optional[str]:
    """
    Formats a SQL query by replacing placeholders with values from a dictionary.

    Args:
        raw_query (str): The SQL query template containing placeholders in str.format style
            (e.g., {event_date_placeholders}, {id_hsm_placeholders}).
        replacements (dict): A dictionary mapping placeholder names to their values.
        query_processor_name (str, optional): Name of a custom query processor to apply
            additional formatting. Defaults to None.

    Returns:
        str: The formatted query with all placeholders replaced by their corresponding values.

    Raises:
        ValueError: If raw_query is None or if a placeholder is missing from replacements.
        TypeError: If replacements is not a dictionary.

    Examples:
        >>> query = "SELECT * FROM table WHERE date = {event_date_placeholders} AND id = {id_hsm_placeholders}"
        >>> replacements = {"event_date_placeholders": "2025-11-03", "id_hsm_placeholders": 123}
        >>> format_query(query, replacements)
        "SELECT * FROM table WHERE date = 2025-11-03 AND id = 123"

    Notes:
        - Placeholders in raw_query must follow Python's str.format syntax (e.g., {placeholder_name})
        - If query_processor_name is provided, the function will attempt to apply the specified
          processor before formatting the query
    """
    if raw_query is None:
        raise ValueError("Query cannot be None")
    if not isinstance(replacements, dict):
        raise TypeError("replacements must be a dict")

    # Apply query processor if provided
    if query_processor_name:
        processor_func = get_query_processor(query_processor_name)
        if processor_func:
            log(f"Applying query processor: {query_processor_name}")
            return processor_func(raw_query, replacements)

        log(f"Warning: Query processor '{query_processor_name}' not found, using original query")

    if isinstance(replacements, dict) and "value" in replacements and "__prefect_kind" in replacements:
        replacements = json.loads(replacements["value"])
        print(f"replacements modificado: {replacements}")
    return raw_query.format_map(replacements)


@task
def check_flow_status(flow_environment: str, id_hsm: int, billing_project_id: str, bucket_name: str) -> Optional[bool]:
    """
    Verifica se o fluxo está ativo e dentro do prazo de validade consultando o BigQuery.
    Args:
        flow_environment: Ambiente do fluxo ('staging' ou 'production')
        id_hsm: ID do template HSM
        billing_project_id: ID do projeto GCP para billing
        bucket_name: Nome do bucket GCS para carregamento de credenciais
    Returns:
        True se o fluxo estiver ativo e válido, None caso contrário."""

    log(f"\nStarting flow status check for id_hsm={id_hsm} in environment={flow_environment}.")

    if flow_environment not in ["staging", "production"]:
        log(f"\n⚠️  Invalid flow_environment: {flow_environment}. Must be 'staging' or 'production'.")
        return None

    query = f"""
        SELECT ativo, data_limite_disparo, nome_campanha
        FROM `rj-crm-registry.brutos_wetalkie_staging.disparos_ativos`
        WHERE id_hsm = '{id_hsm}' AND ambiente = '{flow_environment}'
        LIMIT 1
    """
    dfr = download_data_from_bigquery(
        query=query,
        billing_project_id=billing_project_id,
        bucket_name=bucket_name,
    )
    log(f"DEBUG: Flow status query result:\n{dfr} \nwith query {query}")
    if dfr.empty:
        log(f"\n⚠️  No configuration found for id_hsm={id_hsm} in environment={flow_environment}.")
        return None

    row = dfr.iloc[0]

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_ERRORS")
    if not webhook_url:
        print("DISCORD_WEBHOOK_URL_ERRORS environment variable not set. Cannot send notification.")

    if not row.get("ativo") or row.get("ativo") not in (1, "1"):
        log(f"\n⚠️  Flow is not active for id_hsm={id_hsm} in environment={flow_environment}.")
        message = f"""
    Prefect flow run desativado em https://docs.google.com/spreadsheets/d/1O-noD696ZjIr9X_Vl4ZKyFDyg0q9KHe9jacExdAp4ck/!
    📋 **Campanha:** {row.get("nome_campanha")}
    🆔 **Template ID:** {id_hsm}
    💻 **Ambiente:** {flow_environment}

    Desligue o scheduler no prefect ou mude o status para ativo para reativar o fluxo.
    """
        send_discord_notification(webhook_url, message)
        return None

    current_date = datetime.now(timezone("America/Sao_Paulo")).date()

    expiration_date = row.get("data_limite_disparo") if not pd.isnull(row.get("data_limite_disparo")) else current_date

    if expiration_date < current_date:
        log(f"\n⚠️  Flow for id_hsm={id_hsm} in environment={flow_environment} has expired on {expiration_date}.")
        message = f"""
    Prefect flow run atingiu a data limite em https://docs.google.com/spreadsheets/d/1O-noD696ZjIr9X_Vl4ZKyFDyg0q9KHe9jacExdAp4ck/!
    📋 **Campanha:** {row.get("nome_campanha")}
    🆔 **Template ID:** {id_hsm}
    💻 **Ambiente:** {flow_environment}
    📆 **Data limite do disparo:** {expiration_date}

    Desligue o scheduler no prefect ou altere a data limite.
    """
        send_discord_notification(webhook_url, message)
        return None

    log(f"\n✅  Active flow found for id_hsm={id_hsm} in environment={flow_environment}.")
    return True


def get_value_from_case_insensitive_key(d: Dict, target_key: str) -> Any:
        """Busca uma chave em um dicionário ignorando maiúsculas/minúsculas e retorna o valor."""
        target_lower = target_key.lower()
        if not isinstance(d, dict):
            return None
        for k, v in d.items():
            if k.lower() == target_lower:
                return v
        return None


def get_failed_phones(billing_project_id: str) -> set:
    """
    Busca telefones tiveram falha de não ter whatsapp ou bloqueio no último disparo,
    passo necessário já que o telefone principal não vem do RMI. Caso contrário, seria
    só necessário filtrar pela estratégia de envio.
    """
    # Em alguns raros casos, o webhook retorna "FAILED", mas depois a pessoa recebe a mensagem.
    query = f"""
        WITH status_por_disparo AS (
            SELECT
                flatTarget,
                createDate,
                MAX(
                    CASE
                        WHEN status = "PROCESSING" THEN 1
                        WHEN status = "FAILED" and faultdescription like "%131026%" THEN 2
                        WHEN status = "SENT" THEN 3
                        WHEN status = "DELIVERED" THEN 4
                        WHEN status = "READ" THEN 5
                    END
                ) AS id_status_disparo
            FROM `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
            GROUP BY flatTarget, createDate
        ),

        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY flatTarget
                    ORDER BY createDate DESC
                ) AS rn
            FROM status_por_disparo
        )

        SELECT flatTarget
        FROM ranked
        WHERE rn = 1
        AND id_status_disparo = 2
        -- rn = 1 para pegar o último disparo e id_status_disparo = 2 para selecionar apenas os com falha
    """
    try:
        failed_df = download_data_from_bigquery(
            query=query,
            billing_project_id=billing_project_id,
            bucket_name=billing_project_id
        )
        print(f"DEBUG: Primeiro ID com falha detectado: {failed_df.iloc[0]}... (total {failed_df.shape[0]})")
        failed_phones = set(str(x) for x in failed_df['flatTarget'].tolist())
        return failed_phones    
    except Exception as e:
        log(f"Erro ao buscar falhas para retentativa: {e}")
        return set()


def get_failed_cpfs(billing_project_id: str, id_hsm: int,) -> set:
    """
    Busca CPFs tiveram falha de não ter whatsapp ou bloqueio no disparo das últimas 2 horas,
    """
    # Em alguns raros casos, o webhook retorna "FAILED", mas depois a pessoa recebe a mensagem.
    query = f"""
        WITH status_por_disparo AS (
            SELECT
                targetExternalId,
                createDate,
                MAX(
                    CASE
                        WHEN status = "PROCESSING" THEN 1
                        WHEN status = "FAILED" and faultdescription like "%131026%" THEN 2
                        WHEN status = "SENT" THEN 3
                        WHEN status = "DELIVERED" THEN 4
                        WHEN status = "READ" THEN 5
                    END
                ) AS id_status_disparo
            FROM `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
            WHERE templateId = {id_hsm}
            AND sendDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
            GROUP BY targetExternalId, createDate
        ),

        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY targetExternalId
                    ORDER BY createDate DESC
                ) AS rn
            FROM status_por_disparo
        )

        SELECT targetExternalId
        FROM ranked
        WHERE rn = 1
        AND id_status_disparo = 2
        -- rn = 1 para pegar o último disparo e id_status_disparo = 2 para selecionar apenas os com falha
    """
    try:
        failed_df = download_data_from_bigquery(
            query=query,
            billing_project_id=billing_project_id,
            bucket_name=billing_project_id
        )
        print(f"DEBUG: Primeiro ID com falha detectado para retentativa: {failed_df.iloc[0]}... (total {failed_df.shape[0]})")
        failed_cpfs = set(str(x) for x in failed_df['targetExternalId'].tolist())
        print(f"DEBUG failed_cpfs {failed_cpfs}")
    except Exception as e:
        log(f"Erro ao buscar falhas para retentativa: {e}")
        return set()

    if not failed_cpfs:
        log(f"Nenhuma falha detectada para nas últimas 2 horas.")
        return set()
    
    return failed_cpfs
    

@task
def remove_failed_phones(
    original_destinations: List[Dict],
    billing_project_id: str,
    max_dispatch_retries: int,
) -> List[Dict]:

    failed_phones = get_failed_phones(billing_project_id=billing_project_id)

    if not failed_phones:
        return original_destinations

    print(f"We have on DL {len(failed_phones)} destinations with previously failed phones.")
    new_destinations = []
    for dest in original_destinations:
        # Busca to ignorando o "case"
        to_number = get_value_from_case_insensitive_key(dest, 'to')

        # if to_number is not None and str(to_number) not in failed_phones and max_dispatch_retries==0:
        #     new_destinations.append(dest)
        if (to_number is None or str(to_number) in failed_phones) and max_dispatch_retries==0:
            pass  # Se o telefone principal falhou e não há retentativas, removemos o destino completamente
        elif str(to_number) in failed_phones and max_dispatch_retries>0:
            # Atualiza o campo 'to' com None para os telefones que falharam, forçando a retentativa a usar o próximo número da lista 'others'
            new_dest = dest.copy()
            new_dest['to'] = None
            print(f"DEBUG: {to_number} falhou no último disparo e foi alterado para None = {new_dest}")
            new_destinations.append(new_dest)
        else:
            new_destinations.append(dest)

    log(f"Removed {len(original_destinations) - len(new_destinations)} destinations with failed phones. Remaining destinations: {len(new_destinations)}.")
    return new_destinations


@task
def get_retry_destinations(
    id_hsm: int,
    original_destinations: List[Dict],
    billing_project_id: str,
    attempt_number: int  # 1 para primeira retentativa, 2 para segunda...
) -> List[Dict]:
    """
    Identifica quais CPFs falharam e prepara a lista para retentativa com o próximo número da lista 'others'.
    Suporta chaves com variações de maiúsculas/minúsculas e estruturas aninhadas.

    Exemplo de como deve estar o schema da query:
    {
       "celular_disparo": "5521999999999",
       "externalId": "12345678901",
       "vars": {
         "nome_usuario": "João Silva"
       },
       "others": [
         "5521888888888",
         "5521777777777"
      ]
    }
    """
    failed_ids = []
    if attempt_number > 0:
        # Só roda quando não for preencher os nulos gerados pela task remove_failed_phones 
        failed_ids = get_failed_cpfs(billing_project_id=billing_project_id, id_hsm=id_hsm)

        if not failed_ids or len(failed_ids) == 0:
            log(f"Nenhuma falha detectada para a tentativa {attempt_number}.")
            return []
    

    retry_destinations = []
    for dest in original_destinations:
        # Busca externalId e others ignorando o "case"
        ext_id = get_value_from_case_insensitive_key(dest, 'externalId')
        others = get_value_from_case_insensitive_key(dest, 'others') or []
        to_number = get_value_from_case_insensitive_key(dest, 'to')
        print(f"DEBUG dest {dest}")

        if (ext_id is None or str(ext_id) in failed_ids or to_number is None) and len(others) >= attempt_number:
            new_dest = dest.copy()
            # Atualiza o campo 'to' com o número da repescagem (tentativa 1 pega others[0])
            new_dest['to'] = others[attempt_number - 1]
            print(f"DEBUG: new_dest alterado = {new_dest}")
            retry_destinations.append(new_dest)
        elif attempt_number == 0:
            retry_destinations.append(dest)            

    log(f"Preparados {len(retry_destinations)} destinos para a retentativa {attempt_number}.")
    return retry_destinations

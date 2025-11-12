# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Tasks migradas do template disparo do Prefect 1.4 para 3.0 - SMAS Disparo CADUNICO
Baseado em pipelines_rj_crm_registry/pipelines/templates/disparo/tasks.py
"""

import json
import random
from datetime import datetime
from math import ceil
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401
from prefect import task  # pylint: disable=E0611, E0401
from pytz import timezone

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
    validate_environment_config,
)


@task
def add_contacts_to_whitelist(
    destinations: List[Dict],
    percentage_to_insert: int,
    group_name: str,
    environment: str,
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
        print("\n⚠️  Authentication failed. Cannot add contacts to whitelist.")
        return

    # Find or create the group
    group = manager.find_group_by_name(group_name)
    if not group:
        group = manager.create_group(group_name)

    if not group:
        print(f"\n⚠️  Could not find or create group '{group_name}'. Aborting.")
        return

    group_id = group["id"]

    # Get existing numbers to avoid duplicates
    existing_numbers_set = manager.get_existing_numbers_set()
    new_numbers_to_add = [num for num in selected_numbers if num not in existing_numbers_set]

    if not new_numbers_to_add:
        print(f"\n✅  All selected numbers are already in the whitelist for group '{group_name}'.")
        return

    print(f"Adding {len(new_numbers_to_add)} new contacts to group '{group_name}' (ID: {group_id}).")

    if manager.add_numbers_to_group(group_id, new_numbers_to_add):
        print("\n✅  Successfully added contacts to the whitelist.")
    else:
        print("\n⚠️  Failed to add contacts to the whitelist.")


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

    # Validate destinations first
    validated_destinations, validation_stats = validate_destinations(destinations)
    log_validation_summary(validation_stats, "create_dispatch_payload")

    # Validate complete payload
    payload = validate_dispatch_payload(
        campaign_name=campaign_name, cost_center_id=cost_center_id, destinations=validated_destinations
    )

    log(f"Payload created successfully for {len(validated_destinations)} validated destinations")

    # Return as dict for backward compatibility
    return payload.dict()


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
def get_destinations(
    destinations: Union[None, List[Dict], str],
    query: str,
    billing_project_id: str = "rj-smas",
) -> List[Dict]:
    """
    Get destinations from the query or from the parameter with validation.

    Returns validated destinations with mandatory externalId field.
    """
    if query:
        log("\nQuery was found")

        destinations = download_data_from_bigquery(
            query=query,
            billing_project_id=billing_project_id,
            bucket_name=billing_project_id,
        )
        log(f"response from query {destinations.head()}")
        destinations = destinations.iloc[:, 0].tolist()
        destinations = [json.loads(str(item).replace("celular_disparo", "to")) for item in destinations]
    else:
        if isinstance(destinations, str):
            destinations = json.loads(destinations)
        else: return []

    # Validate destinations using centralized validation
    if destinations:
        validated_destinations, validation_stats = validate_destinations(destinations)
        log_validation_summary(validation_stats, "get_destinations")
        # Convert back to dict format for backward compatibility
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

    try:
        return raw_query.format_map(replacements)
    except KeyError as error:
        missing = error.args[0] if error.args else str(error)
        raise ValueError(f"Missing replacement for placeholder '{missing}'") from error

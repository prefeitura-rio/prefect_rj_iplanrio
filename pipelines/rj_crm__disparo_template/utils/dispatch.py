# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Tasks migradas do template disparo do Prefect 1.4 para 3.0
Baseado em pipelines_rj_crm_registry/pipelines/templates/disparo/tasks.py
"""

import asyncio
import json
import os
import random
import time
from datetime import datetime
from math import ceil
from typing import Any, Dict, List, Optional, Tuple, Union

import asyncssh
import pandas as pd
from iplanrio.pipelines_utils.dbt import execute_dbt_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.env import getenv_or_action
from iplanrio.pipelines_utils.logging import log  # pylint: disable=E0611, E0401
from prefect import task  # pylint: disable=E0611, E0401
from prefect.exceptions import PrefectException  # pylint: disable=E0611, E0401
from pytz import timezone

from pipelines.rj_crm__disparo_template.utils.discord import send_discord_notification  # pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.enrichers import DF_ENRICHERS, get_df_enricher  # pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.processors import get_query_processor  # pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.tasks import download_data_from_bigquery  # pylint: disable=E0611, E0401
# pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.schemas import SfDispatchRow  # pylint: disable=E0611, E0401
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
            phone = dest_json.get("telefone")
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

    # Add numbers with and without 9 after ddd
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
            phone = dest.get("telefone")
            print(f"DEBUG: Processing destinations for removal, found phone: {phone} in destination: {dest}")
            if phone:
                phone_numbers.append(phone)
        except Exception as err:
            print(f"\n⚠️  Warning: Could not process destination for removal: {dest}, error: {err}")

    if not phone_numbers:
        print("\n⚠️  No valid phone numbers found in destinations to remove from whitelist.")
        return

    # Add numbers on list with and without 9 after ddd
    normalized_numbers = []
    for num in phone_numbers:
        normalized_numbers.extend(normalize_numbers(num))
    print(f"Numbers to remove from whitelist: {phone_numbers}")
    print(f"Normalized numbers to remove: {normalized_numbers}")
    
    # Remove duplicates to get the final list of numbers to remove
    selected_numbers = list(set(normalized_numbers))
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
    # telefone: None e others: [prox_num1, prox_num2...], mas o schema exige que telefone seja string.
    # Poderia aqui remover esses casos do payload
    # Exemplo do dado aqui: [{'telefone': None, 'cpf': '00000000011', 'vars': {...}, 'others': ['5511984677798']}, ...]
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
    return payload.model_dump(exclude={"destinations": {"__all__": {"others"}}})


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
            "telefone": destination.telefone,
            "cpf": destination.cpf,
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
            "telefone",
            "cpf",
            "vars",
        ]
    ]

    log(f"DataFrame created with {len(dfr)} validated records")
    log("All records have mandatory cpf field populated")

    # Validate that no cpf is None (should not happen with our validation)
    null_cpfs = dfr["cpf"].isnull().sum()
    if null_cpfs > 0:
        log(f"WARNING: Found {null_cpfs} records with null cpf after validation")

    return dfr


@task
def create_log_df(
    df: pd.DataFrame,
    dispatch_date: str,
    campaign_name: str,
) -> pd.DataFrame:
    """
    Constrói o DataFrame de log para a camada bronze do BigQuery a partir do
    DataFrame de disparo do flow SF.

    Schema de saída fixo (independente da query):
        dispatch_date  | campaign_name | SubscriberKey | telefone | data

    - dispatch_date e campaign_name são metadados do disparo.
    - SubscriberKey e telefone são as colunas de identificação obrigatórias.
    - data: JSON string com todas as colunas restantes do DataFrame (excluindo
      'others', 'dispatch_date', 'campaign_name', 'SubscriberKey', 'telefone').

    Cada linha é validada via SfDispatchRow (Pydantic) antes de ser incluída.
    Linhas inválidas são logadas e descartadas; se nenhuma linha for válida,
    lança ValueError.

    Args:
        df: DataFrame de disparo (current_df), já filtrado e sem 'others'.
        dispatch_date: String com a data/hora do disparo.
        campaign_name: Nome da campanha.

    Returns:
        DataFrame com colunas: dispatch_date, campaign_name, SubscriberKey, telefone, data.

    Raises:
        ValueError: Se nenhuma linha for válida após validação.
    """
    # Colunas que viram campos fixos no schema de saída — não entram no JSON de `data`
    fixed_columns = {"dispatch_date", "campaign_name", "SubscriberKey", "telefone"}

    # Colunas do df que sobram para o JSON de `data`
    extra_columns = [col for col in df.columns if col not in fixed_columns]

    records = []
    invalid_count = 0

    for i, row in df.iterrows():
        try:
            validated = SfDispatchRow(
                dispatch_date=str(dispatch_date),
                campaign_name=str(campaign_name),
                cpf=str(row.get("SubscriberKey", "")),
                telefone=str(row.get("telefone", "")),
            )
        except Exception as e:  # pylint: disable=broad-except
            invalid_count += 1
            log(f"create_log_df: linha {i} descartada por validação inválida: {e}")
            continue

        # Monta o JSON com os campos extras, convertendo tipos não-serializáveis para string
        extra_data = {}
        for col in extra_columns:
            val = row.get(col)
            # Converte tipos pandas/numpy não serializáveis
            if hasattr(val, "item"):
                try:
                    val = val.item()
                except ValueError:
                    # val tem mais de um elemento (ex.: coluna array/repeated do BigQuery,
                    # ou coluna duplicada no df) — não dá pra reduzir a escalar
                    val = val.tolist() if hasattr(val, "tolist") else list(val)
            extra_data[col] = val

        records.append({
            "dispatch_date": validated.dispatch_date,
            "campaign_name": validated.campaign_name,
            "SubscriberKey": validated.cpf,
            "telefone": validated.telefone,
            "data": json.dumps(extra_data, ensure_ascii=False, default=str),
        })

    if not records:
        raise ValueError(
            f"create_log_df: nenhuma linha válida após validação. "
            f"{invalid_count} linhas descartadas."
        )

    if invalid_count > 0:
        log(f"create_log_df: {invalid_count} linhas descartadas por validação inválida.")

    log_df = pd.DataFrame(records, columns=["dispatch_date", "campaign_name", "SubscriberKey", "telefone", "data"])
    log(f"create_log_df: DataFrame de log criado com {len(log_df)} registros.")
    return log_df


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
def get_already_dispatched_data(billing_project_id: str, dispatch_interval_days: int) -> pd.DataFrame:
    """
    Busca no BigQuery a lista de CPFs ou telefones que já tiveram um disparo
    bem-sucedido ou em processamento nos últimos dispatch_interval_days dias. Filtramos pela data de envio ou entrega
    porque a coluna status_disparo guarda apenas o status final, e se uma pessoa leu a mensagem
    uma semana depois ela deveria poder receber outro disparo hoje.
    Para verificar se a pessoa já recebeu um disparo hoje usar dispatch_interval_days=0
    """
    query = f"""
        SELECT DISTINCT cpf , contato_telefone as telefone, status_disparo as status, nome_hsm as nome_campanha, data_particao
        FROM `rj-crm-registry.brutos_salesforce.status_disparo`
        WHERE data_particao >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL {dispatch_interval_days} DAY)
          AND processado_datahora >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL {dispatch_interval_days} DAY)
    """
    log(f"Buscando disparos já realizados hoje e na campanha para evitar duplicidade:\n{query}")
    try:
        df = download_data_from_bigquery(
            query=query, billing_project_id=billing_project_id, bucket_name=billing_project_id
        )
        return df
    except Exception as err:
        log(f"Erro ao buscar disparos realizados: {err}. Retornando DataFrame vazio.", level="warning")
        return pd.DataFrame(columns=["cpf", "telefone", "status", "nome_campanha", "data_particao"])


@task
def filter_duplicated(
    df: pd.DataFrame,
    column: str,
    filter_indicator: bool,
    label: str,
) -> pd.DataFrame:
    """
    Remove duplicate entries from a DataFrame based on a specified column.
    Only executes if filter_indicator is True.
    """
    if df is None or df.empty:
        return df

    if filter_indicator and column in df.columns:
        n_before = len(df)
        df = df.drop_duplicates(subset=[column])
        log(f"Removed {n_before - len(df)} duplicated {label}. Remaining: {len(df)}")
    return df


@task
def filter_already_dispatched_phones_or_cpfs(
    df: pd.DataFrame,
    already_dispatched_df: pd.DataFrame,
    current_filter: str = 'cpf'
) -> pd.DataFrame:
    """
    Filters out rows from the DataFrame where the identifier (cpf or phone) 
    has already been dispatched today.
    """
    if df is None or df.empty or already_dispatched_df.empty:
        return df

    # Mapeamos o filtro informado para as colunas corretas de cada DataFrame: (col_envio, col_controle)
    # - col_envio: nome da coluna no DataFrame de envio (df), que para CPF é 'cpf'.
    # - col_controle: nome da coluna no DataFrame de controle (already_dispatched_df), que para CPF também é 'cpf'.
    mapping = {
        "cpf": ("cpf", "cpf"),
        "SubscriberKey": ("cpf", "cpf"),
        "telefone": ("telefone", "telefone"),
        None: ("cpf", "cpf")
    }
    
    col_envio, col_controle = mapping.get(current_filter, ("cpf", "cpf"))

    if col_envio not in df.columns:
        log(f"Coluna {col_envio} não encontrada no DataFrame de envio. Ignorando filtro.", level="warning")
        return df

    if col_controle not in already_dispatched_df.columns:
        log(f"Coluna {col_controle} não encontrada no controle de já disparados. Ignorando filtro.", level="warning")
        return df

    n_before = len(df)
    dispatched_set = set(already_dispatched_df[col_controle].dropna())
    df = df[~df[col_envio].isin(dispatched_set)]
    log(f"Removed {n_before - len(df)} already dispatched {col_envio}. Remaining: {len(df)}")
    
    return df


def normalize_keys(d: Dict) -> Dict:
        """Helper para normalizar chaves do dicionário para o padrão esperado pelo schema."""
        if not isinstance(d, dict):
            return d
        
        mapping = {
            "celular_disparo": "telefone",
            "to": "telefone",
            "telefone": "telefone",
            "externalid": "cpf",
            "external_id": "cpf",
            "externalId": "cpf",
            "cpf": "cpf",
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
        # Normaliza as chaves (ex: EXTERNALID -> cpf, celular_disparo -> telefone)
        destinations = [normalize_keys(d) for d in destinations]
        print(f"Exemplo de destino após a normalização: {destinations[0]}")

        validated_destinations, validation_stats = validate_destinations(destinations)
        log_validation_summary(validation_stats, "get_destinations")
        return [dest.model_dump() for dest in validated_destinations]

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
        phone = destination.telefone  # Pydantic model attribute
        cpf = destination.cpf

        if phone not in seen_phones:
            seen_phones.add(phone)
            unique_destinations.append(destination.model_dump())  # Convert back to dict
        else:
            duplicates_count += 1
            log(f"Duplicate phone removed: {phone[:8]}**** (cpf: {cpf})")

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
        cpf = destination.cpf  # Pydantic model attribute

        if cpf not in seen_cpfs:
            seen_cpfs.add(cpf)
            unique_destinations.append(destination.model_dump())  # Convert back to dict
        else:
            duplicates_count += 1
            log(f"Duplicate CPF removed: {cpf[:4]}**** (phone: {destination.telefone})")

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
def check_flow_status(
    flow_environment: str,
    billing_project_id: str,
    bucket_name: str,
    campaign_name: Optional[str] = None,
) -> Optional[bool]:
    """
    Verifica se o fluxo está ativo e dentro do prazo de validade consultando o BigQuery.
    Args:
        flow_environment: Ambiente do fluxo ('staging' ou 'production')
        billing_project_id: ID do projeto GCP para billing
        bucket_name: Nome do bucket GCS para carregamento de credenciais
        campaign_name: Nome da campanha
    Returns:
        True se o fluxo estiver ativo e válido, None caso contrário."""

    log(f"\nStarting flow status check for campaign_name={campaign_name} in environment={flow_environment}.")

    if flow_environment not in ["staging", "production"]:
        log(f"\n⚠️  Invalid flow_environment: {flow_environment}. Must be 'staging' or 'production'.")
        return None

    if not campaign_name:
        log(f"\n⚠️  Campaign name must be provided.")
        return None

    filter_condition = f"nome_campanha = '{campaign_name}'"

    query = f"""
        SELECT ativo, data_limite_disparo, nome_campanha
        FROM `rj-crm-registry.brutos_salesforce_staging.disparos_ativos`
        WHERE {filter_condition} AND ambiente = '{flow_environment}'
        LIMIT 1
    """
    dfr = download_data_from_bigquery(
        query=query,
        billing_project_id=billing_project_id,
        bucket_name=bucket_name,
    )
    log(f"DEBUG: Flow status query result:\n{dfr} \nwith query {query}")
    if dfr.empty:
        log(f"\n⚠️  No configuration found for campaign_name={campaign_name} in environment={flow_environment}.")
        return None

    row = dfr.iloc[0]

    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_ERRORS")
    if not webhook_url:
        print("DISCORD_WEBHOOK_URL_ERRORS environment variable not set. Cannot send notification.")

    ativo_raw = row.get("ativo")
    is_ativo = str(ativo_raw).strip().lower() in ("1", "true", "yes", "sim", "ativo") if ativo_raw is not None else False
    if not is_ativo:
        log(f"\n⚠️  Flow is not active for {row.get('nome_campanha')} in environment={flow_environment}.")
        message = f"""
    <@821121576455634955> <@1458456241683824744> <@302518123066556426>
    Prefect flow run desativado em https://docs.google.com/spreadsheets/d/1O-noD696ZjIr9X_Vl4ZKyFDyg0q9KHe9jacExdAp4ck/!
    📋 **Campanha:** {row.get("nome_campanha")}
    💻 **Ambiente:** {flow_environment}

    Desligue o scheduler no prefect ou mude o status para ativo para reativar o fluxo.
    """
        send_discord_notification(webhook_url, message)
        return None

    current_date = datetime.now(timezone("America/Sao_Paulo")).date()

    raw_expiration = row.get("data_limite_disparo")
    # NULL/NaT means no expiration — the flow runs indefinitely.
    # Normalise to datetime.date so the comparison with current_date is always type-safe.
    expiration_date = None
    try:
        if raw_expiration is not None and not pd.isnull(raw_expiration):
            expiration_date = datetime.strptime(str(raw_expiration)[:10], "%Y-%m-%d").date()
    except (TypeError, ValueError):
        log(f"WARNING: data_limite_disparo value {raw_expiration!r} could not be parsed as a date. Treating as no expiration.")
        expiration_date = None

    if expiration_date is not None and expiration_date < current_date:
        log(f"\n⚠️  Flow for campaign_name={campaign_name} in environment={flow_environment} has expired on {expiration_date}.")
        message = f"""
    <@821121576455634955> <@1458456241683824744> <@302518123066556426>
    Prefect flow run atingiu a data limite em https://docs.google.com/spreadsheets/d/1O-noD696ZjIr9X_Vl4ZKyFDyg0q9KHe9jacExdAp4ck/!
    📋 **Campanha:** {row.get("nome_campanha")}
    💻 **Ambiente:** {flow_environment}
    📆 **Data limite do disparo:** {expiration_date}

    Desligue o scheduler no prefect ou altere a data limite.
    """
        send_discord_notification(webhook_url, message)
        return None

    log(f"\n✅  Active flow found for campaign_name={campaign_name} in environment={flow_environment}.")
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


@task
def get_failed_phones(billing_project_id: str, campaign_name: str = None) -> set:
    """
    Busca telefones que tiveram falha de não ter whatsapp ou bloqueio no último disparo,
    passo necessário já que o telefone principal não vem do RMI. Caso contrário, seria
    só necessário filtrar pela estratégia de envio.

    Combina (UNION) duas fontes:
    1. fluxo_atendimento (Wetalkie): falha 131026 no último disparo dos últimos 6 meses.
    2. int_crm_status_disparo (Salesforce/dbt): indicador_falha nos últimos 6 meses.
       Se campaign_name for informado, filtra também pelo nome da jornada nessa segunda query.
    """
    # Em alguns raros casos, o webhook retorna "FAILED", mas depois a pessoa recebe a mensagem.
    campaign_filter = f"AND LOWER(nome_hsm) = LOWER('{campaign_name}')" if campaign_name else ""

    query = f"""
        -- Fonte 1: Wetalkie — falha 131026 no último disparo dos últimos 6 meses
        WITH status_por_disparo AS (
            SELECT
                flatTarget,
                createDate,
                MAX(
                    CASE
                        WHEN status = "PROCESSING" THEN 1
                        WHEN status = "FAILED" and (faultdescription like "%131026%" or faultdescription LIKE "%131048%") THEN 2
                        WHEN status = "SENT" THEN 3
                        WHEN status = "DELIVERED" THEN 4
                        WHEN status = "READ" THEN 5
                    END
                ) AS id_status_disparo
            FROM `rj-crm-registry.brutos_wetalkie_staging.fluxo_atendimento_*`
            WHERE DATE(createDate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
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
        ),

        wetalkie_failed AS (
            SELECT flatTarget AS telefone
            FROM ranked
            WHERE rn = 1
            AND id_status_disparo = 2
            -- rn = 1 para pegar o último disparo e id_status_disparo = 2 para selecionar apenas os com falha
        ),

        -- Fonte 2: int_crm_status_disparo (Salesforce) — falha nos últimos 6 meses
        sf_failed AS (
            SELECT DISTINCT contato_telefone AS telefone
            FROM `rj-crm-registry.brutos_salesforce.status_disparo`
            WHERE indicador_quarentena = TRUE
            {campaign_filter}
        )

        SELECT telefone FROM wetalkie_failed
        UNION DISTINCT
        SELECT telefone FROM sf_failed
    """
    try:
        failed_df = download_data_from_bigquery(
            query=query,
            billing_project_id=billing_project_id,
            bucket_name=billing_project_id
        )
        if not failed_df.empty:
            print(f"DEBUG: Primeiro ID com falha detectado: {failed_df.iloc[0]}... (total {failed_df.shape[0]})")
        failed_phones = set(str(x) for x in failed_df['telefone'].tolist())
        return failed_phones
    except Exception as e:
        log(f"Erro ao buscar falhas para retentativa: {e}")
        return set()


@task
def get_failed_cpfs(billing_project_id: str, campaign_name: str,) -> set:
    """
    Busca CPFs tiveram falha de não ter whatsapp ou bloqueio no disparo das últimas 2 horas,
    """
    campaign_filter = f"AND LOWER(nome_hsm) = LOWER('{campaign_name}')" if campaign_name else ""
    query = f"""
        -- Agora só teremos Salesforce como fonte
        SELECT DISTINCT cpf
        FROM `rj-crm-registry.brutos_salesforce.status_disparo`
        WHERE indicador_quarentena = TRUE
        AND processado_datahora >= datetime_sub(current_datetime("America/Sao_Paulo"), INTERVAL 4 hour)
        AND data_particao >= date_sub(current_date("America/Sao_Paulo"), INTERVAL 1 day)
        {campaign_filter}
    """
    try:
        failed_df = download_data_from_bigquery(
            query=query,
            billing_project_id=billing_project_id,
            bucket_name=billing_project_id
        )
        if not failed_df.empty:
            print(f"DEBUG: Primeiro ID com falha detectado para retentativa: {failed_df.iloc[0]}... (total {failed_df.shape[0]})")
        failed_cpfs = set(str(x) for x in failed_df['cpf'].tolist())
        print(f"DEBUG failed_cpfs {failed_cpfs}")
    except Exception as e:
        log(f"Erro ao buscar falhas para retentativa: {e}")
        return set()

    if not failed_cpfs:
        log(f"Nenhuma falha detectada para nas últimas 2 horas.")
        return set()
    
    return failed_cpfs
    

def check_campaign_success(
    billing_project_id: str,
    campaign_name: str,
    dispatch_date: str,
) -> bool:
    """
    Verifica se já existe pelo menos um disparo com sucesso confirmado (entrega_datahora
    preenchida, sem falha/quarentena) para a campanha desde o horário do disparo (dispatch_date).

    Usada pelo monitoramento pós-SFTP para decidir se deve alertar no Discord por falta
    de confirmação de entrega.
    """
    query = f"""
        SELECT COUNT(*) AS total_sucesso
        FROM `rj-crm-registry.brutos_salesforce.status_disparo`
        WHERE LOWER(nome_hsm) = LOWER('{campaign_name}')
          AND processado_datahora >= '{dispatch_date}'
          AND data_particao >= DATE('{dispatch_date}')
    """
    try:
        df = download_data_from_bigquery(
            query=query, billing_project_id=billing_project_id, bucket_name=billing_project_id
        )
        total = int(df.iloc[0]["total_sucesso"]) if not df.empty else 0
        log(f"check_campaign_success: {total} disparo(s) com recebimento de webhook confirmado para '{campaign_name}' desde {dispatch_date}.")
        return total > 0
    except Exception as e:
        log(f"Erro ao verificar sucesso do disparo: {e}", level="warning")
        return False


@task
def monitor_dispatch_status(
    campaign_name: str,
    billing_project_id: str,
    dispatch_date: str,
    git_repository_path: str,
    initial_wait_minutes: int,
    check_interval_minutes: int,
    max_wait_minutes: int,
) -> bool:
    """
    Aguarda initial_wait_minutes, depois materializa int_crm_status_disparo e checa se há
    sucesso confirmado para a campanha a cada check_interval_minutes, até max_wait_minutes.
    Sai assim que encontrar sucesso. Alerta no Discord (DISCORD_WEBHOOK_URL_ERRORS) se nenhum
    sucesso for confirmado dentro do prazo.

    Returns:
        bool: True se algum sucesso foi confirmado, False caso contrário.
    """
    print(f"⏳ Aguardando {initial_wait_minutes} minutos antes da primeira materialização/checagem de sucesso...")
    time.sleep(initial_wait_minutes * 60)

    elapsed_minutes = initial_wait_minutes
    campaign_found = False

    while True:
        execute_dbt_task(
            select="+int_crm_status_disparo",
            target="prod",
            git_repository_path=git_repository_path,
        )

        campaign_found = check_campaign_success(
            billing_project_id=billing_project_id,
            campaign_name=campaign_name,
            dispatch_date=dispatch_date,
        )
        print(f"🔍 Checagem em {elapsed_minutes} min: {'✅ sucesso encontrado' if campaign_found else '⚠️  nenhum sucesso ainda'}.")

        if campaign_found or elapsed_minutes >= max_wait_minutes:
            break

        print(f"⏳ Aguardando mais {check_interval_minutes} minutos antes da próxima checagem ({elapsed_minutes}/{max_wait_minutes} min)...")
        time.sleep(check_interval_minutes * 60)
        elapsed_minutes += check_interval_minutes

    if not campaign_found:
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL_ERRORS")
        message = f"""
<@821121576455634955> <@1458456241683824744> <@302518123066556426>
🚨 **Nenhum webhook de sucesso recebido!**
📋 **Campanha:** {campaign_name}
⏱️ **Prazo monitorado:** {max_wait_minutes} minutos após o envio via SFTP.
"""
        if webhook_url:
            send_discord_notification(webhook_url, message)
        else:
            log("DISCORD_WEBHOOK_URL_ERRORS environment variable not set. Cannot send alert.", level="warning")

    return campaign_found


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
        # Busca telefone ignorando o "case"
        to_number = get_value_from_case_insensitive_key(dest, 'telefone')

        # if to_number is not None and str(to_number) not in failed_phones and max_dispatch_retries==0:
        #     new_destinations.append(dest)
        if (to_number is None or str(to_number) in failed_phones) and max_dispatch_retries==0:
            pass  # Se o telefone principal falhou e não há retentativas, removemos o destino completamente
        elif str(to_number) in failed_phones and max_dispatch_retries>0:
            # Atualiza o campo 'telefone' com None para os telefones que falharam, forçando a retentativa a usar o próximo número da lista 'others'
            new_dest = dest.copy()
            new_dest['telefone'] = None
            print(f"DEBUG: {to_number} falhou no último disparo e foi alterado para None = {new_dest}")
            new_destinations.append(new_dest)
        else:
            new_destinations.append(dest)

    log(f"Removed {len(original_destinations) - len(new_destinations)} destinations with failed phones. Remaining destinations: {len(new_destinations)}.")
    return new_destinations


@task
def get_retry_destinations(
    campaign_name: str,
    original_destinations: List[Dict],
    billing_project_id: str,
    attempt_number: int  # 1 para primeira retentativa, 2 para segunda...
) -> List[Dict]:
    """
    Identifica quais CPFs falharam e prepara a lista para retentativa com o próximo número da lista 'others'.
    Suporta chaves com variações de maiúsculas/minúsculas e estruturas aninhadas.

    Exemplo de como deve estar o schema da query:
    {
       "telefone": "5521999999999",
       "cpf": "12345678901",
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
        failed_ids = get_failed_cpfs(billing_project_id=billing_project_id, campaign_name=campaign_name)

        if not failed_ids or len(failed_ids) == 0:
            log(f"Nenhuma falha detectada para a tentativa {attempt_number}.")
            return []
    

    retry_destinations = []
    for dest in original_destinations:
        # Busca cpf e others ignorando o "case"
        ext_id = get_value_from_case_insensitive_key(dest, 'cpf')
        others = get_value_from_case_insensitive_key(dest, 'others') or []
        to_number = get_value_from_case_insensitive_key(dest, 'telefone')
        log(f"DEBUG dest {dest}")

        # Condições de elegibilidade para retry: telefone faltando/inválido + retry attempt válido
        is_ext_missing = (ext_id is None)
        is_failed_id = (str(ext_id) in failed_ids)
        is_to_number_missing = (to_number is None)
        can_retry = (attempt_number > 0 and len(others) >= attempt_number)

        if (is_ext_missing or is_failed_id or is_to_number_missing) and can_retry:
            new_dest = dest.copy()
            # Atualiza o campo 'telefone' com o número da repescagem (tentativa 1 pega others[0])
            new_dest['telefone'] = others[attempt_number - 1]
            log(f"DEBUG: new_dest alterado = {new_dest}")
            retry_destinations.append(new_dest)
        elif attempt_number == 0:
            retry_destinations.append(dest)            

    log(f"Preparados {len(retry_destinations)} destinos para a retentativa {attempt_number}.")
    return retry_destinations


@task
def apply_df_enricher(
    df: pd.DataFrame,
    enricher_name: str,
    enricher_params: Optional[dict] = None,
) -> pd.DataFrame:
    """
    Aplica uma função de enriquecimento registrada em DF_ENRICHERS (utils/enrichers.py)
    sobre o DataFrame retornado pela query, antes dos filtros/dedup/CSV.

    Permite plugar novas fontes de enriquecimento (ex.: outras APIs externas)
    apenas registrando uma nova função em DF_ENRICHERS, sem alterar o flow.

    Args:
        df: DataFrame retornado pela query de destinos.
        enricher_name: Chave registrada em DF_ENRICHERS.
        enricher_params: Parâmetros livres repassados pra função do enricher.

    Returns:
        DataFrame enriquecido (pode ter menos linhas que o original, se o
        enricher filtrar registros sem dado correspondente).
    """
    enricher_fn = get_df_enricher(enricher_name)
    if enricher_fn is None:
        raise ValueError(f"Unknown df enricher: {enricher_name!r}. Registered: {list(DF_ENRICHERS)}")

    return enricher_fn(df, enricher_params or {})


@task
def save_csv_for_sftp(
    df: pd.DataFrame,
    data_extension_filename: str,
    de_columns: Optional[List[str]] = None,
    output_folder: str = "./data_sftp/",
    csv_separator: str = ";",
) -> Tuple[str, str]:
    """
    Salva o DataFrame da query como CSV para envio ao Salesforce via SFTP.

    Se `de_columns` for informado, o CSV é restrito a 'telefone' + 'SubscriberKey' +
    `de_columns` (os campos que a Data Extension espera) — qualquer coluna de controle
    interno do flow (ex.: 'others', 'cpf') é descartada, mesmo que a query as
    retorne. Sem `de_columns`, mantém o comportamento legado de só descartar 'others'.

    `csv_separator` define o delimitador do CSV (padrão ';'); algumas Data Extensions
    esperam outro separador — configurável por campanha via `csv_separator` no scheduler.

    Returns:
        Tuple[str, str]: (caminho do arquivo CSV salvo, data do disparo)
    """
    now = datetime.now(timezone("America/Sao_Paulo"))
    dispatch_date = now.strftime("%Y-%m-%d %H:%M:%S")
    timestamp = now.strftime("%Y%m%d%H%M%S")  # TODO: alterar para salvar em segundos
    # timestamp = now.strftime("%Y%m%d%H%M")
    filename = f"{data_extension_filename}_{timestamp}.csv"

    if de_columns is not None:
        keep_columns = [col for col in ["telefone", "SubscriberKey", *de_columns] if col in df.columns]
        csv_df = df[keep_columns].copy()
    else:
        csv_df = df.drop(columns=["others"], errors="ignore")
    csv_df["LOCALE"] = "BR"
    # Só entra no CSV quando a campanha pede via de_columns (hoje: só as jornadas 1746)
    if de_columns is not None and "NOME_ARQUIVO" in de_columns:
        csv_df["NOME_ARQUIVO"] = filename

    os.makedirs(output_folder, exist_ok=True)
    csv_path = os.path.join(output_folder, filename)
    csv_df.to_csv(csv_path, index=False, sep=csv_separator)

    log(f"CSV criado em {csv_path} com {len(csv_df)} registros")
    return csv_path, dispatch_date


@task
def send_to_sftp(
    csv_path: str,
    infisical_secret_path: str = None,
    sftp_host: str = None,
    sftp_user: str = None,
    sftp_password: str = None,
    sftp_port: int = 22,
    sftp_remote_path: str = "/",
    sftp_host_key: str = None,
) -> None:
    """
    Envia um arquivo CSV para o servidor SFTP do Salesforce.

    A identidade do servidor é verificada por host key pinning: a conexão
    é recusada (asyncssh.HostKeyNotVerifiable) se a chave apresentada não
    bater byte a byte com `sftp_host_key`. Sem esse pinning, qualquer
    servidor na rota de rede poderia se passar pelo SFTP da Salesforce sem
    ser detectado — os CSVs enviados aqui carregam CPF e telefone de
    cidadãos, então um MITM bem-sucedido seria um vazamento de dado pessoal
    sob a LGPD.

    Usamos asyncssh em vez de paramiko porque o servidor SFTP da Salesforce
    (Globalscape EFT) só oferece o algoritmo de host key legado "ssh-rsa"
    (SHA-1). O Paramiko 5.x removeu esse algoritmo da lista aceita por
    padrão e não expõe uma forma pública de reabilitá-lo (só via monkey-patch
    de atributos privados — o que essa função fazia antes, e que também
    desligava a verificação de assinatura por completo). O asyncssh aceita
    "ssh-rsa" via parâmetro documentado (server_host_key_algs) e continua
    fazendo a verificação criptográfica real da assinatura do servidor.

    Args:
        csv_path: Caminho local do CSV a ser enviado
        infisical_secret_path: Caminho no Infisical para buscar as credenciais
        sftp_host: Endereço do servidor SFTP
        sftp_user: Usuário SFTP
        sftp_password: Senha SFTP
        sftp_port: Porta do servidor SFTP (padrão: 22)
        sftp_remote_path: Diretório remoto onde o arquivo será depositado
        sftp_host_key: Chave pública do servidor, no formato "ssh-rsa AAAA..."
            (mesmo formato de uma linha de known_hosts). Precisa ser
            confirmada com a Salesforce por um canal separado da própria
            conexão SSH (suporte, documentação oficial) antes de ser
            cadastrada — nunca aceitar só o que a primeira conexão apresentar.
    """
    if infisical_secret_path:
        sftp_host = sftp_host or getenv_or_action("sf_sftp_host")
        sftp_user = sftp_user or getenv_or_action("sf_sftp_user")
        sftp_password = sftp_password or getenv_or_action("sf_sftp_password")
        sftp_port = int(getenv_or_action("sf_sftp_port", sftp_port))
        sftp_remote_path = getenv_or_action("sf_sftp_path", sftp_remote_path)
        sftp_host_key = sftp_host_key or getenv_or_action("sf_sftp_host_key")

    SFTP_TIMEOUT_SECONDS = 30
    SFTP_KEEPALIVE_SECONDS = 15

    filename = os.path.basename(csv_path)
    remote_dir = sftp_remote_path.rstrip("/") if sftp_remote_path else ""
    remote_file = f"{remote_dir}/{filename}" if remote_dir else filename

    pinned_key = asyncssh.import_public_key(sftp_host_key)

    async def _upload() -> None:
        log(f"Conectando ao SFTP {sftp_host}:{sftp_port} como {sftp_user}")
        async with asyncssh.connect(
            sftp_host,
            port=sftp_port,
            username=sftp_user,
            password=sftp_password,
            server_host_key_algs=[pinned_key.get_algorithm()],
            # known_hosts espera (host_keys, ca_keys, revoked_keys); só
            # fixamos a chave de host, sem CA nem lista de revogação.
            known_hosts=([pinned_key], [], []),
            connect_timeout=SFTP_TIMEOUT_SECONDS,
            keepalive_interval=SFTP_KEEPALIVE_SECONDS,
        ) as conn:
            log("Host key validada contra o fingerprint fixado. Conexão estabelecida com sucesso ao SFTP!")

            if not os.path.exists(csv_path):
                log(f"Arquivo não encontrado: {csv_path}")
                return

            async with conn.start_sftp_client() as sftp:
                log(f"Enviando {filename}...")
                await sftp.put(csv_path, remote_file)
                log(f"Arquivo {filename} enviado com sucesso para {remote_file}.")

    try:
        asyncio.run(_upload())
    except asyncssh.HostKeyNotVerifiable as e:
        log(
            f"Host key do servidor SFTP não confere com sftp_host_key — conexão recusada. "
            f"Possível MITM ou chave do servidor desatualizada (nesse caso, confirme a nova "
            f"chave com a Salesforce por um canal separado antes de atualizar o secret). "
            f"Detalhe: {e}"
        )
        raise
    except Exception as e:
        log(f"Erro: {str(e)}")
        raise

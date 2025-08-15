# -*- coding: utf-8 -*-
"""
Tasks migradas do template disparo do Prefect 1.4 para 3.0 - SMAS Disparo CADUNICO
Baseado em pipelines_rj_crm_registry/pipelines/templates/disparo/tasks.py
"""

import json
from datetime import datetime
from math import ceil
from typing import Dict, List, Union

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from prefect import task
from pytz import timezone

from pipelines.rj_smas__disparo_cadunico.utils.tasks import task_download_data_from_bigquery


@task
def create_dispatch_payload(campaign_name: str, cost_center_id: int, destinations: Union[List, pd.DataFrame]) -> Dict:
    """
    Cria o payload para o dispatch
    """
    return {
        "campaignName": campaign_name,
        "costCenterId": cost_center_id,
        "destinations": destinations,
    }


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
    """
    data = []
    for destination in original_destinations:
        row = {
            "id_hsm": id_hsm,
            "dispatch_date": dispatch_date,
            "campaignName": campaign_name,
            "costCenterId": cost_center_id,
            "to": destination["to"],
            "externalId": destination.get("externalId", None),
            "vars": destination.get("vars", None),
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
    log(f"dfr content: {dfr}")
    log(f"Total records in dfr: {len(dfr)} (should match all dispatched destinations)")
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
def printar(text):
    """exibe o texto passado como parâmetro"""
    log(f"Printando {text}")


@task
def get_destinations(
    destinations: Union[None, List[str]],
    query: str,
    billing_project_id: str = "rj-smas",
    query_processor_name: str = None,
) -> List[Dict]:
    """
    Get destinations from the query or from the parameter.
    If query_processor_name is provided, it will look up and apply the corresponding processor.
    (Função do template disparo)
    """
    if query:
        log("\nQuery was found")

        # Apply query processor if name provided
        final_query = query
        if query_processor_name:
            from pipelines.rj_smas__disparo_cadunico.processors import get_query_processor

            processor_func = get_query_processor(query_processor_name)
            if processor_func:
                log(f"Applying query processor: {query_processor_name}")
                final_query = processor_func(query)
            else:
                log(f"Warning: Query processor '{query_processor_name}' not found, using original query")

        destinations = task_download_data_from_bigquery(
            query=final_query,
            billing_project_id=billing_project_id,
            bucket_name=billing_project_id,
        )
        log(f"response from query {destinations.head()}")
        destinations = destinations.iloc[:, 0].tolist()
        destinations = [json.loads(str(item).replace("celular_disparo", "to")) for item in destinations]
    elif isinstance(destinations, str):
        destinations = json.loads(destinations)
    return destinations


@task
def remove_duplicate_phones(destinations: List[Dict]) -> List[Dict]:
    """
    Remove duplicate phone numbers from destinations list.
    Keeps only the first occurrence of each phone number.
    (Função do template disparo)
    """
    if not destinations:
        log("No destinations to process")
        return destinations

    seen_phones = set()
    unique_destinations = []
    duplicates_count = 0

    for destination in destinations:
        phone = destination.get("to")
        if phone and phone not in seen_phones:
            seen_phones.add(phone)
            unique_destinations.append(destination)
        elif phone in seen_phones:
            duplicates_count += 1

    log(f"Removed {duplicates_count} duplicate phone numbers")
    log(f"Total unique destinations: {len(unique_destinations)}")

    return unique_destinations

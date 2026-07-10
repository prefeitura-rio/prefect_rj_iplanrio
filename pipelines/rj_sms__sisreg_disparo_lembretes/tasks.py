# -*- coding: utf-8 -*-
"""
Tasks for SISREG SMS dispatch pipeline
Adapted from disparo template for Prefect 3.x
"""

import json
from datetime import datetime
from typing import Dict, List, Union

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from numpy import ceil
from prefect import task
from pytz import timezone

from .utils.tasks import download_data_from_bigquery


@task
def create_dispatch_payload(campaign_name: str, cost_center_id: int, destinations: Union[List, pd.DataFrame]) -> Dict:
    """
    Create payload for dispatch
    """
    return {
        "campaignName": campaign_name,
        "costCenterId": cost_center_id,
        "destinations": destinations,
    }


@task
def dispatch(api: object, id_hsm: int, dispatch_payload: dict, chunk: int) -> str:
    """
    Dispatch messages in chunks via Wetalkie API
    """
    destinations = dispatch_payload["destinations"]
    total = len(destinations)
    campaign_name = dispatch_payload["campaignName"]

    dispatch_date = datetime.now(timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")

    for i, start in enumerate(range(0, total, chunk), 1):
        end = start + chunk
        batch = destinations[start:end]
        dispatch_payload["destinations"] = batch
        dispatch_payload["campaignName"] = f"{campaign_name}-{dispatch_date[:10]}-lote{i}"

        log(f"Disparando lote {i} de {int(ceil(total / chunk))} com {len(batch)} destinos")

        response = api.post(path=f"/callcenter/hsm/send/{id_hsm}", json=dispatch_payload)

        if response.status_code != 201:
            log(f"Falha no disparo do lote {i}: {response.text}")
            raise Exception(f"Dispatch failed for batch {i}: {response.text}")

        log(f"Disparo do lote {i} realizado com sucesso!")

    log("Disparo realizado com sucesso!")
    return dispatch_date


@task
def create_dispatch_dfr(id_hsm: int, dispatch_payload: dict, dispatch_date: str) -> pd.DataFrame:
    """
    Create DataFrame with dispatch information for storage
    """
    data = []
    for destination in dispatch_payload["destinations"]:
        row = {
            "id_hsm": id_hsm,
            "dispatch_date": dispatch_date,
            "campaignName": dispatch_payload["campaignName"],
            "costCenterId": dispatch_payload["costCenterId"],
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
    log(f"Created dispatch DataFrame with {len(dfr)} rows")
    return dfr


@task
def check_api_status(api: object) -> bool:
    """
    Check if the Wetalkie API is working by returning status 200
    """
    try:
        response = api.get("/")
        if response.status_code == 200:
            log("API está funcionando corretamente.")
            return True

        log(f"API retornou status {response.status_code}.")
        return False
    except Exception as error:
        log(f"Erro ao acessar a API: {error}")
        return False


@task
def get_destinations(
    destinations: Union[None, List[str]], query: str, billing_project_id: str = "rj-crm-registry"
) -> List[Dict]:
    """
    Get destinations from BigQuery query or from parameter
    """
    if query:
        log("Query was found")
        destinations_df = download_data_from_bigquery(
            query=query,
            billing_project_id=billing_project_id,
            bucket_name=billing_project_id,
        )
        log(f"Response from query: {len(destinations_df)} rows")
        destinations = destinations_df.iloc[:, 0].tolist()
        log(f"First 3 destinations: {destinations[:3] if destinations else 'None'}")
        destinations = [json.loads(str(item).replace("celular_disparo", "to")) for item in destinations]
    elif isinstance(destinations, str):
        destinations = json.loads(destinations)

    return destinations if destinations else []


@task
def remove_duplicate_phones(destinations: List[Dict]) -> List[Dict]:
    """
    Remove duplicate phone numbers from destinations list.
    Keeps only the first occurrence of each phone number.
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


@task
def create_date_partitions(
    dataframe: pd.DataFrame, partition_column: str, file_format: str = "csv", root_folder: str = "./data_dispatch/"
) -> str:
    """
    Create date partitions for data storage
    """
    import os
    import uuid

    os.makedirs(root_folder, exist_ok=True)
    filename = f"{uuid.uuid4()}.{file_format}"
    file_path = os.path.join(root_folder, filename)

    if file_format == "csv":
        dataframe.to_csv(file_path, index=False)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    log(f"Created partition file: {file_path}")
    return file_path

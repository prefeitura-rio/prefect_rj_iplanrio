# -*- coding: utf-8 -*-
"""
Tasks for rj_iplanrio__betterstack_api pipeline
"""

import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from prefect import task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.env import getenv_or_action

from pipelines.rj_iplanrio__betterstack_api.constants import BetterStackConstants


@task
def get_betterstack_credentials(infisical_secret_path: str = "/api-betterstack") -> str:
    """
    Recupera as credenciais da API BetterStack do Infisical.
    """
    log("Recuperando credenciais do BetterStack")
    token = getenv_or_action("betterstack_token")
    if not token:
        raise ValueError("BetterStack token not found in Infisical")
    return token


@task
def calculate_date_range(from_date: str = None, to_date: str = None) -> Dict[str, str]:
    """
    Calcula o range de datas.
    Se não fornecido, as default é D-1 (ontem).
    """
    if from_date and to_date:
        return {"from": from_date, "to": to_date}

    # Default logic: get yesterday's data
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # Se quiser pegar apenas o dia de ontem, o range é ontem -> hoje (exclusive) ou ontem -> ontem?
    # BetterStack API docs: "from" and "to" are timestamps or dates.
    # Usually "from" inclusive, "to" exclusive or inclusive depending on API.
    # User said: "pegar o D-1 e ir adicionando de forma incremental"

    return {"from": yesterday_str, "to": yesterday_str}


@task(retries=3, retry_delay_seconds=60)
def fetch_response_times(token: str, date_range: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Busca dados de response-times da API v2.
    """
    url = f"{BetterStackConstants.BASE_URL_V2.value}/monitors/{BetterStackConstants.MONITOR_ID.value}/response-times"

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "from": date_range["from"],
        "to": date_range["to"]
    }

    log(f"Fetching response times from {url} with params {params}")

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log(f"Erro ao buscar response-times: {e}")
        if hasattr(e, "response") and e.response is not None:
            log(f"Response status: {e.response.status_code}")
            log(f"Response text: {e.response.text[:500]}")
        raise

    data = response.json()

    # Structure: data -> attributes -> regions (list)
    try:
        if "data" in data and "attributes" in data["data"]:
            regions = data["data"]["attributes"].get("regions", [])
            return regions
        else:
            log("Unexpected JSON structure for response times")
            return []
    except Exception as e:
        log(f"Error parsing response times: {e}")
        return []


@task(retries=3, retry_delay_seconds=60)
def fetch_incidents(token: str, date_range: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Busca dados de incidents da API v3.
    """
    url = f"{BetterStackConstants.BASE_URL_V3.value}/incidents"

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "from": date_range["from"],
        "to": date_range["to"],
        "monitor_id": BetterStackConstants.MONITOR_ID.value,
        "per_page": 50 # Maximize page size just in case
    }

    log(f"Fetching incidents from {url} with params {params}")

    all_incidents = []

    while url:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            log(f"Erro ao buscar incidents: {e}")
            if hasattr(e, "response") and e.response is not None:
                log(f"Response status: {e.response.status_code}")
                log(f"Response text: {e.response.text[:500]}")
            raise

        data = response.json()

        # Structure: data (list), pagination (dict)
        incidents = data.get("data", [])
        all_incidents.extend(incidents)

        # Pagination handling
        pagination = data.get("pagination", {})
        next_url = pagination.get("next")

        if next_url:
            url = next_url
            params = {} # params are usually encoded in the next_url
        else:
            url = None

    return all_incidents


@task
def transform_response_times(data: List[Dict[str, Any]], extraction_date: str) -> pd.DataFrame:
    """
    Transforma dados de response-times.
    Flatten da estrutura: Regions -> Response Times
    """
    if not data:
        return pd.DataFrame()

    flattened_data = []

    for region_data in data:
        region_name = region_data.get("region")
        response_times_list = region_data.get("response_times", [])

        for measurement in response_times_list:
            # Create a flat record combining region info and measurement info
            record = measurement.copy()
            record["region"] = region_name
            flattened_data.append(record)

    df = pd.DataFrame(flattened_data)

    if not df.empty:
        df["data_particao"] = extraction_date

    return df


@task
def transform_incidents(data: List[Dict[str, Any]], extraction_date: str) -> pd.DataFrame:
    """
    Transforma dados de incidents.
    """
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Process complex columns (attributes, relationships) if necessary
    # For now, keep as is, but BigQuery might require stringifying dicts if schema is auto-detected as string
    # or specific structs. Let's convert dicts/lists to strings to be safe for a "brutos" layer.

    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].astype(str)

    if not df.empty:
        df["data_particao"] = extraction_date

    return df

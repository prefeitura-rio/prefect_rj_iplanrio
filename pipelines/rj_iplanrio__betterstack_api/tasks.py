# -*- coding: utf-8 -*-
"""
Tasks for rj_iplanrio__betterstack_api pipeline
"""

import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

from prefect import task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.env import getenv_or_action

from pipelines.rj_iplanrio__betterstack_api.constants import BetterStackConstants


@task
def get_betterstack_credentials() -> str:
    """
    Recupera as credenciais da API BetterStack do Infisical.
    """
    log("Recuperando credenciais do BetterStack")

    # Try with BETTERSTACK_TOKEN first, then legacy betterstack_token
    token = getenv_or_action("BETTERSTACK_TOKEN", action="ignore")
    if not token:
        token = getenv_or_action("betterstack_token", action="ignore")

    if not token:
        raise ValueError("BetterStack token not found in environment")

    return token


@task
def get_betterstack_monitor_id() -> str:
    """
    Recupera o ID do monitor da BetterStack do Infisical.
    """
    log("Recuperando MONITOR_ID do BetterStack")
    monitor_id = getenv_or_action("MONITOR_ID")
    return monitor_id



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
def fetch_incidents(token: str, monitor_id: str, date_range: Dict[str, str]) -> List[Dict[str, Any]]:

    """
    Busca dados de incidents da API v3.
    """
    url = f"{BetterStackConstants.BASE_URL_V3.value}/incidents"

    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "from": date_range["from"],
        "to": date_range["to"],
        "monitor_id": monitor_id,
        "per_page": 50 # Maximize page size just in case
    }


    log(f"Fetching incidents from {url} with params {params}")

    all_incidents = []

    while url:
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=BetterStackConstants.TIMEOUT.value,
            )
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.Timeout as e:
            log(f"Timeout ao buscar incidents: {e}")
            raise
        except requests.exceptions.RequestException as e:
            log(f"Erro de requisição ao buscar incidents: {e}")
            if hasattr(e, "response") and e.response is not None:
                log(f"Response status: {e.response.status_code}")
                log(f"Response text: {e.response.text[:500]}")
            raise
        except json.JSONDecodeError as e:
            log(f"Error decoding incidents JSON: {e}")
            raise

        # Structure: data (list), pagination (dict)
        try:
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
        except KeyError as e:
            log(f"Error parsing incidents structure: {e}")
            url = None # Stop loop if structure is broken

    return all_incidents





@task
def transform_incidents(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Transforma dados de incidents.
    """
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    if not df.empty:
        # Dynamic partitioning based on started_at in attributes
        # First, ensure we can access attributes easily if it's a dict
        def extract_date(row):
            try:
                attrs = row.get("attributes", {})
                ts = attrs.get("started_at") or attrs.get("created_at")
                if ts:
                    return ts[:10] # YYYY-MM-DD
            except Exception:
                pass
            return datetime.now().strftime("%Y-%m-%d")

        df["data_particao"] = df.apply(extract_date, axis=1)

        # Cast all columns to string to avoid schema mismatches in BigQuery brutos layer
        for col in df.columns:
            if col != "data_particao":
                df[col] = df[col].astype(str)

    return df



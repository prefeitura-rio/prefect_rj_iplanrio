# -*- coding: utf-8 -*-
"""
Tasks for rj_crm__wetalkie_api_hsm_info pipeline
"""

import pandas as pd
import requests
from typing import List, Dict, Any
from datetime import datetime

from prefect import task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.env import getenv_or_action

from pipelines.rj_crm__wetalkie_api_hsm_info.constants import Constants


@task
def get_wetalkie_token(infisical_secret_path: str = None) -> str:
    """
    Retrieves the WeTalkie API token using credentials from Infisical (or env vars).
    """
    # Keys used in rj_crm__api_wetalkie
    username = getenv_or_action("wetalkie_user", action="ignore")
    password = getenv_or_action("wetalkie_pass", action="ignore")
    
    # Also check specific keys if the generic ones fail or just to be safe
    if not username:
         username = getenv_or_action("WETALKIE_USERNAME", action="ignore")
    if not password:
         password = getenv_or_action("WETALKIE_PASSWORD", action="ignore")

    if not username or not password:
        raise ValueError("WeTalkie credentials not found.")

    url = f"{Constants.BASE_URL.value}/{Constants.LOGIN_ROUTE.value}"
    payload = {"username": username, "password": password}

    try:
        log(f"Authenticating to {url}")
        response = requests.post(url, json=payload, timeout=Constants.TIMEOUT.value)
        response.raise_for_status()
        data = response.json()

        # Token extraction logic matching check_api_schema.py and ApiHandler
        token = None
        if "token" in data:
            token = data["token"]
        elif "access_token" in data:
            token = data["access_token"]
        elif "authToken" in data:
            token = data["authToken"]
        elif "jwt" in data:
            token = data["jwt"]
        elif "data" in data and "item" in data["data"] and "token" in data["data"]["item"]:
            token = data["data"]["item"]["token"]

        if not token:
            raise ValueError(f"Token not found in response: {data.keys()}")

        return token
    except Exception as e:
        log(f"Authentication failed: {e}", level="error")
        raise


@task(retries=3, retry_delay_seconds=60)
def fetch_hsm_templates(token: str) -> List[Dict[str, Any]]:
    """
    Fetches all HSM templates (active and inactive) from WeTalkie API.
    """
    all_items = []
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    fields = "id,name,message,metaMessageTemplateCategory,qualityScore,modelRejection,metaMessageTemplateStatus,metaMessageTemplateId,templateId,whatsappConfig"

    for active_status in [True, False]:
        status_str = "active" if active_status else "inactive"
        url = f"{Constants.BASE_URL.value}/{Constants.HSM_ROUTE.value};active={str(active_status).lower()}"
        
        page = 0
        has_next = True
        
        log(f"Fetching {status_str} templates from {url}")

        while has_next:
            params = {
                "fields": fields,
                "pageNumber": page,
                "pageSize": 100
            }

            try:
                response = requests.get(url, headers=headers, params=params, timeout=Constants.TIMEOUT.value)
                response.raise_for_status()
                data = response.json()

                # Extract items
                items = []
                if "data" in data and "item" in data["data"] and "elements" in data["data"]["item"]:
                    items = data["data"]["item"]["elements"]
                
                # Tag items with status (backup, though metaMessageTemplateStatus might exist)
                # Client requested to remove this column
                # for item in items:
                #     item["is_active_query"] = active_status

                if items:
                    all_items.extend(items)
                    log(f"  Page {page}: {len(items)} items")
                else:
                    log(f"  Page {page}: No items")

                # Pagination check
                if "data" in data and "item" in data["data"] and "hasNextPage" in data["data"]["item"]:
                    has_next = data["data"]["item"]["hasNextPage"]
                else:
                    has_next = False
                
                page += 1
                
                # Safety break avoids infinite loops
                if page > 100:
                    log("  Page limit reached (safety break)", level="warning")
                    break

            except Exception as e:
                log(f"Error fetching {status_str} templates page {page}: {e}", level="error")
                raise

    log(f"Total templates fetched: {len(all_items)}")
    return all_items


@task
def transform_hsm_templates(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Transforms the raw template data into a DataFrame for BigQuery.
    """
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # List of columns to flatten (dict -> code, description)
    cols_to_flatten = [
        "modelRejection",
        "metaMessageTemplateStatus",
        "metaMessageTemplateCategory",
        "qualityScore"
    ]

    for col in cols_to_flatten:
        if col in df.columns:
            # Create new columns with _code and _description suffixes
            # Handle cases where value might be None or not a dict
            df[f"{col}_code"] = df[col].apply(lambda x: x.get("code") if isinstance(x, dict) else None)
            df[f"{col}_description"] = df[col].apply(lambda x: x.get("description") if isinstance(x, dict) else None)
            
            # Drop original column
            df = df.drop(columns=[col])

    # Convert all columns to string to ensure schema compatibility
    for col in df.columns:
        df[col] = df[col].astype(str)

    # Add partition column (current date as it's a full snapshot)
    df[Constants.PARTITION_COLUMN.value] = datetime.now().strftime("%Y-%m-%d")

    return df

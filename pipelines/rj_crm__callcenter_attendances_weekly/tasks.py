# -*- coding: utf-8 -*-
"""
Tasks para pipeline SMAS Call Center Attendances Weekly
"""

from datetime import datetime, timedelta
from typing import Dict, Optional

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from prefect import task
from pytz import timezone


@task
def calculate_date_range(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, str]:
    """
    Calculate date range for attendances query.
    If start_date and end_date are None, calculate last 7 days period.

    Args:
        start_date: Start date in YYYY-MM-DD format or None
        end_date: End date in YYYY-MM-DD format or None

    Returns:
        Dictionary with 'start_date' and 'end_date' keys
    """
    tz = timezone("America/Sao_Paulo")

    if start_date is None or end_date is None:
        # Calculate last 7 days (from 8 days ago to 1 day ago)
        today = datetime.now(tz).date()
        calculated_end_date = today - timedelta(days=1)
        calculated_start_date = calculated_end_date - timedelta(days=6)

        result = {
            "start_date": calculated_start_date.strftime("%Y-%m-%d"),
            "end_date": calculated_end_date.strftime("%Y-%m-%d"),
        }

        log(f"Calculated date range: {result['start_date']} to {result['end_date']}")
    else:
        result = {"start_date": start_date, "end_date": end_date}

        log(f"Using provided date range: {result['start_date']} to {result['end_date']}")

    return result


@task
def get_weekly_attendances(api: object, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get attendances from the Wetalkie API for a specific date range

    Args:
        api: Authenticated API handler
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        DataFrame with attendances data
    """
    log(f"Getting attendances from {start_date} to {end_date}")

    # Build query parameters
    params = {"startDate": start_date, "endDate": end_date}

    response = api.get(path="/callcenter/attendances", params=params)

    if response.status_code != 200:
        log(f"API request failed with status {response.status_code}: {response.text}", level="error")
        response.raise_for_status()

    log(f"API response status: {response.status_code}")

    try:
        response_data = response.json()
        log(
            f"Response data structure: {list(response_data.keys()) if isinstance(response_data, dict) else type(response_data)}"
        )
    except Exception as e:
        log(f"Failed to parse JSON response: {e}", level="error")
        raise

    # Check if API returned a "message" response (indicates no data available)
    if "message" in response_data and "data" not in response_data:
        log(f"API returned message response (no data available): {response_data.get('message')}")
        return pd.DataFrame()  # Return empty DataFrame - no data to process

    # Extract attendances from response
    if "data" in response_data and "items" in response_data["data"]:
        attendances = response_data["data"]["items"]
    elif isinstance(response_data, list):
        attendances = response_data
    else:
        log(f"Unexpected response structure: {response_data}", level="warning")
        return pd.DataFrame()

    if not attendances:
        log("No attendances found in the API response", level="warning")
        return pd.DataFrame()

    # Process attendances data
    data = []
    for item in attendances:
        data.append(
            {
                "end_date": item.get("endDate"),
                "begin_date": item.get("beginDate"),
                "ura_name": item.get("ura", {}).get("name") if item.get("ura") else None,
                "id_ura": item.get("ura", {}).get("id") if item.get("ura") else None,
                "channel": item.get("channel", "").lower() if item.get("channel") else None,
                "id_reply": item.get("serial"),
                "protocol": item.get("protocol"),
                "json_data": item,
            }
        )

    log(f"Processed {len(data)} attendances")

    dfr = pd.DataFrame(data)

    if not dfr.empty:
        dfr = dfr[
            [
                "id_ura",
                "id_reply",
                "ura_name",
                "protocol",
                "channel",
                "begin_date",
                "end_date",
                "json_data",
            ]
        ]

    return dfr


@task
def criar_dataframe_de_lista(dados_processados: list) -> pd.DataFrame:
    """
    Converts a list of processed data into a pandas DataFrame.

    Args:
        dados_processados: List of dictionaries containing processed data

    Returns:
        A pandas DataFrame created from the input list
    """
    return pd.DataFrame(dados_processados)

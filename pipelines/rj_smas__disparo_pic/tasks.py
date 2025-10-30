# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'
"""
Tasks migradas do template disparo do Prefect 1.4 para 3.0 - SMAS Disparo PIC Lembrete.
Baseado em pipelines_rj_crm_registry/pipelines/templates/disparo/tasks.py.
"""

from typing import Tuple

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from prefect import task


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
def format_query(raw_query: str, event_date: str, id_hsm: int) -> str:
    """
    Format query to receive event date and change id_hsm
    """
    if event_date is None:
        raise ValueError("event_date cannot be None when formatting query.")
    formatted_query = raw_query.format(event_date_placeholder=event_date, id_hsm_placeholder=id_hsm)
    return formatted_query

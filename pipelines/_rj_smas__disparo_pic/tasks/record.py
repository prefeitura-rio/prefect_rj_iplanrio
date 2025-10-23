# -*- coding: utf-8 -*-
"""
Task para criar registros de disparos efetuados - SMAS Disparo PIC
"""

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from prefect import task


@task
def create_dispatch_record(
    destinations: list[dict],
    id_hsm: int,
    campaign_name: str,
    cost_center_id: int,
    dispatch_date: str,
) -> pd.DataFrame:
    """
    Cria DataFrame com registros dos disparos efetuados.

    Transforma a lista de destinatários em um DataFrame estruturado para
    armazenamento no BigQuery, incluindo metadados do disparo (id_hsm,
    data, campanha, centro de custo).

    Args:
        destinations: Lista de destinatários enviados
        id_hsm: ID do template HSM usado
        campaign_name: Nome da campanha
        cost_center_id: ID do centro de custo
        dispatch_date: Data/hora do disparo no formato 'YYYY-MM-DD HH:MM:SS'

    Returns:
        pd.DataFrame: DataFrame com colunas id_hsm, dispatch_date, campaignName,
                      costCenterId, to, externalId, vars

    Raises:
        ValueError: Se não houver destinatários para criar o registro
    """
    if not destinations:
        raise ValueError("No destinations to create record")

    log(f"Creating dispatch record for {len(destinations)} destinations")

    data = []
    for destination in destinations:
        row = {
            "id_hsm": id_hsm,
            "dispatch_date": dispatch_date,
            "campaignName": campaign_name,
            "costCenterId": cost_center_id,
            "to": destination.get("to"),
            "externalId": destination.get("externalId"),
            "vars": destination.get("vars", {}),
        }
        data.append(row)

    dfr = pd.DataFrame(data)

    # Ensure column order matches BigQuery table schema
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

    log(f"DataFrame created with {len(dfr)} records")

    # Validation
    null_phones = dfr["to"].isnull().sum()
    null_external_ids = dfr["externalId"].isnull().sum()

    if null_phones > 0:
        log(f"WARNING: Found {null_phones} records with null phone numbers")

    if null_external_ids > 0:
        log(f"WARNING: Found {null_external_ids} records with null externalId")

    return dfr

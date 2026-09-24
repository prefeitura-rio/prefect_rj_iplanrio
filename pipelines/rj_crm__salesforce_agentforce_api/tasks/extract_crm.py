# -*- coding: utf-8 -*-
"""
Extração via Salesforce CRM REST API (SOQL).

Usado para objetos do CRM (não Data Cloud):
  - MessagingEndUser
  - MessagingSession

Pagina via nextRecordsUrl até buscar todos os registros.
"""

from __future__ import annotations

import pandas as pd
import requests
from prefect import task

_QUERY_PATH = "/services/data/v67.0/query"


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120])
def extract_from_crm_rest(
    crm_session: dict,
    soql: str,
    table_name: str = "desconhecida",
) -> pd.DataFrame:
    """
    Executa uma query SOQL no CRM REST API e retorna um DataFrame com todos os registros.

    Args:
        crm_session : Dict com 'instance_url' e 'access_token'.
        soql        : Query SOQL completa (sem watermark — já interpolado).
        table_name  : Nome da tabela (para logs).

    Returns:
        DataFrame com todos os registros retornados.
    """
    instance_url = crm_session["instance_url"]
    token = crm_session["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    print(f"[CRM] '{table_name}': executando query SOQL...")

    url = f"{instance_url}{_QUERY_PATH}"
    resp = requests.get(url, headers=headers, params={"q": soql.strip()}, timeout=60)
    if not resp.ok:
        print(f"[CRM] Erro {resp.status_code}: {resp.text[:300]}")
        resp.raise_for_status()

    data = resp.json()
    records = data.get("records", [])

    while not data.get("done"):
        next_url = f"{instance_url}{data['nextRecordsUrl']}"
        resp = requests.get(next_url, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        records.extend(data.get("records", []))
        print(f"[CRM] '{table_name}': {len(records)} registros buscados...")

    print(f"[CRM] '{table_name}': {len(records)} registros no total.")

    if not records:
        return pd.DataFrame()

    # Remove o campo 'attributes' de metadata do Salesforce
    rows = [{k: v for k, v in r.items() if k != "attributes"} for r in records]
    return pd.DataFrame(rows)

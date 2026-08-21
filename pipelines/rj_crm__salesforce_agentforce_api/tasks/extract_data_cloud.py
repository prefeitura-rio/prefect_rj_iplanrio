# -*- coding: utf-8 -*-
"""
Extração de DMOs do Salesforce Data Cloud via Connect API REST.

Usado por:
  - Fase 1 (STDM): AiAgentSession, AiAgentInteraction, Steps, Messages
  - Fase 2b (MCE): MCE_Sent, MCE_Open, MCE_Click, MCE_Bounce, MCE_Unsub, MCE_Subscriber
  - Fase 4 (GenAI): GatewayRequest, Generation, Quality, Category, Feedback, Detail

Autenticação: OAuth2 Client Credentials (dc_session de get_data_cloud_session).

Endpoint: POST /services/data/v67.0/ssot/query-sql?dataspace={dataspace}&workloadName=BatchQuery

Resposta:
  {
    "data": [[val, ...], ...],          # array de arrays (uma linha = um array de valores)
    "metadata": [{"name": ..., "type": ...}, ...],
    "returnedRows": N,
    "nextPageUrl": "..."                # presente quando há mais páginas
  }

IMPORTANTE:
  - Nomes de tabela: ssot__<NomeDMO>__dlm  (prefixo ssot__ obrigatório)
  - Nomes de coluna: ssot__<NomeCampo>__c  (idem)
  - Nunca use SELECT * em produção — liste colunas explicitamente
"""

from __future__ import annotations

import pandas as pd
import requests
from prefect import task

_QUERY_ENDPOINT = "/services/data/v67.0/ssot/query-sql"
_WORKLOAD = "BatchQuery"


def _run_query(
    instance_url: str,
    access_token: str,
    sql: str,
    dataspace: str = "default",
) -> tuple[list[list], list[str]]:
    """
    Executa uma query SQL no Data Cloud e coleta todas as páginas.

    Returns:
        (rows, col_names) onde rows é lista de listas e col_names é lista de strings.
    """
    url = f"{instance_url}{_QUERY_ENDPOINT}?dataspace={dataspace}&workloadName={_WORKLOAD}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    resp = requests.post(url, headers=headers, json={"sql": sql}, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    col_names = [c["name"] for c in data.get("metadata", [])]
    all_rows: list[list] = list(data.get("data", []))

    # Paginação
    next_url = data.get("nextPageUrl")
    while next_url:
        page_resp = requests.get(
            f"{instance_url}{next_url}" if not next_url.startswith("http") else next_url,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=60,
        )
        page_resp.raise_for_status()
        page_data = page_resp.json()
        all_rows.extend(page_data.get("data", []))
        next_url = page_data.get("nextPageUrl")

    return all_rows, col_names


@task(
    log_prints=True,
    retries=3,
    retry_delay_seconds=[30, 60, 120],
)
def extract_from_data_cloud(
    dc_session: dict,
    query: str,
    table_name: str = "desconhecida",
) -> pd.DataFrame:
    """
    Executa uma query SQL no Data Cloud e retorna um DataFrame.

    Args:
        dc_session  : dict com 'access_token', 'instance_url' e 'dataspace'
                      (retornado por get_data_cloud_session).
        query       : SQL com colunas explícitas e filtro de watermark.
                      Ex: "SELECT ssot__Id__c, ssot__StartTimestamp__c
                           FROM ssot__AiAgentSession__dlm
                           WHERE ssot__StartTimestamp__c >= '2024-01-01T00:00:00Z'"
        table_name  : Nome da tabela (para logs). Não afeta a query.

    Returns:
        pd.DataFrame com os registros retornados, ou DataFrame vazio se não houver dados.

    Raises:
        RuntimeError: Se a query falhar por razão diferente de tabela inexistente.
    """
    access_token = dc_session["access_token"]
    instance_url = dc_session["instance_url"]
    dataspace = dc_session.get("dataspace", "default")

    print(f"[DC] Executando query em '{table_name}'...")
    print(f"[DC] Query: {query[:200]}...")

    try:
        rows, col_names = _run_query(
            instance_url=instance_url,
            access_token=access_token,
            sql=query,
            dataspace=dataspace,
        )

        if not rows:
            print(f"[DC] '{table_name}': nenhum registro retornado.")
            return pd.DataFrame(columns=col_names)

        df = pd.DataFrame(rows, columns=col_names)
        print(f"[DC] '{table_name}': {len(df)} linhas, {len(df.columns)} colunas.")
        return df

    except requests.HTTPError as exc:
        body = exc.response.text if exc.response is not None else ""
        # Tabela inexistente → retorna DataFrame vazio em vez de explodir
        if "does not exist" in body or "not found" in body.lower():
            print(f"[DC] WARN: '{table_name}' nao encontrada no Data Cloud — retornando vazio.")
            return pd.DataFrame()
        raise RuntimeError(f"[DC] Erro HTTP ao extrair '{table_name}': {exc}\n{body}") from exc
    except Exception as exc:
        raise RuntimeError(f"[DC] Erro ao extrair '{table_name}': {exc}") from exc

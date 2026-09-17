# -*- coding: utf-8 -*-
"""
Extração de DMOs do Salesforce Data Cloud via Connect API REST.

Usado por:
  - Fase 1 (STDM): AiAgentSession, AiAgentInteraction, Steps, Messages
  - Fase 2b (MCE): MCE_Sent, MCE_Open, MCE_Click, MCE_Bounce, MCE_Unsub, MCE_Subscriber
  - Fase 4 (GenAI): GatewayRequest, Generation, Quality, Category, Feedback, Detail

Autenticação: OAuth2 Client Credentials (dc_session de get_data_cloud_session).

Endpoint: POST /services/data/v67.0/ssot/query-sql?dataspace={dataspace}&workloadName=BatchQuery

Resposta real (confirmado em 04/09/2026 contra um dia de pico — 8.245 linhas
batidas pelo filtro, buscando 'ai_agent_session' de 07/08):
  {
    "data": [[val, ...], ...],   # array de arrays — só a fatia desta resposta
    "metadata": [{"name": ..., "type": ...}, ...],
    "returnedRows": N,           # quantas vieram NESTA resposta (ex.: 1415)
    "status": {
        "rowCount": M,           # total que a query bate (ex.: 8245)
        "rowsProcessed": M,
        "queryId": "...",
        "completionStatus": "ResultsProduced",
        ...
    }
  }
NÃO existe "nextPageUrl" nem "nextBatchId" — os dois nomes que o código usava
antes de 04/09/2026 e que nunca bateram com a resposta real, causando corte
silencioso em todo dia/janela cujo resultado excedesse o tamanho de uma única
resposta (~1400-1700 linhas, parece ser limite de tamanho de payload, não de
contagem fixa — varia por query). Também não existe endpoint de continuação
via queryId (GET .../ssot/query/{queryId} devolve 404 NOT_FOUND, testado).
A única paginação que funciona é reenviar a SQL com ORDER BY + LIMIT/OFFSET,
um POST por página — daí o parâmetro order_by_col obrigatório abaixo.

Achado e corrigido em 04/09/2026 — ver quick/agentforce_ai_agent_backfill/
(scripts/investigação que motivou o backfill de agosto) para o histórico.

IMPORTANTE:
  - Nomes de tabela: ssot__<NomeDMO>__dlm  (prefixo ssot__ obrigatório)
  - Nomes de coluna: ssot__<NomeCampo>__c  (idem)
  - Nunca use SELECT * em produção — liste colunas explicitamente
  - Toda query passada aqui precisa de um order_by_col (normalmente
    ssot__Id__c) — é a coluna usada pra paginação determinística por OFFSET
"""

from __future__ import annotations

import pandas as pd
import requests
from prefect import task

_QUERY_ENDPOINT = "/services/data/v67.0/ssot/query-sql"
_WORKLOAD = "BatchQuery"

# Tamanho de página conservador — o teto real observado do servidor variou
# entre ~1400 e ~1700 linhas por resposta dependendo da query; 1000 fica
# folgado abaixo disso. Não é limite de contagem fixa da API (não documentado
# publicamente), então mantém folga em vez de chutar o teto exato.
_PAGE_SIZE = 1000


def _run_query(
    instance_url: str,
    access_token: str,
    sql: str,
    order_by_col: str,
    dataspace: str = "default",
    page_size: int = _PAGE_SIZE,
) -> tuple[list[list], list[str]]:
    """
    Executa uma query SQL no Data Cloud e pagina até coletar tudo.

    Pagina via ORDER BY + LIMIT/OFFSET na própria SQL (ver docstring do
    módulo — não existe continuação via nextPageUrl/nextBatchId/queryId nessa
    API, apesar do código anterior assumir que existia).

    Returns:
        (rows, col_names) onde rows é lista de listas e col_names é lista de strings.
    """
    url = f"{instance_url}{_QUERY_ENDPOINT}?dataspace={dataspace}&workloadName={_WORKLOAD}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    all_rows: list[list] = []
    col_names: list[str] = []
    total_esperado: int | None = None
    offset = 0
    pagina = 0

    while True:
        pagina += 1
        sql_pagina = f"{sql.rstrip().rstrip(';')} ORDER BY {order_by_col} LIMIT {page_size} OFFSET {offset}"
        resp = requests.post(url, headers=headers, json={"sql": sql_pagina}, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if pagina == 1:
            col_names = [c["name"] for c in data.get("metadata", [])]
            status = data.get("status") or {}
            total_esperado = status.get("rowCount")

        novas = data.get("data", [])
        all_rows.extend(novas)

        if len(novas) < page_size:
            break  # última página (veio menos que o pedido)
        offset += page_size

    if total_esperado is not None and len(all_rows) != total_esperado:
        print(
            f"[DC][PAGINACAO] AVISO: {len(all_rows)} linhas coletadas, "
            f"servidor reportou rowCount={total_esperado} — possível corte."
        )

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
    order_by_col: str = "ssot__Id__c",
) -> pd.DataFrame:
    """
    Executa uma query SQL no Data Cloud e retorna um DataFrame.

    Pagina internamente via ORDER BY + LIMIT/OFFSET (ver docstring do módulo) —
    a query passada aqui NÃO deve ter LIMIT/OFFSET/ORDER BY próprio, isso é
    adicionado por página automaticamente.

    Args:
        dc_session  : dict com 'access_token', 'instance_url' e 'dataspace'
                      (retornado por get_data_cloud_session).
        query       : SQL com colunas explícitas e filtro de watermark, SEM
                      LIMIT/OFFSET/ORDER BY.
                      Ex: "SELECT ssot__Id__c, ssot__StartTimestamp__c
                           FROM ssot__AiAgentSession__dlm
                           WHERE ssot__StartTimestamp__c >= '2024-01-01T00:00:00Z'"
        table_name  : Nome da tabela (para logs). Não afeta a query.
        order_by_col: Coluna estável pra paginação determinística por OFFSET.
                      Default 'ssot__Id__c' — vale pra toda query deste
                      pipeline, que sempre traz o id como primeira coluna do
                      SELECT. Só precisa mudar se algum dia existir uma query
                      sem essa coluna.

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
            order_by_col=order_by_col,
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

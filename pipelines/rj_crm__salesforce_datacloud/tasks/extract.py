# -*- coding: utf-8 -*-
"""
Extração da Salesforce — as duas tasks de extração do pipeline:
  - extract_from_data_cloud : Data Cloud (DMOs/DLLs), SQL — source='data_cloud'
  - extract_from_crm_rest   : CRM (MessagingEndUser, MessagingSession), SOQL —
                              source='crm_rest'. Pagina via nextRecordsUrl.

As duas só retentam erro temporário (utils/retry.py) — 4xx falha na hora.

--- Data Cloud ---

Extração de DMOs/DLLs do Salesforce Data Cloud via Connect API REST.

Usado por toda tabela de tabelas.yaml com source='data_cloud' (tudo que não é
crm_rest). Até 2026-09-29 existia também um source 'data_cloud_chunked'
(tasks/extract_chunked.py) — criado quando esta extração não paginava
direito; depois do conserto de 04/09 os dois paginavam igual (mesmo
ORDER BY + LIMIT/OFFSET) e o chunked só carregava a staging em N jobs em vez
de 1, sem economizar memória. Foi unificado aqui.

Autenticação: OAuth2 Client Credentials (dc_session de get_data_cloud_session).

Endpoint: POST /services/data/v67.0/ssot/query-sql?dataspace={dataspace}&workloadName=BatchQuery

Resposta real (confirmado em 04/09/2026 contra um dia de pico — 8.245 linhas
batidas pelo filtro, buscando 'ai_agent_session' de 07/08):
  {
    "data": [[val, ...], ...],   # array de arrays — só a fatia desta resposta
    "metadata": [{"name": ..., "type": ...}, ...],
    "returnedRows": N,           # quantas vieram NESTA resposta (ex.: 1415)
    "status": {
        "rowCount": M,           # total que a query bate (ex.: 8245) — MAS
                                 # com LIMIT na SQL vira o tamanho da página
                                 # (sempre 1000 aqui), não o total
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
  - Nomes de tabela: ssot__<NomeDMO>__dlm  (prefixo ssot__ obrigatório em DMO;
    DLL não tem prefixo, ex.: MessagingEventsWhatsAppV2_00Das_4CAB1BC2__dll)
  - Nomes de coluna: ssot__<NomeCampo>__c  (idem)
  - Nunca use SELECT * em produção — liste colunas explicitamente
  - Toda query passada aqui precisa de um order_by_col (normalmente
    ssot__Id__c; em DLL, a chave dela) — é a coluna usada pra paginação
    determinística por OFFSET (order_by_col em tabelas.yaml)
"""

from __future__ import annotations

import pandas as pd
import requests
from prefect import task

from pipelines.rj_crm__salesforce_datacloud.utils.retry import so_erro_temporario

_QUERY_ENDPOINT = "/services/data/v67.0/ssot/query-sql"
_WORKLOAD = "BatchQuery"

# Tamanho de página conservador — o teto real observado do servidor variou
# entre ~1400 e ~1700 linhas por resposta dependendo da query; 1000 fica
# folgado abaixo disso. Não é limite de contagem fixa da API (não documentado
# publicamente), então mantém folga em vez de chutar o teto exato.
_PAGE_SIZE = 1000

# Limite de segurança contra loop infinito de paginação (herdado do antigo
# extract_chunked.py). Muito acima do maior volume real: tracing ~200k/dia.
_MAX_ROWS = 5_000_000

# Por página — 120s (o antigo chunked usava 120, este usava 60; ficou o maior).
_TIMEOUT = 120


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
    offset = 0
    pagina = 0

    while True:
        pagina += 1
        sql_pagina = f"{sql.rstrip().rstrip(';')} ORDER BY {order_by_col} LIMIT {page_size} OFFSET {offset}"
        resp = requests.post(url, headers=headers, json={"sql": sql_pagina}, timeout=_TIMEOUT)
        if not resp.ok:
            print(f"[DC] Erro {resp.status_code} na query. SQL enviado:\n{sql_pagina}")
            print(f"[DC] Resposta do Salesforce: {resp.text[:1000]}")
        resp.raise_for_status()
        data = resp.json()

        if pagina == 1:
            col_names = [c["name"] for c in data.get("metadata", [])]

        novas = data.get("data", [])
        all_rows.extend(novas)

        if len(novas) < page_size:
            break  # última página (veio menos que o pedido)
        offset += page_size
        if offset >= _MAX_ROWS:
            print(f"[DC][PAGINACAO] WARN: atingiu o limite de segurança ({_MAX_ROWS} linhas) — parando.")
            break

    # Sem conferência contra status.rowCount: com LIMIT na SQL ele é o tamanho
    # da página (1000), não o total — comparar dava "possível corte" falso em
    # toda extração com mais de 1 página (removido 2026-09-29). Paginação
    # conferida contra COUNT(*) na fonte no mesmo dia: bate linha a linha.
    print(f"[DC][PAGINACAO] {len(all_rows)} linha(s) em {pagina} pagina(s).")

    return all_rows, col_names


@task(
    log_prints=True,
    retries=3,
    retry_delay_seconds=[30, 60, 120],
    retry_condition_fn=so_erro_temporario,
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
                      Default 'ssot__Id__c' (DMOs); DLL passa a própria chave
                      (order_by_col em tabelas.yaml).

    Returns:
        pd.DataFrame com os registros retornados, ou DataFrame vazio se não houver dados.

    Raises:
        RuntimeError: Se a query falhar — inclusive tabela inexistente (antes
                      devolvia vazio em silêncio; agora a tabela falha e o
                      flow avisa no Discord, ou aborta se for crítica).
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
        raise RuntimeError(f"[DC] Erro HTTP ao extrair '{table_name}': {exc}\n{body}") from exc
    except Exception as exc:
        raise RuntimeError(f"[DC] Erro ao extrair '{table_name}': {exc}") from exc


# ---------------------------------------------------------------------------
# CRM REST (SOQL)
# ---------------------------------------------------------------------------

_CRM_QUERY_PATH = "/services/data/v67.0/query"


@task(
    log_prints=True,
    retries=3,
    retry_delay_seconds=[30, 60, 120],
    retry_condition_fn=so_erro_temporario,
)
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

    url = f"{instance_url}{_CRM_QUERY_PATH}"
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

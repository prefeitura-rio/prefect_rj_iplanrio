# -*- coding: utf-8 -*-
"""
Extração chunked do Data Cloud para tabelas grandes (ex: TelemetryTraceSpan).

Usado por:
  - Fase 3 (Platform Tracing): ssot__TelemetryTraceSpan__dlm (~200k+ registros/dia)

Estratégia: paginação via LIMIT/OFFSET na query SQL.
  - Cada página executa um POST no endpoint /ssot/query-sql com LIMIT {chunk_size} OFFSET {N}
  - O loop para quando a página retorna menos linhas que chunk_size (última página)
  - Limite de segurança max_rows evita loops infinitos em tabelas muito grandes
"""

from __future__ import annotations

import pandas as pd
import requests
from prefect import task


_DEFAULT_CHUNK_SIZE = 1_000  # ver nota em extract_data_cloud.py: o servidor
# corta o payload em ~1400-1700 linhas por resposta, independente do LIMIT
# pedido (confirmado em 04/09/2026 contra ai_agent_session, mesma API/endpoint).
# Um chunk_size acima disso faz o loop parar cedo achando que chegou na
# última página (len(rows) < chunk_size), quando na verdade só bateu no teto
# do servidor — era 50_000 antes, quase certamente truncando silenciosamente
# em produção, já que esta tabela tem ~200k+ registros/dia.
_DEFAULT_MAX_ROWS = 5_000_000  # limite de segurança para evitar loop infinito


_QUERY_ENDPOINT = "/services/data/v67.0/ssot/query-sql"
_WORKLOAD = "BatchQuery"


def _query_page(
    instance_url: str,
    access_token: str,
    sql: str,
    dataspace: str,
    offset: int,
    chunk_size: int,
    order_by_col: str,
) -> tuple[list[list], list[str]]:
    """
    Executa uma query SQL no Data Cloud com LIMIT/OFFSET e retorna (rows, col_names).

    O Data Cloud REST não suporta jobs assíncronos com cursor — usa LIMIT/OFFSET diretamente
    na query SQL para paginar. Não existe 'nextPageUrl' real na resposta desta API (era lido
    aqui antes mas nunca usado por quem chama — removido; ver extract_data_cloud.py para o
    histórico de por que esse campo não existe de verdade). order_by_col garante ordem
    estável entre chamadas — sem ORDER BY, OFFSET não tem garantia de não pular/repetir
    linha de uma página pra outra.
    """
    paged_sql = f"{sql.rstrip(';')} ORDER BY {order_by_col} LIMIT {chunk_size} OFFSET {offset}"
    url = f"{instance_url}{_QUERY_ENDPOINT}?dataspace={dataspace}&workloadName={_WORKLOAD}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, json={"sql": paged_sql}, timeout=120)
    if not resp.ok:
        print(f"[CHUNKED] Erro {resp.status_code} na query. SQL enviado:\n{paged_sql}")
        print(f"[CHUNKED] Resposta do Salesforce: {resp.text}")
    resp.raise_for_status()
    data = resp.json()

    col_names = [c["name"] for c in data.get("metadata", [])]
    rows = data.get("data", [])

    return rows, col_names


# ---------------------------------------------------------------------------
# Task principal — retorna lista de DataFrames (um por chunk)
# ---------------------------------------------------------------------------


@task(
    log_prints=True,
    retries=3,
    retry_delay_seconds=[30, 60, 120],
)
def extract_chunked_from_data_cloud(
    dc_session: dict,
    query: str,
    table_name: str = "desconhecida",
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
    max_rows: int = _DEFAULT_MAX_ROWS,
    order_by_col: str = "ssot__Id__c",
) -> list[pd.DataFrame]:
    """
    Extrai uma tabela grande do Data Cloud em chunks via LIMIT/OFFSET.

    Cada chunk é um DataFrame de até chunk_size linhas. A task retorna uma lista
    de DataFrames — o flow é responsável por carregar cada chunk no BQ de forma
    incremental (load_bigquery_chunk).

    Args:
        dc_session  : dict com 'access_token', 'instance_url', 'dataspace'
                      (retornado por get_data_cloud_session).
        query       : SQL com filtro de watermark e SEM LIMIT/OFFSET/ORDER BY
                      (o chunking adiciona os três).
        table_name  : Nome da tabela (para logs).
        chunk_size  : Linhas por chunk. Padrão: 1.000 (ver nota em
                      _DEFAULT_CHUNK_SIZE — o servidor corta o payload nesse
                      teto independente do que for pedido).
        max_rows    : Limite de segurança. Padrão: 5.000.000.
        order_by_col: Coluna estável pra paginação determinística. Default
                      'ssot__Id__c' — vale pra query deste pipeline.

    Returns:
        Lista de DataFrames, um por chunk. Lista vazia se não houver dados.
    """
    access_token = dc_session["access_token"]
    instance_url = dc_session["instance_url"]
    dataspace = dc_session.get("dataspace", "default")

    print(f"[CHUNKED] Iniciando extração chunked de '{table_name}'...")

    chunks: list[pd.DataFrame] = []
    offset = 0
    chunk_num = 1
    col_names: list[str] = []

    while offset < max_rows:
        print(f"[CHUNKED] '{table_name}' — chunk {chunk_num} | offset={offset}")
        rows, col_names = _query_page(
            instance_url=instance_url,
            access_token=access_token,
            sql=query,
            dataspace=dataspace,
            offset=offset,
            chunk_size=chunk_size,
            order_by_col=order_by_col,
        )

        if not rows:
            print(f"[CHUNKED] Chunk {chunk_num} vazio — parando paginação.")
            break

        df_chunk = pd.DataFrame(rows, columns=col_names)
        print(f"[CHUNKED] Chunk {chunk_num}: {len(df_chunk)} linhas.")
        chunks.append(df_chunk)
        offset += len(rows)
        chunk_num += 1

        if len(rows) < chunk_size:
            # Última página (menos linhas que o tamanho do chunk)
            break

    total = sum(len(c) for c in chunks)
    if total >= max_rows:
        print(f"[CHUNKED] WARN: '{table_name}' atingiu o limite de segurança ({max_rows} linhas).")

    print(f"[CHUNKED] '{table_name}' concluído: {len(chunks)} chunks, {total} linhas total.")
    return chunks

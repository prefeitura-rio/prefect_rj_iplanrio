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


_DEFAULT_CHUNK_SIZE = 50_000
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
) -> tuple[list[list], list[str], str | None]:
    """
    Executa uma query SQL no Data Cloud com LIMIT/OFFSET e retorna (rows, col_names, next_url).

    O Data Cloud REST não suporta jobs assíncronos com cursor — usa LIMIT/OFFSET diretamente
    na query SQL para paginar.
    """
    paged_sql = f"{sql.rstrip(';')} LIMIT {chunk_size} OFFSET {offset}"
    url = f"{instance_url}{_QUERY_ENDPOINT}?dataspace={dataspace}&workloadName={_WORKLOAD}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    resp = requests.post(url, headers=headers, json={"sql": paged_sql}, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    col_names = [c["name"] for c in data.get("metadata", [])]
    rows = data.get("data", [])
    next_url = data.get("nextPageUrl")

    return rows, col_names, next_url


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
) -> list[pd.DataFrame]:
    """
    Extrai uma tabela grande do Data Cloud em chunks via LIMIT/OFFSET.

    Cada chunk é um DataFrame de até chunk_size linhas. A task retorna uma lista
    de DataFrames — o flow é responsável por carregar cada chunk no BQ de forma
    incremental (load_bigquery_chunk).

    Args:
        dc_session  : dict com 'access_token', 'instance_url', 'dataspace'
                      (retornado por get_data_cloud_session).
        query       : SQL com filtro de watermark e SEM LIMIT/OFFSET (o chunking adiciona).
        table_name  : Nome da tabela (para logs).
        chunk_size  : Linhas por chunk. Padrão: 50.000.
        max_rows    : Limite de segurança. Padrão: 5.000.000.

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
        rows, col_names, _ = _query_page(
            instance_url=instance_url,
            access_token=access_token,
            sql=query,
            dataspace=dataspace,
            offset=offset,
            chunk_size=chunk_size,
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

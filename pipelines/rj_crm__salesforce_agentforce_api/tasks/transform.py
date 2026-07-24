# -*- coding: utf-8 -*-
"""
Transformações aplicadas a todos os DataFrames antes de carregar no BigQuery.

Operações:
  - snake_case nos nomes de colunas
  - Remoção do prefixo ssot__ e sufixo __c dos campos do Data Cloud
  - Conversão de tipos (datas, duração em ns → ms)
  - Adição de coluna _loaded_at (timestamp de ingestão)
  - Adição de coluna data_particao (data de execução, para particionamento)
  - Cast de colunas para os tipos esperados pelo schema BQ
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone

import pandas as pd
from prefect import task


# ---------------------------------------------------------------------------
# Funções utilitárias (internas)
# ---------------------------------------------------------------------------


def _to_snake_case(name: str) -> str:
    """Converte CamelCase ou PascalCase para snake_case."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
    return s.lower()


def _clean_dc_field_name(name: str) -> str:
    """
    Remove prefixo ssot__ e sufixo __c/__dlm/__dll dos campos do Data Cloud.
    Ex: 'ssot__StartTime__c' → 'start_time'
        'AiAgentSession__dlm' → 'ai_agent_session'
    """
    cleaned = re.sub(r"^ssot__", "", name)
    cleaned = re.sub(r"(__c|__dlm|__dll)$", "", cleaned)
    return _to_snake_case(cleaned)


def _normalize_columns(df: pd.DataFrame, is_data_cloud: bool = False) -> pd.DataFrame:
    """Renomeia colunas para snake_case, removendo prefixos DC se necessário."""
    if is_data_cloud:
        df.columns = [_clean_dc_field_name(c) for c in df.columns]
    else:
        df.columns = [_to_snake_case(c) for c in df.columns]
    # Remover colunas duplicadas após normalização (edge case)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def _convert_duration_ns_to_ms(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Converte colunas de duração de nanosegundos para milissegundos."""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 1_000_000
            df = df.rename(columns={col: col.replace("_ns", "_ms")})
    return df


def _parse_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Converte colunas de data/datetime para datetime com timezone UTC."""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df


# ---------------------------------------------------------------------------
# Task principal
# ---------------------------------------------------------------------------


@task(log_prints=True)
def transform_dataframe(
    df: pd.DataFrame,
    table_name: str = "desconhecida",
    is_data_cloud: bool = False,
    date_columns: list[str] | None = None,
    duration_ns_columns: list[str] | None = None,
    partition_date: date | None = None,
) -> pd.DataFrame:
    """
    Aplica transformações padrão a um DataFrame antes do carregamento no BQ.

    Args:
        df              : DataFrame bruto da extração.
        table_name      : Nome da tabela (para logs).
        is_data_cloud   : Se True, remove prefixo ssot__ e sufixo __c dos campos.
        date_columns    : Colunas para converter para datetime UTC.
                          (após normalização de nomes, já em snake_case)
        duration_ns_columns: Colunas em nanosegundos para converter para ms.
        partition_date  : Data de partição. Padrão: hoje.

    Returns:
        DataFrame transformado, pronto para carga no BigQuery.
    """
    if df.empty:
        print(f"[TRANSFORM] '{table_name}': DataFrame vazio — pulando transformações.")
        return df

    print(f"[TRANSFORM] '{table_name}': {len(df)} linhas, {len(df.columns)} colunas.")

    # 1. Normalizar nomes de colunas
    df = _normalize_columns(df, is_data_cloud=is_data_cloud)
    print(f"[TRANSFORM] Colunas normalizadas: {list(df.columns)[:10]}...")

    # 2. Converter datas
    if date_columns:
        df = _parse_dates(df, date_columns)

    # 3. Converter durações ns → ms
    if duration_ns_columns:
        df = _convert_duration_ns_to_ms(df, duration_ns_columns)

    # 4. Adicionar _loaded_at (timestamp de ingestão UTC)
    df["_loaded_at"] = datetime.now(tz=timezone.utc)

    # 5. Adicionar data_particao
    if partition_date is None:
        partition_date = date.today()
    df["data_particao"] = str(partition_date)

    # 6. Remover strings vazias (Bulk API retorna "" para NULL)
    df = df.replace("", None)

    print(
        f"[TRANSFORM] '{table_name}': transformação concluída — "
        f"{len(df)} linhas, {len(df.columns)} colunas finais."
    )
    return df

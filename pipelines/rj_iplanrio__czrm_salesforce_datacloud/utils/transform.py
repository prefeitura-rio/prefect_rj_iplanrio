"""Adaptações mínimas do DataFrame bruto para compatibilidade com BigQuery.

- Converte timestamps (``TimestampTZ``, ``DateTime``) para datetime UTC.
- Converte datas (``Date``) para ``datetime.date``.
- Converte booleanos que chegam como texto (``'true'``/``'false'``).
- Adiciona ``_loaded_at`` com o instante da carga em UTC.
- Loga warning quando o DataFrame traz colunas ausentes no schema descoberto.

Nomes de colunas, estrutura e demais valores são preservados como vieram da API.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.log import logger_da_pipeline
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.schema import SchemaTabela
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.tabelas import Tabela

logger = logger_da_pipeline(__name__)

MAPA_BOOLEANO = {"true": True, "false": False, "1": True, "0": False}


def converter_booleanas(df: pd.DataFrame, colunas: list[str]) -> pd.DataFrame:
    """Converte texto ``'true'``/``'false'`` para BOOLEAN, preservando nulos."""
    for coluna in colunas:
        if coluna in df.columns:
            df[coluna] = df[coluna].astype("string").str.lower().map(MAPA_BOOLEANO).astype("boolean")
    return df


def alertar_colunas_novas(df: pd.DataFrame, schema: SchemaTabela, tabela: str) -> None:
    """Loga warning para colunas presentes no DataFrame mas ausentes no schema.

    Indica que a DLO ganhou colunas desde a descoberta de schema deste run.
    As colunas novas são carregadas normalmente — o aviso serve para que o
    operador saiba que o BQ pode precisar de um ``garantir_tabelas`` manual.

    :param df: DataFrame extraído da DLO.
    :param schema: schema descoberto no início do run via ``SELECT * LIMIT 0``.
    :param tabela: nome lógico da tabela, usado no log.
    """
    conhecidas = set(schema.colunas)
    novas = [c for c in df.columns if c not in conhecidas and c != "_loaded_at"]
    if novas:
        logger.warning(
            "Colunas novas detectadas em '%s' (ausentes no schema do run): %s — "
            "rode garantir_tabelas para registrá-las no BigQuery.",
            tabela,
            novas,
        )


def transformar(
    df: pd.DataFrame,
    tabela: Tabela,
    schema: SchemaTabela,
    agora: datetime | None = None,
) -> pd.DataFrame:
    """Adapta o DataFrame bruto para o BigQuery sem alterar estrutura ou nomes.

    :param df: DataFrame como veio da extração.
    :param tabela: configuração da tabela (booleanos).
    :param schema: schema com listas de colunas timestamp e date.
    :param agora: instante de referência da carga; padrão é agora em UTC.
    :returns: DataFrame pronto para serialização em Parquet.
    """
    if df.empty:
        return df

    agora = agora or datetime.now(tz=timezone.utc)
    df = df.copy()

    alertar_colunas_novas(df, schema, tabela.nome)

    for coluna in schema.datas:
        if coluna in df.columns:
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce", utc=True, format="ISO8601")

    for coluna in schema.datas_date:
        if coluna in df.columns:
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce").dt.date

    df = converter_booleanas(df, tabela.booleanas)
    df["_loaded_at"] = agora

    return df

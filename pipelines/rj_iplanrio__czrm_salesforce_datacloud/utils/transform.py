"""Transformações aplicadas ao DataFrame antes da carga (puro, sem I/O).

Operações:
  - snake_case + remoção de prefixo/sufixo do Data Cloud
  - descarte de coluna que não está no schema (drift) com aviso
  - datas em UTC (TimestampTZ)
  - booleanos que chegam como texto ('true'/'false')
  - colunas de controle: ``_loaded_at``, ``data_criacao``, ``data_atualizacao``

``data_criacao`` e ``data_atualizacao`` são **dias do calendário de São
Paulo**; os instantes (``created_date``, ``system_modstamp``) continuam em
UTC. Dia é conceito de fuso; instante não — converter instante cedo demais
perde a informação e cria ambiguidade de 3h.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.schema import SchemaTabela, nome_final
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.tabelas import Tabela
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

SP = ZoneInfo("America/Sao_Paulo")


def _dia_sp(serie: pd.Series, fallback: date) -> pd.Series:
    """Converte uma coluna de timestamp UTC em dia do calendário de SP.

    :param serie: coluna já convertida para datetime UTC.
    :param fallback: dia usado quando o valor é nulo.
    :returns: série de ``datetime.date``.
    """
    return serie.dt.tz_convert(SP).dt.date.fillna(fallback)


def _converter_booleanas(df: pd.DataFrame, colunas: list[str]) -> pd.DataFrame:
    """Converte texto 'true'/'false' em BOOLEAN, preservando nulos."""
    mapa = {"true": True, "false": False, "1": True, "0": False}
    for coluna in colunas:
        if coluna in df.columns:
            df[coluna] = df[coluna].astype("string").str.lower().map(mapa).astype("boolean")
    return df


def transformar(
    df: pd.DataFrame,
    tabela: Tabela,
    schema: SchemaTabela,
    agora: datetime | None = None,
) -> pd.DataFrame:
    """Normaliza o DataFrame bruto para o schema da tabela.

    :param df: DataFrame como veio da extração.
    :param tabela: configuração da tabela (renomeações, booleanos).
    :param schema: schema descoberto a partir da DLO.
    :param agora: instante da carga. Padrão: agora em UTC.
    :returns: DataFrame pronto para a staging (colunas na ordem do schema).
    :raises ValueError: Se faltar uma coluna obrigatória do schema.
    """
    if df.empty:
        return df

    agora = agora or datetime.now(tz=timezone.utc)

    df = df.copy()
    df.columns = [nome_final(c, tabela.renomear) for c in df.columns]
    # Deduplica DEPOIS de renomear: nomes brutos diferentes podem cair no mesmo
    # nome final (ex.: 'Foo__c' e 'ssot__Foo__c' -> 'foo'). Fica a primeira
    # ocorrência, igual ao que utils/schema.py:montar faz.
    df = df.loc[:, ~df.columns.duplicated()].copy()

    controle = {"_loaded_at", "data_criacao", "data_atualizacao"}
    permitidas = (set(schema.colunas) - controle) | {"created_date", "system_modstamp"}
    desconhecidas = [c for c in df.columns if c not in permitidas]
    if desconhecidas:
        logger.warning(
            "Colunas fora do schema descartadas em '%s': %s (rode novamente para o schema ser refeito)",
            tabela.nome,
            desconhecidas,
        )
        df = df.drop(columns=desconhecidas)

    for coluna in schema.datas:
        if coluna in df.columns:
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce", utc=True, format="ISO8601")

    # Colunas DATE (sem hora) viram datetime.date — o schema é descoberto em
    # runtime, então uma DLO pode passar a devolver coluna 'Date' a qualquer run.
    for coluna in schema.datas_date:
        if coluna in df.columns:
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce").dt.date

    df = _converter_booleanas(df, tabela.booleanas)

    fallback = agora.astimezone(SP).date()
    criacao = df["created_date"] if "created_date" in df.columns else None
    modificacao = df["system_modstamp"] if "system_modstamp" in df.columns else None
    df["data_criacao"] = _dia_sp(criacao, fallback) if criacao is not None else fallback
    df["data_atualizacao"] = _dia_sp(modificacao, fallback) if modificacao is not None else fallback

    df["_loaded_at"] = agora
    df = df.replace("", None)

    faltando = [c for c in schema.colunas if c not in df.columns]
    if faltando:
        raise ValueError(f"tabela '{tabela.nome}': colunas do schema ausentes no DataFrame: {faltando}")

    return df[list(schema.colunas)]

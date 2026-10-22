# -*- coding: utf-8 -*-
"""Utilitários gerais do pipeline rj_segur__forca_municipal."""

import hashlib
import json
from datetime import date, datetime, timedelta
from typing import Generator, Iterator, Optional

import pandas as pd
from fastkml import Placemark
from prefect.logging import get_logger
from pydantic_settings import BaseSettings

from pipelines.rj_segur__forca_municipal.constants import SP_TZ

logger = get_logger("toolkit")


def _add_id_hash(data: list[dict]) -> list[dict]:
    """Adiciona id_hash a cada row com base nos dados brutos da API.

    Hasheado antes da criação do DataFrame para evitar inconsistências de dtype
    inference do pandas (ex: coluna inferida como int64 num run e float64 em outro
    quando None aparece). json.dumps com sort_keys garante representação canônica
    e determinística independente de tipo ou ordem de chaves.

    Usa os 32 chars do MD5 (128 bits) — suficiente para o volume dessa API.
    """
    for row in data:
        row["id_hash"] = hashlib.md5(
            json.dumps(row, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()
    return data


def _parse_date_range(data_inicio: str, data_fim: str) -> tuple[str, str]:
    """
    Valida e normaliza o intervalo de datas.

    Retorna (data_inicio, data_fim) como strings ISO canônicas.
    Levanta ValueError com mensagem clara em caso de formato inválido ou
    inconsistência lógica entre os valores.
    """
    try:
        d_inicio = date.fromisoformat(data_inicio)
    except ValueError:
        raise ValueError(f"data_inicio inválida: '{data_inicio}'. Use YYYY-MM-DD.")

    try:
        d_fim = date.fromisoformat(data_fim)
    except ValueError:
        raise ValueError(f"data_fim inválida: '{data_fim}'. Use YYYY-MM-DD.")

    if d_inicio > d_fim:
        raise ValueError(
            f"data_inicio ({data_inicio}) deve ser <= data_fim ({data_fim})."
        )

    today = datetime.now(tz=SP_TZ).date()
    if d_fim > today:
        logger.info(
            f"data_fim ({data_fim}) está no futuro — a API provavelmente retornará vazio.",
            level="warning",
        )

    return d_inicio.isoformat(), d_fim.isoformat()


def _date_range(data_inicio: str, data_fim: str) -> Generator[str, None, None]:
    """Gera datas ISO de data_inicio até data_fim (inclusive)."""
    d = date.fromisoformat(data_inicio)
    d_fim = date.fromisoformat(data_fim)
    while d <= d_fim:
        yield d.isoformat()
        d += timedelta(days=1)


def _iter_placemarks(
    node: object,
    folder_name: Optional[str] = None,
) -> Iterator[tuple[Placemark, Optional[str]]]:
    """Itera placemarks em qualquer nível de aninhamento KML (Document/Folder/Placemark).

    Suporta placemarks diretamente sob Document, dentro de Folder, e em pastas aninhadas.
    folder_name é o nome do container imediato do placemark (None se não estiver em Folder).
    """
    for feature in getattr(node, "features", []):
        if isinstance(feature, Placemark):
            yield feature, folder_name
        else:
            yield from _iter_placemarks(
                node=feature,
                folder_name=getattr(feature, "name", None) or folder_name,
            )


def _cycle_prefix(cycle_num: Optional[int], total_cycles: Optional[int]) -> str:
    """Retorna prefixo de ciclo para logs.

    Com total conhecido: '[Ciclo X/N (P%)]'
    Sem total (acumulação dinâmica): '[Ciclo X]'
    """
    if cycle_num is None:
        return "[Ciclo]"
    if total_cycles is None:
        return f"[Ciclo {cycle_num}]"
    pct = round(cycle_num / total_cycles * 100)
    return f"[Ciclo {cycle_num}/{total_cycles} ({pct}%)]"


class ForcaMunicipalSettings(BaseSettings):
    """The login, the password and the address of the API, from the environment."""

    API_FORCA_MUNICIPAL__API_LOGIN: str | None = None
    API_FORCA_MUNICIPAL__API_PASSWORD: str | None = None
    API_FORCA_MUNICIPAL__API_URL: str | None = None
    API_FORCA_MUNICIPAL__USE_PROXY_URL: bool = False
    API_FORCA_MUNICIPAL__PROXY_URL: str | None = None


def add_date_columns(dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
    """Add `ano_particao`, `mes_particao` and `data_particao`, taken from a date column.

    Raise `ValueError` if the dataframe already has one of them.
    """
    names = ["ano_particao", "mes_particao", "data_particao"]
    if any(name in dataframe.columns for name in names):
        raise ValueError("The dataframe already has a column of the date partition")

    dates = pd.to_datetime(dataframe[column].astype(str), errors="coerce")
    dataframe["ano_particao"] = dates.dt.year.astype("Int64").astype(str).replace("<NA>", pd.NA)
    dataframe["mes_particao"] = dates.dt.month.astype("Int64").astype(str).replace("<NA>", pd.NA)
    dataframe["data_particao"] = dates.dt.date
    return dataframe

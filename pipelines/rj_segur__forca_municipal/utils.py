# -*- coding: utf-8 -*-
"""Utilitários gerais do pipeline rj_segur__forca_municipal."""

from datetime import date, datetime, timedelta
from typing import Generator, Iterator, Optional

from fastkml import Placemark
from iplanrio.pipelines_utils.logging import log

from pipelines.rj_segur__forca_municipal.constants import SP_TZ


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
        log(
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

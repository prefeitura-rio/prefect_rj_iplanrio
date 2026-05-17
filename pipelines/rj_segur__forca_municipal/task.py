# -*- coding: utf-8 -*-
"""Tasks do pipeline rj_segur__forca_municipal."""

import asyncio
import hashlib
import io
import json
import uuid
import warnings
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from fastkml import KML
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import parse_date_columns, to_partitions
from prefect import task
from prefect.context import TaskRunContext, get_run_context

from pipelines.rj_segur__forca_municipal.client import FMApi
from pipelines.rj_segur__forca_municipal.constants import (
    DEFAULT_CONCURRENCY,
    DEFAULT_PAGE_SIZE,
    DEFAULT_UNIT_CONCURRENCY,
    HASH_REGISTRY_LOOKBACK_DAYS,
    SP_TZ,
    TMP_BASE,
)
from pipelines.rj_segur__forca_municipal.hash_registry import (
    is_hash_registered,
    query_distinct_ids,
    register_hash,
)


@dataclass
class SaveResult:
    path: str
    content_hash: str
    partition_date: date
    now: datetime
    project_id: str
    row_count: int
    col_count: int
    parquet_size_mb: float


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date_range(
    data_inicio: str,
    data_fim: str,
    hora_inicio: str,
    hora_fim: str,
) -> tuple[str, str, str, str]:
    """
    Valida e normaliza os parâmetros de data/hora do intervalo.

    Retorna (data_inicio, data_fim, hora_inicio, hora_fim) normalizados via
    isoformat, garantindo strings canônicas independente do input.
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

    try:
        time.fromisoformat(hora_inicio)
    except ValueError:
        raise ValueError(f"hora_inicio inválida: '{hora_inicio}'. Use HH:MM:SS.")

    try:
        time.fromisoformat(hora_fim)
    except ValueError:
        raise ValueError(f"hora_fim inválida: '{hora_fim}'. Use HH:MM:SS.")

    return d_inicio.isoformat(), d_fim.isoformat(), hora_inicio, hora_fim


def _content_hash(df: pd.DataFrame) -> str:
    """Gera um hash MD5 truncado (12 chars) do conteúdo bruto do DataFrame."""
    raw = pd.util.hash_pandas_object(df, index=False).values.tobytes()  # type: ignore[attr-defined]
    return hashlib.md5(raw).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Extraction tasks
# ---------------------------------------------------------------------------


@task
def extract_simple_task(
    endpoint: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> pd.DataFrame:
    """Extrai todos os registros de um endpoint paginado."""

    async def _run() -> list:
        sem = asyncio.Semaphore(concurrency)
        async with FMApi() as api:
            return await api.get_paginated(endpoint, page_size=page_size, sem=sem)

    items = asyncio.run(_run())
    if not items:
        log(f"Nenhum dado retornado de {endpoint}", level="warning")
        return pd.DataFrame()
    df = pd.DataFrame(items)
    log(f"DataFrame: {df.shape[0]} linhas × {df.shape[1]} colunas")
    return df


@task
def extract_unit_positions_task(
    endpoint_unit_positions: str,
    project_id: str,
    dataset_id: str,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    hora_inicio: str = "00:00:00",
    hora_fim: str = "23:59:59",
    page_size: int = DEFAULT_PAGE_SIZE,
    concurrency: int = DEFAULT_CONCURRENCY,
    unit_concurrency: int = DEFAULT_UNIT_CONCURRENCY,
) -> pd.DataFrame:
    """Busca posições GPS de todas as unidades históricas para o intervalo especificado."""
    if data_inicio is None:
        data_inicio = (datetime.now(tz=SP_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
    if data_fim is None:
        data_fim = data_inicio

    data_inicio, data_fim, hora_inicio, hora_fim = _parse_date_range(
        data_inicio, data_fim, hora_inicio, hora_fim
    )

    log(f"Período: {data_inicio} {hora_inicio} → {data_fim} {hora_fim}")

    n_days = (date.fromisoformat(data_fim) - date.fromisoformat(data_inicio)).days
    lookback = HASH_REGISTRY_LOOKBACK_DAYS + n_days
    _tbl = f"`{project_id}.{dataset_id}_staging.unidades_historico`"
    unit_ids = query_distinct_ids(
        project_id=project_id,
        query=f"""
            SELECT DISTINCT UnitId
            FROM {_tbl}
            WHERE DATE(data_particao) >= DATE_SUB(
                DATE((SELECT MAX(data_particao) FROM {_tbl})),
                INTERVAL {lookback} DAY
            )
              AND UnitId IS NOT NULL
        """,
    )
    log(f"{len(unit_ids)} unidades encontradas no histórico BQ (MAX(data_particao) - {lookback}d)")

    if not unit_ids:
        log("Nenhuma unidade no histórico BQ.", level="warning")
        return pd.DataFrame()

    async def _run() -> list:
        page_sem = asyncio.Semaphore(concurrency)
        unit_sem = asyncio.Semaphore(unit_concurrency)
        total = len(unit_ids)
        completed = 0

        async with FMApi() as api:

            async def _fetch_unit(uid: str) -> list:
                nonlocal completed
                async with unit_sem:
                    positions = await api.get_paginated(
                        endpoint_unit_positions,
                        page_size=page_size,
                        extra_params={
                            "unitId": uid,
                            "dataInicio": data_inicio,
                            "dataFim": data_fim,
                            "horaInicio": hora_inicio,
                            "horaFim": hora_fim,
                        },
                        sem=page_sem,
                    )
                completed += 1
                log(f"Unidade {completed}/{total} ({uid}): {len(positions)} posições")
                return positions

            results = await asyncio.gather(*[_fetch_unit(uid) for uid in unit_ids])
            return [p for sub in results for p in sub]

    all_positions = asyncio.run(_run())
    if not all_positions:
        log("Nenhuma posição encontrada para o período.", level="warning")
        return pd.DataFrame()
    df = pd.DataFrame(all_positions)
    log(f"Total: {df.shape[0]} posições")
    return df


@task
def extract_qmd_details_task(
    endpoint_qmd_detalhes: str,
    project_id: str,
    dataset_id: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> pd.DataFrame:
    """Busca detalhes completos de todos os QMDs históricos (missões, serviços, execuções)."""

    _tbl = f"`{project_id}.{dataset_id}_staging.qmd`"
    qmd_ids = query_distinct_ids(
        project_id=project_id,
        query=f"""
            SELECT DISTINCT Id
            FROM {_tbl}
            WHERE DATE(data_particao) >= DATE_SUB(
                DATE((SELECT MAX(data_particao) FROM {_tbl})),
                INTERVAL {HASH_REGISTRY_LOOKBACK_DAYS} DAY
            )
              AND Id IS NOT NULL
        """,
    )
    log(f"{len(qmd_ids)} QMDs encontrados no histórico BQ (MAX(data_particao) - {HASH_REGISTRY_LOOKBACK_DAYS}d) — buscando detalhes...")

    if not qmd_ids:
        log("Nenhum QMD no histórico BQ.", level="warning")
        return pd.DataFrame()

    async def _run() -> list:
        sem = asyncio.Semaphore(concurrency)
        async with FMApi() as api:

            total = len(qmd_ids)
            completed = 0

            async def _fetch_one(qmd_id: str) -> dict:
                nonlocal completed
                body = await api.get_json(endpoint_qmd_detalhes.format(id=qmd_id))
                completed += 1
                log(f"QMD {completed}/{total} ({qmd_id}) — detalhes obtidos")
                return {
                    k: (
                        json.dumps(v, ensure_ascii=False)
                        if isinstance(v, (list, dict))
                        else v
                    )
                    for k, v in body.items()
                }

            return list(
                await asyncio.gather(*[_fetch_one(qmd_id) for qmd_id in qmd_ids])
            )

    rows = asyncio.run(_run())
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    log(f"DataFrame: {df.shape[0]} linhas × {df.shape[1]} colunas")
    return df


@task
def extract_qmd_kml_task(
    endpoint_qmd_kml: str,
    project_id: str,
    dataset_id: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> pd.DataFrame:
    """Busca KMLs de todos os QMDs históricos e extrai geometrias e metadados."""

    _tbl = f"`{project_id}.{dataset_id}_staging.qmd`"
    qmd_ids = query_distinct_ids(
        project_id=project_id,
        query=f"""
            SELECT DISTINCT Id
            FROM {_tbl}
            WHERE DATE(data_particao) >= DATE_SUB(
                DATE((SELECT MAX(data_particao) FROM {_tbl})),
                INTERVAL {HASH_REGISTRY_LOOKBACK_DAYS} DAY
            )
              AND Id IS NOT NULL
        """,
    )
    log(f"{len(qmd_ids)} QMDs encontrados no histórico BQ (MAX(data_particao) - {HASH_REGISTRY_LOOKBACK_DAYS}d) — buscando KMLs...")

    if not qmd_ids:
        log("Nenhum QMD no histórico BQ.", level="warning")
        return pd.DataFrame()

    async def _run() -> list:
        sem = asyncio.Semaphore(concurrency)
        total = len(qmd_ids)
        completed = 0

        async with FMApi() as api:

            async def _fetch_kml(qmd_id: str) -> list:
                nonlocal completed
                try:
                    kml_text = await api.get_text(endpoint_qmd_kml.format(id=qmd_id))
                except Exception as exc:
                    completed += 1
                    log(f"QMD {qmd_id}: KML não disponível ({exc})", level="warning")
                    return []

                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        k = KML.parse(io.StringIO(kml_text), strict=False)
                except Exception as exc:
                    completed += 1
                    log(f"QMD {qmd_id}: erro ao parsear KML ({exc})", level="warning")
                    return []

                doc = next(iter(k.features), None)
                if not doc:
                    completed += 1
                    log(f"QMD {qmd_id}: KML sem Document.", level="warning")
                    return []

                rows = []
                for folder in getattr(doc, "features", []):
                    folder_name = getattr(folder, "name", None)
                    for placemark in getattr(folder, "features", []):
                        geom = getattr(placemark, "geometry", None)
                        ed = getattr(placemark, "extended_data", None)
                        ed_dict: dict = {}
                        if ed:
                            for elem in getattr(ed, "elements", []):
                                key = getattr(elem, "name", None)
                                if key:
                                    ed_dict[key] = getattr(elem, "value", None)
                        rows.append(
                            {
                                "qmd_id": qmd_id,
                                "kml_folder": folder_name,
                                "name": getattr(placemark, "name", None),
                                "description": getattr(placemark, "description", None),
                                "geometry_wkt": geom.wkt if geom else None,
                                "geometry_type": geom.geom_type if geom else None,
                                "extended_data": (
                                    json.dumps(ed_dict, ensure_ascii=False)
                                    if ed_dict
                                    else None
                                ),
                            }
                        )

                completed += 1
                log(f"KML {completed}/{total} ({qmd_id}): {len(rows)} placemarks")
                return rows

            results = await asyncio.gather(*[_fetch_kml(qmd_id) for qmd_id in qmd_ids])
            return [r for sub in results for r in sub]

    rows = asyncio.run(_run())
    if not rows:
        log("Nenhum KML retornado.", level="warning")
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    log(f"DataFrame: {df.shape[0]} linhas × {df.shape[1]} colunas")
    return df


# ---------------------------------------------------------------------------
# Transform / save task
# ---------------------------------------------------------------------------


@task
def save_partitions_task(
    df: pd.DataFrame,
    table_id: str,
    project_id: str,
    dataset_id: str = "brutos_forca_municipal",
) -> Optional[SaveResult]:
    """
    Adiciona updated_at e id_hash ao DataFrame, salva em partições Hive (parquet).

    Fluxo:
      1. Calcula hash MD5 do conteúdo bruto da API (antes de qualquer coluna extra).
      2. Consulta o hash registry no BQ — se o hash já existir em qualquer partição,
         encerra sem escrever nada (evita duplicatas cross-partition).
      3. Adiciona updated_at (datetime America/São Paulo) e id_hash ao DataFrame.
      4. Particiona por data de updated_at e salva como parquet com id_hash como sufixo.

    O registro do hash no BQ é responsabilidade do flow, após upload bem-sucedido.
    Retorna SaveResult com path, hash e datas, ou None se o hash já estava registrado.
    """
    df.sort_values(by=sorted(df.columns), na_position="first", inplace=True)
    df.reset_index(drop=True, inplace=True)

    content_hash = _content_hash(df)

    if is_hash_registered(content_hash, dataset_id, table_id, project_id):
        log(
            f"Hash {content_hash} já registrado para {project_id}.{dataset_id}_staging.{table_id}"
            " — conteúdo não mudou, skip."
        )
        return None

    # Captura metadados antes de adicionar colunas extras
    row_count = len(df)
    col_count = len(df.columns)

    now = datetime.now(tz=SP_TZ).replace(tzinfo=None)
    partition_date = now.date()

    df["id_hash"] = content_hash
    df["updated_at"] = now

    savepath = f"{TMP_BASE}/{table_id}/{uuid.uuid4()}"
    Path(savepath).mkdir(parents=True, exist_ok=True)

    df, partition_cols = parse_date_columns(df, "updated_at")
    df = df.astype(str)

    to_partitions(
        data=df,
        partition_columns=partition_cols,
        savepath=savepath,
        data_type="parquet",
        suffix=content_hash,
    )

    parquet_size_mb = round(
        sum(f.stat().st_size for f in Path(savepath).rglob("*.parquet"))
        / (1024 * 1024),
        4,
    )

    log(
        f"Partições salvas: {savepath} (hash={content_hash}, {row_count} linhas, {parquet_size_mb} MB)"
    )
    return SaveResult(
        path=savepath,
        content_hash=content_hash,
        partition_date=partition_date,
        now=now,
        project_id=project_id,
        row_count=row_count,
        col_count=col_count,
        parquet_size_mb=parquet_size_mb,
    )


@task
def register_hash_task(
    result: SaveResult,
    dataset_id: str,
    table_id: str,
) -> None:
    """Registra o hash no BQ após upload bem-sucedido."""

    context = get_run_context()
    flow_run_id = None
    if isinstance(context, TaskRunContext):
        flow_run_id = str(context.task_run.flow_run_id)
    register_hash(
        content_hash=result.content_hash,
        dataset_id=dataset_id,
        table_id=table_id,
        project_id=result.project_id,
        partition_date=result.partition_date,
        now=result.now,
        flow_run_id=flow_run_id,
        row_count=result.row_count,
        col_count=result.col_count,
        parquet_size_mb=result.parquet_size_mb,
    )

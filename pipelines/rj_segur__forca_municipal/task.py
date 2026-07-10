# -*- coding: utf-8 -*-
"""Tasks do pipeline rj_segur__forca_municipal."""

import asyncio
import io
import json
import math
import shutil
import uuid
import warnings
from collections.abc import AsyncGenerator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd
from fastkml import KML
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.pandas import parse_date_columns, to_partitions
from prefect import task

from pipelines.rj_segur__forca_municipal.bq import (
    delete_gcs_partition,
    query_distinct_ids,
)
from pipelines.rj_segur__forca_municipal.client import FMApi
from pipelines.rj_segur__forca_municipal.constants import (
    DEFAULT_CONCURRENCY,
    DEFAULT_PAGE_SIZE,
    DEFAULT_QMD_ID_CONCURRENCY,
    DEFAULT_UNIT_ID_CONCURRENCY,
    MAX_ITEM_ERROR_RATE,
    MAX_ROWS_PER_CYCLE,
    SERIES_TABLE_IDS,
    SINGLE_PAGE_ENDPOINTS,
    SP_TZ,
    TMP_BASE,
)
from pipelines.rj_segur__forca_municipal.utils import (
    _add_id_hash,
    _cycle_prefix,
    _date_range,
    _iter_placemarks,
    _parse_date_range,
)

# ---------------------------------------------------------------------------
# Helpers privados
# ---------------------------------------------------------------------------


def _process_cycle(
    data: list[dict],
    table_id: str,
    dataset_id: str,
    dump_mode: str,
    updated_at: datetime,
    partition_col: str = "updated_at",
    cycle_num: Optional[int] = None,
    total_cycles: Optional[int] = None,
) -> bool:
    """
    Processa um ciclo de dados: monta DataFrame → id_hash → updated_at → particiona → upload GCS.

    id_hash: hash de 16 dígitos hex por linha, calculado sobre as colunas originais
             da API (antes de adicionar updated_at), para ser um id estável de conteúdo.
    updated_at: timestamp fixo do início do run — idêntico para todos os ciclos.
    partition_col: coluna usada para particionamento — "updated_at" (padrão) ou "Date"
                   (unit_positions, para manter partição por data GPS real).

    No cycle_num == 1 de tabelas snapshot, lê as partições do próprio DataFrame
    (após parse_date_columns) e deleta cada uma no GCS antes de subir — garantindo
    que o delete só ocorre com dados locais prontos. Tabelas série nunca deletam.

    Retorna True se houve dados para enviar, False se data estava vazio.
    """
    if not data:
        return False

    data = _add_id_hash(data=data)
    df = pd.DataFrame(data=data, dtype=object)
    row_count, col_count = len(df), len(df.columns)

    # updated_at: mesmo timestamp para todos os ciclos do run
    df["updated_at"] = updated_at

    df, partition_cols = parse_date_columns(
        dataframe=df, partition_date_column=partition_col
    )
    df = df.astype(str)

    savepath = f"{TMP_BASE}/{table_id}/{uuid.uuid4()}"
    Path(savepath).mkdir(parents=True, exist_ok=True)

    # No primeiro ciclo de tabelas snapshot, deleta a partição GCS depois de ter
    # dados locais prontos e antes de subir — evita delete sem dados e garante
    # que ciclos subsequentes não apaguem o que o ciclo anterior já subiu.
    suffix = (
        f"{updated_at.strftime('%H%M%S')}_{cycle_num:04d}"
        if table_id in SERIES_TABLE_IDS
        else f"{cycle_num:04d}"
    )

    to_partitions(
        data=df,
        partition_columns=partition_cols,
        savepath=savepath,
        data_type="parquet",
        suffix=suffix,
    )

    if cycle_num == 1 and table_id not in SERIES_TABLE_IDS:
        delete_gcs_partition(
            dataset_id=dataset_id,
            table_id=table_id,
            partition_cols=partition_cols,
            unique_partitions=pd.DataFrame(df[partition_cols]).drop_duplicates(),
        )

    prefix = _cycle_prefix(cycle_num=cycle_num, total_cycles=total_cycles)
    log(f"{prefix} {row_count} linhas, {col_count} colunas → enviando...")

    create_table_and_upload_to_gcs(
        data_path=savepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        source_format="parquet",
        only_staging_dataset=True,
    )

    shutil.rmtree(savepath, ignore_errors=True)
    log(f"{prefix} {row_count} linhas enviadas — concluído.")
    return True


async def _flush_buffer(
    batches: AsyncGenerator[list[dict], None],
    table_id: str,
    dataset_id: str,
    dump_mode: str,
    updated_at: datetime,
    partition_col: str = "updated_at",
    total_cycles: Optional[int] = None,
) -> int:
    """
    Consome um async generator de batches de rows, fazendo upload a cada MAX_ROWS_PER_CYCLE.

    Acumula rows num buffer e dispara _process_cycle (com while) assim que o buffer
    atinge MAX_ROWS_PER_CYCLE — garantindo que qualquer overshoot é drenado antes
    do próximo batch. O flush final cobre rows restantes após o último batch.

    Retorna o total de rows processadas.
    """
    buffer: list[dict] = []
    cycle_num = 0
    total = 0

    async for batch in batches:
        buffer.extend(batch)
        total += len(batch)

        while len(buffer) >= MAX_ROWS_PER_CYCLE:
            cycle_num += 1
            chunk = buffer[:MAX_ROWS_PER_CYCLE]
            buffer = buffer[MAX_ROWS_PER_CYCLE:]
            _process_cycle(
                data=chunk,
                table_id=table_id,
                dataset_id=dataset_id,
                dump_mode=dump_mode,
                updated_at=updated_at,
                partition_col=partition_col,
                cycle_num=cycle_num,
                total_cycles=total_cycles,
            )

    if buffer:
        cycle_num += 1
        _process_cycle(
            data=buffer,
            table_id=table_id,
            dataset_id=dataset_id,
            dump_mode=dump_mode,
            updated_at=updated_at,
            partition_col=partition_col,
            cycle_num=cycle_num,
            total_cycles=total_cycles,
        )

    return total


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def run_standard_endpoint_task(
    endpoint: str,
    table_id: str,
    dataset_id: str,
    project_id: str,
    dump_mode: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> None:
    """Extrai, processa e envia todos os dados de um endpoint paginado em ciclos.

    Tabelas snapshot: deleta a partição do dia antes de escrever — idempotente por re-run.
    Tabelas série (SERIES_TABLE_IDS): acumula snapshots intradiários sem pre-delete;
    o suffix inclui o timestamp do run para garantir unicidade entre execuções.
    """
    is_series = table_id in SERIES_TABLE_IDS
    now = datetime.now(tz=SP_TZ).replace(tzinfo=None)

    if is_series:
        log(
            f"Modo série — suffix base: {now.strftime('%H%M%S')} (sem pre-delete de partição)."
        )

    async def _run() -> None:
        sem = asyncio.Semaphore(concurrency)
        async with FMApi() as api:
            total_pages, first_items = await api.get_first(
                path=endpoint, page_size=page_size, sem=sem
            )

            if not first_items and total_pages <= 1:
                log(f"Nenhum dado retornado de {endpoint}", level="warning")
                return

            if total_pages > 1 and table_id in SINGLE_PAGE_ENDPOINTS:
                log(
                    f"{endpoint}: {total_pages} página(s) retornadas"
                    f" — paginação não documentada para '{table_id}', validar comportamento.",
                    level="warning",
                )

            pages_per_cycle = math.ceil(MAX_ROWS_PER_CYCLE / page_size)
            total_cycles = math.ceil(total_pages / pages_per_cycle)
            log(f"{total_pages} página(s) → {total_cycles} ciclo(s)")

            async def _batches() -> AsyncGenerator[list[dict], None]:
                yield first_items
                for start in range(2, total_pages + 1, pages_per_cycle):
                    end = min(start + pages_per_cycle - 1, total_pages)
                    yield await api.get_pages_range(
                        path=endpoint,
                        page_start=start,
                        page_end=end,
                        page_size=page_size,
                        sem=sem,
                    )

            await _flush_buffer(
                batches=_batches(),
                table_id=table_id,
                dataset_id=dataset_id,
                dump_mode=dump_mode,
                updated_at=now,
                total_cycles=total_cycles,
            )

    asyncio.run(_run())


@task
def run_unit_positions_task(
    endpoint: str,
    table_id: str,
    project_id: str,
    dataset_id: str,
    dump_mode: str,
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    page_size: int = DEFAULT_PAGE_SIZE,
    concurrency: int = DEFAULT_CONCURRENCY,
    unit_id_concurrency: int = DEFAULT_UNIT_ID_CONCURRENCY,
) -> None:
    """
    Busca posições GPS por unidade, dia a dia.

    O loop externo é sequencial por data — se a pipeline quebrar num dia,
    os dias anteriores já estão gravados e é possível retomar a partir do dia falho.

    Dentro de cada dia, todos os unit_ids são buscados em paralelo (bounded por
    unit_id_concurrency), sem ordem definida entre eles. A partição do dia é deletada
    antes de qualquer escrita, garantindo idempotência por re-run.

    IDs de unidade são obtidos via query na última partição de unidades_historico (snapshot).
    """
    if data_inicio is None:
        data_inicio = (datetime.now(tz=SP_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
    if data_fim is None:
        data_fim = data_inicio

    data_inicio, data_fim = _parse_date_range(
        data_inicio=data_inicio,
        data_fim=data_fim,
    )
    log(f"Período: {data_inicio} → {data_fim}")

    now = datetime.now(tz=SP_TZ).replace(tzinfo=None)

    _tbl = f"`{project_id}.{dataset_id}_staging.unidades_historico`"
    unit_ids = query_distinct_ids(
        project_id=project_id,
        query=f"""
            SELECT DISTINCT UnitId
            FROM {_tbl}
            WHERE data_particao = (SELECT MAX(data_particao) FROM {_tbl})
              AND UnitId IS NOT NULL
            ORDER BY UnitId
        """,
    )
    log(f"{len(unit_ids)} unidades encontradas na última partição do histórico BQ")

    if not unit_ids:
        log("Nenhuma unidade no histórico BQ.", level="warning")
        return

    n_units = len(unit_ids)
    n_days = (date.fromisoformat(data_fim) - date.fromisoformat(data_inicio)).days + 1

    total_unit_days = n_units * n_days

    async def _run() -> None:
        http_sem = asyncio.Semaphore(concurrency)
        errors = 0

        async with FMApi() as api:

            async def _fetch_unit_day(uid: str, day: str) -> list[dict]:
                extra = {
                    "unitId": uid.lower(),
                    "dataInicio": day,
                    "dataFim": day,
                    "horaInicio": "00:00:00",
                    "horaFim": "23:59:59",
                }
                total_pages, first_items = await api.get_first(
                    path=endpoint,
                    page_size=page_size,
                    extra_params=extra,
                    sem=http_sem,
                )
                rest = (
                    await api.get_pages_range(
                        path=endpoint,
                        page_start=2,
                        page_end=total_pages,
                        page_size=page_size,
                        extra_params=extra,
                        sem=http_sem,
                    )
                    if total_pages >= 2
                    else []
                )
                rows = first_items + rest
                for row in rows:
                    row["data_coleta"] = day
                return rows

            for day_num, day in enumerate(
                _date_range(data_inicio=data_inicio, data_fim=data_fim), start=1
            ):
                d_pct = round(day_num / n_days * 100)
                n_batches = math.ceil(n_units / unit_id_concurrency)
                log(
                    f"[Dia {day_num}/{n_days} ({d_pct}%)] {day}"
                    f" — {n_units} unidades em {n_batches} lote(s) de {unit_id_concurrency}..."
                )

                async def _day_batches(day: str) -> AsyncGenerator[list[dict], None]:
                    nonlocal errors
                    for batch_start in range(0, n_units, unit_id_concurrency):
                        batch_ids = unit_ids[
                            batch_start : batch_start + unit_id_concurrency
                        ]
                        outcomes = await asyncio.gather(
                            *[_fetch_unit_day(uid=uid, day=day) for uid in batch_ids],
                            return_exceptions=True,
                        )
                        batch: list[dict] = []
                        for uid, outcome in zip(batch_ids, outcomes):
                            if isinstance(outcome, BaseException):
                                errors += 1
                                log(
                                    f"  unit {uid} {day}: {type(outcome).__name__} — ignorado",
                                    level="warning",
                                )
                            else:
                                batch.extend(outcome)
                        if batch:
                            yield batch

                total_collected = await _flush_buffer(
                    batches=_day_batches(day=day),
                    table_id=table_id,
                    dataset_id=dataset_id,
                    dump_mode=dump_mode,
                    updated_at=now,
                    partition_col="data_coleta",
                )

                if not total_collected:
                    log(f"  Nenhuma posição para {day} — pulando.", level="warning")
                else:
                    log(f"  {total_collected} posições coletadas.")

        if errors and errors / total_unit_days > MAX_ITEM_ERROR_RATE:
            raise RuntimeError(
                f"unit_positions: {errors}/{total_unit_days} unidade-dias falharam"
                f" ({errors / total_unit_days:.1%} > limite {MAX_ITEM_ERROR_RATE:.1%})"
            )

    asyncio.run(_run())


@task
def run_qmd_details_task(
    endpoint: str,
    table_id: str,
    project_id: str,
    dataset_id: str,
    dump_mode: str,
    concurrency: int = DEFAULT_CONCURRENCY,
    id_concurrency: int = DEFAULT_QMD_ID_CONCURRENCY,
) -> None:
    """
    Busca detalhes completos dos QMDs em lotes de id_concurrency IDs.

    IDs são obtidos via query na última partição da staging de qmd (snapshot mais recente).
    A partição do dia é deletada antes de qualquer escrita — idempotente por re-run.
    O buffer acumula resultados e dispara um ciclo (upload) a cada MAX_ROWS_PER_CYCLE linhas.
    """
    now = datetime.now(tz=SP_TZ).replace(tzinfo=None)

    _tbl = f"`{project_id}.{dataset_id}_staging.qmd`"
    qmd_ids = query_distinct_ids(
        project_id=project_id,
        query=f"""
            SELECT DISTINCT Id
            FROM {_tbl}
            WHERE data_particao = (SELECT MAX(data_particao) FROM {_tbl})
              AND Id IS NOT NULL
            ORDER BY Id
        """,
    )
    log(f"{len(qmd_ids)} QMDs encontrados na última partição do catálogo BQ")

    if not qmd_ids:
        log("Nenhum QMD no catálogo BQ.", level="warning")
        return

    total = len(qmd_ids)

    async def _run() -> None:
        errors = 0
        sem = asyncio.Semaphore(concurrency)
        async with FMApi() as api:

            async def _fetch_one(qmd_id: str) -> Optional[dict]:
                nonlocal errors
                async with sem:
                    try:
                        body = await api.get_json(path=endpoint.format(id=qmd_id))
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code in (404, 410):
                            log(
                                f"  QMD {qmd_id}: HTTP {exc.response.status_code}"
                                f" — não encontrado, ignorado",
                                level="warning",
                            )
                            return None
                        errors += 1
                        log(
                            f"  QMD {qmd_id}: HTTP {exc.response.status_code} — erro",
                            level="warning",
                        )
                        return None
                    except Exception as exc:
                        errors += 1
                        log(
                            f"  QMD {qmd_id}: {type(exc).__name__} — erro",
                            level="warning",
                        )
                        return None
                    return {
                        k: (
                            json.dumps(v, ensure_ascii=False)
                            if isinstance(v, (list, dict))
                            else v
                        )
                        for k, v in body.items()
                    }

            async def _batches() -> AsyncGenerator[list[dict], None]:
                for batch_start in range(0, total, id_concurrency):
                    batch = qmd_ids[batch_start : batch_start + id_concurrency]
                    done = batch_start + len(batch)
                    pct = round(done / total * 100)
                    results: list[Optional[dict]] = list(
                        await asyncio.gather(*[_fetch_one(qid) for qid in batch])
                    )
                    rows = [r for r in results if r is not None]
                    log(
                        f"  IDs {batch_start + 1}-{done}/{total} ({pct}%)"
                        f" — {len(rows)} detalhes"
                    )
                    if rows:
                        yield rows

            await _flush_buffer(
                batches=_batches(),
                table_id=table_id,
                dataset_id=dataset_id,
                dump_mode=dump_mode,
                updated_at=now,
            )

        if errors and errors / total > MAX_ITEM_ERROR_RATE:
            raise RuntimeError(
                f"qmd_detalhes: {errors}/{total} IDs falharam"
                f" ({errors / total:.1%} > limite {MAX_ITEM_ERROR_RATE:.1%})"
            )

    asyncio.run(_run())


@task
def run_qmd_kml_task(
    endpoint: str,
    table_id: str,
    project_id: str,
    dataset_id: str,
    dump_mode: str,
    concurrency: int = DEFAULT_CONCURRENCY,
    id_concurrency: int = DEFAULT_QMD_ID_CONCURRENCY,
) -> None:
    """
    Busca KMLs dos QMDs em lotes de id_concurrency IDs.

    IDs são obtidos via query na última partição da staging de qmd (snapshot mais recente).
    A partição do dia é deletada antes de qualquer escrita — idempotente por re-run.
    O buffer acumula placemarks e dispara um ciclo (upload) a cada MAX_ROWS_PER_CYCLE linhas.
    """
    now = datetime.now(tz=SP_TZ).replace(tzinfo=None)

    _tbl = f"`{project_id}.{dataset_id}_staging.qmd`"
    qmd_ids = query_distinct_ids(
        project_id=project_id,
        query=f"""
            SELECT DISTINCT Id
            FROM {_tbl}
            WHERE data_particao = (SELECT MAX(data_particao) FROM {_tbl})
              AND Id IS NOT NULL
            ORDER BY Id
        """,
    )
    log(f"{len(qmd_ids)} QMDs encontrados na última partição do catálogo BQ")

    if not qmd_ids:
        log("Nenhum QMD no catálogo BQ.", level="warning")
        return

    total = len(qmd_ids)

    async def _run() -> None:
        attempted = 0
        completed = 0
        sem = asyncio.Semaphore(concurrency)

        async with FMApi() as api:

            async def _fetch_kml(qmd_id: str) -> list[dict]:
                nonlocal attempted, completed
                async with sem:
                    attempted += 1
                    pct = round(attempted / total * 100)

                    try:
                        kml_text = await api.get_text(path=endpoint.format(id=qmd_id))
                    except Exception as exc:
                        log(
                            f"  KML {attempted}/{total} ({pct}%) — {qmd_id}:"
                            f" não disponível ({exc})",
                            level="warning",
                        )
                        return []

                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            k = KML.parse(io.StringIO(kml_text), strict=False)
                    except Exception as exc:
                        log(
                            f"  KML {attempted}/{total} ({pct}%) — {qmd_id}:"
                            f" erro ao parsear ({exc})",
                            level="warning",
                        )
                        return []

                    doc = next(iter(k.features), None)
                    if not doc:
                        log(
                            f"  KML {attempted}/{total} ({pct}%) — {qmd_id}: sem Document.",
                            level="warning",
                        )
                        return []

                    rows = []
                    for placemark, folder_name in _iter_placemarks(node=doc):
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
                    log(
                        f"  KML {attempted}/{total} ({pct}%) — {qmd_id}:"
                        f" {len(rows)} placemark(s) [ok: {completed}]"
                    )
                return rows

            async def _batches() -> AsyncGenerator[list[dict], None]:
                for batch_start in range(0, total, id_concurrency):
                    batch = qmd_ids[batch_start : batch_start + id_concurrency]
                    done = batch_start + len(batch)
                    pct = round(done / total * 100)
                    results = await asyncio.gather(*[_fetch_kml(qid) for qid in batch])
                    rows = [r for sub in results for r in sub]
                    log(
                        f"  IDs {batch_start + 1}-{done}/{total} ({pct}%)"
                        f" — {len(rows)} placemark(s)"
                    )
                    if rows:
                        yield rows

            await _flush_buffer(
                batches=_batches(),
                table_id=table_id,
                dataset_id=dataset_id,
                dump_mode=dump_mode,
                updated_at=now,
            )

        errors = attempted - completed
        if errors and errors / total > MAX_ITEM_ERROR_RATE:
            raise RuntimeError(
                f"qmd_kml: {errors}/{total} IDs sem rows"
                f" ({errors / total:.1%} > limite {MAX_ITEM_ERROR_RATE:.1%})"
            )

    asyncio.run(_run())

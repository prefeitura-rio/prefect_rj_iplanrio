# -*- coding: utf-8 -*-
"""
Flow para extrair dados da API CIVITAS/CORIO (Força Municipal) e enviar para BigQuery.

Sistema: HxGN OnCall - Gestão de Ocorrências e Unidades da Guarda Municipal RJ
"""

from typing import Optional

import basedosdados as bd
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_segur__forca_municipal.constants import (
    DEFAULT_CONCURRENCY,
    DEFAULT_PAGE_SIZE,
    ENDPOINT_MAP,
)
from pipelines.rj_segur__forca_municipal.task import (
    SaveResult,
    extract_qmd_details_task,
    extract_qmd_kml_task,
    extract_simple_task,
    extract_unit_positions_task,
    register_hash_task,
    save_partitions_task,
)

ALL_TABLE_IDS = set(ENDPOINT_MAP)


@flow(log_prints=True)
def rj_segur__forca_municipal(
    table_id: str = "table_id",
    dataset_id: str = "brutos_forca_municipal",
    dump_mode: str = "append",
    biglake_table: bool = True,
    page_size: int = DEFAULT_PAGE_SIZE,
    concurrency: int = DEFAULT_CONCURRENCY,
    # Parâmetros de janela temporal — usados apenas por unit_positions
    # Se None, usa a data atual (America/São Paulo). Passe valores explícitos para backfill.
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
    hora_inicio: str = "00:00:00",
    hora_fim: str = "23:59:59",
):
    """
    Extrai dados de um endpoint da API HxGN OnCall (Força Municipal) e carrega no BigQuery.

    Args:
        table_id: Tabela alvo. Opções: unidades_ativas, unidades_historico,
                  ocorrencias_ativas, ocorrencias_historico, ocorrencias_ativas_v2,
                  qmd, qmd_servicos, qmd_missoes, qmd_plano, qmd_ativos,
                  unit_positions, qmd_detalhes, qmd_kml.
        dataset_id: Dataset do BigQuery.
        dump_mode: "append" para acumular dados, "overwrite" para substituir.
        biglake_table: Se deve criar tabela como BigLake.
        page_size: Registros por página (max 500).
        concurrency: Número máximo de requisições HTTP simultâneas.
        data_inicio: Data de início para unit_positions (YYYY-MM-DD). Default: D-1.
        data_fim: Data de fim para unit_positions (YYYY-MM-DD). Default: data_inicio.
        hora_inicio: Hora de início para unit_positions (HH:MM:SS). Default: 00:00:00.
        hora_fim: Hora de fim para unit_positions (HH:MM:SS). Default: 23:59:59.
    """
    rename_current_flow_run_task(new_name=f"{dataset_id}.{table_id}")
    inject_bd_credentials_task(environment="prod")
    project_id = bd.Base()._get_project_id("prod")

    if table_id not in ALL_TABLE_IDS:
        raise ValueError(
            f"table_id '{table_id}' inválido. Opções: {sorted(ALL_TABLE_IDS)}"
        )

    log(f"Configurado para {concurrency} requisições simultâneas.")
    if table_id == "unit_positions":
        df = extract_unit_positions_task(
            endpoint_unit_positions=ENDPOINT_MAP[table_id],
            project_id=project_id,
            dataset_id=dataset_id,
            data_inicio=data_inicio,
            data_fim=data_fim,
            hora_inicio=hora_inicio,
            hora_fim=hora_fim,
            page_size=page_size,
            concurrency=concurrency,
        )
    elif table_id == "qmd_detalhes":
        df = extract_qmd_details_task(
            endpoint_qmd_detalhes=ENDPOINT_MAP[table_id],
            project_id=project_id,
            dataset_id=dataset_id,
            page_size=page_size,
            concurrency=concurrency,
        )
    elif table_id == "qmd_kml":
        df = extract_qmd_kml_task(
            endpoint_qmd_kml=ENDPOINT_MAP[table_id],
            project_id=project_id,
            dataset_id=dataset_id,
            page_size=page_size,
            concurrency=concurrency,
        )
    else:
        df = extract_simple_task(
            endpoint=ENDPOINT_MAP[table_id],
            page_size=page_size,
            concurrency=concurrency,
        )

    if df.empty:
        log(f"Nenhum dado retornado para {table_id}. Encerrando flow.")
        return

    result: Optional[SaveResult] = save_partitions_task(
        df=df, table_id=table_id, dataset_id=dataset_id, project_id=project_id
    )

    if result is None:
        log(
            f"Conteúdo de {dataset_id}_staging.{table_id} não mudou desde a última extração — sem upload."
        )
        return

    create_table_and_upload_to_gcs_task(
        data_path=result.path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
        source_format="parquet",
        only_staging_dataset=True,
    )

    register_hash_task(
        result=result,
        dataset_id=dataset_id,
        table_id=table_id,
    )

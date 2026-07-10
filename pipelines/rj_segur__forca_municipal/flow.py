# -*- coding: utf-8 -*-
"""
Flow para extrair dados da API CIVITAS/CORIO (Força Municipal) e enviar para BigQuery.

Sistema: HxGN OnCall - Gestão de Ocorrências e Unidades da Guarda Municipal RJ.
"""

from typing import Optional

import basedosdados as bd
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_segur__forca_municipal.constants import (
    DEFAULT_CONCURRENCY,
    DEFAULT_PAGE_SIZE,
    DEFAULT_QMD_ID_CONCURRENCY,
    DEFAULT_UNIT_ID_CONCURRENCY,
    TABLE_CONFIG,
)
from pipelines.rj_segur__forca_municipal.task import (
    run_qmd_details_task,
    run_qmd_kml_task,
    run_standard_endpoint_task,
    run_unit_positions_task,
)

ALL_TABLE_IDS = set(TABLE_CONFIG)


@flow(log_prints=True)
def rj_segur__forca_municipal(
    table_id: str = "table_id",
    dataset_id: str = "brutos_forca_municipal",
    dump_mode: str = "append",
    page_size: int = DEFAULT_PAGE_SIZE,
    concurrency: int = DEFAULT_CONCURRENCY,
    qmd_id_concurrency: int = DEFAULT_QMD_ID_CONCURRENCY,
    unit_id_concurrency: int = DEFAULT_UNIT_ID_CONCURRENCY,
    # Parâmetros de janela temporal — usados apenas por unit_positions
    # Se None, usa D-1 (America/São Paulo). Passe valores explícitos para backfill.
    data_inicio: Optional[str] = None,
    data_fim: Optional[str] = None,
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
        page_size: Registros por página (max 500).
        concurrency: Número máximo de requisições HTTP simultâneas.
        qmd_id_concurrency: IDs QMD buscados em paralelo (qmd_detalhes e qmd_kml).
        unit_id_concurrency: IDs de unidade buscados em paralelo por dia (unit_positions).
        data_inicio: Data de início para unit_positions (YYYY-MM-DD). Default: D-1.
        data_fim: Data de fim para unit_positions (YYYY-MM-DD). Default: data_inicio.
    """
    rename_current_flow_run_task(new_name=f"{dataset_id}.{table_id}")
    inject_bd_credentials_task(environment="prod")
    project_id = bd.Base()._get_project_id("prod")

    if table_id not in ALL_TABLE_IDS:
        raise ValueError(
            f"table_id '{table_id}' inválido. Opções: {sorted(ALL_TABLE_IDS)}"
        )

    if table_id == "unit_positions":
        run_unit_positions_task(
            endpoint=TABLE_CONFIG[table_id],
            table_id=table_id,
            project_id=project_id,
            dataset_id=dataset_id,
            dump_mode=dump_mode,
            page_size=page_size,
            concurrency=concurrency,
            unit_id_concurrency=unit_id_concurrency,
            data_inicio=data_inicio,
            data_fim=data_fim,
        )
    elif table_id == "qmd_detalhes":
        run_qmd_details_task(
            endpoint=TABLE_CONFIG[table_id],
            table_id=table_id,
            project_id=project_id,
            dataset_id=dataset_id,
            dump_mode=dump_mode,
            concurrency=concurrency,
            id_concurrency=qmd_id_concurrency,
        )
    elif table_id == "qmd_kml":
        run_qmd_kml_task(
            endpoint=TABLE_CONFIG[table_id],
            table_id=table_id,
            project_id=project_id,
            dataset_id=dataset_id,
            dump_mode=dump_mode,
            concurrency=concurrency,
            id_concurrency=qmd_id_concurrency,
        )
    else:
        run_standard_endpoint_task(
            endpoint=TABLE_CONFIG[table_id],
            table_id=table_id,
            project_id=project_id,
            dataset_id=dataset_id,
            dump_mode=dump_mode,
            page_size=page_size,
            concurrency=concurrency,
        )

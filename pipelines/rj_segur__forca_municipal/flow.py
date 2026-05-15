# -*- coding: utf-8 -*-
"""
Flow para extrair dados da API CIVITAS/CORIO (Força Municipal) e enviar para BigQuery

Sistema: HxGN OnCall - Gestão de Ocorrências e Unidades da Guarda Municipal RJ
"""

from typing import Optional

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_segur__forca_municipal.task import (
    fetch_endpoint_task,
    fetch_qmd_details_task,
    fetch_qmd_kml_task,
    fetch_unit_positions_task,
    get_authenticated_api_task,
    save_partitions_task,
)

# Endpoints paginados simples: table_id → path
ENDPOINT_MAP: dict[str, str] = {
    "unidades_ativas": "/api/unidades/ativas",
    "unidades_historico": "/api/unidades/historico",
    "ocorrencias_ativas": "/api/ocorrencias/ativas",
    "ocorrencias_historico": "/api/ocorrencias/historico",
    "ocorrencias_ativas_v2": "/api/ocorrencias/ativas/v2",
    "qmd": "/api/qmd",
    "qmd_servicos": "/api/qmd/servicos",
    "qmd_missoes": "/api/qmd/missao",
    "qmd_plano": "/api/qmd/plano",
    "qmd_ativos": "/api/qmd/ativos",
    "unit_positions": "/api/unit/positions",
    "qmd_detalhes": "/api/qmd",
    "qmd_kml": "/api/qmd",
}

# Endpoints com lógica própria (não entram no ENDPOINT_MAP)
SPECIAL_ENDPOINTS = {"unit_positions", "qmd_detalhes", "qmd_kml"}

ALL_TABLE_IDS = set(ENDPOINT_MAP) | SPECIAL_ENDPOINTS


@flow(log_prints=True)
def rj_segur__forca_municipal(
    table_id: str = "table_id",
    dataset_id: str = "brutos_forca_municipal",
    dump_mode: str = "append",
    biglake_table: bool = True,
    page_size: int = 500,
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
        data_inicio: Data de início para unit_positions (YYYY-MM-DD). Default: hoje.
        data_fim: Data de fim para unit_positions (YYYY-MM-DD). Default: data_inicio.
        hora_inicio: Hora de início para unit_positions (HH:MM:SS). Default: 00:00:00.
        hora_fim: Hora de fim para unit_positions (HH:MM:SS). Default: 23:59:59.
    """
    rename_current_flow_run_task(new_name=f"{dataset_id}.{table_id}")
    inject_bd_credentials_task(environment="prod")

    if table_id not in ALL_TABLE_IDS:
        raise ValueError(
            f"table_id '{table_id}' inválido. Opções: {sorted(ALL_TABLE_IDS)}"
        )

    api = get_authenticated_api_task()

    if table_id == "unit_positions":
        df = fetch_unit_positions_task(
            api=api,
            data_inicio=data_inicio,
            data_fim=data_fim,
            hora_inicio=hora_inicio,
            hora_fim=hora_fim,
            page_size=page_size,
            endpoint=ENDPOINT_MAP[table_id],
            endpoint_unidades_ativas=ENDPOINT_MAP["unidades_ativas"],
        )
    elif table_id == "qmd_detalhes":
        df = fetch_qmd_details_task(
            api=api,
            page_size=page_size,
            endpoint=ENDPOINT_MAP[table_id],
            endpoint_qmd=ENDPOINT_MAP["qmd"],
        )
    elif table_id == "qmd_kml":
        df = fetch_qmd_kml_task(
            api=api,
            page_size=page_size,
            endpoint=ENDPOINT_MAP[table_id],
            endpoint_qmd=ENDPOINT_MAP["qmd"],
        )
    else:
        df = fetch_endpoint_task(
            api=api, endpoint=ENDPOINT_MAP[table_id], page_size=page_size
        )

    if df.empty:
        log(f"Nenhum dado retornado para {table_id}. Encerrando flow.")
        return

    path = save_partitions_task(df=df, table_id=table_id)

    create_table_and_upload_to_gcs_task(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
        source_format="parquet",
        only_staging_dataset=True,
    )

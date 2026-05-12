# -*- coding: utf-8 -*-
"""
Flow para extrair dados da API CIVITAS/CORIO (Força Municipal) e enviar para BigQuery.

Sistema: HxGN OnCall - Gestão de Ocorrências e Unidades da Guarda Municipal RJ
"""

from typing import Optional

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_segur__forca_municipal.constants import EndpointConfig
from pipelines.rj_segur__forca_municipal.task import get_single_endpoint


@flow(log_prints=True)
def rj_segur__forca_municipal(
    table_id: str,
    dataset_id: Optional[str] = "forca_municipal",
    endpoint: Optional[str] = None,
    dump_mode: str = "overwrite",
    biglake_table: bool = True,
):
    """
    Extrai dados da API CIVITAS/CORIO e carrega no BigQuery.

    Args:
        table_id: ID da tabela no BigQuery (também usado para identificar o endpoint).
                  Valores válidos: unidades_ativas, unidades_historico, unit_positions,
                  ocorrencias_ativas, ocorrencias_historico, ocorrencias_ativas_v2,
                  qmd, qmd_ativos, qmd_servicos, qmd_missoes, qmd_plano
        dataset_id: ID do dataset no BigQuery (padrão: "forca_municipal")
        endpoint: Endpoint da API (opcional). Se não informado, usa o mapeamento
                  do table_id via EndpointConfig
        dump_mode: Modo de escrita no BigQuery ("overwrite" ou "append")
        biglake_table: Se True, cria tabela BigLake

    Examples:
        # Extrai unidades ativas usando mapeamento automático
        rj_segur__forca_municipal(table_id="unidades_ativas")

        # Extrai ocorrências com endpoint customizado
        rj_segur__forca_municipal(
            table_id="ocorrencias_ativas",
            endpoint="/ocorrencias/custom"
        )
    """
    rename_current_flow_run_task(new_name=f"{dataset_id}.{table_id}")
    inject_bd_credentials_task(environment="prod")

    # Obtém o endpoint da API baseado no table_id (se não foi fornecido)
    if endpoint is None:
        endpoint = EndpointConfig.get_endpoint(table_id)
        print(f"📍 Endpoint identificado: {endpoint}")
        print(f"📝 Descrição: {EndpointConfig.get_description(table_id)}")
        print(f"📂 Categoria: {EndpointConfig.get_category(table_id)}")
    else:
        print(f"📍 Usando endpoint customizado: {endpoint}")

    # Extrai dados da API
    path = get_single_endpoint(endpoint, paginated=True, page_size=100)

    # Carrega no BigQuery
    create_table_and_upload_to_gcs_task(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

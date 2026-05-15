# -*- coding: utf-8 -*-
"""
Flow para extrair dados da API CIVITAS/CORIO (Força Municipal) e enviar para BigQuery.

Sistema: HxGN OnCall - Gestão de Ocorrências e Unidades da Guarda Municipal RJ
"""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_segur__forca_municipal.task import (
    fetch_endpoint_task,
    get_authenticated_api_task,
    save_partitions_task,
)

# Mapeamento de table_id → endpoint da API HxGN OnCall
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
}


@flow(log_prints=True)
def rj_segur__forca_municipal(
    table_id: str,
    dataset_id: str = "brutos_forca_municipal",
    dump_mode: str = "append",
    biglake_table: bool = True,
    page_size: int = 500,
):
    """
    Extrai dados de um endpoint da API HxGN OnCall (Força Municipal) e carrega no BigQuery.

    Cada execução:
      1. Autentica na API.
      2. Busca todos os registros do endpoint (paginado).
      3. Adiciona coluna updated_at com o datetime da extração (America/São Paulo).
      4. Salva os dados em partições parquet por data de updated_at.
      5. Faz upload para GCS e cria/atualiza a tabela no BigQuery.

    Args:
        table_id: Tabela alvo. Opções: unidades_ativas, unidades_historico,
                  ocorrencias_ativas, ocorrencias_historico, ocorrencias_ativas_v2,
                  qmd, qmd_servicos, qmd_missoes, qmd_plano, qmd_ativos.
        dataset_id: Dataset do BigQuery.
        dump_mode: "append" para acumular dados, "overwrite" para substituir.
        biglake_table: Se deve criar tabela como BigLake.
        page_size: Registros por página (max 500).
    """
    rename_current_flow_run_task(new_name=f"{dataset_id}.{table_id}")
    inject_bd_credentials_task(environment="prod")

    if table_id not in ENDPOINT_MAP:
        raise ValueError(
            f"table_id '{table_id}' inválido. Opções: {list(ENDPOINT_MAP.keys())}"
        )

    endpoint = ENDPOINT_MAP[table_id]

    api = get_authenticated_api_task()
    df = fetch_endpoint_task(api=api, endpoint=endpoint, page_size=page_size)

    if df.empty:
        log(f"Nenhum dado retornado de {endpoint}. Encerrando flow.")
        return

    path = save_partitions_task(df=df, table_id=table_id)

    create_table_and_upload_to_gcs_task(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
        source_format="parquet",
    )

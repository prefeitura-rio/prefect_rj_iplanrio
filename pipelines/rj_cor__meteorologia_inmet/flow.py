# -*- coding: utf-8 -*-
"""
Flow para coleta de dados meteorológicos do INMET - COR.

Este flow é responsável por coletar dados meteorológicos das estações do INMET
localizadas no município do Rio de Janeiro e carregar no BigQuery.

Os dados incluem informações sobre temperatura, pressão, umidade, velocidade do vento,
precipitação e radiação solar, coletados a cada hora.

Migrado de Prefect 1.4 para Prefect 3.0..
"""

from typing import Optional

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_cor__meteorologia_inmet.tasks import (
    download_meteorological_data_task,
    get_dates_task,
    save_data_to_partitions_task,
    transform_meteorological_data_task,
)


@flow(log_prints=True)
def rj_cor__meteorologia_inmet(
    dataset_id: str = "clima_estacao_meteorologica",
    table_id: str = "meteorologia_inmet",
    dump_mode: str = "append",
    data_inicio: Optional[str] = "",
    data_fim: Optional[str] = "",
):
    """
    Flow principal para coleta e carga de dados meteorológicos do INMET no BigQuery.

    Este flow orquestra o processo completo de:
    1. Determinação do período de coleta (padrão: ontem até hoje)
    2. Download dos dados da API do INMET
    3. Transformação e limpeza dos dados
    4. Particionamento por data
    5. Upload para Google Cloud Storage
    6. Criação/atualização da tabela no BigQuery
    7. (Opcional) Materialização via dbt

    Os dados são coletados de 9 estações meteorológicas do INMET localizadas
    no município do Rio de Janeiro, com frequência horária.

    Args:
        dataset_id: ID do dataset no BigQuery (padrão: 'clima_estacao_meteorologica')
        table_id: ID da tabela no BigQuery (padrão: 'meteorologia_inmet')
        dump_mode: Modo de dump ('append' ou 'overwrite'). Padrão: 'append'
        data_inicio: Data de início no formato 'YYYY-MM-DD'. Vazio usa ontem
        data_fim: Data de fim no formato 'YYYY-MM-DD'. Vazio usa hoje
        materialize_after_dump: Se True, executa materialização dbt após o dump

    Returns:
        None

    Examples:
        Execução padrão (coleta de ontem até hoje):
        >>> rj_cor__clima_estacao_meteorologica()

        Backfill de período específico:
        >>> rj_cor__clima_estacao_meteorologica(
        ...     data_inicio="2026-01-01",
        ...     data_fim="2026-01-31"
        ... )

    Notes:
        - O flow é executado automaticamente a cada hora via schedule
        - Em modo padrão, coleta dados das últimas 24h para evitar perda
          de dados devido à diferença de timezone (UTC vs America/Sao_Paulo)
        - As credenciais do BigQuery e da API INMET são obtidas do Infisical
        - Os dados são salvos particionados por data
        - A materialização dbt é opcional e executada apenas se solicitada
    """
    # Renomear o flow run para facilitar identificação
    rename_current_flow_run_task(new_name=f"{dataset_id}_{table_id}")

    # Injetar credenciais do BigQuery
    inject_bd_credentials_task(environment="prod")

    # Determinar datas de coleta
    data_inicio_, data_fim_, backfill = get_dates_task(data_inicio, data_fim)

    # Download dos dados da API do INMET
    dados = download_meteorological_data_task(data_inicio_, data_fim_)

    # Transformar e limpar os dados
    dados = transform_meteorological_data_task(dados, backfill)

    # Salvar dados em partições
    path = save_data_to_partitions_task(dados=dados)

    # Criar tabela e fazer upload para GCS e BigQuery
    create_table_and_upload_to_gcs_task(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=False,
    )

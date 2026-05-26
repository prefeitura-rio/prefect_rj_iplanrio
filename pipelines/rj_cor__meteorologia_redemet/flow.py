# -*- coding: utf-8 -*-
"""
Flows para coleta de dados meteorológicos do REDEMET - COR.

Este módulo contém dois flows:
1. rj_cor__meteorologia_redemet - Coleta horária de dados meteorológicos
2. rj_cor__meteorologia_redemet_estacoes - Atualização mensal das estações

Os dados são coletados das estações meteorológicas (aeródromos) do REDEMET
localizadas no município do Rio de Janeiro.

Migrado de Prefect 1.4 para Prefect 3.0.
"""

from typing import Optional

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_cor__meteorologia_redemet.tasks import (
    check_for_new_stations_task,
    download_meteorological_data_task,
    download_stations_data_task,
    get_dates_task,
    save_data_to_partitions_task,
    transform_meteorological_data_task,
    transform_stations_data_task,
)


@flow(log_prints=True)
def rj_cor__meteorologia_redemet(
    dataset_id: str = "clima_estacao_meteorologica",
    table_id: str = "meteorologia_redemet",
    first_date: Optional[str] = None,
    last_date: Optional[str] = None,
):
    """
    Flow principal para coleta e carga de dados meteorológicos do REDEMET no BigQuery.

    Este flow orquestra o processo completo de:
    1. Determinação do período de coleta (padrão: ontem até hoje)
    2. Download dos dados da API do REDEMET
    3. Transformação e limpeza dos dados
    4. Particionamento por data de medição
    5. Salvamento em partições CSV

    Os dados são coletados de 5 estações meteorológicas (aeródromos) do REDEMET
    localizadas no município do Rio de Janeiro, com frequência horária.

    Args:
        dataset_id: ID do dataset no BigQuery (padrão: 'clima_estacao_meteorologica')
        table_id: ID da tabela no BigQuery (padrão: 'meteorologia_redemet')
        first_date: Data de início no formato 'YYYY-MM-DD'. None usa ontem
        last_date: Data de fim no formato 'YYYY-MM-DD'. None usa hoje

    Returns:
        None

    Examples:
        Execução padrão (coleta de ontem até hoje):
        >>> rj_cor__meteorologia_redemet()

        Backfill de período específico:
        >>> rj_cor__meteorologia_redemet(
        ...     first_date="2026-01-01",
        ...     last_date="2026-01-31"
        ... )

    Notes:
        - O flow é executado automaticamente a cada hora via schedule
        - Em modo padrão, coleta dados das últimas 24h para evitar perda
          de dados devido à diferença de timezone (UTC vs America/Sao_Paulo)
        - As credenciais da API REDEMET são obtidas via variável de ambiente
        - Os dados são salvos particionados por ano, mês e dia
        - Estações monitoradas: SBAF, SBGL, SBJR, SBRJ, SBSC
    """
    # Renomear o flow run para facilitar identificação
    rename_current_flow_run_task(new_name=f"{dataset_id}_{table_id}")

    # Injetar credenciais do BigQuery
    inject_bd_credentials_task(environment="prod")

    # Determinar datas de coleta
    first_date_, last_date_, backfill = get_dates_task(first_date, last_date)

    # Download dos dados da API do REDEMET
    dataframe = download_meteorological_data_task(first_date_, last_date_)

    # Transformar e limpar os dados
    dataframe = transform_meteorological_data_task(dataframe, backfill)

    # Salvar dados em partições
    path = save_data_to_partitions_task(dataframe=dataframe, partition_column="data_medicao")

    print(f"✅ Pipeline executada com sucesso! Dados salvos para {dataset_id}.{table_id}")

    create_table_and_upload_to_gcs_task(
        dataset_id=dataset_id,
        table_id=table_id,
        data_path=path,
        dump_mode="append",
    )


@flow(log_prints=True)
def rj_cor__meteorologia_redemet_estacoes(
    dataset_id: str = "clima_estacao_meteorologica",
    table_id: str = "estacoes_redemet",
):
    """
    Flow para atualização das informações de estações meteorológicas do REDEMET.

    Este flow orquestra o processo de:
    1. Download da lista completa de estações do Brasil
    2. Filtragem de estações do Rio de Janeiro
    3. Transformação e limpeza dos dados
    4. Verificação de novas estações
    5. Salvamento em partições CSV

    Executado mensalmente para manter atualizado o cadastro de estações.

    Args:
        dataset_id: ID do dataset no BigQuery (padrão: 'clima_estacao_meteorologica')
        table_id: ID da tabela no BigQuery (padrão: 'estacoes_redemet')

    Returns:
        None

    Examples:
        Execução padrão:
        >>> rj_cor__meteorologia_redemet_estacoes()

    Notes:
        - O flow é executado automaticamente a cada 30 dias via schedule
        - Verifica se há novas estações no Rio de Janeiro
        - Os dados sobrescrevem a tabela anterior (modo overwrite)
        - Particiona por data_atualizacao
    """
    # Renomear o flow run para facilitar identificação
    rename_current_flow_run_task(new_name=f"{dataset_id}_{table_id}_estacoes")

    # Injetar credenciais do BigQuery
    inject_bd_credentials_task(environment="prod")

    # Download dos dados de estações
    dataframe = download_stations_data_task()

    # Transformar e filtrar estações do RJ
    dataframe = transform_stations_data_task(dataframe)

    # Verificar se há novas estações
    check_for_new_stations_task(dataframe)

    # Salvar dados em partições
    path = save_data_to_partitions_task(dataframe=dataframe, partition_column="data_atualizacao")

    print(f"✅ Atualização de estações concluída! Dados salvos para {dataset_id}.{table_id}")

    create_table_and_upload_to_gcs_task(
        dataset_id=dataset_id,
        table_id=table_id,
        data_path=path,
        dump_mode="append",
    )

# if __name__ == "__main__":
    # Teste do flow de dados meteorológicos
    # rj_cor__meteorologia_redemet(
    #     dataset_id="clima_estacao_meteorologica",
    #     table_id="meteorologia_redemet",
    #     first_date="2026-05-25",
    #     last_date="2026-05-25",
    # )

    # Teste do flow de estações
    # rj_cor__meteorologia_redemet_estacoes(
    #     dataset_id="clima_estacao_meteorologica",
    #     table_id="estacoes_redemet",
    # )

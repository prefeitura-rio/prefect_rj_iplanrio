# -*- coding: utf-8 -*-
"""
Flow para coleta de dados de precipitação do WebSirene - COR.

Este flow é responsável por coletar dados de precipitação das estações pluviométricas
do AlertaRio (WebSirene) e carregar no BigQuery.

Os dados incluem informações sobre acumulados de chuva em diferentes períodos:
15 minutos, 1 hora, 4 horas, 24 horas, 96 horas e mensal.

Migrado de Prefect 1.4 para Prefect 3.0.
"""

from typing import Optional

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_cor__precipitacao_websirene.tasks import (
    download_dados_task,
    salvar_dados_task,
    tratar_dados_task,
)


@flow(log_prints=True)
def rj_cor__precipitacao_websirene(
    dataset_id: str = "clima_pluviometro",
    table_id: str = "taxa_precipitacao_websirene",
    dump_mode: str = "append",
):
    """
    Flow principal para coleta e carga de dados de precipitação do WebSirene no BigQuery.

    Este flow orquestra o processo completo de:
    1. Download dos dados da API do WebSirene (AlertaRio)
    2. Transformação e limpeza dos dados
    3. Conversão de timezone (UTC -> America/Sao_Paulo)
    4. Particionamento por data de medição
    5. Upload para Google Cloud Storage
    6. Criação/atualização da tabela no BigQuery
    7. (Opcional) Materialização via dbt

    Os dados são coletados de todas as estações pluviométricas do AlertaRio
    disponíveis no momento da coleta, com acumulados em diferentes períodos.

    Args:
        dataset_id: ID do dataset no BigQuery (padrão: 'clima_pluviometro')
        table_id: ID da tabela no BigQuery (padrão: 'taxa_precipitacao_websirene')
        dump_mode: Modo de dump ('append' ou 'overwrite'). Padrão: 'append'

    Returns:
        None

    Examples:
        Execução padrão (coleta dos dados mais recentes):
        >>> rj_cor__precipitacao_websirene()


    Notes:
        - O flow é executado automaticamente a cada 15 minutos via schedule
        - As credenciais do BigQuery são obtidas do Infisical
        - Os dados são salvos particionados por data de medição
        - A materialização dbt é opcional e executada apenas se solicitada
        - Se não houver dados novos (empty_data=True), o flow pula o upload
    """
    # Renomear o flow run para facilitar identificação
    rename_current_flow_run_task(new_name=f"{dataset_id}_{table_id}")

    # Injetar credenciais do BigQuery
    inject_bd_credentials_task(environment="prod")

    # Download dos dados da API do WebSirene
    dataframe = download_dados_task()

    # Transformar e limpar os dados
    dataframe, empty_data = tratar_dados_task(dfr=dataframe)

    # Se houver dados, processa e faz upload
    if not empty_data:
        # Salvar dados em partições
        path = salvar_dados_task(dfr=dataframe)

        # Criar tabela e fazer upload para GCS e BigQuery
        create_table_and_upload_to_gcs_task(
            data_path=path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            biglake_table=False,
        )

    else:
        print("Não há dados novos para processar. Flow finalizado sem upload.")

"""
Flow para coleta de dados de precipitação do WebSirene - COR.

Este flow é responsável por coletar dados de precipitação das estações pluviométricas
do AlertaRio (WebSirene) e carregar no BigQuery.

Os dados incluem informações sobre acumulados de chuva em diferentes períodos:
15 minutos, 1 hora, 4 horas, 24 horas, 96 horas e mensal.

Migrado de Prefect 1.4 para Prefect 3.0.
"""


from pathlib import Path

from prefect import flow

from pipelines.rj_cor__precipitacao_websirene.tasks import (
    download_dados_task,
    salvar_dados_task,
    tratar_dados_task,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{dataset_id}_{table_id}")
async def rj_cor__precipitacao_websirene(
    *,
    cloud: Cloud,
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

    # Injetar credenciais do BigQuery

    # Download dos dados da API do WebSirene
    dataframe = download_dados_task()

    # Transformar e limpar os dados
    dataframe, empty_data = tratar_dados_task(dfr=dataframe)

    # Se houver dados, processa e faz upload
    if not empty_data:
        # Salvar dados em partições
        path = salvar_dados_task(dfr=dataframe)

        # Criar tabela e fazer upload para GCS e BigQuery
        table = Table(dataset_id=dataset_id, table_id=table_id)
        await upload_files_to_gcs(
            Path(path),
            table,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format="csv",
            options=capture_options(dump_mode),
        )
        await publish_table_in_bq(table, cloud)

    else:
        print("Não há dados novos para processar. Flow finalizado sem upload.")

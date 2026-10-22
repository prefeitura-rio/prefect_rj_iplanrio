"""
Flow para atualização de estações meteorológicas do REDEMET - COR.

Este módulo contém o flow de atualização mensal das estações meteorológicas
do REDEMET localizadas no município do Rio de Janeiro.

Migrado de Prefect 1.4 para Prefect 3.0.
"""

from pathlib import Path

from prefect import flow

from pipelines.rj_cor__meteorologia_redemet_estacoes.tasks import (
    check_for_new_stations_task,
    download_stations_data_task,
    save_data_to_partitions_task,
    transform_stations_data_task,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{dataset_id}_{table_id}")
async def rj_cor__meteorologia_redemet_estacoes(
    *,
    cloud: Cloud,
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

    # Injetar credenciais do BigQuery

    # Download dos dados de estações
    dataframe = download_stations_data_task()

    # Transformar e filtrar estações do RJ
    dataframe = transform_stations_data_task(dataframe)

    # Verificar se há novas estações
    check_for_new_stations_task(dataframe)

    # Salvar dados em partições
    path = save_data_to_partitions_task(dataframe=dataframe, partition_column="data_atualizacao")

    print(f"✅ Atualização de estações concluída! Dados salvos para {dataset_id}.{table_id}")

    table = Table(dataset_id=dataset_id, table_id=table_id)
    await upload_files_to_gcs(
        Path(path),
        table,
        bucket=cloud["bucket"],
        dump_mode="append",
        source_format="csv",
        options=capture_options("append"),
    )
    await publish_table_in_bq(table, cloud)


if __name__ == "__main__":
    # Teste do flow de estações
    rj_cor__meteorologia_redemet_estacoes(
        dataset_id="clima_estacao_meteorologica",
        table_id="estacoes_redemet",
    )

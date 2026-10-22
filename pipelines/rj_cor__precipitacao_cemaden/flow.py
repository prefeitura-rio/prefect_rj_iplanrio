"""
Flow para coleta e processamento de dados de precipitação do CEMADEN - COR.

Migrado de Prefect 1.4 para Prefect 3.0.
"""

from pathlib import Path

from prefect import flow

from pipelines.rj_cor__precipitacao_cemaden.tasks import (
    check_for_new_stations_task,
    download_cemaden_data_task,
    save_data_to_partitions_task,
    transform_cemaden_data_task,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True)
async def rj_cor__precipitacao_cemaden(
    *,
    cloud: Cloud,
    dataset_id: str = "clima_pluviometro",
    table_id: str = "taxa_precipitacao_cemaden",
    dump_mode: str = "append",
):
    """
    Flow para coleta de dados de precipitação do CEMADEN.

    Este flow coleta dados do CEMADEN (Centro Nacional de Monitoramento e Alertas
    de Desastres Naturais) para estações pluviométricas localizadas no município
    do Rio de Janeiro.

    Os dados incluem acumulados de precipitação em diferentes intervalos de tempo
    (10min, 15min, 1h, 3h, 6h, 12h, 24h, 96h), além de informações de localização
    das estações.

    Args:
        dataset_id: ID do dataset no BigQuery.
        table_id: ID da tabela no BigQuery.
        dump_mode: Modo de salvamento ("append" ou "overwrite").

    Returns:
        None

    Examples:
        Executar o flow manualmente:
        >>> rj_cor__precipitacao_cemaden()

        Executar com parâmetros customizados:
        >>> rj_cor__precipitacao_cemaden(
        ...     dataset_id="clima_pluviometro",
        ...     table_id="taxa_precipitacao_cemaden_staging",
        ...     dump_mode="overwrite"
        ... )

    Notes:
        - O CEMADEN fornece dados de precipitação em tempo quasi-real
        - Apenas estações do Rio de Janeiro são processadas (código IBGE: 3304557)
        - Apenas estações pluviométricas são incluídas (tipo = 1)
        - Schedule recomendado: A cada 5 minutos
        - Timezone convertido de UTC para America/Sao_Paulo
        - Duplicatas são automaticamente removidas
        - Sistema verifica automaticamente novas estações
    """
    print("🌧️  Iniciando coleta de dados do CEMADEN...")

    # Step 1: Download dos dados do CEMADEN
    print("📥 Fazendo download dos dados do CEMADEN...")
    dfr = download_cemaden_data_task()

    # Step 2: Transformar e limpar os dados
    print("🔄 Transformando dados...")
    dfr_transformed = transform_cemaden_data_task(dfr)

    # Step 3: Verificar novas estações (não interrompe o flow)
    print("🔍 Verificando novas estações...")
    check_for_new_stations_task(dfr_transformed)

    # Step 4: Salvar dados em partições
    print("💾 Salvando dados em partições...")
    prepath = save_data_to_partitions_task(
        dfr=dfr_transformed,
        partition_column="data_medicao",
    )

    # Step 5: Injetar credenciais e fazer upload para BigQuery
    print(f"☁️  Fazendo upload para BigQuery ({dataset_id}.{table_id})...")
    table = Table(dataset_id=dataset_id, table_id=table_id)
    await upload_files_to_gcs(
        Path(str(prepath)),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="csv",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)

    print("✅ Flow concluído com sucesso!")
    print(f"   Dados salvos em: {dataset_id}.{table_id}")

# if __name__ == "__main__":
#     rj_cor__precipitacao_cemaden()
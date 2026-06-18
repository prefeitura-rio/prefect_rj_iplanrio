# -*- coding: utf-8 -*-
"""
Flow para coleta e processamento de dados de precipitação do AlertaRio - COR.

Migrado de Prefect 1.4 para Prefect 3.0.
"""
from typing import Optional

from iplanrio.pipelines_utils.bd import (
    create_table_and_upload_to_gcs_task,
)
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from prefect import flow

from pipelines.rj_cor__precipitacao_alertario.tasks import (
    download_alertario_data_task,
    save_data_to_partitions_task,
    transform_meteorological_data_task,
    transform_pluviometric_data_task,
)


@flow(log_prints=True)
def rj_cor__precipitacao_alertario(
    dataset_id_pluviometric: Optional[str],
    table_id_pluviometric: Optional[str],
    dataset_id_meteorological: Optional[str] = None,
    table_id_meteorological: Optional[str] = None,
    dump_mode_pluviometric: Optional[str] = "append",
    dump_mode_meteorological: Optional[str] = "append",
):
    """
    Flow para coleta de dados de precipitação e meteorologia do AlertaRio.

    Este flow coleta dados do sistema AlertaRio (Sistema de Alerta de Chuvas Intensas
    da Prefeitura do Rio de Janeiro) e processa dois tipos de dados:
    - Dados pluviométricos: medições de chuva de pluviômetros
    - Dados meteorológicos: temperatura, umidade, pressão e vento

    Os dados são salvos em duas tabelas distintas no BigQuery, ambas particionadas
    por data de medição.

    Args:
        dataset_id_pluviometric: ID do dataset no BigQuery para dados pluviométricos.
        dataset_id_meteorological: ID do dataset no BigQuery para dados meteorológicos.
        table_id_pluviometric: ID da tabela de dados pluviométricos.
        table_id_meteorological: ID da tabela de dados meteorológicos.
        dump_mode_pluviometric: Modo de salvamento para dados pluviométricos
            ("append" ou "overwrite").
        dump_mode_meteorological: Modo de salvamento para dados meteorológicos
            ("append" ou "overwrite").

    Returns:
        None

    Examples:
        Executar o flow manualmente:
        >>> rj_cor__precipitacao_alertario()

        Executar com parâmetros customizados:
        >>> rj_cor__precipitacao_alertario(
        ...     dump_mode_pluviometric="overwrite",
        ...     dump_mode_meteorological="overwrite"
        ... )

    Notes:
        - O AlertaRio fornece dados em tempo real de pluviômetros e estações
        - Os dados vêm em formato HTML e são parseados com BeautifulSoup
        - Schedule recomendado: A cada 2 minutos (dados em tempo real)
        - Dados pluviométricos incluem acumulados de chuva em diversos intervalos
        - Dados meteorológicos incluem temperatura, umidade, pressão e vento
    """
    inject_bd_credentials_task(environment="prod")
    print("🌧️  Iniciando coleta de dados do AlertaRio...")

    # Step 1: Download dos dados do AlertaRio (retorna 2 DataFrames)
    print("📥 Fazendo download dos dados do AlertaRio...")
    dfr_pluviometric, dfr_meteorological = download_alertario_data_task()

    if dataset_id_pluviometric is not None:
        print("🔄 Transformando dados pluviométricos...")
        dfr_pluviometric_transformed = transform_pluviometric_data_task(dfr_pluviometric)
        print("💾 Salvando dados pluviométricos em partições...")
        prepath_pluviometric = save_data_to_partitions_task(
            dfr=dfr_pluviometric_transformed,
            data_name="pluviometric",
            partition_column="data_medicao",
        )

        print(f"☁️  Fazendo upload dos dados pluviométricos para BigQuery ({dataset_id_pluviometric}.{table_id_pluviometric})")
        create_table_and_upload_to_gcs_task(
            data_path=str(prepath_pluviometric),
            dataset_id=dataset_id_pluviometric,
            table_id=table_id_pluviometric,
            dump_mode=dump_mode_pluviometric,
        )
        print(f"   - Dados pluviométricos salvos em: {dataset_id_pluviometric}.{table_id_pluviometric}")
    else:
        print("⚠️  dataset_id_pluviometric não fornecido. Pulando upload dos dados pluviométricos para BigQuery.")


    if dataset_id_meteorological is not None:
        print("🔄 Transformando dados meteorológicos...")
        dfr_meteorological_transformed = transform_meteorological_data_task(dfr_meteorological)
        print("💾 Salvando dados meteorológicos em partições...")
        prepath_meteorological = save_data_to_partitions_task(
            dfr=dfr_meteorological_transformed,
            data_name="meteorological",
            partition_column="data_medicao",
        )

        # Step 4b: Fazer upload para BigQuery (dados meteorológicos)
        print(f"☁️  Fazendo upload dos dados meteorológicos para BigQuery ({dataset_id_meteorological}.{table_id_meteorological})...")
        create_table_and_upload_to_gcs_task(
            data_path=str(prepath_meteorological),
            dataset_id=dataset_id_meteorological,
            table_id=table_id_meteorological,
            dump_mode=dump_mode_meteorological,
        )

        print(f"  - Dados meteorológicos salvos em: {dataset_id_meteorological}.{table_id_meteorological}")
    else:
        print("⚠️ dataset_id_meteorological não fornecido. Pulando upload dos dados meteorológicos para BigQuery.")

    print("✅ Flow concluído com sucesso!")

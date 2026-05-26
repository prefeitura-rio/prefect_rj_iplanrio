# -*- coding: utf-8 -*-
"""
Flow para coleta e processamento de dados de precipitação e fluviometria do INEA - COR.

Migrado de Prefect 1.4 para Prefect 3.0.
"""

from pathlib import Path

from iplanrio.pipelines_utils.bd import (
    create_table_and_upload_to_gcs_task,
)
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from prefect import flow

from pipelines.rj_cor__precipitacao_inea.tasks import (
    check_for_new_stations_task,
    download_inea_data_task,
    save_data_to_partitions_task,
    transform_inea_data_task,
)


@flow(log_prints=True)
def rj_cor__precipitacao_inea(
    dataset_id_pluviometric: str = "clima_pluviometro",
    table_id_pluviometric: str = "taxa_precipitacao_inea",
    dataset_id_fluviometric: str = "clima_fluviometro",
    table_id_fluviometric: str = "lamina_agua_inea",
    dump_mode: str = "append",
):
    """
    Flow para coleta de dados de precipitação e fluviometria do INEA.

    Este flow coleta dados do INEA (Instituto Estadual do Ambiente) do Sistema
    de Alerta de Cheias do Rio de Janeiro e processa dois tipos de dados:
    - Dados pluviométricos: acumulados de precipitação
    - Dados fluviométricos: altura da água em rios

    Os dados são salvos em duas tabelas distintas no BigQuery, ambas particionadas
    por data de medição.

    Args:
        dataset_id_pluviometric: ID do dataset para dados pluviométricos.
        table_id_pluviometric: ID da tabela para dados pluviométricos.
        dataset_id_fluviometric: ID do dataset para dados fluviométricos.
        table_id_fluviometric: ID da tabela para dados fluviométricos.
        dump_mode: Modo de salvamento ("append" ou "overwrite").

    Returns:
        None

    Examples:
        Executar o flow manualmente:
        >>> rj_cor__precipitacao_inea()

        Executar com parâmetros customizados:
        >>> rj_cor__precipitacao_inea(
        ...     dataset_id_pluviometric="clima_pluviometro_staging",
        ...     dump_mode="overwrite"
        ... )

    Notes:
        - O INEA fornece dados de 5 estações meteorológicas
        - Estações: Campo Grande, Capela Mayrink, Eletrobras, Realengo, São Cristóvão
        - Dados baixados de arquivos Excel via HTTP
        - Schedule recomendado: A cada 5 minutos
        - Dados pluviométricos: 7 acumulados (5min a mensal)
        - Dados fluviométricos: altura da água (cm)
        - Duplicatas são automaticamente removidas
        - Sistema verifica automaticamente novas estações
    """
    print("🌧️  Iniciando coleta de dados do INEA...")

    # Step 1: Download dos dados do INEA
    print("📥 Fazendo download dos dados do INEA...")
    dfr = download_inea_data_task()

    # Step 2: Transformar e separar em pluviométrico e fluviométrico
    print("🔄 Transformando dados...")
    dfr_pluviometric, dfr_fluviometric = transform_inea_data_task(dfr)

    # Step 3: Verificar novas estações (não interrompe o flow)
    print("🔍 Verificando novas estações...")
    check_for_new_stations_task(dfr_pluviometric)

    # Step 4a: Salvar dados pluviométricos em partições
    print("💾 Salvando dados pluviométricos em partições...")
    prepath_pluviometric = save_data_to_partitions_task(
        dfr=dfr_pluviometric,
        data_name="pluviometric",
        partition_column="data_medicao",
    )

    # Step 4b: Salvar dados fluviométricos em partições
    print("💾 Salvando dados fluviométricos em partições...")
    prepath_fluviometric = save_data_to_partitions_task(
        dfr=dfr_fluviometric,
        data_name="fluviometric",
        partition_column="data_medicao",
    )

    # Step 5a: Injetar credenciais e fazer upload de dados pluviométricos
    print(f"☁️  Fazendo upload dos dados pluviométricos para BigQuery ({dataset_id_pluviometric}.{table_id_pluviometric})...")
    inject_bd_credentials_task()
    create_table_and_upload_to_gcs_task(
        data_path=str(prepath_pluviometric),
        dataset_id=dataset_id_pluviometric,
        table_id=table_id_pluviometric,
        dump_mode=dump_mode,
    )

    # Step 5b: Fazer upload de dados fluviométricos
    print(f"☁️  Fazendo upload dos dados fluviométricos para BigQuery ({dataset_id_fluviometric}.{table_id_fluviometric})...")
    create_table_and_upload_to_gcs_task(
        data_path=str(prepath_fluviometric),
        dataset_id=dataset_id_fluviometric,
        table_id=table_id_fluviometric,
        dump_mode=dump_mode,
    )

    print("✅ Flow concluído com sucesso!")
    print(f"   - Dados pluviométricos salvos em: {dataset_id_pluviometric}.{table_id_pluviometric}")
    print(f"   - Dados fluviométricos salvos em: {dataset_id_fluviometric}.{table_id_fluviometric}")

# if __name__ == "__main__":
#     rj_cor__precipitacao_inea()
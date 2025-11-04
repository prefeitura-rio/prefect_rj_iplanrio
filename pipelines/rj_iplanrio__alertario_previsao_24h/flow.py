# -*- coding: utf-8 -*-
"""
Flow para extrair previsões meteorológicas do AlertaRio e carregar no BigQuery
"""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__alertario_previsao_24h.constants import (
    AlertaRioConstants,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.tasks import (
    create_dim_mares_df,
    create_dim_previsao_periodo_df,
    create_dim_temperatura_zona_df,
    create_previsao_diaria_df,
    fetch_xml_from_url,
    parse_xml_to_dict,
)
from pipelines.rj_iplanrio__alertario_previsao_24h.utils.tasks import (
    create_date_partitions,
)


@flow(log_prints=True)
def rj_iplanrio__alertario_previsao_24h(
    dataset_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
):
    """
    Flow para extrair previsões meteorológicas do AlertaRio e carregar no BigQuery.

    Busca o XML de previsão do AlertaRio, faz parsing e carrega em 4 tabelas:
    - previsao_diaria: Tabela fato com dados agregados
    - dim_previsao_periodo: Detalhes por período do dia
    - dim_temperatura_zona: Temperaturas por zona da cidade
    - dim_mares: Informações de tábua de marés

    Args:
        dataset_id: ID do dataset no BigQuery (default: brutos_alertario)
        dump_mode: Modo de dump (default: append)
        materialize_after_dump: Se deve materializar após dump (default: False)
    """

    # Usar valores dos constants como padrão
    dataset_id = dataset_id or AlertaRioConstants.DATASET_ID.value
    dump_mode = dump_mode or AlertaRioConstants.DUMP_MODE.value
    materialize_after_dump = (
        materialize_after_dump
        if materialize_after_dump is not None
        else AlertaRioConstants.MATERIALIZE_AFTER_DUMP.value
    )

    file_format = AlertaRioConstants.FILE_FORMAT.value
    biglake_table = AlertaRioConstants.BIGLAKE_TABLE.value
    partition_column = AlertaRioConstants.PARTITION_COLUMN.value

    # Renomear flow run para melhor identificação
    rename_current_flow_run_task(new_name=f"alertario_previsao_24h_{dataset_id}")

    # Injetar credenciais do BD
    inject_bd_credentials_task(environment="prod")

    # Buscar XML do AlertaRio
    xml_content = fetch_xml_from_url()

    # Fazer parsing do XML
    parsed_data = parse_xml_to_dict(xml_content)

    # Criar DataFrames para cada tabela
    df_previsao_diaria = create_previsao_diaria_df(parsed_data)
    df_dim_periodo = create_dim_previsao_periodo_df(parsed_data)
    df_dim_temperatura = create_dim_temperatura_zona_df(parsed_data)
    df_dim_mares = create_dim_mares_df(parsed_data)

    # Upload tabela 1: previsao_diaria
    root_folder_1 = AlertaRioConstants.ROOT_FOLDER.value + "previsao_diaria/"
    partitions_path_1 = create_date_partitions(
        dataframe=df_previsao_diaria,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_1,
    )
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path_1,
        dataset_id=dataset_id,
        table_id=AlertaRioConstants.TABLE_PREVISAO_DIARIA.value,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    # Upload tabela 2: dim_previsao_periodo
    root_folder_2 = AlertaRioConstants.ROOT_FOLDER.value + "dim_previsao_periodo/"
    partitions_path_2 = create_date_partitions(
        dataframe=df_dim_periodo,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_2,
    )
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path_2,
        dataset_id=dataset_id,
        table_id=AlertaRioConstants.TABLE_DIM_PREVISAO_PERIODO.value,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    # Upload tabela 3: dim_temperatura_zona
    root_folder_3 = AlertaRioConstants.ROOT_FOLDER.value + "dim_temperatura_zona/"
    partitions_path_3 = create_date_partitions(
        dataframe=df_dim_temperatura,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_3,
    )
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path_3,
        dataset_id=dataset_id,
        table_id=AlertaRioConstants.TABLE_DIM_TEMPERATURA_ZONA.value,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    # Upload tabela 4: dim_mares
    root_folder_4 = AlertaRioConstants.ROOT_FOLDER.value + "dim_mares/"
    partitions_path_4 = create_date_partitions(
        dataframe=df_dim_mares,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder_4,
    )
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path_4,
        dataset_id=dataset_id,
        table_id=AlertaRioConstants.TABLE_DIM_MARES.value,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

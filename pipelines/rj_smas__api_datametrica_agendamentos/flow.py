# -*- coding: utf-8 -*-
"""
Flow migrado do Prefect 1.4 para 3.0 - SMAS API Datametrica Agendamentos
"""

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.dbt import execute_dbt_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_smas__api_datametrica_agendamentos.constants import (
    DatametricaConstants,
)
from pipelines.rj_smas__api_datametrica_agendamentos.tasks import (
    calculate_target_date,
    convert_agendamentos_to_dataframe,
    fetch_agendamentos_from_api,
    get_bigquery_config,
    get_datametrica_credentials,
    transform_agendamentos_data,
)
from pipelines.rj_smas__api_datametrica_agendamentos.utils.tasks import (
    create_date_partitions,
)


@flow(log_prints=True)
def rj_smas__api_datametrica_agendamentos(
    # Parâmetros opcionais para override manual na UI
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    materialize_after_dump: bool | None = None,
    date: str | None = None,
    infisical_secret_path: str | None = "/api-datametrica",
):
    """
    Flow para extrair agendamentos da API Datametrica e carregar no BigQuery.

    Regra de negócio para datas:
    - Dias normais: busca dados para 2 dias à frente
    - Quinta e sexta-feira: busca dados para 4 dias à frente (cobrindo fim de semana)

    Args:
        dataset_id: ID do dataset no BigQuery (default: None = obtém do Infisical)
        table_id: ID da tabela no BigQuery (default: None = obtém do Infisical)
        dump_mode: Modo de dump (default: append)
        materialize_after_dump: Se deve materializar após dump (default: True)
        date: Data para buscar agendamentos no formato YYYY-MM-DD (default: None = usa regra de negócio)
        infisical_secret_path: Caminho dos secrets no Infisical (default: /api-datametrica)
    """

    # Obter configuração do BigQuery do Infisical se não fornecida via parâmetros
    if dataset_id is None or table_id is None:
        bigquery_config = get_bigquery_config(infisical_secret_path=infisical_secret_path)
        dataset_id = dataset_id or bigquery_config["dataset_id"]
        table_id = table_id or bigquery_config["table_id"]

    # Usar valores dos constants como padrão para outros parâmetros
    dump_mode = dump_mode or DatametricaConstants.DUMP_MODE.value
    materialize_after_dump = (
        materialize_after_dump
        if materialize_after_dump is not None
        else DatametricaConstants.MATERIALIZE_AFTER_DUMP.value
    )

    partition_column = DatametricaConstants.PARTITION_COLUMN.value
    file_format = DatametricaConstants.FILE_FORMAT.value
    root_folder = DatametricaConstants.ROOT_FOLDER.value
    biglake_table = DatametricaConstants.BIGLAKE_TABLE.value

    # Renomear flow run para melhor identificação
    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")

    # Injetar credenciais do BD
    crd = inject_bd_credentials_task(environment="prod")  # noqa

    # Obter credenciais da API Datametrica
    credentials = get_datametrica_credentials(infisical_secret_path=infisical_secret_path)

    # Calcular data target baseada na regra de negócio (a menos que date seja fornecido explicitamente)
    target_date = date if date is not None else calculate_target_date()

    # Buscar dados da API
    raw_data = fetch_agendamentos_from_api(credentials=credentials, date=target_date)

    # Transformar os dados
    processed_data = transform_agendamentos_data(raw_data)

    # Converter para DataFrame
    df = convert_agendamentos_to_dataframe(processed_data)

    # Criar partições por data
    partitions_path = create_date_partitions(
        dataframe=df,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=root_folder,
    )

    # Upload para GCS e BigQuery
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=biglake_table,
    )

    if materialize_after_dump:
        dbt_select = "raw_cadunico_agendamentos"
        execute_dbt_task(select=dbt_select, target="prod")

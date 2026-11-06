# -*- coding: utf-8 -*-
"""
Flow para dump do banco de dados de OCR (Radares de Trânsito) da CETRIO para o BigQuery.

Este flow faz a ingestão de dados de fluxo de veículos capturados por radares de trânsito,
extraindo informações do Data Warehouse OCR e carregando no BigQuery para análise de
mobilidade urbana.
"""

from typing import Optional

from iplanrio.pipelines_templates.dump_db.tasks import (
    dump_upload_batch_task,
    format_partitioned_query_task,
    get_database_username_and_password_from_secret_task,
    parse_comma_separated_string_to_list_task,
)
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow


@flow(log_prints=True)
def rj_cetrio__transporte_rodoviario_radar_transito(
    db_database: str = "DWOCR_Staging",
    db_host: str = "10.39.64.50",
    db_port: str = "1433",
    db_type: str = "sql_server",
    db_charset: Optional[str] = "NOT_SET",
    execute_query: str = "execute_query",
    dataset_id: str = "transporte_rodoviario_radar_transito",
    table_id: str = "fluxo_veiculos",
    infisical_secret_path: str = "/db-ocr-radar",
    dump_mode: str = "overwrite",
    partition_date_format: str = "%Y-%m-%d",
    partition_columns: Optional[str] = None,
    lower_bound_date: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    retry_dump_upload_attempts: int = 2,
    batch_size: int = 50000,
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
    max_concurrency: int = 1,
    only_staging_dataset: bool = False,
    add_timestamp_column: bool = True,
):
    """
    Flow para dump de dados de radares de trânsito do Data Warehouse OCR para o BigQuery.

    Extrai dados de fluxo de veículos capturados por equipamentos OCR (radares),
    incluindo informações de localização, período, e contagens de registros e placas.

    Args:
        db_database: Nome do banco de dados (padrão: DWOCR_Staging)
        db_host: Endereço do servidor SQL Server (padrão: 10.39.64.50)
        db_port: Porta do servidor (padrão: 1433)
        db_type: Tipo de banco de dados (padrão: sql_server)
        db_charset: Charset do banco (padrão: NOT_SET)
        execute_query: Query SQL a ser executada
        dataset_id: ID do dataset no BigQuery (padrão: transporte_rodoviario_radar_transito)
        table_id: ID da tabela no BigQuery
        infisical_secret_path: Caminho do secret no Infisical (padrão: /db-ocr-radar)
        dump_mode: Modo de dump - overwrite ou append (padrão: overwrite)
        partition_date_format: Formato da data de particionamento (padrão: %Y-%m-%d)
        partition_columns: Colunas de particionamento (separadas por vírgula)
        lower_bound_date: Data inicial para particionamento
        break_query_frequency: Frequência de quebra da query (day, month, year)
        break_query_start: Data inicial da quebra de query
        break_query_end: Data final da quebra de query
        retry_dump_upload_attempts: Número de tentativas de retry (padrão: 2)
        batch_size: Tamanho do batch para upload (padrão: 50000)
        batch_data_type: Tipo de dados do batch (padrão: csv)
        biglake_table: Se deve criar tabela BigLake (padrão: True)
        log_number_of_batches: Número de batches para logging (padrão: 100)
        max_concurrency: Concorrência máxima (padrão: 1)
        only_staging_dataset: Se deve usar apenas dataset staging (padrão: False)
        add_timestamp_column: Se deve adicionar coluna de timestamp (padrão: True)
    """
    rename_current_flow_run_task(new_name=table_id)
    inject_bd_credentials_task(environment="prod")
    secrets = get_database_username_and_password_from_secret_task(
        infisical_secret_path=infisical_secret_path
    )
    partition_columns_list = parse_comma_separated_string_to_list_task(text=partition_columns)

    formated_query = format_partitioned_query_task(
        query=execute_query,
        dataset_id=dataset_id,
        table_id=table_id,
        database_type=db_type,
        partition_columns=partition_columns_list,
        lower_bound_date=lower_bound_date,
        date_format=partition_date_format,
        break_query_start=break_query_start,
        break_query_end=break_query_end,
        break_query_frequency=break_query_frequency,
    )

    dump_upload_batch_task(
        queries=formated_query,
        batch_size=batch_size,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        partition_columns=partition_columns_list,
        batch_data_type=batch_data_type,
        biglake_table=biglake_table,
        log_number_of_batches=log_number_of_batches,
        retry_dump_upload_attempts=retry_dump_upload_attempts,
        database_type=db_type,
        hostname=db_host,
        port=db_port,
        user=secrets["DB_USERNAME"],
        password=secrets["DB_PASSWORD"],
        database=db_database,
        charset=db_charset,
        max_concurrency=max_concurrency,
        only_staging_dataset=only_staging_dataset,
        add_timestamp_column=add_timestamp_column,
    )

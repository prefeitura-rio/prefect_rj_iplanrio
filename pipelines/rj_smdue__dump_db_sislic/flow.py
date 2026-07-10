# -*- coding: utf-8 -*-
"""
Este flow é utilizado para fazer dump do banco de dados SISLIC para o BigQuery.
O SISLIC (Sistema de Licenciamento) é o sistema de gestão de processos de licenciamento
de obras da Secretaria Municipal de Urbanismo.
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
def rj_smdue__dump_db_sislic(
    db_database: str = "SMU_PRD",
    db_host: str = "10.2.221.101",
    db_port: str = "1433",
    db_type: str = "sql_server",
    db_charset: Optional[str] = "utf8",
    execute_query: str = "execute_query",
    dataset_id: str = "adm_licenca_urbanismo",
    table_id: str = "table_id",
    infisical_secret_path: str = "/db-sislic",
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
    Flow principal para dump do banco SISLIC para o BigQuery.

    Args:
        db_database: Nome do banco de dados (padrão: SMU_PRD)
        db_host: Host do banco de dados (padrão: 10.2.221.101)
        db_port: Porta do banco de dados (padrão: 1433)
        db_type: Tipo do banco de dados (padrão: sql_server)
        db_charset: Charset do banco de dados
        execute_query: Query SQL a ser executada
        dataset_id: ID do dataset no BigQuery (padrão: adm_licenca_urbanismo)
        table_id: ID da tabela no BigQuery
        infisical_secret_path: Caminho do secret no Infisical (padrão: /db-sislic)
        dump_mode: Modo de dump (overwrite/append)
        partition_date_format: Formato da data de particionamento
        partition_columns: Colunas de particionamento
        lower_bound_date: Data limite inferior para particionamento
        break_query_frequency: Frequência de quebra de query
        break_query_start: Data de início da quebra de query
        break_query_end: Data de fim da quebra de query
        retry_dump_upload_attempts: Tentativas de retry no upload
        batch_size: Tamanho do batch
        batch_data_type: Tipo de dado do batch (csv/parquet)
        biglake_table: Se deve criar tabela BigLake
        log_number_of_batches: Número de batches para log
        max_concurrency: Concorrência máxima
        only_staging_dataset: Se deve usar apenas dataset de staging
        add_timestamp_column: Se deve adicionar coluna de timestamp
    """
    rename_current_flow_run_task(new_name=table_id)
    inject_bd_credentials_task(environment="prod")
    secrets = get_database_username_and_password_from_secret_task(infisical_secret_path=infisical_secret_path)
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

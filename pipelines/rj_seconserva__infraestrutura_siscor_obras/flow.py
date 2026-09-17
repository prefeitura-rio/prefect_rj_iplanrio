# -*- coding: utf-8 -*-
"""
Flow unificado para dump de tabelas do banco de dados SISCOR - SECONSERVA

Este flow é responsável por extrair dados do banco SQL Server SISCOR
(Sistema de Conservação e Obras) e carregar no BigQuery. Processa qualquer
tabela baseado no parâmetro table_id.

Migrado de Prefect 1.4 para Prefect 3.0.
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

from pipelines.rj_seconserva__infraestrutura_siscor_obras.constants import (
    TABLE_CONFIGS,
    Constants,
)


@flow(log_prints=True)
def rj_seconserva__infraestrutura_siscor_obras(
    table_id: str,
    db_database: str = Constants.DB_DATABASE.value,
    db_host: str = Constants.DB_HOST.value,
    db_port: str = Constants.DB_PORT.value,
    db_type: str = Constants.DB_TYPE.value,
    db_charset: Optional[str] = "utf8",
    dataset_id: str = Constants.DATASET_ID.value,
    infisical_secret_path: str = Constants.INFISICAL_SECRET_PATH.value,
    dump_mode: Optional[str] = None,
    partition_date_format: str = "%Y-%m-%d",
    partition_columns: Optional[str] = None,
    lower_bound_date: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    retry_dump_upload_attempts: int = 2,
    batch_size: int = 50000,
    batch_data_type: str = "csv",
    biglake_table: Optional[bool] = None,
    log_number_of_batches: int = 100,
    max_concurrency: int = 1,
    only_staging_dataset: bool = False,
    add_timestamp_column: bool = True,
):
    """
    Flow unificado para dump de tabelas do SISCOR da SECONSERVA.

    Este flow extrai dados do banco SQL Server SISCOR (Sistema de Conservação e Obras)
    e carrega no BigQuery. Processa qualquer tabela configurada em TABLE_CONFIGS
    baseado no parâmetro table_id.

    O SISCOR é responsável por gerenciar processos de autorização de obras,
    incluindo informações sobre requerentes, executores, localização, pareceres
    e situação dos processos de obras públicas e privadas.

    Args:
        table_id: Nome da tabela a ser processada (ex: 'processo_autorizacao_obra')
        db_database: Nome do banco de dados SQL Server
        db_host: Host do banco de dados
        db_port: Porta do banco de dados
        db_type: Tipo do banco ('sql_server', 'mysql', 'postgresql')
        db_charset: Charset da conexão
        dataset_id: ID do dataset no BigQuery
        infisical_secret_path: Caminho do secret no Infisical com credenciais do banco
        dump_mode: Modo de dump ('overwrite' ou 'append'). Se None, usa configuração da tabela
        partition_date_format: Formato de data para particionamento
        partition_columns: Colunas de particionamento (separadas por vírgula)
        lower_bound_date: Data limite inferior para filtro
        break_query_frequency: Frequência de quebra de query ('day', 'week', 'month')
        break_query_start: Data de início para quebra de query
        break_query_end: Data de fim para quebra de query
        retry_dump_upload_attempts: Número de tentativas de retry
        batch_size: Tamanho do lote para processamento
        batch_data_type: Tipo de dado do batch ('csv' ou 'parquet')
        biglake_table: Se deve criar tabela BigLake. Se None, usa configuração da tabela
        log_number_of_batches: Número de batches para log
        max_concurrency: Número máximo de processos concorrentes
        only_staging_dataset: Se deve usar apenas dataset de staging
        add_timestamp_column: Se deve adicionar coluna de timestamp

    Raises:
        ValueError: Se table_id não estiver configurado em TABLE_CONFIGS

    Examples:
        Para processar a tabela processo_autorizacao_obra:
        >>> rj_seconserva__infraestrutura_siscor_obras(
        ...     table_id="processo_autorizacao_obra"
        ... )

        Para processar com modo específico:
        >>> rj_seconserva__infraestrutura_siscor_obras(
        ...     table_id="processo_autorizacao_obra",
        ...     dump_mode="append"
        ... )
    """
    # Renomear o flow run para facilitar identificação
    rename_current_flow_run_task(new_name=f"siscor_obras_{table_id}")

    # Injetar credenciais do BigQuery
    inject_bd_credentials_task(environment="prod")

    # Validar e obter configuração da tabela
    if table_id not in TABLE_CONFIGS:
        raise ValueError(
            f"Tabela '{table_id}' não configurada. "
            f"Tabelas disponíveis: {list(TABLE_CONFIGS.keys())}"
        )

    config = TABLE_CONFIGS[table_id]

    # Usar configurações da tabela se não fornecidas como parâmetro
    dump_mode = dump_mode or config.dump_mode
    biglake_table = biglake_table if biglake_table is not None else config.biglake_table

    # Obter credenciais do banco de dados do Infisical
    secrets = get_database_username_and_password_from_secret_task(
        infisical_secret_path=infisical_secret_path
    )

    # Parsear colunas de particionamento
    partition_columns_list = parse_comma_separated_string_to_list_task(text=partition_columns)

    # Formatar query com particionamento se necessário
    formatted_query = format_partitioned_query_task(
        query=config.execute_query,
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

    # Executar dump e upload para BigQuery
    dump_upload_batch_task(
        queries=formatted_query,
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

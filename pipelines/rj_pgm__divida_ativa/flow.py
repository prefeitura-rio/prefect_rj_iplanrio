# -*- coding: utf-8 -*-
"""
Flow de ingestão de dados do sistema DAM (Dívida Ativa Municipal) da PGM.

Este módulo implementa um flow do Prefect 3.0 para extrair dados do banco de dados
SQL Server do sistema de Dívida Ativa Municipal e carregar no BigQuery, utilizando
particionamento e processamento em lotes para otimização de performance.
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
def rj_pgm__divida_ativa(
    db_database: str = "DAM_PRD",
    db_host: str = "10.2.221.127",
    db_port: str = "1433",
    db_type: str = "sql_server",
    db_charset: Optional[str] = "NOT_SET",
    execute_query: str = "execute_query",
    dataset_id: str = "brutos_divida_ativa",
    table_id: str = "table_id",
    infisical_secret_path: str = "/db-divida-ativa",
    dump_mode: str = "overwrite",
    partition_date_format: str = "%Y-%m-%d",
    partition_columns: Optional[str] = None,
    lower_bound_date: Optional[str] = None,
    break_query_frequency: Optional[str] = None,
    break_query_start: Optional[str] = None,
    break_query_end: Optional[str] = None,
    retry_dump_upload_attempts: int = 3,
    batch_size: int = 50000,
    batch_data_type: str = "csv",
    biglake_table: bool = True,
    log_number_of_batches: int = 100,
    max_concurrency: int = 1,
    only_staging_dataset: bool = False,
    add_timestamp_column: bool = True,
):
    """
    Flow principal para ingestão de dados de Dívida Ativa Municipal no BigQuery.

    Este flow realiza a extração de dados do banco SQL Server do sistema DAM (Dívida Ativa
    Municipal) da Procuradoria Geral do Município e carrega os dados no BigQuery. O processo
    inclui autenticação via Infisical, formatação de queries com particionamento opcional,
    e upload em lotes para otimizar performance e uso de memória.

    Args:
        db_database: Nome do banco de dados no SQL Server.
            Default: "DAM_PRD"
        db_host: Endereço IP ou hostname do servidor SQL Server.
            Default: "10.2.221.127"
        db_port: Porta de conexão do SQL Server.
            Default: "1433"
        db_type: Tipo do banco de dados (sql_server, mysql, postgres, etc).
            Default: "sql_server"
        db_charset: Charset a ser utilizado na conexão. Use "NOT_SET" para não especificar.
            Default: "NOT_SET"
        execute_query: Query SQL a ser executada para extração dos dados.
            Default: "execute_query"
        dataset_id: ID do dataset no BigQuery onde os dados serão carregados.
            Default: "brutos_divida_ativa"
        table_id: ID da tabela no BigQuery onde os dados serão carregados.
            Default: "table_id"
        infisical_secret_path: Caminho do secret no Infisical contendo credenciais do banco.
            Default: "/db-divida-ativa"
        dump_mode: Modo de carga dos dados ("overwrite" para substituir, "append" para adicionar).
            Default: "overwrite"
        partition_date_format: Formato de data para particionamento (ex: "%Y-%m-%d", "%Y").
            Default: "%Y-%m-%d"
        partition_columns: Colunas a serem usadas para particionamento, separadas por vírgula.
            Default: None
        lower_bound_date: Data limite inferior para queries particionadas (ex: "2020-01-01").
            Suporta valores especiais como "current_year".
            Default: None
        break_query_frequency: Frequência de quebra da query para processamento incremental
            (ex: "1D" para diário, "1M" para mensal).
            Default: None
        break_query_start: Data inicial para quebra incremental de queries.
            Default: None
        break_query_end: Data final para quebra incremental de queries.
            Default: None
        retry_dump_upload_attempts: Número de tentativas de retry em caso de falha no upload.
            Default: 3
        batch_size: Tamanho do lote para processamento (número de linhas por batch).
            Default: 50000
        batch_data_type: Formato dos dados em lote ("csv" ou "parquet").
            Default: "csv"
        biglake_table: Se True, cria tabela como BigLake table para acesso externo.
            Default: True
        log_number_of_batches: Intervalo de batches para logging de progresso.
            Default: 100
        max_concurrency: Número máximo de uploads concorrentes.
            Default: 1
        only_staging_dataset: Se True, carrega apenas no dataset de staging (não em produção).
            Default: False
        add_timestamp_column: Se True, adiciona coluna com timestamp da ingestão.
            Default: True

    Returns:
        None. O flow executa as tasks de ingestão e upload sem retorno explícito.

    Example:
        >>> # Exemplo de execução via schedule no prefect.yaml:
        >>> # parameters:
        >>> #   table_id: CDA
        >>> #   execute_query: SELECT * FROM DAM_PRD.dbo.CDA

    Notes:
        - As credenciais de acesso ao banco são obtidas via Infisical
        - O flow injeta automaticamente as credenciais do BD no ambiente
        - O nome do flow run é renomeado para o table_id para facilitar monitoramento
        - Suporta particionamento por data com diferentes formatos e frequências
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

"""Flow para coleta de precipitação do AlertaRio via SFTP em landing zone GCS."""

from prefect import flow, get_run_logger
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from iplanrio.pipelines_utils.bd import (
    create_table_and_upload_to_gcs_task,
)
from iplanrio.pipelines_utils.env import getenv_or_action
from tasks import (
    get_max_date_from_bigquery_task,
    get_bucket_files_with_datetime_filter_task,
    process_multiple_xml_files_task,
    download_xml_files_from_list_task
)
from prefect.logging import get_logger
logger = get_run_logger()


@flow(log_prints=True, name="rj-cor-precipitacao-alertario-sftp")
def rj_cor__precipitacao_alertario_sftp(
    dataset_id_pluviometric: str = 'clima_pluviometro',
    table_id_pluviometric: str = 'taxa_precipitacao_alertario_5min',
    dataset_id_meteorological: str = 'clima_estacao_meteorologica',
    table_id_meteorological: str = 'meteorologia_alertario',
    dump_mode: str = "append",
    project_id: str = "rj-iplanrio",
    max_date_bigquery: str | None = None
) -> None:
    """Coleta e processa dados de precipitação do AlertaRio via SFTP.

    Flow orquestrado que coleta arquivos XML depositados via SFTP na landing zone
    GCS, extrai dados de precipitação pluviométrica e meteorológica, aplica
    transformações de limpeza e carrega os dados em tabelas BigQuery particionadas
    por data (ano/mês/dia).

    **Fluxo de Execução:**

    1. **Setup:** Renomeia a execução e injeta credenciais do banco de dados
    2. **Descoberta:** Obtém a data máxima já processada no BigQuery (marca d'água)
    3. **Filtragem:** Lista arquivos XML no bucket GCS com timestamp > marca d'água
    4. **Download:** Baixa o conteúdo de todos os XMLs novos do GCS
    5. **Processamento:** Para cada XML:
        - Parse XML e extração de registros pluviométricos e meteorológicos
        - Transformação: limpeza, deduplicação e seleção de colunas
        - Particionamento: agrupa dados por ano/mês/dia
        - Serialização: salva cada partição como arquivo CSV
    6. **Ingestão:** Upload das partições para tabelas BigQuery respectivas

    **Dados Extraídos:**

    - **Pluviométricos:** Acumulados de chuva em intervalos (5min, 10min, 15min, 30min,
      1h, 2h, 3h, 4h, 6h, 12h, 24h, 96h e acumulado mensal) por estação
    - **Meteorológicos:** Temperatura, umidade, sensação térmica, pressão, ponto de
      orvalho e velocidade/direção do vento por estação

    **Parâmetros:**

    :param dataset_id_pluviometric:
        ID do dataset BigQuery para armazenar dados pluviométricos.
        Padrão: 'clima_pluviometro'
    :param table_id_pluviometric:
        ID da tabela pluviométrica (será particionada por data_medicao).
        Padrão: 'taxa_precipitacao_alertario_5min'
    :param dataset_id_meteorological:
        ID do dataset BigQuery para armazenar dados meteorológicos.
        Padrão: 'clima_estacao_meteorologica'
    :param table_id_meteorological:
        ID da tabela meteorológica (será particionada por data_medicao).
        Padrão: 'meteorologia_alertario'
    :param dump_mode:
        Modo de inserção no BigQuery ('append' adiciona dados, 'overwrite' substitui).
        Padrão: 'append'
    :param project_id:
        ID do projeto Google Cloud onde estão os datasets.
        Padrão: 'rj-iplanrio'
    :param max_date_bigquery:
        Data máxima em formato ISO (YYYY-MM-DD HH:MM:SS) para filtrar arquivos.
        Se None, consulta automaticamente a data máxima na tabela BigQuery.
        Padrão: None

    :return: None

    **Exemplo de Uso:**

    .. code-block:: python

        from pipelines.rj_cor__precipitacao_alertario_sftp.flow import (
            rj_cor__precipitacao_alertario_sftp
        )

        # Execução com parâmetros padrão (recomendado para produção)
        rj_cor__precipitacao_alertario_sftp()

        # Execução com data inicial específica (útil para reprocessamento)
        rj_cor__precipitacao_alertario_sftp(
            max_date_bigquery="2026-07-01 00:00:00"
        )

        # Execução em modo sobrescrita (cuidado: substitui dados existentes)
        rj_cor__precipitacao_alertario_sftp(
            dump_mode="overwrite",
            max_date_bigquery="2026-08-01 00:00:00"
        )

    """
    # Setup
    rename_current_flow_run_task(new_name="precipitacao-alertario-sftp")
    inject_bd_credentials_task(environment="prod")

    bucket_name = getenv_or_action("bucket-nimbus")
    prefix = getenv_or_action("prefix")

    logger.info("🌧️  Iniciando coleta de dados de precipitação AlertaRio via SFTP")

    # Step 1: Listar arquivos XML novos
    logger.info("📥 Listando arquivos XML na landing zone...")
    if max_date_bigquery is None:
        bq = get_max_date_from_bigquery_task(
            project_id=project_id
        )
    else:
        bq = max_date_bigquery

    xml_files = get_bucket_files_with_datetime_filter_task(
        max_datetime_from_bq=bq,
        bucket_name=bucket_name,
        prefix=prefix
    )

    content = download_xml_files_from_list_task(
        bucket_name=bucket_name,
        file_names=xml_files
    )

    # Validar se houve download de arquivos
    if not content:
        logger.warning("Nenhum arquivo XML foi baixado. Finalizando pipeline.")
        return

    pluviometric_path, meteorological_path = process_multiple_xml_files_task(
        xml_contents=content
    )

    if dataset_id_pluviometric is not None and pluviometric_path is not None:
        logger.info("📤 Enviando dados pluviométricos para BigQuery: %s", pluviometric_path)
        create_table_and_upload_to_gcs_task(
            data_path=pluviometric_path,
            dataset_id=dataset_id_pluviometric,
            table_id=table_id_pluviometric,
            dump_mode=dump_mode,
        )

    if dataset_id_meteorological is not None and meteorological_path is not None:
        logger.info("📤 Enviando dados meteorológicos para BigQuery: %s", meteorological_path)
        create_table_and_upload_to_gcs_task(
            data_path=meteorological_path,
            dataset_id=dataset_id_meteorological,
            table_id=table_id_meteorological,
            dump_mode=dump_mode
    )
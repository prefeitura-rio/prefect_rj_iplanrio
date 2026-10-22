"""Flow para coleta de precipitação do AlertaRio via SFTP em landing zone GCS."""

from pathlib import Path

from prefect import flow
from prefect.logging import get_logger
from tasks import (
    download_xml_files_from_list_task,
    get_bucket_files_with_datetime_filter_task,
    get_max_date_from_bigquery_task,
    process_multiple_xml_files_task,
)

from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options, read_env

logger = get_logger("toolkit")


@flow(log_prints=True, name="rj-cor-precipitacao-alertario-sftp", flow_run_name="precipitacao-alertario-sftp")
async def rj_cor__precipitacao_alertario_sftp(
    *,
    cloud: Cloud,
    dataset_id_pluviometric: str = 'clima_pluviometro',
    table_id_pluviometric: str = 'taxa_precipitacao_alertario_5min',
    dataset_id_meteorological: str = 'clima_estacao_meteorologica',
    table_id_meteorological: str = 'meteorologia_alertario',
    dump_mode: str = "append",
    project_id: str = "rj-iplanrio",
    max_date_bigquery: str | None = None
) -> None:
    """Coleta dados de precipitação do AlertaRio via arquivos XML em GCS.

    Flow que processa arquivos XML depositados na landing zone GCS por
    sistema SFTP. Extrai dados de precipitação e meteorologia, transforma
    para formato padrão e carrega em tabelas BigQuery particionadas.

    **Processo:**
    1. Renomeia execução do flow
    2. Injeta credenciais do banco de dados
    3. Lista arquivos XML no bucket da landing zone
    4. Para cada arquivo:
        - Faz download do XML do GCS
        - Faz parse e extrai dados pluviométricos e meteorológicos
        - Transforma dados para formato padrão
        - Salva dados como csv particionado
        - Faz upload para BigQuery em tabelas separadas

    **Parâmetros:**

    :param dataset_id_pluviometric: ID do dataset BigQuery para dados
        pluviométricos (padrão: clima_pluviometro).
    :param table_id_pluviometric: ID da tabela pluviométrica
        (padrão: taxa_precipitacao_alertario_5min).
    :param dataset_id_meteorological: ID do dataset BigQuery para dados
        meteorológicos (padrão: clima_estacao_meteorologica).
    :param table_id_meteorological: ID da tabela meteorológica
        (padrão: meteorologia_alertario).
    :param dump_mode: Modo de salvamento no BigQuery
        (padrão: "append", alternativa: "overwrite").
    :param project_id: ID do projeto Google Cloud
        (padrão: rj-iplanrio).
    :param max_date_bigquery: Data máxima para filtro de arquivos no formato string.
        Se None, consulta automaticamente a data máxima do BigQuery
        (padrão: None).

    **Returns:**
        None
    """
    # Setup

    bucket_name = read_env("bucket-nimbus")
    prefix = read_env("prefix")

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
        table = Table(dataset_id=dataset_id_pluviometric, table_id=table_id_pluviometric)
        await upload_files_to_gcs(
            Path(pluviometric_path),
            table,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format="csv",
            options=capture_options(dump_mode),
        )
        await publish_table_in_bq(table, cloud)

    if dataset_id_meteorological is not None and meteorological_path is not None:
        logger.info("📤 Enviando dados meteorológicos para BigQuery: %s", meteorological_path)
        table = Table(dataset_id=dataset_id_meteorological, table_id=table_id_meteorological)
        await upload_files_to_gcs(
            Path(meteorological_path),
            table,
            bucket=cloud["bucket"],
            dump_mode=dump_mode,
            source_format="csv",
            options=capture_options(dump_mode),
        )
        await publish_table_in_bq(table, cloud)

"""Tasks para pipeline de precipitação AlertaRio via SFTP."""

import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from prefect import task

from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

from pipelines.rj_cor__precipitacao_alertario_sftp import utils

logger = get_logger(__name__)


def extract_datetime_from_blob_name(blob_name: str) -> datetime | None:
    """Extrai o datetime completo do nome do arquivo no bucket GCS.

    Procura por padrões de datetime no nome do arquivo.
    Formato esperado: Chuvas_YYYYMMDDHHMMSS.xml (ex: Chuvas_20260717113729.xml)

    :param blob_name: Nome do blob no GCS.
    :returns: Datetime extraído ou None se não encontrado.
    """
    # Padrão para YYYYMMDDHHMMSS (formato: Chuvas_20260717113729.xml)
    match = re.search(r"Chuvas_(\d{14})", blob_name)
    if match:
        try:
            datetime_str = match.group(1)
            year = int(datetime_str[0:4])
            month = int(datetime_str[4:6])
            day = int(datetime_str[6:8])
            hour = int(datetime_str[8:10])
            minute = int(datetime_str[10:12])
            second = int(datetime_str[12:14])
            return datetime(year, month, day, hour, minute, second)
        except (ValueError, TypeError, IndexError):
            pass

    return None


def get_name_bucket_folder() -> tuple[str, str]:
    """Obtém o nome do bucket e da pasta a partir da variável de ambiente.

    :returns: Tupla (bucket_name, folder_name).
    """
    bucket_name = os.getenv("BUCKET", "")
    folder_name = os.getenv("FOLDER", "")

    if not bucket_name:
        logger.error("Variável de ambiente BUCKET não definida")
        raise ValueError("Variável de ambiente BUCKET não definida")

    if not folder_name:
        logger.error("Variável de ambiente FOLDER não definida")
        raise ValueError("Variável de ambiente FOLDER não definida")

    return bucket_name, folder_name


@task
def parse_xml_task(
    xml_content: str,
    source_file: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse XML AlertaRio em DataFrames pluviométrico e meteorológico.

    Extrai registros do XML, cria DataFrames a partir deles
    e retorna dois DataFrames: um com dados de chuva e outro
    com dados meteorológicos.

    :param xml_content: Conteúdo XML como string.
    :param source_file: Nome do arquivo origem.
    :returns: Tupla (DataFrame pluviométrico, DataFrame meteorológico).
    """
    logger.info("Fazendo parsing do arquivo XML: %s", source_file)

    pluviometric_records, meteorological_records = utils.parse_xml_to_records(
        xml_content=xml_content,
        source_file=source_file,
    )

    dfr_pluv = pd.DataFrame(pluviometric_records)
    dfr_met = pd.DataFrame(meteorological_records)

    logger.info(
        "Parse concluído: %d pluviométricos, %d meteorológicos",
        len(dfr_pluv),
        len(dfr_met),
    )

    return dfr_pluv, dfr_met


@task
def transform_pluviometric_data_task(dfr: pd.DataFrame) -> pd.DataFrame:
    """Transforma dados pluviométricos brutos para formato BigQuery.

    Aplica renomeação de colunas, parsing de datas, limpeza de
    duplicatas e seleção de colunas esperadas.

    :param dfr: DataFrame com dados brutos.
    :returns: DataFrame transformado.
    """
    logger.info("Transformando dados pluviométricos")

    dfr_transformed = utils.transform_pluviometric_dataframe(dfr)

    return dfr_transformed


@task
def transform_meteorological_data_task(dfr: pd.DataFrame) -> pd.DataFrame:
    """Transforma dados meteorológicos brutos para formato BigQuery.

    Aplica renomeação de colunas, parsing de datas, limpeza de
    duplicatas e seleção de colunas esperadas.

    :param dfr: DataFrame com dados brutos.
    :returns: DataFrame transformado.
    """
    logger.info("Transformando dados meteorológicos")

    dfr_transformed = utils.transform_meteorological_dataframe(dfr)

    return dfr_transformed


@task
def save_pluviometric_data_to_parquet_task(
    dfr: pd.DataFrame,
) -> Path:
    """Salva dados pluviométricos em partições Parquet.

    Particiona os dados por data (ano/mes/data) e salva
    cada partição como arquivo Parquet comprimido.

    :param dfr: DataFrame com dados processados.
    :returns: Caminho do diretório raiz das partições.
    """
    logger.info("Salvando dados pluviométricos em partições Parquet")

    path = utils.save_dataframe_to_parquet_partitions(
        dfr=dfr,
        data_type="pluviometric",
        partition_column="data_medicao",
    )

    logger.info("Dados pluviométricos salvos em: %s", path)

    return path


@task
def save_meteorological_data_to_parquet_task(
    dfr: pd.DataFrame,
) -> Path:
    """Salva dados meteorológicos em partições Parquet.

    Particiona os dados por data (ano/mes/data) e salva
    cada partição como arquivo Parquet comprimido.

    :param dfr: DataFrame com dados processados.
    :returns: Caminho do diretório raiz das partições.
    """
    logger.info("Salvando dados meteorológicos em partições Parquet")

    path = utils.save_dataframe_to_parquet_partitions(
        dfr=dfr,
        data_type="meteorological",
        partition_column="data_medicao",
    )

    logger.info("Dados meteorológicos salvos em: %s", path)

    return path


@task(retries=3, retry_delay_seconds=10)
def get_max_date_from_bigquery_task(
    project_id: str = "rj-cor",
    dataset_id: str = "clima_pluviometro_staging",
    table_id: str = "taxa_precipitacao_alertario_5min",
) -> datetime | None:
    """Obtém a data máxima de medicação do BigQuery.

    Executa a query SQL para obter a data máxima de data_medicao
    da tabela de precipitação alertário.

    :param project_id: ID do projeto GCP.
    :param dataset_id: ID do dataset BigQuery.
    :param table_id: ID da tabela BigQuery.
    :returns: Data máxima como datetime ou None se nenhum dado existir.
    """
    logger.info(
        "Obtendo data máxima do BigQuery: %s.%s.%s",
        project_id,
        dataset_id,
        table_id,
    )

    try:
        query = load_query(
            __file__,
            "get_max_update",
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )

        client = bigquery.Client(project=project_id)
        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            max_date = row.max_data_medicao
            if max_date:
                logger.info("Data máxima encontrada: %s", max_date)
                return max_date
            else:
                logger.warning("Nenhuma data máxima encontrada na tabela")
                return None

    except Exception as e:
        logger.error("Erro ao obter data máxima do BigQuery: %s", e)
        raise


@task(retries=3, retry_delay_seconds=10)
def get_bucket_files_with_datetime_filter_task(
    bucket_name: str = "",
    prefix: str = "",
    max_datetime_from_bq: datetime | None = None,
) -> list[str]:
    """Filtra arquivos do bucket com datetime maior que o do BigQuery.

    Lista arquivos XML no bucket com prefixo especificado e retorna apenas
    aqueles cujo datetime extraído do nome é maior que o datetime máximo do BigQuery.

    :param bucket_name: Nome do bucket GCS.
    :param prefix: Prefixo dos arquivos no bucket.
    :param max_datetime_from_bq: Datetime máximo do BigQuery para comparação.
    :returns: Lista de nomes de arquivo XML com datetimes maiores.
    """
    logger.info(
        "Listando arquivos do bucket %s com prefixo %s",
        bucket_name,
        prefix,
    )

    if max_datetime_from_bq:
        logger.info("Filtrando arquivos com datetime maior que: %s", max_datetime_from_bq)

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        xml_files = [blob.name for blob in blobs if blob.name.endswith(".xml")]

        logger.info("Total de arquivos XML encontrados: %d", len(xml_files))

        if not max_datetime_from_bq:
            logger.warning(
                "Datetime máximo do BigQuery não definido, retornando todos os arquivos"
            )
            return xml_files

        filtered_files = []
        for file_name in xml_files:
            file_datetime = extract_datetime_from_blob_name(file_name)

            if file_datetime is None:
                logger.warning(
                    "Não foi possível extrair datetime do arquivo: %s", file_name
                )
                continue

            # Comparar datetimes completos
            if file_datetime > max_datetime_from_bq:
                filtered_files.append(file_name)
                logger.info(
                    "Arquivo incluído (%s > %s): %s",
                    file_datetime,
                    max_datetime_from_bq,
                    file_name,
                )
            else:
                logger.debug(
                    "Arquivo excluído (%s <= %s): %s",
                    file_datetime,
                    max_datetime_from_bq,
                    file_name,
                )

        logger.info(
            "Arquivos após filtro: %d de %d", len(filtered_files), len(xml_files)
        )

        return filtered_files

    except Exception as e:
        logger.error("Erro ao listar e filtrar arquivos do GCS: %s", e)
        raise

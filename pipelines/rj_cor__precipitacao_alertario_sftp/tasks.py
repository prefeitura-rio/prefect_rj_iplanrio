"""Tasks para pipeline de precipitação AlertaRio via SFTP."""

from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import storage
from prefect import task
from prefect_rj_iplanrio.logging import get_logger

from pipelines.rj_cor__precipitacao_alertario_sftp import utils

logger = get_logger(__name__)


@task(retries=3, retry_delay_seconds=10)
def list_xml_files_from_gcs_task(
    bucket_name: str,
    prefix: str = "Chuvas_",
) -> list[str]:
    """Lista arquivos XML novos no bucket GCS da landing zone.

    Conecta ao GCS, lista blobs com o prefixo especificado, filtra por
    extensão .xml e retorna lista de nomes de arquivo.

    :param bucket_name: Nome do bucket GCS (ex: rj-iplanrio-filemage.nimbus).
    :param prefix: Prefixo do arquivo para filtrar (ex: Chuvas_).
    :returns: Lista de nomes de arquivo XML encontrados.
    """
    logger.info("Listando arquivos XML no bucket %s com prefixo %s", bucket_name, prefix)

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=prefix))

        xml_files = [blob.name for blob in blobs if blob.name.endswith(".xml")]

        logger.info("Encontrados %d arquivos XML", len(xml_files))

        return xml_files

    except Exception as e:
        logger.error("Erro ao listar arquivos do GCS: %s", e)
        raise


@task(retries=3, retry_delay_seconds=10)
def download_xml_from_gcs_task(
    bucket_name: str,
    file_name: str,
) -> str:
    """Baixa conteúdo XML do bucket GCS.

    Conecta ao GCS, faz download do blob especificado e retorna
    seu conteúdo como string.

    :param bucket_name: Nome do bucket GCS.
    :param file_name: Nome do arquivo a ser baixado.
    :returns: Conteúdo XML como string.
    :raises google.cloud.exceptions.NotFound: Se arquivo não existir.
    """
    logger.info("Baixando arquivo %s do bucket %s", file_name, bucket_name)

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        xml_content = blob.download_as_string().decode("utf-8")

        logger.info("Arquivo %s baixado com sucesso", file_name)

        return xml_content

    except Exception as e:
        logger.error("Erro ao baixar arquivo %s: %s", file_name, e)
        raise


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

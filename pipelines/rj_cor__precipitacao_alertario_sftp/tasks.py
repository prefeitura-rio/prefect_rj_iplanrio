"""Tasks para pipeline de precipitação AlertaRio.

Encapsula operações do Prefect relacionadas a download, processamento
e ingestão de dados de precipitação AlertaRio no BigQuery.
"""

import os
import re
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from prefect import task
from prefect.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

from pipelines.rj_cor__precipitacao_alertario_sftp import utils

logger = get_logger(__name__)




@task(retries=3, retry_delay_seconds=10)
def get_max_date_from_bigquery_task(
    project_id,
    dataset_id: str = "clima_pluviometro_staging",
    table_id: str = "taxa_precipitacao_alertario_5min",
) -> datetime | None:
    """Obtém a data máxima de medição do BigQuery.

    Executa a query SQL para obter a data máxima de ``data_medicao``
    da tabela de precipitação AlertaRio, servindo como marca d'água
    para filtrar arquivos novos no bucket GCS.

    :param project_id: ID do projeto GCP.
    :param dataset_id: ID do dataset BigQuery.
    :param table_id: ID da tabela BigQuery.
    :param credentials_path: Caminho para o arquivo de credenciais.
    :returns: Data máxima como datetime ou None se nenhum dado existir.
    :raises Exception: Se houver erro ao executar a query.
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
                logger.info("Nenhuma data máxima encontrada na tabela")
                return None

    except Exception as e:
        logger.error("Erro ao obter data máxima do BigQuery: %s", e)
        raise

def extract_datetime_from_blob_name(blob_name: str) -> datetime | None:
    """Extrai o datetime completo do nome do arquivo AlertaRio.

    Procura pelo padrão ``Chuvas_YYYYMMDDHHMMSS.xml`` no nome do arquivo.
    Exemplo: ``Chuvas_20260717113729.xml`` → ``datetime(2026, 7, 17, 11, 37, 29)``.

    :param blob_name: Nome do blob no GCS.
    :returns: Datetime extraído ou None se o padrão não for encontrado.
    """
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


@task(retries=3, retry_delay_seconds=10)
def get_bucket_files_with_datetime_filter_task(
    bucket_name: str,
    prefix: str,
    max_datetime_from_bq: datetime | str | None,
) -> list[str]:
    """Filtra arquivos XML do bucket com datetime maior que a marca d'água.

    Lista todos os arquivos XML no bucket com prefixo especificado e retorna
    apenas aqueles cujo datetime extraído do nome é maior que o máximo
    encontrado no BigQuery, minimizando re-processamento.

    :param bucket_name: Nome do bucket GCS.
    :param prefix: Prefixo dos arquivos no bucket.
    :param credentials_path: Caminho para o arquivo de credenciais.
    :returns: Lista de nomes de arquivo XML com datetimes maiores.
    :raises Exception: Se houver erro ao listar ou filtrar arquivos.
    """
    logger.info(
        "Listando arquivos do bucket %s com prefixo %s",
        bucket_name,
        prefix,
    )

    # Converter string para datetime se necessário
    if isinstance(max_datetime_from_bq, str):
        try:
            max_datetime_from_bq = datetime.fromisoformat(max_datetime_from_bq)
        except ValueError:
            try:
                max_datetime_from_bq = datetime.strptime(
                    max_datetime_from_bq, "%Y-%m-%d %H:%M:%S"
                )
            except ValueError:
                logger.warning(
                    "Não foi possível converter datetime: %s", max_datetime_from_bq
                )
                max_datetime_from_bq = None

    if max_datetime_from_bq:
        logger.info("Filtrando arquivos com datetime maior que: %s", max_datetime_from_bq)

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        blobs = list(bucket.list_blobs(prefix=prefix))

        xml_files = [blob.name for blob in blobs if blob.name.endswith(".xml")]

        logger.info("Total de arquivos XML encontrados: %d", len(xml_files))

        if not max_datetime_from_bq:
            logger.info(
                "Datetime máximo do BigQuery não definido, retornando todos os arquivos"
            )
            return xml_files

        filtered_files = []
        for file_name in xml_files:
            file_datetime = extract_datetime_from_blob_name(file_name)

            if file_datetime is None:
                logger.debug(
                    "Não foi possível extrair datetime do arquivo: %s", file_name
                )
                continue

            # Comparar datetimes
            if file_datetime > max_datetime_from_bq:
                filtered_files.append(file_name)
                logger.debug(
                    "Arquivo incluído: %s", file_name
                )

        logger.info(
            "Arquivos após filtro: %d de %d", len(filtered_files), len(xml_files)
        )

        return filtered_files

    except Exception as e:
        logger.error("Erro ao listar e filtrar arquivos do GCS: %s", e)
        raise


@task(retries=3, retry_delay_seconds=10)
def download_xml_files_from_list_task(
    bucket_name: str,
    file_names: list[str],
) -> list[str]:
    """Baixa múltiplos arquivos XML do GCS.

    Conecta ao bucket GCS e baixa os arquivos XML especificados,
    retornando uma lista com os conteúdos de cada arquivo como strings.

    :param bucket_name: Nome do bucket GCS.
    :param file_names: Lista de nomes de arquivos XML a baixar.
    :param credentials_path: Caminho para o arquivo de credenciais.
    :returns: Lista com conteúdos XML como strings.
    :raises Exception: Se houver erro ao baixar qualquer arquivo.
    """
    return utils.download_xml_files_from_gcs(
        bucket_name=bucket_name,
        file_names=file_names,
    )


@task
def process_multiple_xml_files_task(
    xml_contents: list[str],
) -> tuple[list[str], list[str]]:
    """Processa múltiplos XMLs e salva um arquivo de partição por timestamp.

    Wrapper Prefect que orquestra o processamento completo: parse, transformação
    e salvamento em partições CSV. Cada XML gera um arquivo de partição separado
    em seus respectivos diretórios (pluviométricos e meteorológicos).

    :param xml_contents: Lista com conteúdos XML como strings.
    :returns: Tupla (lista de caminhos pluviométricos, lista de caminhos meteorológicos) como strings.
    :raises Exception: Se houver erro no processamento de qualquer XML.
    """
    pluviometric_path, meteorological_path = utils.process_multiple_xml_files(
        xml_contents=xml_contents,
    )

    return pluviometric_path, meteorological_path




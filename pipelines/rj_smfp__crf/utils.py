"""Utility functions for GCS ZIP file handling and extraction."""

import io
import re
import zipfile
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.cloud.storage import Bucket, Client
from iplanrio.pipelines_utils.pandas import to_partitions
from loguru import logger
from prefect_rj_iplanrio.sql import load_query

from pipelines.rj_smfp__crf.constants import (
    FwfTableConfig,
    FWF_PERIODOS_CONFIG,
    FWF_PERIODOS_MEI_CONFIG,
    FWF_EVENTOS_CONFIG,
    FWF_EVENTOS_MEI_CONFIG,
)




def get_max_date_from_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
) -> date | None:
    """Obtém a maior data de partição da tabela CRF no BigQuery.

    Executa a query SQL para obter o valor máximo de ``data_particao``
    da tabela especificada, servindo como marca d'água para filtrar
    arquivos ZIP novos no bucket GCS.

    :param project_id: ID do projeto GCP.
    :param dataset_id: ID do dataset BigQuery.
    :param table_id: ID da tabela BigQuery.
    :returns: Data máxima como ``datetime.date`` ou ``None`` se a tabela estiver vazia.
    :raises Exception: Se houver erro ao executar a query.
    """
    logger.info(
        "Obtendo data máxima de partição do BigQuery: %s.%s.%s",
        project_id,
        dataset_id,
        table_id,
    )

    try:
        query = load_query(
            __file__,
            "get_max_partition",
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )

        client = bigquery.Client(project=project_id)
        query_job = client.query(query)
        results = query_job.result()

        for row in results:
            max_date = row.max_data_particao
            if max_date:
                logger.info("Data máxima de partição encontrada: %s", max_date)
                return max_date
            else:
                logger.info("Nenhuma data de partição encontrada na tabela")
                return None

    except Exception as e:
        logger.error("Erro ao obter data máxima de partição do BigQuery: %s", e)
        raise


def extract_date_from_blob_name(blob_name: str) -> date | None:
    """Extrai a data do nome de um arquivo ZIP CRF.

    Procura pelo padrão ``DDMMYYYY`` no nome do arquivo.
    Exemplo: ``BX-22518948-EVE-03012021.zip`` → ``date(2021, 1, 3)``.

    :param blob_name: Nome do blob no GCS (pode incluir prefixo de pasta).
    :returns: Data extraída como ``datetime.date`` ou ``None`` se o padrão não
        for encontrado ou a data for inválida.
    """
    filename = blob_name.split("/")[-1]
    match = re.search(r"-(\d{8})\.zip$", filename, re.IGNORECASE)
    if match:
        date_str = match.group(1)
        try:
            day = int(date_str[0:2])
            month = int(date_str[2:4])
            year = int(date_str[4:8])
            return date(year, month, day)
        except (ValueError, TypeError):
            logger.debug("Data inválida extraída de '%s': %s", blob_name, date_str)
    return None


def list_zip_files_in_gcs_folder(
    bucket: Bucket,
    folder_prefix: str,
    max_date_from_bq: date | str | None = None,
) -> list[str]:
    """Lista arquivos ZIP no GCS filtrando por data maior que a partição do BQ.

    Lista todos os blobs ZIP no bucket com o prefixo especificado e retorna
    apenas aqueles cuja data extraída do nome do arquivo é posterior à data
    máxima de partição encontrada no BigQuery.

    Se ``max_date_from_bq`` for ``None``, todos os arquivos ZIP são retornados.

    :param bucket: Google Cloud Storage bucket object.
    :param folder_prefix: Prefixo do caminho da pasta no bucket (ex.: ``'PERIODOS_EVENTOS/'``).
    :param max_date_from_bq: Data máxima de partição do BigQuery como ``datetime.date``
        ou string ``YYYY-MM-DD``. Arquivos com data igual ou anterior são ignorados.
    :returns: Lista de nomes de blob (caminhos completos) para arquivos ZIP filtrados.
    """
    # Converter string para date se necessário
    if isinstance(max_date_from_bq, str):
        try:
            max_date_from_bq = date.fromisoformat(max_date_from_bq)
        except ValueError:
            logger.warning(
                "Não foi possível converter max_date_bigquery '%s' para date, ignorando filtro",
                max_date_from_bq,
            )
            max_date_from_bq = None

    if max_date_from_bq:
        logger.info(
            "Listando arquivos ZIP do bucket '%s' com prefixo '%s' (filtrando após %s)",
            bucket.name,
            folder_prefix,
            max_date_from_bq,
        )
    else:
        logger.info(
            "Listando arquivos ZIP do bucket '%s' com prefixo '%s' (sem filtro de data)",
            bucket.name,
            folder_prefix,
        )

    blobs = bucket.list_blobs(prefix=folder_prefix)
    zip_blobs = [blob.name for blob in blobs if blob.name.endswith(".zip")]

    logger.info("Total de arquivos ZIP encontrados: %d", len(zip_blobs))

    if not max_date_from_bq:
        return zip_blobs

    filtered = []
    for blob_name in zip_blobs:
        file_date = extract_date_from_blob_name(blob_name)
        if file_date is None:
            logger.debug("Não foi possível extrair data de '%s', ignorando", blob_name)
            continue
        if file_date > max_date_from_bq:
            filtered.append(blob_name)
            logger.debug("Arquivo incluído: %s (data=%s)", blob_name, file_date)
        else:
            logger.debug(
                "Arquivo ignorado: %s (data=%s <= max=%s)",
                blob_name,
                file_date,
                max_date_from_bq,
            )

    logger.info(
        "Arquivos após filtro de data: %d de %d", len(filtered), len(zip_blobs)
    )
    return filtered


def download_and_extract_zip_from_gcs(
    bucket: Bucket, blob_name: str, extract_path: str
) -> str:
    """Download a ZIP file from GCS and extract its contents locally.

    Downloads the ZIP blob from Google Cloud Storage to memory, extracts all
    files to the specified directory, and returns the extraction directory path.

    :param bucket: Google Cloud Storage bucket object.
    :param blob_name: Full blob name/path in the bucket (e.g., 'data/crf/file.zip').
    :param extract_path: Local directory path where files will be extracted.
    :returns: Path to the extraction directory.
    :raises FileNotFoundError: If the blob does not exist in the bucket.
    :raises zipfile.BadZipFile: If the downloaded file is not a valid ZIP.
    """
    blob = bucket.blob(blob_name)

    if not blob.exists():
        raise FileNotFoundError(
            f"Blob '{blob_name}' not found in bucket '{bucket.name}'"
        )

    # Download to memory
    zip_content = blob.download_as_bytes()

    # Extract to local directory
    extract_dir = Path(extract_path)
    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            zip_file.extractall(extract_dir)
    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(f"File '{blob_name}' is not a valid ZIP file") from e

    return str(extract_dir)


def list_extracted_files(extract_path: str) -> list[str]:
    """List all files extracted to a directory.

    :param extract_path: Path to the directory containing extracted files.
    :returns: List of relative file paths (relative to extract_path).
    """
    extract_dir = Path(extract_path)

    if not extract_dir.exists():
        return []

    files = []
    for file in extract_dir.rglob("*"):
        if file.is_file():
            relative_path = file.relative_to(extract_dir)
            files.append(str(relative_path))

    return files


def get_gcs_bucket(project_id: str, bucket_name: str) -> Bucket:
    """Get a GCS bucket object.

    :param project_id: Google Cloud project ID.
    :param bucket_name: Name of the bucket.
    :returns: Google Cloud Storage Bucket object.
    """
    client = Client(project=project_id)
    bucket = client.bucket(bucket_name)
    return bucket


def read_extracted_fwf_file(
    extract_path: str,
    table_id: str,
    extract_base_path: str
) -> tuple[pd.DataFrame, str]:
    """Lê um arquivo de formato de largura fixa (FWF) de um diretório descompactado.

    Busca um arquivo correspondente ao padrão de arquivo do `table_id` no
    diretório de descompactação e o lê usando as especificações de coluna
    pré-definidas para a tabela. Adiciona automaticamente as colunas
    `arquivo_origem` (nome completo do arquivo) e `data_referencia` (data extraída
    do nome do arquivo no formato AAAAMMDD).

    :param extract_path: Caminho do diretório contendo os arquivos descompactados.
    :param table_id: Identificador da tabela. Valores válidos: 'periodos',
        'periodos_mei', 'eventos', 'eventos_mei'.
    :returns: DataFrame contendo os dados do arquivo FWF parseado com colunas
        adicionais `arquivo_origem` e `data_referencia`.
    :raises FileNotFoundError: Se nenhum arquivo for encontrado no diretório.
    :raises ValueError: Se múltiplos arquivos forem encontrados ou table_id inválido.
    :raises pd.errors.ParserError: Se houver erro ao fazer parsing do arquivo.
    """
    # Mapa de configurações por table_id
    config_map = {
        "periodos_simples": FWF_PERIODOS_CONFIG,
        "periodos_mei": FWF_PERIODOS_MEI_CONFIG,
        "eventos_simples": FWF_EVENTOS_CONFIG,
        "eventos_mei": FWF_EVENTOS_MEI_CONFIG,
    }

    if table_id not in config_map:
        raise ValueError(
            f"table_id '{table_id}' inválido. "
            f"Válidos: {', '.join(config_map.keys())}"
        )

    config = config_map[table_id]
    extract_dir = Path(extract_path)

    if not extract_dir.exists():
        raise FileNotFoundError(
            f"Diretório de descompactação não encontrado: {extract_path}"
        )

    # Busca arquivos correspondentes ao padrão
    matching_files = list(extract_dir.glob(config.file_pattern))

    if not matching_files:
        raise FileNotFoundError(
            f"Nenhum arquivo com padrão '{config.file_pattern}' encontrado em "
            f"{extract_path} para a tabela '{table_id}'"
        )

    if len(matching_files) > 1:
        raise ValueError(
            f"Múltiplos arquivos com padrão '{config.file_pattern}' encontrados "
            f"em {extract_path} para a tabela '{table_id}'. "
            f"Arquivos encontrados: {[f.name for f in matching_files]}"
        )

    file_path = matching_files[0]

    try:
        df = pd.read_fwf(
            file_path,
            colspecs=config.colspecs,
            names=config.names,
            dtype=str,
            encoding=config.encoding,
        )

        # Adicionar coluna com nome completo do arquivo
        arquivo_nome = file_path.name
        df["arquivo_origem"] = arquivo_nome

        # Extrair data de referência do nome do arquivo (formato: 00-XXX-AAAAMMDD.txt)
        # Extrai apenas os 8 dígitos da data (AAAAMMDD)
        nome_sem_ext = arquivo_nome.replace(".txt", "").replace(".TXT", "")
        partes = nome_sem_ext.split("-")
        if len(partes) >= 3:
            data_referencia = partes[-1]
            df["data_referencia"] = data_referencia

            df['ano_particao'] = df["data_referencia"].apply(lambda x: str(x)[0:4])[0]
            df['mes_particao'] = df["data_referencia"].apply(lambda x: str(x)[4:6])[0]
            df['data_particao'] = pd.to_datetime(df["data_referencia"], format="%Y%m%d").dt.strftime("%Y-%m-%d")

            data_path = f"{extract_base_path}/{table_id}"
            to_partitions(
            data=df,
            savepath=f"{extract_base_path}/{table_id}",
            data_type="parquet",
            partition_columns=["ano_particao", "mes_particao", "data_particao"],
        )
        return df, data_path

    except Exception as e:
        raise pd.errors.ParserError(
            f"Erro ao fazer parsing do arquivo FWF '{file_path}' para "
            f"a tabela '{table_id}': {e}"
        ) from e


def cleanup_extracted_directory(extract_path: str) -> None:
    """Remove todos os arquivos de um diretório descompactado.

    Deleta recursivamente todos os arquivos no diretório, útil para limpeza
    após o processamento de um lote de arquivos FWF.

    :param extract_path: Caminho do diretório a ser limpo.
    :raises FileNotFoundError: Se o diretório não existir.
    :raises Exception: Se houver erro ao remover arquivos.
    """
    extract_dir = Path(extract_path)

    if not extract_dir.exists():
        raise FileNotFoundError(
            f"Diretório não encontrado: {extract_path}"
        )

    try:
        for file_path in extract_dir.rglob("*"):
            if file_path.is_file():
                file_path.unlink()
    except Exception as e:
        raise Exception(
            f"Erro ao limpar diretório {extract_path}: {e}"
        ) from e

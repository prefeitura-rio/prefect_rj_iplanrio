"""Tasks for GCS ZIP file handling and extraction."""

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger
from iplanrio.pipelines_utils.pandas import to_partitions
import utils


@task
def list_zip_files_task(
    project_id: str, bucket_name: str, folder_prefix: str
) -> list[str]:
    """List all ZIP files in a GCS folder.

    :param project_id: Google Cloud project ID.
    :param bucket_name: Name of the GCS bucket.
    :param folder_prefix: Folder path prefix in the bucket (e.g., 'data/crf/').
    :returns: List of ZIP file blob names found in the folder.
    """
    logger = get_run_logger()
    bucket = utils.get_gcs_bucket(project_id, bucket_name)
    return utils.list_zip_files_in_gcs_folder(bucket, folder_prefix)


@task
def download_and_extract_zip_task(
    project_id: str, bucket_name: str, blob_name: str, extract_path: str
) -> str:
    """Download and extract a ZIP file from GCS.

    Downloads a ZIP file from Google Cloud Storage and extracts its contents
    to a local directory.

    :param project_id: Google Cloud project ID.
    :param bucket_name: Name of the GCS bucket.
    :param blob_name: Full blob name/path in the bucket.
    :param extract_path: Local directory path where files will be extracted.
    :returns: Path to the extraction directory.
    """
    logger = get_run_logger()
    bucket = utils.get_gcs_bucket(project_id, bucket_name)
    return utils.download_and_extract_zip_from_gcs(bucket, blob_name, extract_path)


@task
def list_extracted_files_task(extract_path: str) -> list[str]:
    """List all extracted files in a directory.

    :param extract_path: Path to the directory containing extracted files.
    :returns: List of relative file paths in the directory.
    """
    logger = get_run_logger()
    return utils.list_extracted_files(extract_path)


@task
def read_extracted_fwf_file_task(
    extract_path: str,
    table_id: str,
) -> pd.DataFrame:
    """Lê um arquivo FWF descompactado para um DataFrame.

    Task que encapsula a leitura de arquivo de largura fixa (FWF) do
    diretório descompactado, com suporte para múltiplos formatos de tabela
    CRF (períodos, eventos, eventos_mei).

    :param extract_path: Caminho do diretório contendo os arquivos descompactados.
    :param table_id: Identificador da tabela ('periodos', 'eventos', 'eventos_mei').

    :returns: DataFrame com os dados do arquivo FWF parseado.

    :raises FileNotFoundError: Se nenhum arquivo for encontrado.
    :raises ValueError: Se múltiplos arquivos forem encontrados ou table_id inválido.
    """
    logger = get_run_logger()
    logger.info(
        f"Lendo arquivo FWF da tabela '{table_id}' do diretório {extract_path}"
    )
    return utils.read_extracted_fwf_file(
        extract_path=extract_path,
        table_id=table_id,
    )


@task
def cleanup_extracted_directory_task(extract_path: str) -> None:
    """Remove todos os arquivos de um diretório descompactado.

    Task que deleta recursivamente todos os arquivos no diretório após
    o processamento ser concluído.

    :param extract_path: Caminho do diretório a ser limpo.
    """
    logger = get_run_logger()
    logger.info(f"Limpando diretório descompactado: {extract_path}")
    utils.cleanup_extracted_directory(extract_path)


@task
def process_all_crf_zip_files_task(
    project_id: str,
    bucket_name: str,
    folder_prefix: str,
    extract_base_path: str,
    table_id: str
) -> dict[str, int]:
    """Processa sequencialmente todos os arquivos ZIP CRF do GCS.

    Realiza o ciclo completo para cada arquivo ZIP:
    1. Baixar e descompactar o arquivo ZIP.
    2. Listar arquivos descompactados.
    3. Ler os 4 arquivos FWF (periodos, periodos_mei, eventos, eventos_mei).
    4. Limpar o diretório descompactado.

    :param project_id: Google Cloud project ID.
    :param bucket_name: GCS bucket name contendo os arquivos ZIP.
    :param folder_prefix: Prefixo do caminho da pasta no bucket.
    :param extract_base_path: Diretório base local para arquivos descompactados.

    :returns: Dicionário com estatísticas de processamento.
    """
    logger = get_run_logger()

    # Listar todos os arquivos ZIP no GCS
    bucket = utils.get_gcs_bucket(project_id, bucket_name)
    zip_files = utils.list_zip_files_in_gcs_folder(bucket, folder_prefix)

    logger.info("Processando %d arquivos ZIP", len(zip_files))

    total= 0


    # Processar cada arquivo ZIP sequencialmente
    for blob_name in zip_files:
        # Extrair nome do arquivo ZIP para construir caminho local
        zip_filename = blob_name.split("/")[-1].replace(".zip", "")
        extract_path = f"{extract_base_path}/{zip_filename}"

        logger.info("Iniciando processamento de %s", blob_name)

        # Passo 1: Baixar e descompactar ZIP
        bucket = utils.get_gcs_bucket(project_id, bucket_name)
        extracted_dir = utils.download_and_extract_zip_from_gcs(bucket, blob_name, extract_path)

        # Passo 2: Verificar arquivos descompactados
        extracted_files = utils.list_extracted_files(extract_path=extracted_dir)
        logger.info("Descompactados %d arquivos de %s", len(extracted_files), blob_name)

        # Passo 3: Ler os 4 arquivos FWF sequencialmente
        logger.info("Lendo 4 arquivos FWF de %s", blob_name)
        if table_id == "periodo_simples":
            df = utils.read_extracted_fwf_file(
                extract_path=extracted_dir,
                table_id="periodos",
                extract_base_path=extract_base_path
            )
            periodos_count = len(df)
            total += periodos_count
            logger.info("Processadas %d linhas de periodos", periodos_count)
        elif table_id == "periodos_mei":
            df = utils.read_extracted_fwf_file(
                extract_path=extracted_dir,
                table_id="periodos_mei",
                extract_base_path=extract_base_path
            )
            periodos_mei_count = len(df)
            total += periodos_mei_count
            logger.info("Processadas %d linhas de periodos_mei", periodos_mei_count)

        elif table_id == "eventos_simples":
            df = utils.read_extracted_fwf_file(
                extract_path=extracted_dir,
                table_id="eventos",
                extract_base_path=extract_base_path
            )
            eventos_count = len(df)
            total += eventos_count
            logger.info("Processadas %d linhas de eventos", eventos_count)

        elif table_id == "eventos_mei":
            df = utils.read_extracted_fwf_file(
                extract_path=extracted_dir,
                table_id="eventos_mei",
                extract_base_path=extract_base_path
                )
            eventos_mei_count = len(df)
            total += eventos_mei_count
            logger.info("Processadas %d linhas de eventos_mei", eventos_mei_count)


        # Passo 4: Limpar diretório descompactado
        utils.cleanup_extracted_directory(extract_path=extracted_dir)
        logger.info("Concluido processamento de %s", blob_name)

    logger.info(
        "Processamento concluido. Total: %s=%d",
        table_id,
        total
    )
    breakpoint()
    return df

"""Tasks for GCS ZIP file handling and extraction."""

from datetime import date
from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.rj_smfp__crf.constants import EXTRACT_BASE_PATH
from pipelines.rj_smfp__crf.utils import (
    get_max_date_from_bigquery,
    list_zip_files_in_gcs_folder,
    get_gcs_bucket,
    download_and_extract_zip_from_gcs,
    list_extracted_files,
    read_extracted_fwf_file,
    cleanup_extracted_directory,
)


@task(retries=3, retry_delay_seconds=10)
def get_max_date_from_bigquery_task(
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
    return get_max_date_from_bigquery(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
    )


@task
def list_zip_files_task(
    project_id: str,
    bucket_name: str,
    folder_prefix: str,
    max_date_from_bq: date | None = None,
) -> list[str]:
    """Lista arquivos ZIP no GCS filtrando por data maior que a partição do BQ.

    :param project_id: Google Cloud project ID.
    :param bucket_name: Name of the GCS bucket.
    :param folder_prefix: Folder path prefix in the bucket (e.g., 'data/crf/').
    :param max_date_from_bq: Data máxima de partição do BigQuery. Arquivos com
        data igual ou anterior são ignorados. Se ``None``, retorna todos os ZIPs.
    :returns: List of ZIP file blob names found in the folder after date filter.
    """

    bucket = get_gcs_bucket(project_id, bucket_name)
    return list_zip_files_in_gcs_folder(
        bucket=bucket,
        folder_prefix=folder_prefix,
        max_date_from_bq=max_date_from_bq,
    )


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

    bucket = get_gcs_bucket(project_id, bucket_name)
    return download_and_extract_zip_from_gcs(bucket, blob_name, extract_path)


@task
def list_extracted_files_task(extract_path: str) -> list[str]:
    """List all extracted files in a directory.

    :param extract_path: Path to the directory containing extracted files.
    :returns: List of relative file paths in the directory.
    """

    return list_extracted_files(extract_path)


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

    print(
        f"Lendo arquivo FWF da tabela '{table_id}' do diretório {extract_path}"
    )
    return read_extracted_fwf_file(
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

    print(f"Limpando diretório descompactado: {extract_path}")
    cleanup_extracted_directory(extract_path)


@task
def process_all_crf_zip_files_task(
    project_id: str,
    bucket_name: str,
    folder_prefix: str,
    table_id: str,
    max_date_from_bq: date | None = None,
) -> str | None:
    """Processa sequencialmente todos os arquivos ZIP CRF do GCS.

    Realiza o ciclo completo para cada arquivo ZIP com data posterior à
    partição máxima do BigQuery:
    1. Listar arquivos ZIP no GCS filtrados pela data de partição.
    2. Baixar e descompactar o arquivo ZIP em ``EXTRACT_BASE_PATH``.
    3. Ler o arquivo FWF correspondente ao ``table_id``.
    4. Limpar o diretório descompactado (sempre, via ``try/finally``).

    :param project_id: Google Cloud project ID.
    :param bucket_name: GCS bucket name contendo os arquivos ZIP.
    :param folder_prefix: Prefixo do caminho da pasta no bucket.
    :param table_id: Identificador da tabela CRF a processar.
    :param max_date_from_bq: Data máxima de partição do BigQuery usada como
        marca d'água. Arquivos com data igual ou anterior são ignorados.

    :returns: Caminho local (``data_path``) onde os arquivos parquet foram salvos,
        ou ``None`` se nenhum arquivo ZIP foi encontrado para processar.
    """
    extract_base_path = EXTRACT_BASE_PATH


    # Listar arquivos ZIP no GCS, filtrando pela data de partição do BigQuery
    bucket = get_gcs_bucket(project_id, bucket_name)
    zip_files = list_zip_files_in_gcs_folder(
        bucket=bucket,
        folder_prefix=folder_prefix,
        max_date_from_bq=max_date_from_bq,
    )

    print("Processando %d arquivos ZIP", len(zip_files))

    if not zip_files:
        print("Nenhum arquivo ZIP encontrado para processar")
        return None

    total = 0
    data_path = None


    # Processar cada arquivo ZIP sequencialmente
    for blob_name in zip_files:
        # Extrair nome do arquivo ZIP para construir caminho local
        zip_filename = blob_name.split("/")[-1].replace(".zip", "")
        extract_path = f"{extract_base_path}/{zip_filename}"

        print("Iniciando processamento de %s", blob_name)

        # Passo 1: Baixar e descompactar ZIP
        bucket = get_gcs_bucket(project_id, bucket_name)
        extracted_dir = download_and_extract_zip_from_gcs(bucket, blob_name, extract_path)

        # Passo 2: Verificar arquivos descompactados
        extracted_files = list_extracted_files(extract_path=extracted_dir)
        print("Descompactados %d arquivos de %s", len(extracted_files), blob_name)

        # Passo 3: Ler os 4 arquivos FWF sequencialmente
        print("Lendo 4 arquivos FWF de %s", blob_name)
        if table_id == "periodos_simples":
            df, data_path = read_extracted_fwf_file(
                extract_path=extracted_dir,
                table_id="periodos_simples",
                extract_base_path=extract_base_path
            )
            periodos_count = len(df)
            total += periodos_count
            print("Processadas %d linhas de periodos", periodos_count)
        elif table_id == "periodos_mei":
            df, data_path = read_extracted_fwf_file(
                extract_path=extracted_dir,
                table_id="periodos_mei",
                extract_base_path=extract_base_path
            )
            periodos_mei_count = len(df)
            total += periodos_mei_count
            print("Processadas %d linhas de periodos_mei", periodos_mei_count)

        elif table_id == "eventos_simples":
            df, data_path = read_extracted_fwf_file(
                extract_path=extracted_dir,
                table_id="eventos_simples",
                extract_base_path=extract_base_path
            )
            eventos_count = len(df)
            total += eventos_count
            print("Processadas %d linhas de eventos", eventos_count)

        elif table_id == "eventos_mei":
            df, data_path = read_extracted_fwf_file(
                extract_path=extracted_dir,
                table_id="eventos_mei",
                extract_base_path=extract_base_path
                )
            eventos_mei_count = len(df)
            total += eventos_mei_count
            print("Processadas %d linhas de eventos_mei", eventos_mei_count)


        # Passo 4: Limpar diretório descompactado
        cleanup_extracted_directory(extract_path=extracted_dir)
        print("Concluido processamento de %s", blob_name)

    print(
        "Processamento concluido. Total: %s=%d",
        table_id,
        total
    )

    if data_path is None:
        raise ValueError(
            f"table_id '{table_id}' inválido. "
            "Válidos: 'periodos_simples', 'periodos_mei', 'eventos_simples', 'eventos_mei'."
        )

    return data_path

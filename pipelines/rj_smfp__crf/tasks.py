"""Tasks for GCS ZIP file handling and extraction."""

from datetime import date

from loguru import logger
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
def process_all_crf_zip_files_task(
    project_id: str,
    bucket_name: str,
    folder_prefix: str,
    table_id: str,
    data_inicio: date | str | None = None,
    data_fim: date | str | None = None,
) -> str | None:
    """Processa sequencialmente todos os arquivos ZIP CRF do GCS.

    Realiza o ciclo completo para cada arquivo ZIP dentro do intervalo de datas:
    1. Listar arquivos ZIP no GCS filtrados pelo intervalo ``(data_inicio, data_fim]``.
    2. Baixar e descompactar o arquivo ZIP em ``EXTRACT_BASE_PATH``.
    3. Ler o arquivo FWF correspondente ao ``table_id``.
    4. Limpar o diretório descompactado (sempre, via ``try/finally``).

    :param project_id: Google Cloud project ID.
    :param bucket_name: GCS bucket name contendo os arquivos ZIP.
    :param folder_prefix: Prefixo do caminho da pasta no bucket.
    :param table_id: Identificador da tabela CRF a processar.
    :param data_inicio: Data de início do intervalo (``YYYY-MM-DD`` ou ``date``).
        Arquivos com data igual ou anterior são ignorados. Se ``None``, sem limite inferior.
    :param data_fim: Data de fim do intervalo (``YYYY-MM-DD`` ou ``date``).
        Arquivos com data posterior são ignorados. Se ``None``, sem limite superior.
        Requer que ``data_inicio`` também seja fornecido.

    :returns: Caminho local (``data_path``) onde os arquivos parquet foram salvos,
        ou ``None`` se nenhum arquivo ZIP foi encontrado para processar.
    :raises ValueError: Se ``data_fim`` for fornecido sem ``data_inicio``.
    """
    if data_fim is not None and data_inicio is None:
        raise ValueError(
            "data_fim não pode ser fornecido sem data_inicio. "
            "Forneça data_inicio para definir o início do intervalo."
        )

    extract_base_path = EXTRACT_BASE_PATH
    bucket = get_gcs_bucket(project_id, bucket_name)

    zip_files = list_zip_files_in_gcs_folder(
        bucket=bucket,
        folder_prefix=folder_prefix,
        data_inicio=data_inicio,
        data_fim=data_fim,
    )

    logger.info("Processando {} arquivos ZIP", len(zip_files))

    if not zip_files:
        logger.info("Nenhum arquivo ZIP encontrado para processar")
        return None

    total = 0
    data_path = None

    for blob_name in zip_files:
        zip_filename = blob_name.split("/")[-1].replace(".zip", "")
        extract_path = f"{extract_base_path}/{zip_filename}"

        logger.info("Iniciando processamento de {}", blob_name)

        try:
            extracted_dir = download_and_extract_zip_from_gcs(bucket, blob_name, extract_path)

            extracted_files = list_extracted_files(extract_path=extracted_dir)
            logger.info("Descompactados {} arquivos de {}", len(extracted_files), blob_name)

            df, data_path = read_extracted_fwf_file(
                extract_path=extracted_dir,
                table_id=table_id,
                extract_base_path=extract_base_path,
            )
            total += len(df)
            logger.info("Processadas {} linhas de {}", len(df), table_id)

        finally:
            cleanup_extracted_directory(extract_path=extract_path)
            logger.info("Diretório limpo: {}", extract_path)

        logger.info("Concluído processamento de {}", blob_name)

    logger.info("Processamento concluído. Total de linhas em {}: {}", table_id, total)

    return data_path

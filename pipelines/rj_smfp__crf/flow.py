"""Flow for rj_smfp__crf."""

from prefect import flow

from tasks import process_all_crf_zip_files_task


@flow(log_prints=True)
def rj_smfp__crf(
    table_id : str,
    project_id: str = "rj-rec-rio",
    bucket_name: str = "crf-compressed",
    folder_prefix: str = "PERIODOS_EVENTOS",
    extract_base_path: str = "/tmp/rj_smfp__crf",
) -> None:
    """Processa arquivos CRF do GCS sequencialmente.

    Faz download de arquivos ZIP de uma pasta do GCS, extrai o conteúdo,
    processa os 4 arquivos FWF (períodos, períodos_mei, eventos, eventos_mei),
    limpa os arquivos extraídos e repete para o próximo ZIP.

    Ciclo por arquivo ZIP:
    1. Baixar e descompactar o arquivo ZIP do GCS.
    2. Verificar e listar arquivos descompactados.
    3. Ler sequencialmente os 4 arquivos FWF.
    4. Limpar o diretório descompactado.

    :param project_id: Google Cloud project ID.
    :param bucket_name: GCS bucket name contendo os arquivos ZIP.
    :param folder_prefix: Prefixo do caminho da pasta no bucket.
    :param extract_base_path: Diretório base local para arquivos descompactados.
    """
    process_all_crf_zip_files_task(
        project_id=project_id,
        bucket_name=bucket_name,
        folder_prefix=folder_prefix,
        extract_base_path=extract_base_path,
        table_id=table_id
    )

rj_smfp__crf(table_id="periodo_simples")
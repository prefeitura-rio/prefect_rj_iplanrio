"""Envio e remoção dos arquivos Parquet no GCS."""

from pathlib import Path

from google.cloud import storage

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

# Múltiplo de 256 KiB, exigido pelo upload resumível; evita ler o arquivo inteiro para a memória.
UPLOAD_CHUNK_BYTES = 32 * 1024 * 1024
# <raiz>/<tabela>/<execução>: um prefixo com menos barras apagaria mais do que uma execução.
MIN_PREFIX_SEPARATORS = 2


def blob_prefix(gcs_prefix: str, table: str, run_id: str) -> str:
    """Monta o prefixo dos arquivos de uma tabela neste flow run.

    :param gcs_prefix: Prefixo raiz da pipeline no bucket.
    :param table: Nome da tabela.
    :param run_id: Identificador do flow run.
    :returns: Prefixo sem barra final.
    """
    return f"{gcs_prefix}/{table}/{run_id}"


def upload_file(bucket: storage.Bucket, path: Path, blob_name: str) -> None:
    """Envia um arquivo local para o GCS.

    :param bucket: Bucket de destino.
    :param path: Arquivo local.
    :param blob_name: Nome do objeto no bucket.
    """
    blob = bucket.blob(blob_name, chunk_size=UPLOAD_CHUNK_BYTES)
    blob.upload_from_filename(str(path), content_type="application/octet-stream")


def delete_prefix(project: str, bucket: str, prefix: str) -> int:
    """Apaga todos os objetos de um prefixo de execução.

    :param project: Projeto usado pelo client do GCS.
    :param bucket: Bucket dos arquivos.
    :param prefix: Prefixo da execução, sem barra final.
    :returns: Quantidade de objetos apagados.
    :raises ValueError: Se o prefixo não identificar uma tabela e uma execução.
    """
    if prefix.count("/") < MIN_PREFIX_SEPARATORS:
        raise ValueError(f"Prefixo {prefix!r} é largo demais para ser apagado; esperado <raiz>/<tabela>/<execução>.")
    gcs_bucket = storage.Client(project=project).bucket(bucket)
    blobs = list(gcs_bucket.list_blobs(prefix=f"{prefix}/"))
    for blob in blobs:
        blob.delete()
    logger.info("Removidos %d arquivos de gs://%s/%s", len(blobs), bucket, prefix)
    return len(blobs)

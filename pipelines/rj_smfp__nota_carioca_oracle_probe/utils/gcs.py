"""Envio e remoção dos arquivos Parquet da sonda no GCS."""

import time
from pathlib import Path

from google.cloud import storage

from pipelines.rj_smfp__nota_carioca_oracle_probe.constants import GCS_PREFIX
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

# Múltiplo de 256 KiB, exigido pelo upload resumível; igual ao da pipeline de extração.
UPLOAD_CHUNK_BYTES = 32 * 1024 * 1024


def run_prefix(run_id: str) -> str:
    """Monta o prefixo de tudo que a sonda grava neste flow run.

    :param run_id: Identificador do flow run.
    :returns: ``oracle_probe/<run_id>``, sem barra final.
    :raises ValueError: Se o identificador for vazio.
    """
    if not run_id.strip("/ "):
        raise ValueError("run_id vazio; o prefixo apagado seria largo demais.")
    return f"{GCS_PREFIX}/{run_id}"


def open_bucket(project: str, bucket: str) -> storage.Bucket:
    """Abre o bucket com as credenciais do ambiente.

    :param project: Projeto do GCS.
    :param bucket: Nome do bucket.
    :returns: Bucket pronto para uso.
    """
    return storage.Client(project=project).bucket(bucket)


def log_missing(blob: storage.Blob) -> None:
    """Registra um objeto que já não existia ao ser apagado.

    :param blob: Objeto ausente.
    """
    logger.info("Objeto %s já não existia", blob.name)


def upload_then_delete(bucket: storage.Bucket, path: Path, blob_name: str) -> float:
    """Envia um arquivo ao GCS, mede só o envio e apaga o objeto em seguida, mesmo se o envio falhar.

    :param bucket: Bucket de destino.
    :param path: Arquivo local.
    :param blob_name: Nome do objeto, sob o prefixo da sonda.
    :returns: Duração do envio, em segundos.
    """
    blob = bucket.blob(blob_name, chunk_size=UPLOAD_CHUNK_BYTES)
    try:
        started = time.perf_counter()
        blob.upload_from_filename(str(path), content_type="application/octet-stream")
        return time.perf_counter() - started
    finally:
        bucket.delete_blobs([blob], on_error=log_missing)


def delete_run_prefix(project: str, bucket: str, run_id: str) -> int:
    """Apaga o que sobrou do flow run no GCS.

    :param project: Projeto do GCS.
    :param bucket: Nome do bucket.
    :param run_id: Identificador do flow run.
    :returns: Quantidade de objetos apagados.
    """
    gcs_bucket = open_bucket(project, bucket)
    blobs = list(gcs_bucket.list_blobs(prefix=f"{run_prefix(run_id)}/"))
    gcs_bucket.delete_blobs(blobs, on_error=log_missing)
    return len(blobs)

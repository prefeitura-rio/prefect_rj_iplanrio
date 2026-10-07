"""Envio e remoção dos arquivos Parquet no GCS."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import requests
from google.api_core import exceptions as api_exceptions
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY, ConditionalRetryPolicy, is_generation_specified
from google.resumable_media import InvalidResponse

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

# Múltiplo de 256 KiB, exigido pelo upload resumível; evita ler o arquivo inteiro para a memória.
UPLOAD_CHUNK_BYTES = 8 * 1024 * 1024
# Segundos por requisição (conexão e leitura). Conexões travadas (observado on-prem: alguns fluxos TCP param ou
# se arrastam a ~3 MB/s) devem ser abandonadas e refeitas rápido em uma nova conexão. Pedaços de 8 MiB na menor
# taxa saudável observada (~3 MB/s) levam ~3 s, então 120 s por requisição é folgado.
UPLOAD_TIMEOUT_SECONDS = 120
# Prazo total das retentativas internas da lib (uma por requisição do upload resumível).
UPLOAD_RETRY_DEADLINE_SECONDS = 30 * 60
# Tentativas externas, cada uma refazendo o upload inteiro, com espera de 5, 10, 20 e 40 s entre elas.
UPLOAD_MAX_ATTEMPTS = 5
UPLOAD_INITIAL_BACKOFF_SECONDS = 5.0
UPLOAD_MAX_BACKOFF_SECONDS = 80.0
# 408 e 429 são transitórios; qualquer 5xx também.
RETRYABLE_STATUS_CODES = frozenset({408, 429})
SERVER_ERROR_MIN_STATUS = 500
# <raiz>/<tabela>/<execução>: um prefixo com menos barras apagaria mais do que uma execução.
MIN_PREFIX_SEPARATORS = 2
TRANSPORT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    ConnectionError,
    TimeoutError,
    api_exceptions.ServiceUnavailable,
    api_exceptions.InternalServerError,
    api_exceptions.BadGateway,
    api_exceptions.GatewayTimeout,
    api_exceptions.TooManyRequests,
)


class UploadConflictError(RuntimeError):
    """O objeto já existe no bucket com tamanho diferente do arquivo local."""


@dataclass(frozen=True)
class UploadAttempt:
    """Uma tentativa de envio e as consultas necessárias para decidir o que fazer se ela falhar.

    :param send: Envia o arquivo; levanta a exceção da lib se falhar.
    :param remote_size: Tamanho do objeto no bucket, ou ``None`` se ele não existe.
    :param local_size: Tamanho do arquivo local.
    :param blob_name: Nome do objeto, usado nos logs.
    """

    send: Callable[[], None]
    remote_size: Callable[[], int | None]
    local_size: int
    blob_name: str


def blob_prefix(gcs_prefix: str, table: str, run_id: str) -> str:
    """Monta o prefixo dos arquivos de uma tabela neste flow run.

    :param gcs_prefix: Prefixo raiz da pipeline no bucket.
    :param table: Nome da tabela.
    :param run_id: Identificador do flow run.
    :returns: Prefixo sem barra final.
    """
    return f"{gcs_prefix}/{table}/{run_id}"


def is_transport_error(error: BaseException) -> bool:
    """Diz se a falha é de transporte (rede ou serviço) e, portanto, vale repetir o upload.

    :param error: Exceção levantada pelo upload.
    :returns: ``True`` para erros de conexão/timeout, 408, 429 e 5xx; ``False`` para o resto
        (ex. 403, 404, 412).
    """
    if isinstance(error, TRANSPORT_ERRORS):
        return True
    if isinstance(error, InvalidResponse):
        status = getattr(error.response, "status_code", None)
        return isinstance(status, int) and (status in RETRYABLE_STATUS_CODES or status >= SERVER_ERROR_MIN_STATUS)
    return False


def run_upload(attempt: UploadAttempt, sleep: Callable[[float], None] = time.sleep) -> None:
    """Executa o upload com tentativas externas limitadas.

    Repete só falhas de transporte. Se uma tentativa repetida receber 412 (``if_generation_match=0``), a
    anterior pode ter concluído: o objeto existente só vale como sucesso com o mesmo tamanho do arquivo local.

    :param attempt: Envio e consultas da tentativa.
    :param sleep: Função de espera entre tentativas.
    :raises UploadConflictError: Se o objeto já existe com tamanho diferente.
    :raises Exception: A falha da última tentativa, ou qualquer falha que não seja de transporte.
    """
    backoff = UPLOAD_INITIAL_BACKOFF_SECONDS
    for number in range(1, UPLOAD_MAX_ATTEMPTS + 1):
        try:
            attempt.send()
        except api_exceptions.PreconditionFailed:
            if number == 1:
                raise
            size = attempt.remote_size()
            if size != attempt.local_size:
                raise UploadConflictError(
                    f"{attempt.blob_name} já existe com {size} bytes; o arquivo local tem {attempt.local_size}."
                ) from None
            logger.info("%s já enviado pela tentativa anterior (%d bytes)", attempt.blob_name, size)
            return
        except Exception as error:
            if not is_transport_error(error) or number == UPLOAD_MAX_ATTEMPTS:
                raise
            logger.warning(
                "Falha de transporte no upload de %s (tentativa %d/%d): %r; nova tentativa em %.0f s",
                attempt.blob_name,
                number,
                UPLOAD_MAX_ATTEMPTS,
                error,
                backoff,
            )
            sleep(backoff)
            backoff = min(backoff * 2, UPLOAD_MAX_BACKOFF_SECONDS)
        else:
            return


def upload_file(bucket: storage.Bucket, path: Path, blob_name: str) -> None:
    """Envia um arquivo local para o GCS, com timeouts explícitos e retentativas.

    O nome do objeto é único por execução e faixa, então ``if_generation_match=0`` torna o envio idempotente:
    ele nunca sobrescreve um objeto existente. A retentativa da lib (``retry``) cobre cada requisição do upload
    resumível; ``run_upload`` refaz o envio inteiro quando a lib esgota o prazo.

    :param bucket: Bucket de destino.
    :param path: Arquivo local.
    :param blob_name: Nome do objeto no bucket.
    """
    blob = bucket.blob(blob_name, chunk_size=UPLOAD_CHUNK_BYTES)
    # Com ``if_generation_match`` a lib aplica a política; ela vale também no upload resumível.
    retry = ConditionalRetryPolicy(
        DEFAULT_RETRY.with_timeout(UPLOAD_RETRY_DEADLINE_SECONDS), is_generation_specified, ["query_params"]
    )

    def send() -> None:
        blob.upload_from_filename(
            str(path),
            content_type="application/octet-stream",
            if_generation_match=0,
            retry=retry,
            timeout=UPLOAD_TIMEOUT_SECONDS,
        )

    def remote_size() -> int | None:
        existing = bucket.get_blob(blob_name, timeout=UPLOAD_TIMEOUT_SECONDS)
        return None if existing is None else existing.size

    run_upload(UploadAttempt(send, remote_size, path.stat().st_size, blob_name))


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

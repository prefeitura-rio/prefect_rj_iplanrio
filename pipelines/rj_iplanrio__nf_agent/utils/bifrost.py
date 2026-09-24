"""Batch API do Bifrost com o provider ``vertex`` (Vertex AI Batch Prediction por baixo)."""

import json
import os
from dataclasses import dataclass
from typing import Any

from openai import OpenAI

from .. import constants
from .storage import download_text

API_KEY_ENV = "BIFROST_API_KEY"
BASE_URL_ENV = "BIFROST_BASE_URL"
STORAGE_PREFIX = "bifrost-batch-io"
COMPLETION_WINDOW = "24h"


@dataclass(frozen=True)
class SubmittedBatch:
    """Identificadores de um batch criado."""

    batch_id: str
    input_file_id: str


def build_client() -> OpenAI:
    """Cria o cliente OpenAI apontado para o Bifrost.

    :returns: Cliente configurado.
    :raises RuntimeError: Se ``BIFROST_API_KEY`` ou ``BIFROST_BASE_URL`` estiverem ausentes.
    """
    missing = [env for env in (API_KEY_ENV, BASE_URL_ENV) if not os.environ.get(env)]
    if missing:
        raise RuntimeError(f"Variáveis de ambiente ausentes: {', '.join(missing)}")
    return OpenAI(api_key=os.environ[API_KEY_ENV], base_url=os.environ[BASE_URL_ENV])


def submit_jsonl(client: OpenAI, data: bytes, filename: str, bucket: str) -> SubmittedBatch:
    """Sobe um JSONL e cria o batch que o processa.

    :param client: Cliente do Bifrost.
    :param data: Conteúdo do JSONL.
    :param filename: Nome do arquivo no upload.
    :param bucket: Bucket de transporte do Bifrost (``BIFROST_GCS_BUCKET``).
    :returns: IDs do batch e do arquivo de entrada.
    :raises ValueError: Se o JSONL estiver vazio ou passar de ``BATCH_MAX_BYTES``.
    """
    if not data:
        raise ValueError("JSONL vazio; nada a submeter.")
    if len(data) > constants.BATCH_MAX_BYTES:
        raise ValueError(f"JSONL com {len(data)} bytes passa do limite de {constants.BATCH_MAX_BYTES}.")
    uploaded = client.files.create(
        file=(filename, data, "application/jsonl"),
        purpose="batch",
        extra_body={
            "provider": constants.BIFROST_PROVIDER,
            "storage_config": {"gcs": {"bucket": bucket, "prefix": STORAGE_PREFIX}},
        },
    )
    batch = client.batches.create(
        input_file_id=uploaded.id,
        endpoint="/v1/chat/completions",
        completion_window=COMPLETION_WINDOW,
        extra_body={
            "provider": constants.BIFROST_PROVIDER,
            "model": constants.MODEL_NAME,
            "output_folder": {"url": f"gs://{bucket}/{STORAGE_PREFIX}/output"},
        },
    )
    return SubmittedBatch(batch_id=batch.id, input_file_id=uploaded.id)


def retrieve_batch(client: OpenAI, batch_id: str) -> Any:
    """Consulta o estado de um batch.

    :param client: Cliente do Bifrost.
    :param batch_id: ID retornado na criação.
    :returns: Objeto com ``status``, ``output_file_id`` e ``errors``.
    """
    # Em GET o SDK ignora extra_body; o provider precisa ir na query string.
    return client.batches.retrieve(batch_id, extra_query={"provider": constants.BIFROST_PROVIDER})


def read_batch_output(output_file_id: str | None) -> list[dict]:
    """Lê as linhas de resultado de um batch concluído direto do GCS.

    :param output_file_id: Pasta ``gs://`` de saída informada pelo Vertex.
    :returns: Uma linha decodificada por página.
    :raises ValueError: Se o ID estiver ausente ou não for uma URI ``gs://``.
    """
    if not output_file_id:
        raise ValueError(f"Batch sem output_file_id: {output_file_id!r}")
    if not output_file_id.startswith("gs://"):
        raise ValueError(f"output_file_id fora do formato gs:// esperado do provider vertex: {output_file_id!r}")
    text = download_text(f"{output_file_id.rstrip('/')}/predictions.jsonl")
    return [json.loads(line) for line in text.splitlines() if line.strip()]

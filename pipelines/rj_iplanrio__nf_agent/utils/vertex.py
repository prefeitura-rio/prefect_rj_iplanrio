"""Batch Prediction do Vertex AI (Gemini) chamado direto, sem gateway.

Autentica com uma conta de serviço própria (``RJ_NF_AGENT_VERTEX_CREDENTIALS``, JSON em
base64), separada do ADC usado para BigQuery e GCS. O JSONL de entrada e a saída ficam
no bucket de transporte; o Vertex lê e escreve nele pelo agente de serviço do projeto.
"""

import base64
import binascii
import hashlib
import json
import os
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import StrEnum

from google import genai
from google.cloud import storage
from google.genai import errors as genai_errors
from google.genai import types
from google.oauth2 import service_account

from .. import constants
from .observability import get_logger
from .storage import parse_gcs_uri

logger = get_logger(__name__)

CREDENTIALS_ENV = "RJ_NF_AGENT_VERTEX_CREDENTIALS"
PROJECT_ENV = "VERTEX_PROJECT"
SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)
STORAGE_PREFIX = "vertex-batch-io"
PAGE_UPLOAD_WORKERS = 16
SPEND_CAP_MARKER = "spend cap"
FORBIDDEN = 403
NOT_FOUND = 404


class BatchState(StrEnum):
    """Estado normalizado de um batch, independente dos ``JOB_STATE_*`` do Vertex."""

    IN_PROGRESS = "in_progress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


# PARTIALLY_SUCCEEDED conta como sucesso: o job tem saída; as linhas com erro vêm no próprio output.
VERTEX_STATES = {
    "JOB_STATE_SUCCEEDED": BatchState.SUCCEEDED,
    "JOB_STATE_PARTIALLY_SUCCEEDED": BatchState.SUCCEEDED,
    "JOB_STATE_FAILED": BatchState.FAILED,
    "JOB_STATE_CANCELLED": BatchState.FAILED,
    "JOB_STATE_EXPIRED": BatchState.FAILED,
}


class FatalBatchError(RuntimeError):
    """Erro que repetir não resolve (cota, permissão); a task não deve tentar de novo."""


class SpendCapReachedError(FatalBatchError):
    """O limite de gasto do projeto no Vertex foi atingido."""


class BatchAccessDeniedError(FatalBatchError):
    """A conta de serviço não tem permissão para a operação."""


@dataclass(frozen=True)
class SubmittedBatch:
    """Identificadores de um batch criado."""

    batch_id: str
    input_file_id: str


@dataclass(frozen=True)
class BatchStatus:
    """Estado de um batch no Vertex."""

    state: BatchState
    output_dir: str | None = None
    successful: int | None = None
    failed: int | None = None
    error: str | None = None


def resource_name(batch_id: str) -> str:
    """Converte o ID guardado em ``nf_batch_jobs`` no nome de recurso do job.

    Sessões criadas antes da migração guardam o ID do gateway, que é o nome do recurso
    em base64; as novas guardam o nome do recurso direto.

    :param batch_id: ID do batch.
    :returns: ``projects/<projeto>/locations/<local>/batchPredictionJobs/<id>``.
    :raises ValueError: Se o ID não for nenhum dos dois formatos.
    """
    if batch_id.startswith("projects/"):
        return batch_id
    padded = batch_id + "=" * (-len(batch_id) % 4)
    try:
        decoded = base64.urlsafe_b64decode(padded).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError(f"ID de batch em formato desconhecido: {batch_id!r}") from exc
    if not decoded.startswith("projects/"):
        raise ValueError(f"ID de batch em formato desconhecido: {batch_id!r}")
    return decoded


def load_credentials() -> tuple[service_account.Credentials, str]:
    """Lê a conta de serviço do Vertex do ambiente.

    :returns: Credenciais com escopo ``cloud-platform`` e o projeto onde os jobs rodam
        (``VERTEX_PROJECT``, ou o ``project_id`` da própria conta de serviço).
    :raises RuntimeError: Se a variável estiver ausente.
    :raises ValueError: Se o conteúdo não for JSON em base64 válido.
    """
    encoded = os.environ.get(CREDENTIALS_ENV)
    if not encoded:
        raise RuntimeError(f"Variável de ambiente ausente: {CREDENTIALS_ENV}")
    try:
        info = json.loads(base64.b64decode(encoded, validate=True))
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"{CREDENTIALS_ENV} não contém JSON em base64 válido.") from exc
    project = os.environ.get(PROJECT_ENV) or info.get("project_id")
    if not project:
        raise ValueError(f"Sem projeto: defina {PROJECT_ENV} ou use uma conta de serviço com project_id.")
    return service_account.Credentials.from_service_account_info(info, scopes=list(SCOPES)), project


def raise_for_fatal(exc: genai_errors.APIError) -> None:
    """Converte erros de cota e permissão do Vertex em erros que não se repetem.

    :param exc: Erro devolvido pelo SDK.
    :raises SpendCapReachedError: Se o limite de gasto foi atingido.
    :raises BatchAccessDeniedError: Se for 403 por outro motivo.
    """
    if exc.code != FORBIDDEN:
        return
    message = str(exc)
    if SPEND_CAP_MARKER in message.lower():
        raise SpendCapReachedError(f"Spend cap atingido no Vertex: {message[:300]}") from exc
    raise BatchAccessDeniedError(f"Acesso negado no Vertex: {message[:300]}") from exc


class VertexBatchClient:
    """Cria e acompanha batches do Gemini no Vertex, com E/S no bucket de transporte."""

    def __init__(self, genai_client: genai.Client, storage_client: storage.Client, bucket: str) -> None:
        """Guarda os clientes já autenticados.

        :param genai_client: Cliente do SDK em modo Vertex.
        :param storage_client: Cliente do GCS com a mesma conta de serviço.
        :param bucket: Bucket de entrada e saída dos batches.
        """
        self.genai = genai_client
        self.storage = storage_client
        self.bucket = bucket

    def page_uri(self, pdf_name: str, page_number: int) -> str:
        """URI estável da página de um PDF no bucket (função do nome, sem E/S).

        O nome do PDF vira um hash para a URI não depender de espaços ou acentos.

        :param pdf_name: Nome do PDF sem extensão.
        :param page_number: Página, a partir de 1.
        :returns: ``gs://<bucket>/vertex-batch-io/pages/<hash>/<página>.pdf``.
        """
        digest = hashlib.sha256(pdf_name.encode("utf-8")).hexdigest()[:20]
        return f"gs://{self.bucket}/{STORAGE_PREFIX}/pages/{digest}/{page_number:04d}.pdf"

    def upload_pages(self, pdf_name: str, pages: Sequence[bytes]) -> list[str]:
        """Grava cada página de um PDF como objeto no bucket, em paralelo.

        Regravar a mesma página é seguro: o caminho depende só do nome do PDF.

        :param pdf_name: Nome do PDF sem extensão.
        :param pages: Bytes de cada página, na ordem original.
        :returns: A URI de cada página, na mesma ordem.
        """
        uris = [self.page_uri(pdf_name, number) for number in range(1, len(pages) + 1)]

        def upload(item: tuple[str, bytes]) -> None:
            uri, data = item
            bucket, path = parse_gcs_uri(uri)
            self.storage.bucket(bucket).blob(path).upload_from_string(data, content_type="application/pdf")

        with ThreadPoolExecutor(max_workers=PAGE_UPLOAD_WORKERS) as pool:
            list(pool.map(upload, zip(uris, pages, strict=True)))
        return uris

    def find_job(self, display_name: str) -> str | None:
        """Procura um job pelo nome de exibição.

        :param display_name: Nome dado na criação.
        :returns: Nome de recurso do job, ou ``None``.
        """
        config = types.ListBatchJobsConfig(filter=f'display_name="{display_name}"', page_size=1)
        for job in self.genai.batches.list(config=config):
            return job.name
        return None

    def submit_jsonl(self, data: bytes, name: str) -> SubmittedBatch:
        """Grava o JSONL no GCS e cria o job que o processa, sem duplicar se ele já existir.

        O ``name`` vira o nome de exibição do job. Se uma tentativa anterior criou o job
        mas falhou antes de registrar o evento, a nova tentativa reaproveita o job em vez
        de pagar outro.

        :param data: Conteúdo do JSONL.
        :param name: Nome estável da sessão e fase (ex.: ``nf-classification-<sessão>``).
        :returns: Nome do job e URI do arquivo de entrada.
        :raises ValueError: Se o JSONL estiver vazio ou passar de ``BATCH_MAX_BYTES``.
        :raises FatalBatchError: Se o gasto ou a permissão impedirem a criação.
        """
        if not data:
            raise ValueError("JSONL vazio; nada a submeter.")
        if len(data) > constants.BATCH_MAX_BYTES:
            raise ValueError(f"JSONL com {len(data)} bytes passa do limite de {constants.BATCH_MAX_BYTES}.")
        input_uri = f"gs://{self.bucket}/{STORAGE_PREFIX}/input/{name}.jsonl"
        output_uri = f"gs://{self.bucket}/{STORAGE_PREFIX}/output/{name}/"
        try:
            existing = self.find_job(name)
            if existing:
                logger.info("Job %s já existe (%s); reaproveitando.", name, existing)
                return SubmittedBatch(batch_id=existing, input_file_id=input_uri)
            bucket, path = parse_gcs_uri(input_uri)
            self.storage.bucket(bucket).blob(path).upload_from_string(data, content_type="application/jsonl")
            job = self.genai.batches.create(
                model=constants.MODEL_NAME,
                src=input_uri,
                config=types.CreateBatchJobConfig(display_name=name, dest=output_uri),
            )
        except genai_errors.APIError as exc:
            raise_for_fatal(exc)
            raise
        return SubmittedBatch(batch_id=job.name, input_file_id=input_uri)

    def retrieve(self, batch_id: str) -> BatchStatus:
        """Consulta o estado de um batch.

        :param batch_id: ID guardado no evento (formato novo ou o antigo do gateway).
        :returns: Estado normalizado, pasta de saída e contagens de linhas.
        :raises FatalBatchError: Se a permissão ou o gasto impedirem a consulta.
        """
        try:
            job = self.genai.batches.get(name=resource_name(batch_id))
        except genai_errors.APIError as exc:
            if exc.code == NOT_FOUND:
                return BatchStatus(BatchState.FAILED, error="Job não encontrado no Vertex (expirado ou removido).")
            raise_for_fatal(exc)
            raise
        raw_state = job.state.name if job.state else "JOB_STATE_UNSPECIFIED"
        stats = job.completion_stats
        return BatchStatus(
            state=VERTEX_STATES.get(raw_state, BatchState.IN_PROGRESS),
            output_dir=job.output_info.gcs_output_directory if job.output_info else None,
            successful=stats.successful_count if stats else None,
            failed=stats.failed_count if stats else None,
            error=job.error.message if job.error and job.error.message else None,
        )

    def cancel(self, batch_id: str) -> None:
        """Cancela um job que ainda não terminou.

        :param batch_id: ID guardado no evento (formato novo ou o antigo do gateway).
        :raises FatalBatchError: Se a permissão impedir o cancelamento.
        """
        try:
            self.genai.batches.cancel(name=resource_name(batch_id))
        except genai_errors.APIError as exc:
            raise_for_fatal(exc)
            raise

    def iter_output(self, output_dir: str | None) -> Iterator[dict]:
        """Lê as linhas de resultado de um batch concluído, uma a uma.

        Faz streaming do GCS, sem carregar o arquivo inteiro na memória.

        :param output_dir: Pasta ``gs://`` de saída informada pelo Vertex.
        :returns: Uma linha decodificada por página.
        :raises ValueError: Se a pasta estiver ausente ou não for uma URI ``gs://``.
        :raises FileNotFoundError: Se a pasta não tiver nenhum ``predictions*.jsonl``.
        """
        if not output_dir:
            raise ValueError(f"Batch sem pasta de saída: {output_dir!r}")
        bucket, path = parse_gcs_uri(output_dir)
        prefix = f"{path.rstrip('/')}/"
        blobs = sorted(
            (
                blob
                for blob in self.storage.list_blobs(bucket, prefix=prefix)
                if blob.name.rsplit("/", 1)[-1].startswith("predictions") and blob.name.endswith(".jsonl")
            ),
            key=lambda blob: blob.name,
        )
        if not blobs:
            raise FileNotFoundError(f"Nenhum predictions*.jsonl em {output_dir}")
        for blob in blobs:
            with blob.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        yield json.loads(line)


def build_client(bucket: str) -> VertexBatchClient:
    """Cria o cliente autenticado com a conta de serviço do Vertex.

    :param bucket: Bucket de entrada e saída dos batches.
    :returns: Cliente pronto para submeter e acompanhar batches.
    """
    credentials, project = load_credentials()
    return VertexBatchClient(
        genai.Client(vertexai=True, project=project, location=constants.VERTEX_LOCATION, credentials=credentials),
        storage.Client(project=project, credentials=credentials),
        bucket,
    )

"""Porta do bucket do raw e adaptador do GCS: objetos que nunca são sobrescritos e objetos de trava com geração."""

from typing import Protocol

from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage

CONTENT_TYPE_GZIP = "application/gzip"
CONTENT_TYPE_JSON = "application/json"
GERACAO_AUSENTE = 0
"""``if_generation_match=0`` só deixa gravar se o objeto não existe."""


class ObjetoJaExiste(Exception):  # noqa: N818 - nome do domínio, como as demais exceções do pacote
    """O objeto já existe e o GCS recusou a gravação (precondição ``if_generation_match=0``)."""


class Armazenamento(Protocol):
    """Operações de bucket de que o extrator precisa; cada uma mapeia uma precondição de geração do GCS."""

    bucket: str

    def uri(self, objeto: str) -> str:
        """``gs://bucket/objeto``."""
        ...

    def criar_se_ausente(self, objeto: str, conteudo: bytes) -> int | None:
        """Grava só se o objeto não existe; devolve a geração criada, ou ``None`` se já existia."""
        ...

    def ler(self, objeto: str) -> bytes:
        """Conteúdo do objeto."""
        ...

    def ler_com_geracao(self, objeto: str) -> tuple[bytes, int] | None:
        """Conteúdo e geração atual, ou ``None`` se o objeto não existe."""
        ...

    def substituir_se_geracao(self, objeto: str, conteudo: bytes, geracao: int) -> int | None:
        """Troca o conteúdo só se a geração atual for ``geracao``; devolve a nova, ou ``None`` se mudou."""
        ...

    def apagar_se_geracao(self, objeto: str, geracao: int) -> bool:
        """Apaga só se a geração atual for ``geracao``; devolve se apagou."""
        ...


def gravar_imutavel(armazenamento: Armazenamento, objeto: str, conteudo: bytes) -> None:
    """Grava o objeto sem jamais sobrescrever.

    :raises ObjetoJaExiste: Se o nome já existe.
    """
    if armazenamento.criar_se_ausente(objeto, conteudo) is None:
        raise ObjetoJaExiste(objeto)


def _tipo_de_conteudo(objeto: str) -> str:
    return CONTENT_TYPE_GZIP if objeto.endswith(".gz") else CONTENT_TYPE_JSON


class GcsArmazenamento:
    """Bucket do GCS pela biblioteca oficial; toda gravação carrega a precondição de geração.

    :param cliente: Cliente do GCS (credenciais da conta de serviço do run).
    :param bucket: Nome do bucket.
    """

    def __init__(self, cliente: storage.Client, bucket: str) -> None:
        self.bucket = bucket
        self._bucket = cliente.bucket(bucket)

    def uri(self, objeto: str) -> str:
        """``gs://bucket/objeto``."""
        return f"gs://{self.bucket}/{objeto}"

    def criar_se_ausente(self, objeto: str, conteudo: bytes) -> int | None:
        """Grava com ``if_generation_match=0``."""
        blob = self._bucket.blob(objeto)
        try:
            blob.upload_from_string(
                conteudo, content_type=_tipo_de_conteudo(objeto), if_generation_match=GERACAO_AUSENTE
            )
        except PreconditionFailed:
            return None
        return blob.generation

    def ler(self, objeto: str) -> bytes:
        """Baixa o objeto."""
        return self._bucket.blob(objeto).download_as_bytes()

    def ler_com_geracao(self, objeto: str) -> tuple[bytes, int] | None:
        """Baixa o objeto na geração atual."""
        blob = self._bucket.get_blob(objeto)
        if blob is None or blob.generation is None:
            return None
        try:
            return blob.download_as_bytes(if_generation_match=blob.generation), blob.generation
        except (NotFound, PreconditionFailed):
            return None

    def substituir_se_geracao(self, objeto: str, conteudo: bytes, geracao: int) -> int | None:
        """Regrava com ``if_generation_match=geracao``."""
        blob = self._bucket.blob(objeto)
        try:
            blob.upload_from_string(conteudo, content_type=_tipo_de_conteudo(objeto), if_generation_match=geracao)
        except (NotFound, PreconditionFailed):
            return None
        return blob.generation

    def apagar_se_geracao(self, objeto: str, geracao: int) -> bool:
        """Apaga com ``if_generation_match=geracao``."""
        try:
            self._bucket.blob(objeto).delete(if_generation_match=geracao)
        except (NotFound, PreconditionFailed):
            return False
        return True

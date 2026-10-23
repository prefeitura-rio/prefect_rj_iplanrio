"""Acesso ao bucket do export: ler o manifesto, abrir os CSV gzip e apagar o que o carregador já publicou.

``ObjectStore`` é a fronteira com o GCS; os testes injetam um GCS em memória no lugar de :class:`GcsObjectStore`.
"""

from collections.abc import Sequence
from typing import BinaryIO, Protocol

from google.cloud import storage

DELETE_BATCH_SIZE = 100
"""Máximo de objetos por requisição batch do GCS."""


class ObjectStore(Protocol):
    """O que o carregador pede ao bucket."""

    def read_text(self, bucket: str, name: str) -> str:
        """Lê um objeto como texto UTF-8."""
        ...

    def open_binary(self, bucket: str, name: str) -> BinaryIO:
        """Abre um objeto para leitura em sequência (o chamador fecha)."""
        ...

    def delete(self, bucket: str, names: Sequence[str]) -> None:
        """Remove os objetos."""
        ...


class GcsObjectStore:
    """O bucket de verdade, pelo client do GCS com as credenciais do pod.

    :param project: Projeto usado pelo client.
    """

    def __init__(self, project: str) -> None:
        self._client = storage.Client(project=project)

    def read_text(self, bucket: str, name: str) -> str:
        """Lê um objeto como texto UTF-8."""
        return self._client.bucket(bucket).blob(name).download_as_text(encoding="utf-8")

    def open_binary(self, bucket: str, name: str) -> BinaryIO:
        """Abre o objeto em leitura sequencial, sem baixá-lo inteiro."""
        return self._client.bucket(bucket).blob(name).open("rb")

    def delete(self, bucket: str, names: Sequence[str]) -> None:
        """Remove os objetos em lotes de até 100 por requisição.

        :raises google.api_core.exceptions.GoogleAPICallError: Se algum objeto não puder ser removido, depois de
            tentar todos os do lote.
        """
        target = self._client.bucket(bucket)
        for start in range(0, len(names), DELETE_BATCH_SIZE):
            with self._client.batch(raise_exception=True):
                for name in names[start : start + DELETE_BATCH_SIZE]:
                    target.blob(name).delete()


def open_store(project: str) -> ObjectStore:
    """Cria o acesso ao GCS; as tasks chamam esta função, e os testes a trocam por um GCS em memória."""
    return GcsObjectStore(project)

"""Leitura dos objetos NDJSON.gz do extrator no GCS."""

import gzip
import json
from collections.abc import Mapping

from google.cloud import storage

GZIP_MAGICO = b"\x1f\x8b"


def separar_uri(gcs_uri: str) -> tuple[str, str]:
    """``gs://bucket/caminho/objeto`` em ``(bucket, caminho/objeto)``.

    :raises ValueError: Se não for uma URI ``gs://`` com bucket e objeto.
    """
    if not gcs_uri.startswith("gs://") or "/" not in gcs_uri[5:]:
        raise ValueError(f"URI do GCS inválida: {gcs_uri!r}")
    bucket, nome = gcs_uri[5:].split("/", 1)
    if not bucket or not nome:
        raise ValueError(f"URI do GCS inválida: {gcs_uri!r}")
    return bucket, nome


def indexar_por_nsu(conteudo: bytes) -> dict[int, Mapping[str, object]]:
    """Descompacta (se for gzip) e indexa as linhas NDJSON pelo NSU."""
    texto = gzip.decompress(conteudo) if conteudo[:2] == GZIP_MAGICO else conteudo
    linhas: dict[int, Mapping[str, object]] = {}
    for bruta in texto.decode("utf-8").splitlines():
        if bruta.strip():
            objeto = json.loads(bruta)
            linhas[int(objeto["nsu"])] = objeto
    return linhas


class LeitorGcs:
    """Lê objetos do bucket do extrator; implementa ``LeitorObjetos``."""

    def __init__(self, cliente: storage.Client) -> None:
        self.cliente = cliente

    def ler_linhas(self, gcs_uri: str) -> dict[int, Mapping[str, object]]:
        """Baixa o objeto e devolve as linhas por NSU.

        :raises ValueError: Se a URI for inválida.
        """
        bucket, nome = separar_uri(gcs_uri)
        return indexar_por_nsu(self.cliente.bucket(bucket).blob(nome).download_as_bytes())

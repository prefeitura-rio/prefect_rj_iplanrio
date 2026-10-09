"""Leitura e escrita no Cloud Storage via Application Default Credentials."""

import json
from dataclasses import dataclass
from datetime import datetime

from google.cloud import storage


@dataclass(frozen=True)
class PdfRef:
    """Um PDF de entrada: nome sem extensão (``nome_arquivo``) e URI completa."""

    name: str
    uri: str


def parse_gcs_uri(uri: str) -> tuple[str, str]:
    """Separa ``gs://bucket/caminho`` em bucket e caminho.

    :param uri: URI no formato ``gs://``.
    :returns: ``(bucket, caminho)``; caminho pode ser vazio.
    :raises ValueError: Se a URI não começar com ``gs://`` ou não tiver bucket.
    """
    if not uri.startswith("gs://"):
        raise ValueError(f"Esperava uma URI gs://, recebi {uri!r}")
    bucket, _, path = uri[len("gs://") :].partition("/")
    if not bucket:
        raise ValueError(f"URI sem bucket: {uri!r}")
    return bucket, path


def list_pdfs(prefix_uri: str) -> list[PdfRef]:
    """Lista os arquivos diretamente sob um prefixo (sem descer em subpastas).

    :param prefix_uri: Pasta de origem, ex. ``gs://bucket/files_pdfs/mes_envio=2021-11-01``.
    :returns: PDFs ordenados por nome; a extensão ``.pdf``, se houver, sai do nome.
    """
    bucket, path = parse_gcs_uri(prefix_uri)
    prefix = f"{path.rstrip('/')}/" if path else ""
    refs = []
    for blob in storage.Client().list_blobs(bucket, prefix=prefix, delimiter="/"):
        if blob.name.endswith("/"):
            continue
        filename = blob.name[len(prefix) :]
        name = filename[: -len(".pdf")] if filename.lower().endswith(".pdf") else filename
        refs.append(PdfRef(name=name, uri=f"gs://{bucket}/{blob.name}"))
    return sorted(refs, key=lambda ref: ref.name)


def download_bytes(uri: str) -> bytes:
    """Baixa um objeto do GCS.

    :param uri: URI ``gs://``.
    :returns: Conteúdo do objeto.
    """
    bucket, path = parse_gcs_uri(uri)
    return storage.Client().bucket(bucket).blob(path).download_as_bytes()


def download_text(uri: str) -> str:
    """Baixa um objeto de texto UTF-8 do GCS.

    :param uri: URI ``gs://``.
    :returns: Conteúdo decodificado.
    """
    return download_bytes(uri).decode("utf-8")


def write_ndjson(bucket: str, base_path: str, rows: list[dict], filename_stem: str, generated_at: datetime) -> str:
    """Grava linhas como NDJSON particionado por ``data_geracao``.

    :param bucket: Bucket de destino.
    :param base_path: Prefixo da tabela externa.
    :param rows: Registros serializáveis em JSON.
    :param filename_stem: Nome do arquivo sem extensão; deve ser único por sessão.
    :param generated_at: Momento da geração (define a partição).
    :returns: URI do arquivo gravado.
    """
    path = f"{base_path.rstrip('/')}/data_geracao={generated_at:%Y-%m-%d}/{filename_stem}.ndjson"
    content = "\n".join(json.dumps(row, ensure_ascii=False, default=str) for row in rows)
    storage.Client().bucket(bucket).blob(path).upload_from_string(
        content.encode("utf-8"), content_type="application/x-ndjson"
    )
    return f"gs://{bucket}/{path}"

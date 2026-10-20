"""
Módulo de abstração de armazenamento para obtenção de PDFs e artefatos.
"""

import io
from pathlib import Path
from typing import Optional

from ..constants import (
    GCS_ARTIFACTS_PREFIX,
    GCS_BUCKET_NAME,
    GCS_PDFS_PREFIX,
    LOCAL_ARTIFACTS_DIR,
    LOCAL_PDFS_DIR,
)


def carregar_bytes_pdf(
    nome_arquivo: str,
    is_local: bool = True,
    diretorio_local: Optional[Path] = None,
) -> bytes:
    """Lê bytes do PDF a partir da pasta local da pipeline ou do bucket GCS."""
    if is_local:
        pasta = diretorio_local or LOCAL_PDFS_DIR
        caminho = pasta / nome_arquivo
        if not caminho.exists():
            raise FileNotFoundError(
                f"PDF local não localizado em: {caminho.resolve()}"
            )
        with open(caminho, "rb") as f:
            return f.read()

    # Nuvem: Google Cloud Storage
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"{GCS_PDFS_PREFIX}{nome_arquivo}")
    if not blob.exists():
        raise FileNotFoundError(f"Objeto GCS não encontrado: {blob.name}")
    return blob.download_as_bytes()


def carregar_bytes_artefato(nome_artefato: str, is_local: bool = True) -> bytes:
    """Lê bytes de um arquivo .joblib localmente ou do GCS."""
    if is_local:
        caminho = LOCAL_ARTIFACTS_DIR / nome_artefato
        if not caminho.exists():
            raise FileNotFoundError(f"Artefato local ausente: {caminho}")
        with open(caminho, "rb") as f:
            return f.read()

    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"{GCS_ARTIFACTS_PREFIX}{nome_artefato}")
    return blob.download_as_bytes()
import base64
import os
import uuid
from datetime import datetime
from typing import Literal

import pandas as pd
from prefect import task
from prefect.logging import get_logger

logger = get_logger("toolkit")

# Mapeamento de magic numbers
MAGIC_NUMBERS = {
    b"\x89PNG\r\n\x1a\n": "png",
    b"\xff\xd8\xff": "jpeg",
    b"GIF87a": "gif",
    b"GIF89a": "gif",
    b"%PDF": "pdf",
}


class PdfDetectedError(Exception):
    """Exceção lançada quando um PDF é detectado no lugar de uma imagem."""

    pass


def detect_and_decode(data_b64: str | bytes) -> bytes:
    """Detecta se o Base64 precisa de 1 ou 2 decodificações e retorna os bytes da imagem."""
    if isinstance(data_b64, bytes):
        try:
            data_b64 = data_b64.decode("ascii")
        except UnicodeDecodeError as exc:
            raise ValueError("Bytes recebidos não representam Base64 ASCII válido.") from exc

    data_b64 = data_b64.strip()

    # Adicionar padding se necessário (Base64 deve ter comprimento múltiplo de 4)
    missing_padding = len(data_b64) % 4
    if missing_padding:
        data_b64 += "=" * (4 - missing_padding)

    try:
        step1 = base64.b64decode(data_b64, validate=False)
    except Exception:
        raise ValueError("Base64 inválido na primeira tentativa")

    # Checa se bate com algum magic number
    if any(step1.startswith(m) for m in MAGIC_NUMBERS):
        # Verifica se é PDF
        if step1.startswith(b"%PDF"):
            raise PdfDetectedError("PDF detectado no lugar de imagem")
        return step1

    # Segunda tentativa
    try:
        step2 = base64.b64decode(step1, validate=False)
    except Exception:
        raise ValueError("Base64 inválido na segunda tentativa")

    if any(step2.startswith(m) for m in MAGIC_NUMBERS):
        # Verifica se é PDF
        if step2.startswith(b"%PDF"):
            raise PdfDetectedError("PDF detectado no lugar de imagem")
        return step2

    raise ValueError("Não foi possível identificar o tipo de arquivo após 1 ou 2 decodificações.")


@task
def create_date_partitions(
    dataframe,
    partition_column: str | None = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
):
    """Save a DataFrame in one file, with the columns `ano_particao`, `mes_particao` and `data_particao`."""
    if partition_column is None:
        partition_column = "data_particao"
        dataframe[partition_column] = datetime.now().strftime("%Y-%m-%d")
    else:
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column], errors="coerce")
        dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")
        if dataframe["data_particao"].isnull().any():
            raise ValueError("Some dates in the partition column could not be parsed.")

    dataframe["ano_particao"] = dataframe["data_particao"].str[:4]
    dataframe["mes_particao"] = dataframe["data_particao"].str[5:7]
    columns = [c for c in dataframe.columns if c not in ("ano_particao", "mes_particao", "data_particao")]
    dataframe = dataframe[[*columns, "ano_particao", "mes_particao", "data_particao"]]

    os.makedirs(root_folder, exist_ok=True)
    file_path = os.path.join(root_folder, f"{uuid.uuid4()}.{file_format}")

    if file_format == "csv":
        dataframe.to_csv(file_path, index=False)
    else:
        dataframe.to_parquet(file_path, index=False)

    logger.info("Files saved on %s", root_folder)
    return root_folder

# -*- coding: utf-8 -*-
import base64
import os
import uuid
from datetime import datetime
from typing import Literal

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from prefect import task

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
    partition_column: str = None,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder="./data/",
    append_mode: bool = False,
):
    """
    Create date partitions for a DataFrame and save them to disk.

    Args:
        dataframe: DataFrame to partition
        partition_column: Column to use for date partitioning
        file_format: Format to save files (csv or parquet)
        root_folder: Root folder for saving partitions
        append_mode: If True, keeps existing files. If False, clears root_folder first.
    """

    # Limpar pasta apenas no primeiro batch (quando append_mode=False)
    if not append_mode and os.path.exists(root_folder):
        import shutil

        log(f"Limpando pasta {root_folder} para primeira gravação")
        shutil.rmtree(root_folder)

    if partition_column is None:
        partition_column = "data_particao"
        dataframe[partition_column] = datetime.now().strftime("%Y-%m-%d")
    else:
        dataframe[partition_column] = pd.to_datetime(dataframe[partition_column], errors="coerce")
        dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")

        # Validação aprimorada de datas
        null_dates = dataframe["data_particao"].isnull()
        if null_dates.all():
            raise ValueError(
                f"Todas as datas na coluna '{partition_column}' são inválidas. Nenhum arquivo pode ser criado."
            )
        elif null_dates.any():
            null_count = null_dates.sum()
            log(f"ATENÇÃO: {null_count} registros com datas inválidas serão ignorados.")
            dataframe = dataframe[~null_dates]  # Remove linhas com datas NULL

    dates = dataframe["data_particao"].unique()
    dataframes = [
        (
            date,
            dataframe[dataframe["data_particao"] == date].drop(columns=["data_particao"]),
        )
        for date in dates
    ]

    files_created = 0  # Contador de arquivos criados
    for _date, _dataframe in dataframes:
        partition_folder = os.path.join(
            root_folder,
            f"ano_particao={_date[:4]}/mes_particao={_date[5:7]}/data_particao={_date}",
        )
        os.makedirs(partition_folder, exist_ok=True)

        file_folder = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")

        if file_format == "csv":
            _dataframe.to_csv(file_folder, index=False)
        elif file_format == "parquet":
            _dataframe.to_parquet(file_folder, index=False)

        files_created += 1  # Incrementa contador

    # Validação de arquivos criados
    if files_created == 0:
        raise ValueError(
            f"Nenhum arquivo foi criado em {root_folder}. "
            f"Verifique se o DataFrame tem datas válidas na coluna '{partition_column}'."
        )

    log(f"Files saved on {root_folder}")
    return root_folder

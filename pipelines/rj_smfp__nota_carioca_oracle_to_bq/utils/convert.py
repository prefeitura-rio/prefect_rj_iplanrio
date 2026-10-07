"""Conversão vetorizada de lotes do Oracle para as tabelas Arrow gravadas no Parquet."""

import base64
from datetime import datetime
from typing import assert_never

import numpy as np
import pyarrow as pa

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import (
    ColumnKind,
    OracleColumn,
    column_kind,
    output_type,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import parquet_schema

B64_ALPHABET = np.frombuffer(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", dtype=np.uint8)


def fixed_width_strings(chars: np.ndarray) -> pa.Array:
    """Cria um array de strings a partir de uma matriz ``(n, largura)`` de bytes ASCII.

    :param chars: Matriz ``uint8`` com uma string por linha.
    :returns: Array Arrow ``string`` sem nulos.
    """
    rows, width = chars.shape
    offsets = np.arange(rows + 1, dtype=np.int32) * width
    return pa.StringArray.from_buffers(rows, pa.py_buffer(offsets), pa.py_buffer(np.ascontiguousarray(chars)))


def encode_base64_matrix(data: np.ndarray) -> np.ndarray:
    """Codifica em base64, sem laço Python, cada linha de uma matriz de bytes.

    :param data: Matriz ``uint8`` ``(n, L)``; todas as linhas têm ``L`` bytes.
    :returns: Matriz ``uint8`` ``(n, 4 * ceil(L / 3))`` com os caracteres base64.
    """
    rows, length = data.shape
    padding = -length % 3
    padded = np.zeros((rows, length + padding), dtype=np.uint32)
    padded[:, :length] = data
    groups = padded.reshape(rows, -1, 3)
    packed = (groups[:, :, 0] << 16) | (groups[:, :, 1] << 8) | groups[:, :, 2]
    indexes = np.stack([(packed >> shift) & 63 for shift in (18, 12, 6, 0)], axis=2)
    encoded = B64_ALPHABET[indexes].reshape(rows, -1)
    if padding:
        encoded[:, -padding:] = ord("=")
    return encoded


def base64_strings(values: pa.Array) -> pa.Array:
    """Converte um array de bytes (``RAW``) em strings base64, preservando nulos.

    Usa o caminho vetorizado quando todos os valores não nulos têm o mesmo
    tamanho (caso de ``RAW(16)`` e ``RAW(32)``); senão codifica valor a valor.

    :param values: Array ``binary`` ou ``large_binary``.
    :returns: Array ``string`` com o base64 de cada valor.
    """
    binary = values.cast(pa.binary())
    present = binary.drop_null()
    if len(present) == 0:
        return pa.nulls(len(binary), pa.string())
    offsets = np.frombuffer(present.buffers()[1], dtype=np.int32)[present.offset : present.offset + len(present) + 1]
    lengths = np.diff(offsets)
    if not (lengths == lengths[0]).all():
        return pa.array(
            [None if value is None else base64.b64encode(value).decode("ascii") for value in binary.to_pylist()],
            pa.string(),
        )
    data = np.frombuffer(present.buffers()[2], dtype=np.uint8)[offsets[0] : offsets[-1]].reshape(len(present), -1)
    encoded = fixed_width_strings(encode_base64_matrix(data))
    if len(present) == len(binary):
        return encoded
    positions = np.cumsum(binary.is_valid().to_numpy(zero_copy_only=False)) - 1
    return encoded.take(pa.array(positions, mask=binary.is_null().to_numpy(zero_copy_only=False)))


def iso_date_strings(values: pa.Array) -> pa.Array:
    """Formata datas do Oracle como ``YYYY-MM-DDTHH:MI:SS``, preservando nulos.

    O ``DATE`` do Oracle não tem fração; o cast para segundos é seguro e falha
    se algum valor tiver fração, em vez de truncá-lo em silêncio.

    :param values: Array ``timestamp``.
    :returns: Array ``string``.
    """
    seconds = values.cast(pa.timestamp("s"), safe=True)
    nulls = seconds.is_null().to_numpy(zero_copy_only=False)
    text = np.datetime_as_string(seconds.to_numpy(zero_copy_only=False), unit="s")
    return pa.array(text, pa.string(), mask=nulls)


def convert_column(column: OracleColumn, values: pa.ChunkedArray | pa.Array) -> pa.Array:
    """Converte uma coluna do lote para o tipo gravado no Parquet.

    :param column: Coluna do Oracle.
    :param values: Valores entregues pelo driver.
    :returns: Array com o tipo de :func:`output_type`.
    :raises NotImplementedError: Se o tipo da coluna não for suportado.
    """
    array = values.combine_chunks() if isinstance(values, pa.ChunkedArray) else values
    kind = column_kind(column)
    match kind:
        case ColumnKind.NUMBER:
            return array.cast(output_type(column), safe=True)
        case ColumnKind.TEXT:
            return array.cast(pa.string())
        case ColumnKind.RAW:
            return base64_strings(array)
        case ColumnKind.DATE:
            return iso_date_strings(array)
        case unreachable:
            assert_never(unreachable)


def to_output_table(batch: pa.Table, columns: tuple[OracleColumn, ...], extracted_at: datetime) -> pa.Table:
    """Converte um lote do Oracle na tabela do Parquet, com as colunas ``_airbyte_*``.

    :param batch: Lote entregue por ``fetch_df_batches``, com as colunas na
        ordem de ``columns``.
    :param columns: Colunas do SELECT.
    :param extracted_at: Horário da foto, gravado em ``_airbyte_extracted_at``.
    :returns: Tabela com o schema de :func:`parquet_schema`.
    :raises ValueError: Se as colunas do lote não forem as esperadas.
    :raises NotImplementedError: Se algum tipo não for suportado.
    """
    names = [column.name for column in columns]
    if batch.column_names != names:
        raise ValueError(f"Colunas do lote {batch.column_names} diferem das esperadas {names}")
    count = batch.num_rows
    arrays = [
        pa.repeat(pa.scalar(extracted_at, pa.timestamp("us", tz="UTC")), count),
        pa.repeat(pa.scalar(1, pa.int64()), count),
        *[convert_column(column, batch.column(index)) for index, column in enumerate(columns)],
    ]
    return pa.Table.from_arrays(arrays, schema=parquet_schema(columns))

"""Faixas de ROWID calculadas dos extents (``dba_extents``), sem criar nada no banco.

Alternativa ao ``DBMS_PARALLEL_EXECUTE`` quando o usuário não tem ``EXECUTE`` nele. Os extents de um
mesmo segmento e arquivo são agrupados, em ordem de bloco, até ``chunk_size_blocks``; cada grupo vira
uma faixa ``[primeiro bloco, linha 0] .. [último bloco, linha 32767]``. Como o ROWID estendido carrega o
objeto de dados, blocos de outros segmentos dentro do intervalo não entram na leitura.
"""

from collections.abc import Iterable
from dataclasses import dataclass

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import to_int, validate_identifier
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.session import query_rows

ROWID_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
# Mesmo teto de linha por bloco usado pelo DBMS_PARALLEL_EXECUTE no fim de cada faixa.
MAX_ROW_IN_BLOCK = 32767


@dataclass(frozen=True)
class Extent:
    """Extent de um segmento de tabela.

    :param data_object_id: Objeto de dados do segmento (tabela ou partição).
    :param relative_fno: Número relativo do arquivo.
    :param block_id: Primeiro bloco do extent.
    :param blocks: Quantidade de blocos.
    """

    data_object_id: int
    relative_fno: int
    block_id: int
    blocks: int


@dataclass(frozen=True)
class BlockRange:
    """Intervalo de blocos de um segmento num arquivo.

    :param data_object_id: Objeto de dados do segmento.
    :param relative_fno: Número relativo do arquivo.
    :param first_block: Primeiro bloco.
    :param last_block: Último bloco (inclusivo).
    """

    data_object_id: int
    relative_fno: int
    first_block: int
    last_block: int


def encode_base64_number(value: int, width: int) -> str:
    """Codifica um inteiro não negativo no base64 do ROWID, com ``width`` caracteres.

    :param value: Número a codificar.
    :param width: Quantidade de caracteres.
    :returns: Texto com ``width`` caracteres.
    :raises ValueError: Se o número for negativo ou não couber.
    """
    if value < 0 or value >= 64**width:
        raise ValueError(f"{value} não cabe em {width} caracteres de ROWID")
    digits = []
    for _ in range(width):
        value, digit = divmod(value, 64)
        digits.append(ROWID_ALPHABET[digit])
    return "".join(reversed(digits))


def extended_rowid(data_object_id: int, relative_fno: int, block: int, row: int) -> str:
    """Monta um ROWID estendido (``OOOOOOFFFBBBBBBRRR``), como ``DBMS_ROWID.ROWID_CREATE(1, ...)``.

    :param data_object_id: Objeto de dados.
    :param relative_fno: Número relativo do arquivo.
    :param block: Número do bloco.
    :param row: Número da linha no bloco.
    :returns: ROWID em texto, com 18 caracteres.
    """
    return (
        encode_base64_number(data_object_id, 6)
        + encode_base64_number(relative_fno, 3)
        + encode_base64_number(block, 6)
        + encode_base64_number(row, 3)
    )


def group_extents(extents: Iterable[Extent], chunk_size_blocks: int) -> list[BlockRange]:
    """Agrupa extents do mesmo segmento e arquivo, em ordem de bloco, em intervalos de ~``chunk_size_blocks``.

    :param extents: Extents da tabela, em qualquer ordem.
    :param chunk_size_blocks: Blocos por intervalo; um extent maior que isso fica sozinho.
    :returns: Intervalos disjuntos que cobrem todos os extents.
    """
    ranges: list[BlockRange] = []
    current: BlockRange | None = None
    size = 0
    for extent in sorted(extents, key=lambda item: (item.data_object_id, item.relative_fno, item.block_id)):
        last = extent.block_id + extent.blocks - 1
        same_segment = current is not None and (current.data_object_id, current.relative_fno) == (
            extent.data_object_id,
            extent.relative_fno,
        )
        if current is not None and same_segment and size < chunk_size_blocks:
            current = BlockRange(current.data_object_id, current.relative_fno, current.first_block, last)
            size += extent.blocks
            continue
        if current is not None:
            ranges.append(current)
        current = BlockRange(extent.data_object_id, extent.relative_fno, extent.block_id, last)
        size = extent.blocks
    if current is not None:
        ranges.append(current)
    return ranges


def range_rowids(block_range: BlockRange) -> tuple[str, str]:
    """Converte um intervalo de blocos nos ROWIDs inicial e final da faixa.

    :param block_range: Intervalo de blocos.
    :returns: ROWID inicial e final, em texto.
    """
    start = extended_rowid(block_range.data_object_id, block_range.relative_fno, block_range.first_block, 0)
    end = extended_rowid(block_range.data_object_id, block_range.relative_fno, block_range.last_block, MAX_ROW_IN_BLOCK)
    return start, end


def read_extents(cursor: oracledb.Cursor, schema: str, table: str) -> list[Extent]:
    """Lê os extents dos segmentos da tabela (inclusive partições e subpartições).

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Dono da tabela.
    :param table: Nome da tabela.
    :returns: Extents com o objeto de dados de cada segmento.
    """
    rows = query_rows(
        cursor, "get_extents", {"owner": validate_identifier(schema), "table_name": validate_identifier(table)}
    )
    return [
        Extent(
            to_int(row["data_object_id"]), to_int(row["relative_fno"]), to_int(row["block_id"]), to_int(row["blocks"])
        )
        for row in rows
    ]

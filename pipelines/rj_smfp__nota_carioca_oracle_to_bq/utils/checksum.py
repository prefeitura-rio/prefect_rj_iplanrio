"""Checksum de conteúdo das colunas ``NUMBER`` escolhidas: contagem de não nulos e soma exata, extração vs BigQuery."""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Context, Decimal

import pyarrow as pa

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import ColumnKind, OracleColumn, column_kind
from prefect_rj_iplanrio.sql import load_query

# Contexto sem o arredondamento de 28 dígitos do padrão: as somas de ``NUMBER(20,0)`` passam disso.
EXACT = Context(prec=100)
BQ_COLUMN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,299}$")
DECIMAL256_PRECISION = 76


class ChecksumColumnError(ValueError):
    """Coluna de checksum configurada não existe no Oracle ou não é ``NUMBER``; falha antes de extrair."""


class ChecksumMismatchError(RuntimeError):
    """O checksum do BigQuery difere do extraído do Oracle; a tabela final não é trocada."""


@dataclass(frozen=True)
class ColumnChecksum:
    """Checksum de uma coluna.

    :param count: Valores não nulos.
    :param total: Soma exata, como ``str(Decimal)``, para atravessar processos sem perder precisão.
    """

    count: int
    total: str

    @property
    def amount(self) -> Decimal:
        """Retorna a soma como ``Decimal``."""
        return Decimal(self.total)


def validate_checksum_columns(table_id: str, columns: tuple[OracleColumn, ...], configured: tuple[str, ...]) -> None:
    """Confere que cada coluna de checksum existe na tabela do Oracle e é ``NUMBER``.

    :param table_id: Nome da tabela, usado na mensagem.
    :param columns: Colunas lidas do dicionário do Oracle.
    :param configured: Colunas de checksum da tabela.
    :raises ChecksumColumnError: Se alguma coluna faltar ou não for ``NUMBER``.
    """
    by_name = {column.name: column for column in columns}
    for name in configured:
        column = by_name.get(name)
        if column is None:
            raise ChecksumColumnError(f"{table_id}: coluna de checksum {name} não existe no Oracle.")
        if column_kind(column) is not ColumnKind.NUMBER:
            raise ChecksumColumnError(f"{table_id}: coluna de checksum {name} é {column.data_type}, não NUMBER.")


def chunk_checksums(table: pa.Table, names: tuple[str, ...]) -> dict[str, ColumnChecksum]:
    """Calcula contagem de não nulos e soma exata das colunas já convertidas para o Parquet.

    :param table: Tabela Arrow gravada no Parquet.
    :param names: Colunas ``decimal128`` a somar.
    :returns: Checksum de cada coluna; coluna só com nulos soma zero.
    """
    result: dict[str, ColumnChecksum] = {}
    for name in names:
        column = table.column(name)
        scale = column.type.scale
        total = column.combine_chunks().cast(pa.decimal256(DECIMAL256_PRECISION, scale)).sum().as_py()
        result[name] = ColumnChecksum(count=len(column) - column.null_count, total=str(total or Decimal(0)))
    return result


def merge_checksums(parts: Sequence[Mapping[str, ColumnChecksum]], names: tuple[str, ...]) -> dict[str, ColumnChecksum]:
    """Soma os checksums das faixas.

    :param parts: Checksum de cada faixa.
    :param names: Colunas de checksum da tabela.
    :returns: Totais por coluna; zero se não houver faixas.
    """
    merged: dict[str, ColumnChecksum] = {}
    for name in names:
        total = Decimal(0)
        count = 0
        for part in parts:
            total = EXACT.add(total, part[name].amount)
            count += part[name].count
        merged[name] = ColumnChecksum(count=count, total=str(total))
    return merged


def render_checksum_select(project: str, dataset_id: str, table_id: str, names: tuple[str, ...]) -> str:
    """Monta o SELECT único de contagem e soma das colunas de checksum da tabela temporária.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Tabela temporária.
    :param names: Colunas de checksum.
    :returns: SQL renderizado de ``queries/``.
    :raises ValueError: Se algum nome não for um identificador simples do BigQuery.
    """
    for name in names:
        if not BQ_COLUMN_NAME.match(name):
            raise ValueError(f"Nome de coluna de checksum inválido: {name!r}")
    selects = ",\n    ".join(load_query(QUERIES_ANCHOR, "checksum_column", column=name).strip() for name in names)
    return load_query(
        QUERIES_ANCHOR, "checksum_table", project=project, dataset_id=dataset_id, table_id=table_id, selects=selects
    )


def parse_checksum_row(row: Mapping[str, object], names: tuple[str, ...]) -> dict[str, ColumnChecksum]:
    """Converte a linha do SELECT de checksum.

    :param row: Linha com ``c_<coluna>`` (contagem) e ``s_<coluna>`` (soma como texto; nula sem valores).
    :param names: Colunas de checksum.
    :returns: Checksum de cada coluna.
    :raises TypeError: Se a contagem não for inteira ou a soma não for texto.
    """
    result: dict[str, ColumnChecksum] = {}
    for name in names:
        count, total = row[f"c_{name}"], row[f"s_{name}"]
        if not isinstance(count, int) or not (total is None or isinstance(total, str)):
            raise TypeError(f"Checksum de {name} inesperado: contagem {count!r}, soma {total!r}")
        result[name] = ColumnChecksum(count=count, total=total or "0")
    return result


def assert_checksums_match(
    table_id: str, extracted: Mapping[str, ColumnChecksum], bigquery: Mapping[str, ColumnChecksum]
) -> None:
    """Confere, de forma exata, o checksum extraído do Oracle contra o da tabela temporária.

    :param table_id: Nome da tabela.
    :param extracted: Checksum agregado das faixas extraídas.
    :param bigquery: Checksum lido da tabela temporária.
    :raises ChecksumMismatchError: Se contagem ou soma divergir em qualquer coluna.
    """
    differences = [
        f"{name}: extraído {expected.count} não nulos, soma {expected.amount}; "
        f"BigQuery {bigquery[name].count}, soma {bigquery[name].amount}"
        for name, expected in extracted.items()
        if expected.count != bigquery[name].count or expected.amount != bigquery[name].amount
    ]
    if differences:
        raise ChecksumMismatchError(
            f"{table_id}: checksum de conteúdo divergente ({'; '.join(differences)}); a tabela final não foi alterada."
        )


def format_checksums(checksums: Mapping[str, ColumnChecksum]) -> str:
    """Formata os checksums para o log.

    :param checksums: Checksum por coluna.
    :returns: Texto ``COL: n não nulos, soma S`` separado por ``;``.
    """
    return "; ".join(f"{name}: {value.count:,} não nulos, soma {value.amount}" for name, value in checksums.items())

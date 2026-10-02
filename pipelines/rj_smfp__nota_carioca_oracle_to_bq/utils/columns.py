"""Mapeamento dos tipos das colunas do Oracle para o BigQuery e para o Arrow."""

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import assert_never

import pyarrow as pa

# NUMERIC do BigQuery: 38 dígitos, no máximo 9 deles decimais.
BQ_NUMERIC_MAX_SCALE = 9
BQ_NUMERIC_MAX_INTEGER_DIGITS = 29
QUOTABLE_NAME = re.compile(r'^[^"\x00]{1,128}$')


class ColumnKind(StrEnum):
    """Famílias de tipo do Oracle que a pipeline sabe converter."""

    NUMBER = "number"
    TEXT = "text"
    RAW = "raw"
    DATE = "date"


@dataclass(frozen=True)
class OracleColumn:
    """Coluna de uma tabela do Oracle, como no dicionário de dados.

    :param name: Nome da coluna.
    :param data_type: ``DATA_TYPE`` do dicionário (``NUMBER``, ``VARCHAR2``...).
    :param precision: ``DATA_PRECISION``; nulo para ``NUMBER`` sem precisão.
    :param scale: ``DATA_SCALE``.
    """

    name: str
    data_type: str
    precision: int | None
    scale: int | None

    @property
    def quoted(self) -> str:
        """Retorna o nome entre aspas duplas, pronto para uso em SQL.

        :raises ValueError: Se o nome contiver aspas ou for vazio.
        """
        if not QUOTABLE_NAME.match(self.name):
            raise ValueError(f"Nome de coluna não pode ser citado em SQL: {self.name!r}")
        return f'"{self.name}"'


def column_kind(column: OracleColumn) -> ColumnKind:
    """Classifica o tipo da coluna, recusando tudo que não for explicitamente suportado.

    :param column: Coluna lida do dicionário de dados.
    :returns: A família do tipo.
    :raises NotImplementedError: Se o tipo não for suportado, se ``NUMBER`` não
        tiver precisão ou se não couber no ``NUMERIC`` do BigQuery.
    """
    match column.data_type:
        case "NUMBER":
            check_numeric(column)
            return ColumnKind.NUMBER
        case "VARCHAR2" | "CHAR":
            return ColumnKind.TEXT
        case "RAW":
            return ColumnKind.RAW
        case "DATE":
            return ColumnKind.DATE
        case other:
            raise NotImplementedError(f"Tipo {other} da coluna {column.name} não tem conversão definida.")


def check_numeric(column: OracleColumn) -> tuple[int, int]:
    """Confere que um ``NUMBER`` tem precisão e cabe no ``NUMERIC`` do BigQuery.

    :param column: Coluna ``NUMBER``.
    :returns: Precisão e escala.
    :raises NotImplementedError: Se faltar precisão ou a faixa não couber.
    """
    if column.precision is None:
        raise NotImplementedError(f"NUMBER sem precisão na coluna {column.name}; o tipo exato é desconhecido.")
    scale = column.scale or 0
    if scale < 0 or scale > BQ_NUMERIC_MAX_SCALE or column.precision - scale > BQ_NUMERIC_MAX_INTEGER_DIGITS:
        raise NotImplementedError(
            f"NUMBER({column.precision},{scale}) da coluna {column.name} não cabe em NUMERIC do BigQuery."
        )
    return column.precision, scale


def bq_type(column: OracleColumn) -> str:
    """Retorna o tipo da coluna no BigQuery, no contrato do destino.

    :param column: Coluna do Oracle.
    :returns: ``NUMERIC`` ou ``STRING``.
    :raises NotImplementedError: Se o tipo não for suportado.
    """
    kind = column_kind(column)
    match kind:
        case ColumnKind.NUMBER:
            return "NUMERIC"
        case ColumnKind.TEXT | ColumnKind.RAW | ColumnKind.DATE:
            return "STRING"
        case unreachable:
            assert_never(unreachable)


def fetch_type(column: OracleColumn) -> pa.DataType:
    """Retorna o tipo Arrow que o driver deve entregar para a coluna.

    :param column: Coluna do Oracle.
    :returns: ``decimal128(p, s)`` para números, ``binary`` para ``RAW``,
        ``timestamp`` para ``DATE`` e ``string`` para texto.
    :raises NotImplementedError: Se o tipo não for suportado.
    """
    kind = column_kind(column)
    match kind:
        case ColumnKind.NUMBER:
            precision, scale = check_numeric(column)
            return pa.decimal128(precision, scale)
        case ColumnKind.TEXT:
            return pa.string()
        case ColumnKind.RAW:
            return pa.binary()
        case ColumnKind.DATE:
            return pa.timestamp("us")
        case unreachable:
            assert_never(unreachable)


def output_type(column: OracleColumn) -> pa.DataType:
    """Retorna o tipo Arrow gravado no Parquet para a coluna.

    :param column: Coluna do Oracle.
    :returns: ``decimal128(p, s)`` para números e ``string`` para o resto.
    :raises NotImplementedError: Se o tipo não for suportado.
    """
    return fetch_type(column) if column_kind(column) is ColumnKind.NUMBER else pa.string()


def fetch_schema(columns: tuple[OracleColumn, ...]) -> pa.Schema:
    """Monta o schema pedido ao driver, uma entrada por coluna do SELECT.

    :param columns: Colunas na ordem do SELECT.
    :returns: Schema Arrow para ``requested_schema``.
    """
    return pa.schema([pa.field(column.name, fetch_type(column)) for column in columns])

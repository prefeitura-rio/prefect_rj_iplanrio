"""Consultas ao Oracle de destino: buscar linhas, valores e nomes de coluna."""

from decimal import Decimal
from pathlib import Path
from typing import cast

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import OracleColumn

# load_query resolve queries/ no diretório pai do caminho recebido; a pasta utils/
# aponta para a raiz da pipeline.
QUERIES_ANCHOR = str(Path(__file__).parent)


type Row = tuple[object, ...]


def fetch_all(cursor: oracledb.Cursor) -> list[Row]:
    """Return every row that the cursor has left."""
    return cast("list[Row]", cursor.fetchall())


def fetch_one(cursor: oracledb.Cursor) -> Row | None:
    """Return the next row of the cursor, or nothing if it has none."""
    return cast("Row | None", cursor.fetchone())


def fetch_scalar(cursor: oracledb.Cursor) -> object:
    """Return the first value of the next row, or nothing if there is no row."""
    row = fetch_one(cursor)

    return row[0] if row else None


def column_names(cursor: oracledb.Cursor) -> list[str]:
    """Return the names of the columns of the last query, in lower case."""
    return [str(description[0]).lower() for description in cursor.description or []]


def column_definitions(columns: list[OracleColumn]) -> str:
    """Monta a lista de colunas do ``CREATE TABLE``.

    :param columns: Colunas da tabela.
    :returns: Fragmento SQL com uma coluna por linha.
    """
    return ",\n".join(f"  {column.definition}" for column in columns)


def to_int(value: object) -> int:
    """Converte para inteiro um número lido do banco, tratando nulo como zero.

    :param value: Valor de uma coluna numérica.
    :returns: O valor inteiro.
    :raises TypeError: Se o valor não for numérico.
    """
    if value is None:
        return 0
    if isinstance(value, int | float | Decimal | str):
        return int(value)
    raise TypeError(f"Valor não numérico: {value!r}")

"""Plano de conversão do CSV do BigQuery para as colunas de uma tabela do slot.

As regras são as do control file do SQL*Loader (``bq_to_oracle/utils/sqlldr.py`` e ``columns.py``), com o tipo do
Postgres no lugar do tipo do Oracle:

- texto (``STRING``/``JSON``): vazio e NULL viram NULL (o Oracle não distingue ``''`` de NULL);
- número (``NUMERIC``/``INTEGER``): decimal exato (``numeric``), nunca ponto flutuante; vazio vira NULL;
- data guardada como texto (``STRING``): só ``AAAA-MM-DDTHH:MI:SS`` (19 caracteres); qualquer outro formato reprova
  a carga inteira, como o ``ERRORS=0`` do SQL*Loader; vazio vira NULL;
- qualquer outro par de tipos: ``NotImplementedError``.

O CSV entra numa tabela de staging só de texto e as conversões acontecem no ``INSERT ... SELECT``.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import psycopg
from psycopg import sql

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.manifest import BqField

TEXT_TYPES = ("character varying", "character", "text")
NUMBER_TYPES = ("numeric", "integer", "bigint", "smallint")
TIMESTAMP_TYPE = "timestamp without time zone"
TEXT_BQ_TYPES = ("STRING", "JSON")
NUMBER_BQ_TYPES = ("NUMERIC", "INTEGER")
DATE_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$"
"""Formato único das datas em texto do BigQuery (a máscara ``YYYY-MM-DD"T"HH24:MI:SS`` do control file)."""


@dataclass(frozen=True)
class PgColumn:
    """Uma coluna da tabela do slot, como em ``information_schema.columns``.

    :param name: Nome da coluna.
    :param data_type: Tipo (``character varying``, ``numeric``, ``timestamp without time zone``...).
    :param nullable: Se aceita NULL.
    """

    name: str
    data_type: str
    nullable: bool


@dataclass(frozen=True)
class ColumnConversion:
    """Como uma coluna do slot sai de uma coluna de texto da staging.

    :param name: Coluna de destino.
    :param stage: Coluna da staging de onde vem o texto.
    :param expression: Expressão do ``SELECT`` que converte o texto para o tipo da coluna.
    :param is_date: Se a coluna precisa do formato estrito de data antes da conversão.
    """

    name: str
    stage: str
    expression: sql.Composable
    is_date: bool


@dataclass(frozen=True)
class ConversionPlan:
    """Como carregar o CSV de uma tabela.

    :param stage_columns: Uma coluna de texto da staging por campo do CSV, na ordem dele.
    :param columns: Conversões, na ordem das colunas do slot.
    :param ignored: Campos do export que o slot não tem (metadados ``_bigquery_*``), lidos e descartados.
    """

    stage_columns: tuple[str, ...]
    columns: tuple[ColumnConversion, ...]
    ignored: tuple[str, ...]


def read_columns(connection: psycopg.Connection | psycopg.Cursor, schema: str, table: str) -> list[PgColumn]:
    """Lê as colunas de uma tabela, na ordem em que foram criadas.

    :param connection: Conexão ou cursor do psycopg.
    :param schema: Schema do slot.
    :param table: Tabela do slot.
    :returns: As colunas.
    """
    rows = connection.execute(
        "SELECT column_name, data_type, is_nullable = 'YES' FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        [schema, table],
    ).fetchall()
    return [PgColumn(name=name, data_type=data_type, nullable=nullable) for name, data_type, nullable in rows]


def _expression(column: PgColumn, field_type: str, stage: str) -> tuple[sql.Composable, bool]:
    source = sql.SQL("NULLIF({}, '')").format(sql.Identifier(stage))
    if column.data_type in TEXT_TYPES and field_type in TEXT_BQ_TYPES:
        return source, False
    if column.data_type in NUMBER_TYPES and field_type in NUMBER_BQ_TYPES:
        return sql.SQL("{}::numeric").format(source), False
    if column.data_type == TIMESTAMP_TYPE and field_type == "STRING":
        return sql.SQL("{}::timestamp").format(source), True
    raise NotImplementedError(
        f"Coluna {column.name}: carga de {field_type} do BigQuery em {column.data_type} sem suporte."
    )


def build_plan(columns: Sequence[PgColumn], fields: Sequence[BqField]) -> ConversionPlan:
    """Monta o plano de carga do CSV de uma tabela.

    :param columns: Colunas da tabela do slot.
    :param fields: Campos do export, na ordem do CSV.
    :returns: Conversões por coluna do slot e os campos do export que ficam de fora.
    :raises ValueError: Se faltar no export alguma coluna do slot.
    :raises NotImplementedError: Se algum par de tipos não tiver suporte.
    """
    stage = {field.name.lower(): f"c{index}" for index, field in enumerate(fields)}
    types = {field.name.lower(): field.type for field in fields}
    missing = [column.name for column in columns if column.name.lower() not in stage]
    if missing:
        raise ValueError(f"Colunas da tabela do slot ausentes no export do BigQuery: {missing}")
    conversions = []
    for column in columns:
        key = column.name.lower()
        expression, is_date = _expression(column, types[key], stage[key])
        conversions.append(ColumnConversion(column.name, stage[key], expression, is_date))
    slot_names = {column.name.lower() for column in columns}
    return ConversionPlan(
        stage_columns=tuple(stage.values()),
        columns=tuple(conversions),
        ignored=tuple(field.name for field in fields if field.name.lower() not in slot_names),
    )

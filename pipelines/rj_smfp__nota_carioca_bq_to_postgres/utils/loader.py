"""Carga de um slot: TRUNCATE, COPY em staging de texto, INSERT ... SELECT com conversão e ANALYZE.

Tudo que escreve no slot roda numa só transação: se qualquer arquivo ou linha falhar, o slot volta ao que tinha (e ele
é o slot inativo, que nenhuma view lê). O stream do GCS vai direto para o ``COPY ... FROM STDIN`` do psycopg, sem
gravar em disco, um arquivo por vez; a staging é temporária e esvaziada a cada arquivo.
"""

import gzip
from collections.abc import Callable, Mapping
from dataclasses import dataclass

import psycopg
from psycopg import sql

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.conversion import (
    DATE_PATTERN,
    ConversionPlan,
    build_plan,
    read_columns,
)
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.manifest import TableExport
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.object_store import ObjectStore
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.slots import SLOTS
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.tables import SlotTable
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

COPY_CHUNK_BYTES = 1024 * 1024
STAGE_TABLE = "_nc_stage"


@dataclass(frozen=True)
class LoadRequest:
    """O que carregar e onde.

    :param store: Bucket do export.
    :param bucket: Nome do bucket dos arquivos.
    :param slot: ``a`` ou ``b``: o slot que recebe a carga (o inativo).
    :param exports: Tabela do slot e o export que a alimenta.
    """

    store: ObjectStore
    bucket: str
    slot: str
    exports: Mapping[SlotTable, TableExport]


def _schema(slot: str) -> str:
    if slot not in SLOTS:
        raise ValueError(f"slot inválido: {slot!r}, esperado a ou b")
    return f"exigiveis_{slot}"


def _create_stage(cursor: psycopg.Cursor, plan: ConversionPlan) -> None:
    cursor.execute(sql.SQL("DROP TABLE IF EXISTS pg_temp.{}").format(sql.Identifier(STAGE_TABLE)))
    columns = sql.SQL(", ").join(sql.SQL("{} text").format(sql.Identifier(name)) for name in plan.stage_columns)
    cursor.execute(sql.SQL("CREATE TEMP TABLE {} ({})").format(sql.Identifier(STAGE_TABLE), columns))


def _copy_file(cursor: psycopg.Cursor, request: LoadRequest, name: str, plan: ConversionPlan) -> None:
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in plan.stage_columns)
    statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv)").format(sql.Identifier(STAGE_TABLE), columns)
    with request.store.open_binary(request.bucket, name) as raw, gzip.GzipFile(fileobj=raw) as unzipped:
        with cursor.copy(statement) as copy:
            while chunk := unzipped.read(COPY_CHUNK_BYTES):
                copy.write(chunk)


def _check_dates(cursor: psycopg.Cursor, table: SlotTable, plan: ConversionPlan) -> None:
    """Reprova a carga se uma data estiver fora do formato do export, como o ``ERRORS=0`` do SQL*Loader."""
    for column in plan.columns:
        if not column.is_date:
            continue
        stage = sql.Identifier(column.stage)
        bad = cursor.execute(
            sql.SQL("SELECT {stage} FROM {table} WHERE {stage} <> '' AND {stage} !~ %s LIMIT 1").format(
                stage=stage, table=sql.Identifier(STAGE_TABLE)
            ),
            [DATE_PATTERN],
        ).fetchone()
        if bad is not None:
            raise ValueError(f"{table.pg_name}.{column.name}: valor fora do formato AAAA-MM-DDTHH:MI:SS: {bad[0]!r}")


def _insert(cursor: psycopg.Cursor, schema: str, table: SlotTable, plan: ConversionPlan) -> int:
    names = sql.SQL(", ").join(sql.Identifier(column.name) for column in plan.columns)
    expressions = sql.SQL(", ").join(column.expression for column in plan.columns)
    cursor.execute(
        sql.SQL("INSERT INTO {}.{} ({}) SELECT {} FROM {}").format(
            sql.Identifier(schema), sql.Identifier(table.pg_name), names, expressions, sql.Identifier(STAGE_TABLE)
        )
    )
    return cursor.rowcount


def _load_table(cursor: psycopg.Cursor, request: LoadRequest, table: SlotTable, export: TableExport) -> int:
    schema = _schema(request.slot)
    plan = build_plan(read_columns(cursor, schema, table.pg_name), export.fields)
    if plan.ignored:
        logger.info("%s: campos do export que o slot não tem e são descartados: %s", table.bq_name, plan.ignored)
    cursor.execute(sql.SQL("TRUNCATE {}.{}").format(sql.Identifier(schema), sql.Identifier(table.pg_name)))
    _create_stage(cursor, plan)
    loaded = 0
    for exported in export.files:
        _copy_file(cursor, request, exported.name, plan)
        _check_dates(cursor, table, plan)
        loaded += _insert(cursor, schema, table, plan)
        cursor.execute(sql.SQL("TRUNCATE {}").format(sql.Identifier(STAGE_TABLE)))
    cursor.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(STAGE_TABLE)))
    return loaded


def load_slot(
    connection: psycopg.Connection, request: LoadRequest, on_table: Callable[[SlotTable, int], None] | None = None
) -> dict[str, int]:
    """Esvazia e carrega as tabelas do slot a partir do export, e coleta as estatísticas.

    :param connection: Conexão em autocommit, de um login em ``nc_carregador``.
    :param request: Slot e export.
    :param on_table: Chamada com a tabela e as linhas carregadas, ao fim de cada tabela.
    :returns: Linhas carregadas por tabela do slot (``pg_name``).
    :raises ValueError: Se uma data estiver fora do formato ou o export não bater com o slot.
    :raises psycopg.Error: Se o Postgres recusar um valor (número inválido, NOT NULL, tamanho); nada é mantido.
    """
    loaded: dict[str, int] = {}
    with connection.transaction(), connection.cursor() as cursor:
        for table, export in request.exports.items():
            loaded[table.pg_name] = _load_table(cursor, request, table, export)
            logger.info("%s: %d linhas carregadas em %s", table.pg_name, loaded[table.pg_name], _schema(request.slot))
            if on_table is not None:
                on_table(table, loaded[table.pg_name])
    for table in request.exports:
        connection.execute(
            sql.SQL("ANALYZE {}.{}").format(sql.Identifier(_schema(request.slot)), sql.Identifier(table.pg_name))
        )
    return loaded

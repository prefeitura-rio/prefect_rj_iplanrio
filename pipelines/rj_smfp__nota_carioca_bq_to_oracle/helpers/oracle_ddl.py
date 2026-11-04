"""DDL no Oracle de destino: criar, truncar, indexar e contar as tabelas gerenciadas."""

from dataclasses import dataclass

import oracledb
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import (
    MANAGED_TABLE_MARKER,
    TABLE_PREFIX,
    OracleConfig,
    assert_managed_table,
    assert_privileges,
    connect,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_query import (
    QUERIES_ANCHOR,
    Row,
    column_definitions,
    fetch_one,
    to_int,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_read import read_column_definitions, read_table_layout
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure import layout_differences
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_ddl import index_statement_parts, storage_clause
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import IndexDefinition, TableLayout
from utils.utils import render_query


def definition_differences(existing: list[str], expected: list[str]) -> list[str]:
    """List, position by position, the columns that differ from the original.

    Return the differences as `current → expected`, empty if they are equal.
    """
    size = max(len(existing), len(expected))
    padded_existing = existing + [""] * (size - len(existing))
    padded_expected = expected + [""] * (size - len(expected))
    return [
        f"{found or '(ausente)'} → {wanted or '(ausente)'}"
        for found, wanted in zip(padded_existing, padded_expected, strict=True)
        if found != wanted
    ]


@dataclass(frozen=True)
class TableDefinition:
    """The full definition of the target table.

    `columns` are in the order of the original table, `layout` has its tablespace and
    partitioning, and `source` is the source table, written in the comment.
    """

    columns: list[OracleColumn]
    layout: TableLayout
    source: str


def create_managed_table(cursor: oracledb.Cursor, schema: str, table: str, definition: TableDefinition) -> None:
    """Create the target table and write the mark of the pipeline in the comment."""
    cursor.execute(
        render_query(
            QUERIES_ANCHOR,
            "create_table",
            schema=schema,
            table=table,
            columns=column_definitions(definition.columns),
            storage=storage_clause(definition.layout),
        )
    )
    comment = f"{MANAGED_TABLE_MARKER}: carga a partir de {definition.source}".replace("'", "''")
    cursor.execute(render_query(QUERIES_ANCHOR, "comment_on_table", schema=schema, table=table, comment=comment))


def ensure_table(config: OracleConfig, table: str, definition: TableDefinition) -> str:
    """Give the target table the same definition as the original table.

    It creates the table if it does not exist. If it exists and the pipeline made it
    (the prefix and the mark in the comment), it is reused when the columns, the
    tablespace and the partitions equal those of the original, and dropped and created
    again when they differ. A table without the mark of the pipeline is never changed.
    Return what was done, as text for the log. Raise `PermissionError` if the session
    lacks the privileges or if the table exists without the mark of the pipeline.
    """
    target = f"{config.schema}.{table}"
    binds = {"owner": config.schema, "table_name": table}
    with connect(config) as connection, connection.cursor() as cursor:
        assert_privileges(cursor, config.schema)
        cursor.execute(render_query(QUERIES_ANCHOR, "get_table_comment"), binds)
        row = fetch_one(cursor)
        if row is None:
            create_managed_table(cursor, config.schema, table, definition)
            return f"Tabela {target} criada com a definição da original ({len(definition.columns)} colunas)."

        assert_managed_table(table, comment_of(row))
        existing = [column.definition for column in read_column_definitions(cursor, config.schema, table)]
        differences = definition_differences(existing, [column.definition for column in definition.columns])
        existing_layout = read_table_layout(cursor, config.schema, table)
        if existing_layout is not None:
            differences += layout_differences(existing_layout, definition.layout)
        if not differences:
            return f"Tabela {target} já existe com a definição da original e será recarregada."

        cursor.execute(render_query(QUERIES_ANCHOR, "drop_table", schema=config.schema, table=table))
        create_managed_table(cursor, config.schema, table, definition)
        return f"Tabela {target} divergia da original e foi apagada e recriada. Diferenças: {differences}"


def comment_of(row: Row) -> str | None:
    """Return the comment of a table from the row of `get_table_comment`."""
    return None if row[1] is None else str(row[1])


def assert_managed_existing_table(cursor: oracledb.Cursor, schema: str, table: str) -> None:
    """Confirm that the table exists and belongs to the pipeline before it changes it.

    Raise `PermissionError` if the table does not exist or does not belong to the
    pipeline.
    """
    cursor.execute(render_query(QUERIES_ANCHOR, "get_table_comment"), {"owner": schema, "table_name": table})
    row = fetch_one(cursor)
    if row is None:
        raise PermissionError(f"{schema}.{table} não existe.")
    assert_managed_table(table, comment_of(row))


def truncate_table(config: OracleConfig, table: str) -> None:
    """Empty the target table, after it confirms that it belongs to the pipeline.

    Raise `PermissionError` if the table does not exist or does not belong to the
    pipeline.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        cursor.execute(render_query(QUERIES_ANCHOR, "truncate_table", schema=config.schema, table=table))
    logger.info("Tabela %s.%s esvaziada", config.schema, table)


def foreign_indexes(indexes: list[IndexDefinition], schema: str) -> list[str]:
    """List the indexes that the pipeline did not create and so cannot drop.

    Return their names as `owner.index`: the indexes out of the schema or without the
    prefix of the pipeline.
    """
    return [
        f"{index.owner}.{index.name}"
        for index in indexes
        if index.owner != schema or not index.name.startswith(TABLE_PREFIX)
    ]


def drop_managed_indexes(config: OracleConfig, table: str) -> list[str]:
    """Drop the indexes of the target table, which the parallel load does not accept.

    It drops only indexes with the prefix of the pipeline, in the target schema, on a
    table that the pipeline made. If there is any other index, nothing is dropped.
    Return the names of the dropped indexes. Raise `PermissionError` if the table does
    not belong to the pipeline or has an index that the pipeline did not create.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        layout = read_table_layout(cursor, config.schema, table)
        indexes = list(layout.indexes) if layout else []
        others = foreign_indexes(indexes, config.schema)
        if others:
            raise PermissionError(
                f"{config.schema}.{table} tem índices que a pipeline não criou: {others}. Nada foi alterado; "
                + "a carga em paralelo exige a tabela sem índices."
            )
        for index in indexes:
            cursor.execute(render_query(QUERIES_ANCHOR, "drop_index", schema=config.schema, index=index.name))
    return [index.name for index in indexes]


def create_index(config: OracleConfig, table: str, index: IndexDefinition, parallel_degree: int) -> None:
    """Create an index on the target table, and then remove its parallelism.

    `PARALLEL` speeds up the creation, but if it stayed on the index, the optimizer
    would use parallel queries for whoever reads the table. `index` already has the name
    of the target table, and `parallel_degree` is the degree of the creation. Raise
    `PermissionError` if the table does not belong to the pipeline.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        parts = index_statement_parts(index, parallel_degree)
        cursor.execute(
            render_query(QUERIES_ANCHOR, "create_index", schema=config.schema, table=table, index=index.name, **parts)
        )
        cursor.execute(render_query(QUERIES_ANCHOR, "disable_index_parallel", schema=config.schema, index=index.name))


def gather_table_stats(config: OracleConfig, table: str, parallel_degree: int) -> None:
    """Gather the statistics of the target table for the optimizer.

    The statistics of the indexes are already computed in `CREATE INDEX`.
    `parallel_degree` is the degree of the gather.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            render_query(QUERIES_ANCHOR, "gather_table_stats"),
            {"owner": config.schema, "table_name": table, "degree": parallel_degree},
        )


def count_rows(config: OracleConfig, table: str, parallel_degree: int = 1) -> int:
    """Count the rows of the target table, with a parallel scan.

    `parallel_degree` is the degree of the `PARALLEL` hint of `COUNT(*)`, and 1 is a
    serial scan.
    """
    query = render_query(
        QUERIES_ANCHOR, "count_rows", schema=config.schema, table=table, degree=max(int(parallel_degree), 1)
    )
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(query)
        row = fetch_one(cursor)
    return to_int(row[0] if row else 0)

"""Leitura no Oracle de destino: definições de coluna e layout das tabelas."""

from collections.abc import Mapping

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import OracleColumn, oracle_column
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import OracleConfig, connect
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_query import QUERIES_ANCHOR, column_names, fetch_all
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure import (
    index_from_dictionary,
    index_key,
    inmemory_from_dictionary,
    partitioning_from_dictionary,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import TableLayout
from utils.utils import render_query


def read_column_definitions(cursor: oracledb.Cursor, owner: str, table: str) -> list[OracleColumn]:
    """Read the columns of a table in the Oracle dictionary, in the order of the table.

    Return an empty list if the table does not exist or the session cannot see it.
    """
    cursor.execute(render_query(QUERIES_ANCHOR, "get_column_definitions"), {"owner": owner, "table_name": table})
    names = column_names(cursor)
    return [oracle_column(dict(zip(names, row, strict=True))) for row in fetch_all(cursor)]


def fetch_template_columns(config: OracleConfig, template_schema: str, table: str) -> list[OracleColumn]:
    """Read the definition of the original table, which is the model of the target one.

    `table` is the name of the original table, the same as the table in BigQuery. Return
    its columns in its order. Raise `LookupError` if the original table does not exist
    or is not visible.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        columns = read_column_definitions(cursor, template_schema, table)
    if not columns:
        raise LookupError(
            f"Tabela original {template_schema}.{table} não encontrada; ela define os tipos da tabela de destino."
        )
    return columns


def fetch_rows(cursor: oracledb.Cursor, query: str, binds: Mapping[str, object]) -> list[dict[str, object]]:
    """Run a query and return the rows as dictionaries, with the columns in lower case.

    `query` is the name of the file in `queries/`, with no extension, and `binds` are
    the values of the bind variables.
    """
    cursor.execute(render_query(QUERIES_ANCHOR, query), dict(binds))
    names = column_names(cursor)
    return [dict(zip(names, row, strict=True)) for row in fetch_all(cursor)]


def read_table_layout(cursor: oracledb.Cursor, owner: str, table: str) -> TableLayout | None:
    """Read the tablespace, partitioning, indexes and In-Memory of a table.

    They come from the Oracle dictionary.

    Return `None` if the table does not exist or the session cannot see it.
    """
    binds = {"owner": owner, "table_name": table}
    storage = fetch_rows(cursor, "get_table_tablespace", binds)
    if not storage:
        return None
    partitioning = None
    part_table = fetch_rows(cursor, "get_partitioning", binds)
    if part_table:
        keys = [str(row["column_name"]) for row in fetch_rows(cursor, "get_partition_key_columns", binds)]
        partitions = fetch_rows(cursor, "get_table_partitions", binds)
        partitioning = partitioning_from_dictionary(part_table[0], keys, partitions)

    columns: dict[tuple[object, object], list[str]] = {}
    for row in fetch_rows(cursor, "get_index_columns", binds):
        columns.setdefault((row["index_owner"], row["index_name"]), []).append(index_key(row))
    indexes = tuple(
        index_from_dictionary(row, columns.get((row["owner"], row["index_name"]), []))
        for row in fetch_rows(cursor, "get_indexes", binds)
    )
    tablespace = storage[0]["tablespace_name"] or storage[0]["def_tablespace_name"]
    return TableLayout(
        tablespace=None if tablespace is None else str(tablespace),
        partitioning=partitioning,
        indexes=indexes,
        inmemory=inmemory_from_dictionary(storage[0]),
    )


def fetch_template_layout(config: OracleConfig, template_schema: str, table: str) -> TableLayout:
    """Read the tablespace, partitioning and indexes of the original table.

    Raise `LookupError` if the original table does not exist or is not visible.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        layout = read_table_layout(cursor, template_schema, table)
    if layout is None:
        raise LookupError(f"Tabela original {template_schema}.{table} não encontrada.")
    return layout

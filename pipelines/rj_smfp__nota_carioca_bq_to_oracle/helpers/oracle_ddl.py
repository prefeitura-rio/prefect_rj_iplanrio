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
    """Lista, posição a posição, as colunas cuja definição difere da original.

    :param existing: Definições das colunas da tabela existente.
    :param expected: Definições das colunas da tabela original.
    :returns: Diferenças no formato ``atual → esperada``; vazia se forem iguais.
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
    """Definição completa da tabela de destino.

    :param columns: Colunas, na ordem da tabela original.
    :param layout: Tablespace e particionamento da tabela original.
    :param source: Tabela de origem, gravada no comentário.
    """

    columns: list[OracleColumn]
    layout: TableLayout
    source: str


def create_managed_table(cursor: oracledb.Cursor, schema: str, table: str, definition: TableDefinition) -> None:
    """Cria a tabela de destino e grava a marca da pipeline no comentário.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema da tabela.
    :param table: Nome da tabela.
    :param definition: Colunas, tablespace, particionamento e origem.
    """
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
    """Deixa a tabela de destino com a mesma definição da tabela original.

    Cria a tabela se ela não existir. Se existir e tiver sido criada pela
    pipeline (prefixo e marca no comentário), é reaproveitada quando colunas,
    tablespace e partições são iguais aos da original, ou apagada e recriada
    quando divergem. Tabelas sem a marca da pipeline nunca são alteradas.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :param definition: Colunas, tablespace, particionamento e origem.
    :returns: O que foi feito, em texto, para registrar no log.
    :raises PermissionError: Se a sessão não tiver os privilégios necessários ou
        se a tabela existir sem a marca da pipeline.
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
    """Confirma que a tabela existe e pertence à pipeline antes de alterá-la.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema da tabela.
    :param table: Nome da tabela.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    cursor.execute(render_query(QUERIES_ANCHOR, "get_table_comment"), {"owner": schema, "table_name": table})
    row = fetch_one(cursor)
    if row is None:
        raise PermissionError(f"{schema}.{table} não existe.")
    assert_managed_table(table, comment_of(row))


def truncate_table(config: OracleConfig, table: str) -> None:
    """Esvazia a tabela de destino, após confirmar que ela pertence à pipeline.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :raises PermissionError: Se a tabela não existir ou não pertencer à pipeline.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        cursor.execute(render_query(QUERIES_ANCHOR, "truncate_table", schema=config.schema, table=table))
    logger.info("Tabela %s.%s esvaziada", config.schema, table)


def foreign_indexes(indexes: list[IndexDefinition], schema: str) -> list[str]:
    """Lista os índices que a pipeline não criou e, por isso, não pode apagar.

    :param indexes: Índices da tabela de destino.
    :param schema: Schema da tabela de destino.
    :returns: Nomes, no formato ``dono.índice``, dos índices fora do schema ou sem
        o prefixo da pipeline.
    """
    return [
        f"{index.owner}.{index.name}"
        for index in indexes
        if index.owner != schema or not index.name.startswith(TABLE_PREFIX)
    ]


def drop_managed_indexes(config: OracleConfig, table: str) -> list[str]:
    """Apaga os índices da tabela de destino, que a carga paralela não aceita.

    Só apaga índices com o prefixo da pipeline, no schema de destino e em tabela
    criada pela pipeline. Se houver qualquer outro índice, nada é apagado.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :returns: Nomes dos índices apagados.
    :raises PermissionError: Se a tabela não pertencer à pipeline ou tiver índice
        que a pipeline não criou.
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
    """Cria um índice na tabela de destino e depois tira o paralelismo dele.

    O ``PARALLEL`` acelera a criação, mas, se ficasse gravado no índice, faria o
    otimizador usar consultas paralelas em quem lê a tabela.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :param index: Índice a criar, já com o nome da tabela de destino.
    :param parallel_degree: Grau de paralelismo da criação.
    :raises PermissionError: Se a tabela não pertencer à pipeline.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        parts = index_statement_parts(index, parallel_degree)
        cursor.execute(
            render_query(QUERIES_ANCHOR, "create_index", schema=config.schema, table=table, index=index.name, **parts)
        )
        cursor.execute(render_query(QUERIES_ANCHOR, "disable_index_parallel", schema=config.schema, index=index.name))


def gather_table_stats(config: OracleConfig, table: str, parallel_degree: int) -> None:
    """Coleta as estatísticas da tabela de destino para o otimizador.

    As estatísticas dos índices já são calculadas no ``CREATE INDEX``.

    :param config: Configuração da conexão.
    :param table: Nome da tabela no Oracle.
    :param parallel_degree: Grau de paralelismo da coleta.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(
            render_query(QUERIES_ANCHOR, "gather_table_stats"),
            {"owner": config.schema, "table_name": table, "degree": parallel_degree},
        )


def count_rows(config: OracleConfig, table: str, parallel_degree: int = 1) -> int:
    """Conta as linhas da tabela de destino, com varredura paralela.

    :param config: Configuração da conexão. :param table: Nome da tabela no Oracle.
    :param parallel_degree: Grau da dica ``PARALLEL`` do ``COUNT(*)``; 1 equivale a
    varredura serial. :returns: Número de linhas.
    """
    query = render_query(
        QUERIES_ANCHOR, "count_rows", schema=config.schema, table=table, degree=max(int(parallel_degree), 1)
    )
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(query)
        row = fetch_one(cursor)
    return to_int(row[0] if row else 0)

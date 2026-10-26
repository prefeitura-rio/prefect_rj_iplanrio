"""Seções do relatório de validação lidas do Oracle e do BigQuery."""

from pathlib import Path

import oracledb
from google.cloud import bigquery

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.inmemory import oracle_message, read_status
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import TABLE_PREFIX, OracleConfig, connect
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_query import fetch_all, fetch_one, fetch_scalar, to_int
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_read import fetch_rows
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.slots import CONSUMER_GRANTS, parse_load_comment
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure import plan_structure, slot_indexes
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import TableLayout
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.report import (
    count_divergences,
    format_key_values,
    format_text_table,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.report_compare import (
    compare_grants,
    compare_indexes,
    compare_partitions,
)
from utils.utils import render_query

# load_query resolve queries/ no diretório pai do caminho recebido; a pasta utils/
# aponta para a raiz da pipeline.
QUERIES_ANCHOR = str(Path(__file__).parent)


MEGABYTE = 1024 * 1024


def describe_session(config: OracleConfig) -> str:
    """Describe the session open in Oracle, with no password and no host.

    Return the text with the effective user, the proxy user, the database and the
    version.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(render_query(QUERIES_ANCHOR, "get_session_info"))
        session_user, proxy_user, db_name, service_name, db_version, db_time = fetch_one(cursor) or (None,) * 6
    return format_key_values(
        [
            ("Usuário efetivo da sessão", session_user),
            ("Usuário proxy (quem autenticou)", proxy_user or "(sem proxy)"),
            ("Schema de destino", config.schema),
            ("Banco", db_name),
            ("Service", service_name),
            ("Versão do Oracle", db_version),
            ("Horário no banco", db_time),
        ]
    )


def fetch_table_size_mb(cursor: oracledb.Cursor, owner: str, table: str) -> str:
    """Read the allocated size of the table, if the session can read the dictionary.

    Return the size in MB, or an explanation if it is not possible to read it.
    """
    try:
        cursor.execute(render_query(QUERIES_ANCHOR, "get_table_size_dba"), {"owner": owner, "table_name": table})
    except oracledb.DatabaseError:
        cursor.execute(render_query(QUERIES_ANCHOR, "get_table_size_user"), {"table_name": table})
    size = fetch_scalar(cursor)
    return "sem segmento alocado" if size is None else f"{to_int(size) / MEGABYTE:.2f} MB"


def fetch_indexes_size_mb(cursor: oracledb.Cursor, owner: str, table: str) -> str:
    """Read the allocated size of the table indexes, if `dba_segments` is readable.

    Return the size in MB, or an explanation if it is not possible to read it.
    """
    try:
        cursor.execute(render_query(QUERIES_ANCHOR, "get_indexes_size_dba"), {"owner": owner, "table_name": table})
    except oracledb.DatabaseError:
        return "sem acesso a dba_segments"
    size = fetch_scalar(cursor)
    return "sem segmento alocado" if size is None else f"{to_int(size) / MEGABYTE:.2f} MB"


def table_comment(cursor: oracledb.Cursor, owner: str, table: str) -> tuple[bool, str | None]:
    """Read whether the table exists and its comment, and return `(exists, comment)`."""
    rows = fetch_rows(cursor, "get_table_comment", {"owner": owner, "table_name": table})
    if not rows:
        return False, None
    comment = rows[0]["comments"]
    return True, None if comment is None else str(comment)


def describe_load(comment: str | None) -> str:
    """Describe the BigQuery snapshot and the load time that the comment holds.

    `comment` is the comment of the physical table. Return the text for the report.
    """
    loaded = parse_load_comment(comment)
    return f"foto do BigQuery de {loaded[0]} UTC, carregada em {loaded[1]} UTC" if loaded else "sem carga concluída"


def grants_section(cursor: oracledb.Cursor, schema: str, table: str) -> tuple[int, str]:
    """Check the consumer privileges on the physical table.

    It runs if the session can read `dba_tab_privs`.

    Return the number of divergences and the section of the report.
    """
    try:
        cursor.execute(render_query(QUERIES_ANCHOR, "get_table_grants_dba"), {"owner": schema, "table_name": table})
    except oracledb.DatabaseError:
        return 0, "Grants: sem acesso a dba_tab_privs; não conferidos."
    rows = compare_grants(CONSUMER_GRANTS, [(str(grantee), str(privilege)) for grantee, privilege in fetch_all(cursor)])
    divergences = count_divergences(rows)
    return divergences, f"Grants dos consumidores: {divergences} divergência(s)\n" + format_text_table(
        ["Grantee", "Esperado", "Concedido", "Status"], rows
    )


def inmemory_section(cursor: oracledb.Cursor, schema: str, table: str) -> str:
    """Describe the In-Memory population of the physical table, with no divergence.

    Return the section of the report.
    """
    try:
        status = read_status(cursor, schema, table)
    except oracledb.DatabaseError as error:
        return f"In-Memory: sem acesso a gv$im_segments ({oracle_message(error)})."
    state = "inteira no In-Memory" if status.complete else "população incompleta"
    return f"In-Memory: {status.description} ({state})"


def structure_sections(
    template: TableLayout, loaded: TableLayout, loaded_columns: list[str], slot: str | None
) -> tuple[int, list[str]]:
    """Compare the partitions, In-Memory and indexes of the original and the load.

    `slot` is the suffix of the physical table (`A` or `B`), which goes in the name of
    the indexes, and `None` for the old single table. Return the number of divergences
    and the text sections of the report.
    """
    try:
        expected = plan_structure(template, loaded_columns, TABLE_PREFIX)
        expected_indexes = slot_indexes(expected.layout.indexes, slot) if slot else expected.layout.indexes
    except (NotImplementedError, ValueError) as error:
        return 1, [f"A estrutura da original não pode ser replicada pela carga: {error}"]

    partition_rows = compare_partitions(expected.layout.partitioning, loaded.partitioning)
    partition_divergences = count_divergences(partition_rows)
    tablespace_ok = expected.layout.tablespace == loaded.tablespace
    inmemory_ok = expected.layout.inmemory == loaded.inmemory
    index_rows = compare_indexes(list(expected_indexes), list(loaded.indexes))
    index_divergences = count_divergences(index_rows)
    sections = [
        f"Partições: tablespace original {expected.layout.tablespace or '(padrão)'}, carregada "
        + f"{loaded.tablespace or '(padrão)'} ({'OK' if tablespace_ok else 'DIVERGE'}); "
        + f"{partition_divergences} divergência(s). Partições criadas pelo INTERVAL não entram.\n"
        + format_text_table(["#", "Partição", "Original", "Carregada", "Status"], partition_rows),
        f"In-Memory: esperado {expected.layout.inmemory or 'NO INMEMORY'}, carregada "
        + f"{loaded.inmemory or 'NO INMEMORY'} ({'OK' if inmemory_ok else 'DIVERGE'})",
        f"Índices: {len(expected_indexes)} esperado(s), {len(loaded.indexes)} na carregada, "
        + f"{index_divergences} divergente(s). Esperado: definição da original, utilizável e paralelismo 1.\n"
        + (
            format_text_table(["Índice", "Esperado", "Carregado", "Paralelismo", "Status", "Resultado"], index_rows)
            if index_rows
            else "(a original não tem índices para replicar)"
        ),
    ]
    if expected.skipped_indexes:
        sections.append(
            f"Índices da original não replicados (internos de view materializada): {list(expected.skipped_indexes)}"
        )
    return partition_divergences + index_divergences + (not tablespace_ok) + (not inmemory_ok), sections


def fetch_bigquery_metrics(project: str, dataset_id: str, table_id: str, expressions: list[str]) -> list[object]:
    """Compute the aggregate metrics in BigQuery.

    `expressions` are SQL expressions, in the order of the metrics. Return the values in
    the same order.
    """
    sql = render_query(
        QUERIES_ANCHOR,
        "bigquery_column_metrics",
        expressions=",\n".join(f"  {expression}" for expression in expressions),
        project=project,
        dataset_id=dataset_id,
        table_id=table_id,
    )
    row = next(iter(bigquery.Client(project=project).query(sql).result()))
    return list(row.values())

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
    """Descreve a sessão aberta no Oracle, sem expor senha ou host.

    :param config: Configuração da conexão.
    :returns: Texto com usuário efetivo, usuário proxy, banco e versão.
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
    """Lê o tamanho alocado da tabela, se a sessão tiver acesso ao dicionário.

    :param cursor: Cursor de uma conexão aberta.
    :param owner: Dono da tabela.
    :param table: Nome da tabela.
    :returns: Tamanho em MB, ou uma explicação se não for possível ler.
    """
    try:
        cursor.execute(render_query(QUERIES_ANCHOR, "get_table_size_dba"), {"owner": owner, "table_name": table})
    except oracledb.DatabaseError:
        cursor.execute(render_query(QUERIES_ANCHOR, "get_table_size_user"), {"table_name": table})
    size = fetch_scalar(cursor)
    return "sem segmento alocado" if size is None else f"{to_int(size) / MEGABYTE:.2f} MB"


def fetch_indexes_size_mb(cursor: oracledb.Cursor, owner: str, table: str) -> str:
    """Lê o tamanho alocado dos índices da tabela, se a sessão puder ler ``dba_segments``.

    :param cursor: Cursor de uma conexão aberta.
    :param owner: Dono da tabela.
    :param table: Nome da tabela.
    :returns: Tamanho em MB, ou uma explicação se não for possível ler.
    """
    try:
        cursor.execute(render_query(QUERIES_ANCHOR, "get_indexes_size_dba"), {"owner": owner, "table_name": table})
    except oracledb.DatabaseError:
        return "sem acesso a dba_segments"
    size = fetch_scalar(cursor)
    return "sem segmento alocado" if size is None else f"{to_int(size) / MEGABYTE:.2f} MB"


def table_comment(cursor: oracledb.Cursor, owner: str, table: str) -> tuple[bool, str | None]:
    """Lê se a tabela existe e o comentário dela.

    :param cursor: Cursor de uma conexão aberta.
    :param owner: Dono da tabela.
    :param table: Nome da tabela.
    :returns: ``(existe, comentário)``.
    """
    rows = fetch_rows(cursor, "get_table_comment", {"owner": owner, "table_name": table})
    if not rows:
        return False, None
    comment = rows[0]["comments"]
    return True, None if comment is None else str(comment)


def describe_load(comment: str | None) -> str:
    """Descreve a foto do BigQuery e o horário de carga gravados no comentário.

    :param comment: Comentário da tabela física.
    :returns: Texto para o relatório.
    """
    loaded = parse_load_comment(comment)
    return f"foto do BigQuery de {loaded[0]} UTC, carregada em {loaded[1]} UTC" if loaded else "sem carga concluída"


def grants_section(cursor: oracledb.Cursor, schema: str, table: str) -> tuple[int, str]:
    """Confere os privilégios dos consumidores na tabela física, se a sessão puder ler ``dba_tab_privs``.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema da tabela.
    :param table: Tabela física.
    :returns: Número de divergências e seção do relatório.
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
    """Descreve a população da tabela física no In-Memory, sem contar divergência.

    :param cursor: Cursor de uma conexão aberta.
    :param schema: Schema da tabela.
    :param table: Tabela física.
    :returns: Seção do relatório.
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
    """Compara partições, In-Memory e índices da tabela original com os da tabela carregada.

    :param template: Estrutura da tabela original.
    :param loaded: Estrutura da tabela carregada.
    :param loaded_columns: Colunas criadas na tabela carregada.
    :param slot: Sufixo da tabela física (``A`` ou ``B``), que vai no nome dos
        índices; ``None`` para a tabela única antiga.
    :returns: Número de divergências e seções de texto do relatório.
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
    """Calcula as métricas agregadas no BigQuery.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Nome da tabela.
    :param expressions: Expressões SQL, na ordem das métricas.
    :returns: Valores na mesma ordem das expressões.
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

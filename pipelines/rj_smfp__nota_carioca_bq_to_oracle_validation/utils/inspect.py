"""Leitura, somente consulta, das tabelas no Oracle e no BigQuery para o relatório de validação."""

from dataclasses import dataclass, field
from pathlib import Path

import oracledb
from google.cloud import bigquery

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.bigquery import get_table_schema
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import build_load_plan
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import (
    MANAGED_TABLE_MARKER,
    TABLE_PREFIX,
    OracleConfig,
    connect,
    oracle_table_name,
    read_column_definitions,
    read_table_layout,
    validate_identifier,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.structure import TableLayout, plan_structure
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.utils.report import (
    compare_columns,
    compare_indexes,
    compare_metrics,
    compare_partitions,
    count_divergences,
    format_key_values,
    format_text_table,
    metric_specs,
)
from prefect_rj_iplanrio.sql import load_query

# load_query resolve queries/ no diretório pai do caminho recebido; a pasta utils/ aponta para a raiz da pipeline.
QUERIES_ANCHOR = str(Path(__file__).parent)
MEGABYTE = 1024 * 1024


@dataclass(frozen=True)
class ValidationRequest:
    """Tabela a validar e opções da validação.

    :param project: Projeto do BigQuery.
    :param dataset_id: Dataset de origem.
    :param table_id: Tabela de origem; a tabela original no Oracle tem o mesmo nome.
    :param template_schema: Schema da tabela original no Oracle.
    :param excluded_columns: Colunas da original que a carga não cria (além das
        ``ROWID`` ausentes no BigQuery, excluídas automaticamente).
    :param compute_column_metrics: Se calcula as métricas por coluna, que leem a
        tabela inteira nos dois bancos.
    """

    project: str
    dataset_id: str
    table_id: str
    template_schema: str
    excluded_columns: tuple[str, ...]
    compute_column_metrics: bool


@dataclass
class TableReport:
    """Resultado da validação de uma tabela.

    :param source: Tabela de origem no BigQuery.
    :param target: Tabela de destino no Oracle.
    :param divergences: Número total de divergências encontradas.
    :param sections: Seções de texto do relatório, na ordem de exibição.
    """

    source: str
    target: str
    divergences: int = 0
    sections: list[str] = field(default_factory=list)


def describe_session(config: OracleConfig) -> str:
    """Descreve a sessão aberta no Oracle, sem expor senha ou host.

    :param config: Configuração da conexão.
    :returns: Texto com usuário efetivo, usuário proxy, banco e versão.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(load_query(QUERIES_ANCHOR, "get_session_info"))
        session_user, proxy_user, db_name, service_name, db_version, db_time = cursor.fetchone()
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
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_size_dba"), {"owner": owner, "table_name": table})
    except oracledb.DatabaseError:
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_size_user"), {"table_name": table})
    (size,) = cursor.fetchone()
    return "sem segmento alocado" if size is None else f"{int(size) / MEGABYTE:.2f} MB"


def fetch_indexes_size_mb(cursor: oracledb.Cursor, owner: str, table: str) -> str:
    """Lê o tamanho alocado dos índices da tabela, se a sessão puder ler ``dba_segments``.

    :param cursor: Cursor de uma conexão aberta.
    :param owner: Dono da tabela.
    :param table: Nome da tabela.
    :returns: Tamanho em MB, ou uma explicação se não for possível ler.
    """
    try:
        cursor.execute(load_query(QUERIES_ANCHOR, "get_indexes_size_dba"), {"owner": owner, "table_name": table})
    except oracledb.DatabaseError:
        return "sem acesso a dba_segments"
    (size,) = cursor.fetchone()
    return "sem segmento alocado" if size is None else f"{int(size) / MEGABYTE:.2f} MB"


def structure_sections(template: TableLayout, loaded: TableLayout, loaded_columns: list[str]) -> tuple[int, list[str]]:
    """Compara partições e índices da tabela original com os da tabela carregada.

    :param template: Estrutura da tabela original.
    :param loaded: Estrutura da tabela carregada.
    :param loaded_columns: Colunas criadas na tabela carregada.
    :returns: Número de divergências e seções de texto do relatório.
    """
    try:
        expected = plan_structure(template, loaded_columns, TABLE_PREFIX)
    except (NotImplementedError, ValueError) as error:
        return 1, [f"A estrutura da original não pode ser replicada pela carga: {error}"]

    partition_rows = compare_partitions(expected.layout.partitioning, loaded.partitioning)
    partition_divergences = count_divergences(partition_rows)
    tablespace_ok = expected.layout.tablespace == loaded.tablespace
    index_rows = compare_indexes(list(expected.layout.indexes), list(loaded.indexes))
    index_divergences = count_divergences(index_rows)
    sections = [
        f"Partições: tablespace original {expected.layout.tablespace or '(padrão)'}, carregada "
        f"{loaded.tablespace or '(padrão)'} ({'OK' if tablespace_ok else 'DIVERGE'}); "
        f"{partition_divergences} divergência(s). Partições criadas pelo INTERVAL não entram.\n"
        + format_text_table(["#", "Partição", "Original", "Carregada", "Status"], partition_rows),
        f"Índices: {len(expected.layout.indexes)} esperado(s), {len(loaded.indexes)} na carregada, "
        f"{index_divergences} divergente(s). Esperado: definição da original, utilizável e paralelismo 1.\n"
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
    return partition_divergences + index_divergences + (not tablespace_ok), sections


def fetch_bigquery_metrics(project: str, dataset_id: str, table_id: str, expressions: list[str]) -> list[object]:
    """Calcula as métricas agregadas no BigQuery.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Nome da tabela.
    :param expressions: Expressões SQL, na ordem das métricas.
    :returns: Valores na mesma ordem das expressões.
    """
    sql = load_query(
        QUERIES_ANCHOR,
        "bigquery_column_metrics",
        expressions=",\n".join(f"  {expression}" for expression in expressions),
        project=project,
        dataset_id=dataset_id,
        table_id=table_id,
    )
    row = next(iter(bigquery.Client(project=project).query(sql).result()))
    return list(row.values())


def validate_table(config: OracleConfig, request: ValidationRequest) -> TableReport:  # noqa: PLR0915
    """Compara a tabela carregada no Oracle com a tabela original e com o BigQuery.

    :param config: Configuração da conexão com o Oracle.
    :param request: Tabela a validar e opções.
    :returns: Relatório com as seções de texto e o total de divergências.
    """
    table = oracle_table_name(request.table_id)
    template_name = f"{request.template_schema}.{validate_identifier(request.table_id)}"
    report = TableReport(
        source=f"{request.project}.{request.dataset_id}.{request.table_id}", target=f"{config.schema}.{table}"
    )
    table_schema = get_table_schema(project=request.project, dataset_id=request.dataset_id, table_id=request.table_id)
    bq_fields, bq_rows = table_schema["fields"], int(table_schema["num_rows"])
    binds = {"owner": config.schema, "table_name": table}

    with connect(config) as connection, connection.cursor() as cursor:
        template = read_column_definitions(cursor, request.template_schema, validate_identifier(request.table_id))
        if not template:
            report.divergences = 1
            report.sections.append(f"Tabela original {template_name} não encontrada no Oracle.")
            return report
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_overview"), binds)
        overview = cursor.fetchone()
        if overview is None:
            report.divergences = 1
            report.sections.append(f"A tabela {report.target} não existe no Oracle. Rode a pipeline de carga.")
            return report
        created, last_ddl_time, comment, stats_rows, last_analyzed, tablespace, logging, inmemory = overview

        cursor.execute(load_query(QUERIES_ANCHOR, "count_rows", schema=config.schema, table=table))
        (oracle_rows,) = cursor.fetchone()
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_index_count"), binds)
        (indexes,) = cursor.fetchone()
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_constraint_count"), binds)
        (constraints,) = cursor.fetchone()
        size = fetch_table_size_mb(cursor, config.schema, table)
        indexes_size = fetch_indexes_size_mb(cursor, config.schema, table)
        actual = read_column_definitions(cursor, config.schema, table)
        template_layout = read_table_layout(cursor, request.template_schema, validate_identifier(request.table_id))
        loaded_layout = read_table_layout(cursor, config.schema, table)

        rows_status = "OK" if bq_rows == oracle_rows else "DIVERGE"
        managed = (comment or "").startswith(MANAGED_TABLE_MARKER)
        report.divergences += (rows_status != "OK") + (not managed)
        report.sections.append(
            "Tabela\n"
            + format_key_values(
                [
                    ("Origem (BigQuery)", report.source),
                    ("Destino (Oracle)", report.target),
                    ("Tabela original (modelo)", template_name),
                    ("Linhas no BigQuery", f"{bq_rows:,}".replace(",", ".")),
                    ("Linhas no Oracle (COUNT)", f"{int(oracle_rows):,}".replace(",", ".")),
                    ("Contagem", rows_status),
                    ("Criada pela pipeline de carga", "sim" if managed else "NÃO"),
                    ("Comentário", comment),
                    ("Criada em", created),
                    ("Último DDL", last_ddl_time),
                    ("Tablespace", tablespace),
                    ("Logging", logging),
                    ("InMemory", inmemory),
                    ("Tamanho alocado", size),
                    ("Índices", indexes),
                    ("Tamanho dos índices", indexes_size),
                    ("Constraints (PK/UK/FK)", constraints),
                    ("Estatísticas: linhas", stats_rows),
                    ("Estatísticas: coletadas em", last_analyzed),
                ]
            )
        )

        bq_types = {field["name"].upper(): f"{field['type']} ({field['mode']})" for field in bq_fields}
        try:
            plan = build_load_plan(bq_fields, template, list(request.excluded_columns))
        except (NotImplementedError, ValueError) as error:
            report.divergences += 1
            report.sections.append(f"O schema do BigQuery não pode ser carregado na original: {error}")
            return report
        if plan.excluded:
            excluded_types = {column.name: column.data_type for column in template}
            report.sections.append(
                "Colunas da original que não são criadas na carregada (ROWID ou excluídas por parâmetro): "
                + ", ".join(f"{name} ({excluded_types[name]})" for name in plan.excluded)
            )
        if plan.ignored:
            report.sections.append(
                f"Colunas do BigQuery que não existem na original e não são carregadas: {plan.ignored}"
            )

        column_rows = compare_columns(plan.columns, actual, bq_types)
        column_divergences = count_divergences(column_rows)
        report.divergences += column_divergences
        report.sections.append(
            f"Colunas: {len(plan.columns)} esperadas (original sem as excluídas), {len(actual)} na carregada, "
            f"{column_divergences} divergente(s)\n"
            + format_text_table(["#", "Coluna", "BigQuery", "Original", "Carregada", "Status"], column_rows)
        )
        if template_layout is not None and loaded_layout is not None:
            structure_divergences, sections = structure_sections(
                template_layout, loaded_layout, [column.name for column in plan.columns]
            )
            report.divergences += structure_divergences
            report.sections += sections

        if not request.compute_column_metrics:
            report.sections.append("Métricas por coluna: não calculadas (compute_column_metrics=false).")
            return report
        if column_divergences:
            report.sections.append("Métricas por coluna: não calculadas, porque as colunas divergem.")
            return report

        specs = metric_specs(bq_fields, {column.name: column.data_type for column in plan.columns})
        cursor.execute(
            load_query(
                QUERIES_ANCHOR,
                "oracle_column_metrics",
                expressions=",\n".join(f"  {spec.oracle_expression}" for spec in specs),
                schema=config.schema,
                table=table,
            )
        )
        oracle_values = list(cursor.fetchone())

    bq_values = fetch_bigquery_metrics(
        request.project, request.dataset_id, request.table_id, [spec.bigquery_expression for spec in specs]
    )
    metric_rows = compare_metrics(specs, bq_values, oracle_values)
    metric_divergences = count_divergences(metric_rows)
    report.divergences += metric_divergences
    report.sections.append(
        f"Métricas por coluna: {len(metric_rows)} comparadas, {metric_divergences} divergente(s)\n"
        + format_text_table(["Coluna", "Métrica", "BigQuery", "Oracle", "Status"], metric_rows)
    )
    return report

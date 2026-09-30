"""Leitura, somente consulta, das tabelas no Oracle e no BigQuery para o relatório de validação."""

from dataclasses import dataclass, field
from pathlib import Path

import oracledb
from google.cloud import bigquery

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.bigquery import get_table_schema
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import map_columns
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import (
    MANAGED_TABLE_MARKER,
    OracleConfig,
    connect,
    oracle_table_name,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.utils.report import (
    compare_columns,
    compare_metrics,
    count_divergences,
    format_key_values,
    format_text_table,
    metric_specs,
)
from prefect_rj_iplanrio.sql import load_query

# load_query resolve queries/ no diretório pai do caminho recebido; a pasta utils/ aponta para a raiz da pipeline.
QUERIES_ANCHOR = str(Path(__file__).parent)
MEGABYTE = 1024 * 1024


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


def validate_table(
    config: OracleConfig, project: str, dataset_id: str, table_id: str, compute_column_metrics: bool
) -> TableReport:
    """Compara uma tabela do BigQuery com a tabela carregada no Oracle.

    :param config: Configuração da conexão com o Oracle.
    :param project: Projeto do BigQuery.
    :param dataset_id: Dataset de origem.
    :param table_id: Tabela de origem.
    :param compute_column_metrics: Se calcula as métricas por coluna, que leem a
        tabela inteira nos dois bancos.
    :returns: Relatório com as seções de texto e o total de divergências.
    """
    table = oracle_table_name(table_id)
    report = TableReport(source=f"{project}.{dataset_id}.{table_id}", target=f"{config.schema}.{table}")
    table_schema = get_table_schema(project=project, dataset_id=dataset_id, table_id=table_id)
    bq_fields, bq_rows = table_schema["fields"], int(table_schema["num_rows"])
    binds = {"owner": config.schema, "table_name": table}

    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_overview"), binds)
        overview = cursor.fetchone()
        if overview is None:
            report.divergences = 1
            report.sections.append(f"A tabela {report.target} não existe no Oracle. Rode a pipeline de carga.")
            return report
        created, last_ddl_time, comment, stats_rows, last_analyzed, tablespace, logging = overview

        cursor.execute(load_query(QUERIES_ANCHOR, "count_rows", schema=config.schema, table=table))
        (oracle_rows,) = cursor.fetchone()
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_index_count"), binds)
        (indexes,) = cursor.fetchone()
        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_constraint_count"), binds)
        (constraints,) = cursor.fetchone()
        size = fetch_table_size_mb(cursor, config.schema, table)

        cursor.execute(load_query(QUERIES_ANCHOR, "get_table_columns_detail"), binds)
        names = [description[0].lower() for description in cursor.description]
        oracle_columns = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]

        rows_status = "OK" if bq_rows == oracle_rows else "DIVERGE"
        managed = (comment or "").startswith(MANAGED_TABLE_MARKER)
        report.divergences += (rows_status != "OK") + (not managed)
        report.sections.append(
            "Tabela\n"
            + format_key_values(
                [
                    ("Origem (BigQuery)", report.source),
                    ("Destino (Oracle)", report.target),
                    ("Linhas no BigQuery", f"{bq_rows:,}".replace(",", ".")),
                    ("Linhas no Oracle (COUNT)", f"{int(oracle_rows):,}".replace(",", ".")),
                    ("Contagem", rows_status),
                    ("Criada pela pipeline de carga", "sim" if managed else "NÃO"),
                    ("Comentário", comment),
                    ("Criada em", created),
                    ("Último DDL", last_ddl_time),
                    ("Tablespace", tablespace),
                    ("Logging", logging),
                    ("Tamanho alocado", size),
                    ("Índices", indexes),
                    ("Constraints (PK/UK/FK)", constraints),
                    ("Estatísticas: linhas", stats_rows),
                    ("Estatísticas: coletadas em", last_analyzed),
                ]
            )
        )

        try:
            expected_types = [column.oracle_type for column in map_columns(bq_fields)]
        except (NotImplementedError, ValueError) as error:
            report.divergences += 1
            report.sections.append(f"Schema do BigQuery sem suporte na carga: {error}")
            return report
        column_rows = compare_columns(bq_fields, expected_types, oracle_columns)
        column_divergences = count_divergences(column_rows)
        report.divergences += column_divergences
        report.sections.append(
            f"Colunas: {len(bq_fields)} no BigQuery, {len(oracle_columns)} no Oracle, "
            f"{column_divergences} divergente(s)\n"
            + format_text_table(
                ["#", "Coluna", "BigQuery", "Oracle esperado", "Oracle atual", "Aceita nulo", "Status"], column_rows
            )
        )

        if not compute_column_metrics:
            report.sections.append("Métricas por coluna: não calculadas (compute_column_metrics=false).")
            return report
        if column_divergences:
            report.sections.append("Métricas por coluna: não calculadas, porque as colunas divergem.")
            return report

        specs = metric_specs(bq_fields)
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

    bq_values = fetch_bigquery_metrics(project, dataset_id, table_id, [spec.bigquery_expression for spec in specs])
    metric_rows = compare_metrics(specs, bq_values, oracle_values)
    metric_divergences = count_divergences(metric_rows)
    report.divergences += metric_divergences
    report.sections.append(
        f"Métricas por coluna: {len(metric_rows)} comparadas, {metric_divergences} divergente(s)\n"
        + format_text_table(["Coluna", "Métrica", "BigQuery", "Oracle", "Status"], metric_rows)
    )
    return report

"""Read the tables in Oracle and BigQuery, query only, for the validation report."""

from dataclasses import dataclass, field
from datetime import UTC, datetime

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.bigquery import get_last_modified, get_table_schema
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import RawTextEncoding, build_load_plan
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import (
    MANAGED_TABLE_MARKER,
    OracleConfig,
    connect,
    oracle_table_name,
    validate_identifier,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_query import fetch_one, fetch_scalar, to_int
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_read import (
    read_column_definitions,
    read_table_layout,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.slots import (
    choose_slots,
    parse_load_comment,
    synonym_owners,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.slots_oracle import read_synonyms
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.inspect_sections import (
    QUERIES_ANCHOR,
    describe_load,
    fetch_bigquery_metrics,
    fetch_indexes_size_mb,
    fetch_table_size_mb,
    grants_section,
    inmemory_section,
    structure_sections,
    table_comment,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.report import (
    compare_metrics,
    count_divergences,
    format_key_values,
    format_text_table,
    metric_specs,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.report_compare import (
    compare_columns,
    compare_synonyms,
)
from utils.utils import render_query


@dataclass(frozen=True)
class ValidationRequest:
    """The table to validate and the options of the validation.

    `project`, `dataset_id` and `table_id` name the BigQuery source, and the original
    table in Oracle has the same name. `template_schema` is the schema of the original
    table in Oracle. `excluded_columns` are the columns of the original that the load
    does not create (besides the `ROWID` columns that BigQuery lacks, which are excluded
    on their own). `compute_column_metrics` says whether to compute the metrics for each
    column, which read the whole table in both databases. `raw_text_encoding` is the
    encoding of the text of the `RAW` columns in BigQuery (`base64` or `hex`), the one
    that the load uses.
    """

    project: str
    dataset_id: str
    table_id: str
    template_schema: str
    excluded_columns: tuple[str, ...]
    compute_column_metrics: bool
    raw_text_encoding: RawTextEncoding = "base64"


@dataclass
class TableReport:
    """The result of the validation of one table.

    `source` is the table in BigQuery, `target` is the table in Oracle, and
    `divergences` is the total number of divergences found. `sections` are the text
    sections of the report, in the order they show.
    """

    source: str
    target: str
    divergences: int = 0
    sections: list[str] = field(default_factory=list)


@dataclass
class SwapState:
    """The state of the A and B swap of a table.

    `table` is the physical table to validate: the one in use or, before the first swap,
    the old single table. `slot` is the suffix (`A` or `B`) of the table in use, or
    `None`. `bigquery_changed` says whether BigQuery changed after the loaded snapshot.
    `divergences` and `sections` are the divergences found in the synonyms and the text
    sections of the report.
    """

    table: str
    slot: str | None = None
    bigquery_changed: bool = False
    divergences: int = 0
    sections: list[str] = field(default_factory=list)


def swap_state(cursor: oracledb.Cursor, schema: str, base: str, bigquery_modified: datetime) -> SwapState:
    """Describe the synonyms, the physical table in use and the previous one.

    `base` is the stable name of the table (the one of the synonyms), and
    `bigquery_modified` is the current last change of the table in BigQuery. Return the
    table to validate and the section of the swap.
    """
    synonyms = read_synonyms(cursor, schema, base)
    try:
        plan = choose_slots(base, schema, synonyms)
    except PermissionError as error:
        return SwapState(table=base, divergences=1, sections=[f"Troca A/B: {error}"])
    legacy_exists, legacy_comment = table_comment(cursor, schema, base)
    if plan.active is None:
        if legacy_exists:
            return SwapState(
                table=base,
                sections=[
                    f"Troca A/B: ainda não houve troca; a tabela única antiga {schema}.{base} segue em uso "
                    + f"({describe_load(legacy_comment)}). A próxima carga cria {base}_A e a troca."
                ],
            )
        return SwapState(table=base)

    targets = {synonym.owner: f"{synonym.table_owner}.{synonym.table_name}" for synonym in synonyms}
    rows = compare_synonyms(synonym_owners(schema), targets, f"{schema}.{plan.active}")
    divergences = count_divergences(rows)
    _, active_comment = table_comment(cursor, schema, plan.active)
    previous_exists, previous_comment = table_comment(cursor, schema, plan.inactive)
    loaded = parse_load_comment(active_comment)
    current = f"{bigquery_modified.astimezone(UTC):%Y-%m-%d %H:%M:%S}"
    bigquery_changed = loaded is not None and current > loaded[0]
    pairs: list[tuple[str, object]] = [
        ("Em uso", f"{schema}.{plan.active} ({describe_load(active_comment)})"),
        (
            "Carga anterior",
            f"{schema}.{plan.inactive} ({describe_load(previous_comment)})" if previous_exists else "(não existe)",
        ),
        ("BigQuery agora", f"última alteração {current} UTC"),
    ]
    if legacy_exists:
        divergences += 1
        pairs.append(("Tabela única antiga", f"{schema}.{base} ainda existe e deveria ter sido trocada pelo sinônimo"))
    notes = (
        "\nO BigQuery mudou depois da carga em uso: diferenças de contagem e métricas são esperadas "
        + "até a próxima carga."
        if bigquery_changed
        else ""
    )
    section = (
        f"Troca A/B: {divergences} divergência(s)\n"
        + format_key_values(pairs)
        + "\n"
        + format_text_table(["Sinônimo (dono)", "Aponta para", "Status"], rows)
        + notes
    )
    return SwapState(
        table=plan.active,
        slot=plan.active.rsplit("_", 1)[1],
        bigquery_changed=bigquery_changed,
        divergences=divergences,
        sections=[section],
    )


def validate_table(config: OracleConfig, request: ValidationRequest) -> TableReport:  # noqa: PLR0915  # the report has one section for each check
    """Compare the table loaded in Oracle with the original table and with BigQuery.

    Return the report with the text sections and the total of divergences.
    """
    base = oracle_table_name(request.table_id)
    template_name = f"{request.template_schema}.{validate_identifier(request.table_id)}"
    table_schema = get_table_schema(project=request.project, dataset_id=request.dataset_id, table_id=request.table_id)
    bq_fields, bq_rows = table_schema.fields, table_schema.num_rows
    bq_modified = get_last_modified(project=request.project, dataset_id=request.dataset_id, table_id=request.table_id)

    with connect(config) as connection, connection.cursor() as cursor:
        swap = swap_state(cursor, config.schema, base, bq_modified)
        table = swap.table
        binds = {"owner": config.schema, "table_name": table}
        report = TableReport(
            source=f"{request.project}.{request.dataset_id}.{request.table_id}",
            target=f"{config.schema}.{table}",
            divergences=swap.divergences,
            sections=list(swap.sections),
        )
        template = read_column_definitions(cursor, request.template_schema, validate_identifier(request.table_id))
        if not template:
            report.divergences += 1
            report.sections.append(f"Tabela original {template_name} não encontrada no Oracle.")
            return report
        cursor.execute(render_query(QUERIES_ANCHOR, "get_table_overview"), binds)
        overview = fetch_one(cursor)
        if overview is None:
            report.divergences += 1
            report.sections.append(f"A tabela {report.target} não existe no Oracle. Rode a pipeline de carga.")
            return report
        created, last_ddl_time, comment, stats_rows, last_analyzed, tablespace, logging, inmemory = overview

        cursor.execute(render_query(QUERIES_ANCHOR, "count_rows", schema=config.schema, table=table))
        oracle_rows = to_int(fetch_scalar(cursor))
        cursor.execute(render_query(QUERIES_ANCHOR, "get_table_index_count"), binds)
        indexes = fetch_scalar(cursor)
        cursor.execute(render_query(QUERIES_ANCHOR, "get_table_constraint_count"), binds)
        constraints = fetch_scalar(cursor)
        size = fetch_table_size_mb(cursor, config.schema, table)
        indexes_size = fetch_indexes_size_mb(cursor, config.schema, table)
        actual = read_column_definitions(cursor, config.schema, table)
        template_layout = read_table_layout(cursor, request.template_schema, validate_identifier(request.table_id))
        loaded_layout = read_table_layout(cursor, config.schema, table)

        rows_status = "OK" if bq_rows == oracle_rows else "DIVERGE"
        if rows_status != "OK" and swap.bigquery_changed:
            rows_status += " (o BigQuery mudou depois da carga em uso)"
        managed = str(comment or "").startswith(MANAGED_TABLE_MARKER)
        report.divergences += (bq_rows != oracle_rows) + (not managed)
        report.sections.append(
            "Tabela\n"
            + format_key_values(
                [
                    ("Origem (BigQuery)", report.source),
                    ("Destino (Oracle)", report.target),
                    ("Tabela original (modelo)", template_name),
                    ("Linhas no BigQuery", f"{bq_rows:,}".replace(",", ".")),
                    ("Linhas no Oracle (COUNT)", f"{oracle_rows:,}".replace(",", ".")),
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
            plan = build_load_plan(bq_fields, template, list(request.excluded_columns), request.raw_text_encoding)
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
            + f"{column_divergences} divergente(s)\n"
            + format_text_table(["#", "Coluna", "BigQuery", "Original", "Carregada", "Status"], column_rows)
        )
        if template_layout is not None and loaded_layout is not None:
            structure_divergences, sections = structure_sections(
                template_layout, loaded_layout, [column.name for column in plan.columns], swap.slot
            )
            report.divergences += structure_divergences
            report.sections += sections
        grant_divergences, grants = grants_section(cursor, config.schema, table)
        report.divergences += grant_divergences
        report.sections += [grants, inmemory_section(cursor, config.schema, table)]

        if not request.compute_column_metrics:
            report.sections.append("Métricas por coluna: não calculadas (compute_column_metrics=false).")
            return report
        if column_divergences:
            report.sections.append("Métricas por coluna: não calculadas, porque as colunas divergem.")
            return report

        specs = metric_specs(
            bq_fields, {column.name: column.data_type for column in plan.columns}, request.raw_text_encoding
        )
        cursor.execute(
            render_query(
                QUERIES_ANCHOR,
                "oracle_column_metrics",
                expressions=",\n".join(f"  {spec.oracle_expression}" for spec in specs),
                schema=config.schema,
                table=table,
            )
        )
        oracle_values = list(fetch_one(cursor) or ())

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

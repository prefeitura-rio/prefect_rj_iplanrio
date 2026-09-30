"""Comparação entre BigQuery e Oracle e formatação do relatório de validação, sem I/O."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

NUMERIC_KINDS = {"nao_nulos", "soma", "minimo_num", "maximo_num", "comprimento_total", "comprimento_maximo"}
STATUS_OK = "OK"
STATUS_DIVERGE = "DIVERGE"
METRIC_LABELS = {
    "nao_nulos": "não nulos",
    "soma": "soma",
    "minimo_num": "mínimo",
    "maximo_num": "máximo",
    "minimo": "mínimo",
    "maximo": "máximo",
    "comprimento_total": "comprimento total",
    "comprimento_maximo": "comprimento máximo",
}


@dataclass(frozen=True)
class MetricSpec:
    """Métrica agregada de uma coluna, calculada nos dois bancos.

    :param column: Nome da coluna no BigQuery.
    :param kind: Tipo da métrica (``nao_nulos``, ``soma``, ``minimo_num`` etc.).
    :param bigquery_expression: Expressão SQL no BigQuery.
    :param oracle_expression: Expressão SQL no Oracle.
    """

    column: str
    kind: str
    bigquery_expression: str
    oracle_expression: str


def metric_specs(bq_fields: list[dict[str, str]]) -> list[MetricSpec]:
    """Define as métricas por coluna que comparam o conteúdo das duas tabelas.

    Strings vazias do BigQuery viram ``NULL`` no Oracle, então os não-nulos de
    ``STRING`` desconsideram ``''`` no BigQuery. Nenhuma métrica expõe valores de
    linhas de texto, apenas contagens e comprimentos.

    :param bq_fields: Campos do schema do BigQuery (``name``, ``type``, ``mode``).
    :returns: Métricas na ordem das colunas.
    """
    specs = []
    for field in bq_fields:
        bq, ora = f"`{field['name']}`", f'"{field["name"].upper()}"'
        if field["type"] in ("NUMERIC", "INTEGER"):
            specs += [
                MetricSpec(field["name"], "nao_nulos", f"COUNT({bq})", f"COUNT({ora})"),
                MetricSpec(field["name"], "soma", f"CAST(SUM({bq}) AS STRING)", f"TO_CHAR(SUM({ora}))"),
                MetricSpec(field["name"], "minimo_num", f"CAST(MIN({bq}) AS STRING)", f"TO_CHAR(MIN({ora}))"),
                MetricSpec(field["name"], "maximo_num", f"CAST(MAX({bq}) AS STRING)", f"TO_CHAR(MAX({ora}))"),
            ]
        elif field["type"] == "STRING":
            specs += [
                MetricSpec(field["name"], "nao_nulos", f"COUNTIF({bq} IS NOT NULL AND {bq} != '')", f"COUNT({ora})"),
                MetricSpec(field["name"], "comprimento_total", f"SUM(LENGTH({bq}))", f"SUM(LENGTH({ora}))"),
                MetricSpec(field["name"], "comprimento_maximo", f"MAX(LENGTH({bq}))", f"MAX(LENGTH({ora}))"),
            ]
        elif field["type"] == "JSON":
            specs += [
                MetricSpec(field["name"], "nao_nulos", f"COUNT({bq})", f"COUNT({ora})"),
                MetricSpec(
                    field["name"], "comprimento_total", f"SUM(LENGTH(TO_JSON_STRING({bq})))", f"SUM(LENGTH({ora}))"
                ),
            ]
        elif field["type"] == "TIMESTAMP":
            bq_format = "FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', {}({}), 'UTC')"
            ora_format = "TO_CHAR({}({}) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.FF6')"
            specs += [
                MetricSpec(field["name"], "nao_nulos", f"COUNT({bq})", f"COUNT({ora})"),
                MetricSpec(field["name"], "minimo", bq_format.format("MIN", bq), ora_format.format("MIN", ora)),
                MetricSpec(field["name"], "maximo", bq_format.format("MAX", bq), ora_format.format("MAX", ora)),
            ]
    return specs


def normalize_metric(kind: str, value: object) -> str | None:
    """Normaliza o valor de uma métrica para comparação e exibição.

    Números viram decimal sem zeros à direita, para que ``1.50`` e ``1.5`` sejam
    iguais.

    :param kind: Tipo da métrica.
    :param value: Valor retornado pelo banco.
    :returns: Valor normalizado, ou ``None`` se o banco retornou nulo.
    """
    if value is None or value == "":
        return None
    if kind in NUMERIC_KINDS:
        try:
            return format(Decimal(str(value)).normalize(), "f")
        except InvalidOperation:
            return str(value)
    return str(value)


def compare_metrics(specs: list[MetricSpec], bq_values: list[object], oracle_values: list[object]) -> list[list[str]]:
    """Compara as métricas calculadas nos dois bancos.

    :param specs: Métricas, na mesma ordem dos valores.
    :param bq_values: Valores calculados no BigQuery.
    :param oracle_values: Valores calculados no Oracle.
    :returns: Linhas ``[coluna, métrica, BigQuery, Oracle, status]``.
    """
    rows = []
    for spec, bq_value, oracle_value in zip(specs, bq_values, oracle_values, strict=True):
        bq_norm, oracle_norm = normalize_metric(spec.kind, bq_value), normalize_metric(spec.kind, oracle_value)
        status = STATUS_OK if bq_norm == oracle_norm else STATUS_DIVERGE
        label = METRIC_LABELS.get(spec.kind, spec.kind)
        rows.append([spec.column.upper(), label, bq_norm or "NULL", oracle_norm or "NULL", status])
    return rows


def format_oracle_type(column: dict[str, object]) -> str:
    """Monta o tipo de uma coluna no formato do ``CREATE TABLE``.

    :param column: Linha de ``all_tab_columns`` com ``data_type``, ``data_length``,
        ``data_precision``, ``data_scale`` e ``char_used``.
    :returns: Tipo, por exemplo ``VARCHAR2(4000 BYTE)`` ou ``NUMBER(19)``.
    """
    data_type = str(column["data_type"])
    if data_type in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR"):
        unit = "CHAR" if column["char_used"] == "C" else "BYTE"
        return f"{data_type}({column['data_length']} {unit})"
    if data_type == "NUMBER":
        precision, scale = column["data_precision"], column["data_scale"]
        if precision is None:
            return "NUMBER"
        return f"NUMBER({precision})" if not scale else f"NUMBER({precision},{scale})"
    return data_type


def compare_columns(
    bq_fields: list[dict[str, str]], expected_types: list[str], oracle_columns: list[dict[str, object]]
) -> list[list[str]]:
    """Compara, posição a posição, o schema do BigQuery com as colunas do Oracle.

    A ordem importa porque o CSV exportado segue a ordem do schema do BigQuery.

    :param bq_fields: Campos do schema do BigQuery.
    :param expected_types: Tipo esperado no Oracle para cada campo, na mesma ordem.
    :param oracle_columns: Linhas de ``all_tab_columns`` ordenadas por ``column_id``.
    :returns: Linhas ``[#, coluna, tipo BigQuery, Oracle esperado, Oracle atual,
        aceita nulo, status]``.
    """
    rows = []
    for index in range(max(len(bq_fields), len(oracle_columns))):
        field = bq_fields[index] if index < len(bq_fields) else None
        column = oracle_columns[index] if index < len(oracle_columns) else None
        expected = expected_types[index] if field else "-"
        actual = format_oracle_type(column) if column else "(ausente)"
        name = field["name"].upper() if field else "(ausente)"
        same_name = column is not None and field is not None and column["column_name"] == name
        status = STATUS_OK if same_name and expected == actual else STATUS_DIVERGE
        rows.append(
            [
                str(index + 1),
                name if same_name or column is None else f"{name} ≠ {column['column_name']}",
                f"{field['type']} ({field['mode']})" if field else "-",
                expected,
                actual,
                ("sim" if column["nullable"] == "Y" else "não") if column else "-",
                status,
            ]
        )
    return rows


def count_divergences(rows: list[list[str]]) -> int:
    """Conta as linhas de uma comparação cujo status é divergente.

    :param rows: Linhas cuja última coluna é o status.
    :returns: Número de divergências.
    """
    return sum(1 for row in rows if row[-1] == STATUS_DIVERGE)


def format_text_table(headers: list[str], rows: list[list[str]]) -> str:
    """Formata linhas como tabela de texto com colunas alinhadas.

    :param headers: Cabeçalhos das colunas.
    :param rows: Linhas, com o mesmo número de colunas dos cabeçalhos.
    :returns: Tabela pronta para o log.
    """
    widths = [max(len(str(value)) for value in column) for column in zip(headers, *rows, strict=False)]

    def line(values: list[str]) -> str:
        return " | ".join(str(value).ljust(width) for value, width in zip(values, widths, strict=True)).rstrip()

    separator = "-+-".join("-" * width for width in widths)
    return "\n".join([line(headers), separator, *(line(row) for row in rows)])


def format_key_values(pairs: list[tuple[str, object]]) -> str:
    """Formata pares chave-valor alinhados, um por linha.

    :param pairs: Pares ``(rótulo, valor)``.
    :returns: Texto pronto para o log.
    """
    width = max(len(label) for label, _ in pairs)
    return "\n".join(f"{label.ljust(width)} : {'-' if value is None else value}" for label, value in pairs)

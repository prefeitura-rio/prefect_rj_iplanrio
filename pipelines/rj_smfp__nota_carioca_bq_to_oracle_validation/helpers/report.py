"""Compare BigQuery with Oracle and format the validation report, with no I/O."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import RawTextEncoding
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.helpers.report_compare import STATUS_DIVERGE, STATUS_OK

NUMERIC_KINDS = {"nao_nulos", "soma", "minimo_num", "maximo_num", "comprimento_total", "comprimento_maximo"}


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
    """An aggregate metric of a column, computed in both databases.

    `column` is the name of the column in BigQuery. `kind` is the kind of the metric
    (`nao_nulos`, `soma`, `minimo_num` and so on). `bigquery_expression` and
    `oracle_expression` are the SQL expressions in each database.
    """

    column: str
    kind: str
    bigquery_expression: str
    oracle_expression: str


def metric_specs(
    bq_fields: list[dict[str, str]], target_types: dict[str, str], raw_text_encoding: RawTextEncoding = "base64"
) -> list[MetricSpec]:
    """Define the metrics for each column that compare the content of the two tables.

    Only the loaded columns count (those that the original table has). An empty string
    of BigQuery becomes `NULL` in Oracle, so the non-null count of a `STRING` ignores
    `''` in BigQuery. A date stored as text in BigQuery is converted as in the load
    before the minimum and the maximum are compared. A `RAW` stored in base64 or in
    hexadecimal in BigQuery (`raw_text_encoding`, the same as the load) is decoded, and
    its length (in bytes), minimum and maximum (in hexadecimal) are compared with those
    of Oracle. No metric exposes the values of text rows, only counts and lengths.
    `target_types` is the base type in Oracle of each loaded column, by its name in
    upper case. Return the metrics in the order of the BigQuery columns.
    """
    specs = []
    for field in bq_fields:
        name = field["name"].upper()
        if name not in target_types:
            continue
        bq, ora, target = f"`{field['name']}`", f'"{name}"', target_types[name]
        if field["type"] in ("NUMERIC", "INTEGER"):
            specs += [
                MetricSpec(field["name"], "nao_nulos", f"COUNT({bq})", f"COUNT({ora})"),
                MetricSpec(field["name"], "soma", f"CAST(SUM({bq}) AS STRING)", f"TO_CHAR(SUM({ora}))"),
                MetricSpec(field["name"], "minimo_num", f"CAST(MIN({bq}) AS STRING)", f"TO_CHAR(MIN({ora}))"),
                MetricSpec(field["name"], "maximo_num", f"CAST(MAX({bq}) AS STRING)", f"TO_CHAR(MAX({ora}))"),
            ]
        elif field["type"] == "STRING" and target == "DATE":
            bq_date = f"SAFE_CAST(REPLACE(SUBSTR({bq}, 1, 19), 'T', ' ') AS DATETIME)"
            specs += [
                MetricSpec(field["name"], "nao_nulos", f"COUNTIF({bq} IS NOT NULL AND {bq} != '')", f"COUNT({ora})"),
                MetricSpec(
                    field["name"],
                    "minimo",
                    f"FORMAT_DATETIME('%E4Y-%m-%d %H:%M:%S', MIN({bq_date}))",
                    f"TO_CHAR(MIN({ora}), 'YYYY-MM-DD HH24:MI:SS')",
                ),
                MetricSpec(
                    field["name"],
                    "maximo",
                    f"FORMAT_DATETIME('%E4Y-%m-%d %H:%M:%S', MAX({bq_date}))",
                    f"TO_CHAR(MAX({ora}), 'YYYY-MM-DD HH24:MI:SS')",
                ),
            ]
        elif field["type"] == "STRING" and target == "RAW":
            decode = "FROM_HEX" if raw_text_encoding == "hex" else "FROM_BASE64"
            bq_bytes = f"{decode}(NULLIF({bq}, ''))"
            specs += [
                MetricSpec(field["name"], "nao_nulos", f"COUNTIF({bq} IS NOT NULL AND {bq} != '')", f"COUNT({ora})"),
                MetricSpec(
                    field["name"], "comprimento_total", f"SUM(BYTE_LENGTH({bq_bytes}))", f"SUM(UTL_RAW.LENGTH({ora}))"
                ),
                MetricSpec(field["name"], "minimo", f"UPPER(TO_HEX(MIN({bq_bytes})))", f"RAWTOHEX(MIN({ora}))"),
                MetricSpec(field["name"], "maximo", f"UPPER(TO_HEX(MAX({bq_bytes})))", f"RAWTOHEX(MAX({ora}))"),
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
            bq_format = "FORMAT_TIMESTAMP('%E4Y-%m-%d %H:%M:%E6S', {}({}), 'UTC')"
            ora_format = "TO_CHAR({}({}) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.FF6')"
            specs += [
                MetricSpec(field["name"], "nao_nulos", f"COUNT({bq})", f"COUNT({ora})"),
                MetricSpec(field["name"], "minimo", bq_format.format("MIN", bq), ora_format.format("MIN", ora)),
                MetricSpec(field["name"], "maximo", bq_format.format("MAX", bq), ora_format.format("MAX", ora)),
            ]
    return specs


def normalize_metric(kind: str, value: object) -> str | None:
    """Normalize the value of a metric for the comparison and the display.

    A number becomes a decimal with no trailing zeros, so `1.50` and `1.5` are equal.
    Return the normalized value, or `None` if the database returned null.
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
    """Compare the metrics computed in the two databases.

    `specs` are in the same order as the values. Return the rows `[column, metric,
    BigQuery, Oracle, status]`.
    """
    rows = []
    for spec, bq_value, oracle_value in zip(specs, bq_values, oracle_values, strict=True):
        bq_norm, oracle_norm = normalize_metric(spec.kind, bq_value), normalize_metric(spec.kind, oracle_value)
        status = STATUS_OK if bq_norm == oracle_norm else STATUS_DIVERGE
        label = METRIC_LABELS.get(spec.kind, spec.kind)
        rows.append([spec.column.upper(), label, bq_norm or "NULL", oracle_norm or "NULL", status])
    return rows


def count_divergences(rows: list[list[str]]) -> int:
    """Count the rows of a comparison whose status is a divergence.

    The last column of each row is its status.
    """
    return sum(1 for row in rows if row[-1] == STATUS_DIVERGE)


def format_text_table(headers: list[str], rows: list[list[str]]) -> str:
    """Format rows as a text table with aligned columns, ready for the log.

    Each row has as many columns as the headers.
    """
    table = [headers, *rows]
    widths = [max(len(str(row[index])) for row in table) for index in range(len(headers))]

    def line(values: list[str]) -> str:
        return " | ".join(str(value).ljust(width) for value, width in zip(values, widths, strict=True)).rstrip()

    separator = "-+-".join("-" * width for width in widths)
    return "\n".join([line(headers), separator, *(line(row) for row in rows)])


def format_key_values(pairs: list[tuple[str, object]]) -> str:
    """Format aligned key and value pairs, one on each line, ready for the log."""
    width = max(len(label) for label, _ in pairs)
    return "\n".join(f"{label.ljust(width)} : {'-' if value is None else value}" for label, value in pairs)

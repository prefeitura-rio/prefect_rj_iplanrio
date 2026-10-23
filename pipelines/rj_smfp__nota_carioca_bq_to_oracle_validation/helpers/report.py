"""Comparação entre BigQuery e Oracle e formatação do relatório de validação, sem I/O."""

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


def metric_specs(
    bq_fields: list[dict[str, str]], target_types: dict[str, str], raw_text_encoding: RawTextEncoding = "base64"
) -> list[MetricSpec]:
    """Define as métricas por coluna que comparam o conteúdo das duas tabelas.

    Só entram as colunas carregadas (as que existem na tabela original). Strings
    vazias do BigQuery viram ``NULL`` no Oracle, então os não-nulos de ``STRING``
    desconsideram ``''`` no BigQuery. Datas guardadas como texto no BigQuery são
    convertidas como na carga antes de comparar mínimo e máximo. ``RAW`` guardado em
    base64 ou em hexadecimal no BigQuery (``raw_text_encoding``, o mesmo da carga)
    é decodificado, e comprimento (em bytes), mínimo e máximo (em hexadecimal) são
    comparados com os do Oracle. Nenhuma métrica expõe valores de linhas de texto,
    apenas contagens e comprimentos.

    :param bq_fields: Campos do schema do BigQuery (``name``, ``type``, ``mode``).
    :param target_types: Tipo base no Oracle de cada coluna carregada, pelo nome
        em maiúsculas.
    :param raw_text_encoding: Codificação do texto das colunas ``RAW`` no BigQuery.
    :returns: Métricas na ordem das colunas do BigQuery.
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
                    f"FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', MIN({bq_date}))",
                    f"TO_CHAR(MIN({ora}), 'YYYY-MM-DD HH24:MI:SS')",
                ),
                MetricSpec(
                    field["name"],
                    "maximo",
                    f"FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', MAX({bq_date}))",
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
    table = [headers, *rows]
    widths = [max(len(str(row[index])) for row in table) for index in range(len(headers))]

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

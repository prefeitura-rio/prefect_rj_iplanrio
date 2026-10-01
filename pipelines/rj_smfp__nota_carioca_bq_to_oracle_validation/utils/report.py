"""Comparação entre BigQuery e Oracle e formatação do relatório de validação, sem I/O."""

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.structure import (
    USABLE_INDEX_STATUSES,
    IndexDefinition,
    Partitioning,
    TablePartition,
)

NUMERIC_KINDS = {"nao_nulos", "soma", "minimo_num", "maximo_num", "comprimento_total", "comprimento_maximo"}
STATUS_OK = "OK"
STATUS_DIVERGE = "DIVERGE"
DATE_BOUND_PATTERN = re.compile(r"^TO_DATE\('\s*([0-9-]+ [0-9:]+)'")
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


def metric_specs(bq_fields: list[dict[str, str]], target_types: dict[str, str]) -> list[MetricSpec]:
    """Define as métricas por coluna que comparam o conteúdo das duas tabelas.

    Só entram as colunas carregadas (as que existem na tabela original). Strings
    vazias do BigQuery viram ``NULL`` no Oracle, então os não-nulos de ``STRING``
    desconsideram ``''`` no BigQuery. Datas guardadas como texto no BigQuery são
    convertidas como na carga antes de comparar mínimo e máximo. Nenhuma métrica
    expõe valores de linhas de texto, apenas contagens e comprimentos.

    :param bq_fields: Campos do schema do BigQuery (``name``, ``type``, ``mode``).
    :param target_types: Tipo base no Oracle de cada coluna carregada, pelo nome
        em maiúsculas.
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


def describe_column(column: OracleColumn | None) -> str:
    """Descreve tipo e nulidade de uma coluna como no ``CREATE TABLE``.

    :param column: Coluna, ou ``None`` se ausente.
    :returns: Por exemplo ``VARCHAR2(14 BYTE) NOT NULL``, ou ``(ausente)``.
    """
    if column is None:
        return "(ausente)"
    try:
        ddl_type = column.ddl_type
    except NotImplementedError:
        ddl_type = column.data_type
    return ddl_type + ("" if column.nullable else " NOT NULL")


def compare_columns(
    template: list[OracleColumn], actual: list[OracleColumn], bq_types: dict[str, str]
) -> list[list[str]]:
    """Compara, posição a posição, a tabela original com a tabela carregada.

    Nome, ordem, tipo, tamanho e ``NOT NULL`` precisam ser iguais.

    :param template: Colunas da tabela original, na ordem dela.
    :param actual: Colunas da tabela carregada, na ordem dela.
    :param bq_types: Tipo no BigQuery de cada coluna, pelo nome em maiúsculas.
    :returns: Linhas ``[#, coluna, tipo BigQuery, original, carregada, status]``.
    """
    rows = []
    for index in range(max(len(template), len(actual))):
        original = template[index] if index < len(template) else None
        loaded = actual[index] if index < len(actual) else None
        name = original.name if original else "(ausente)"
        same_name = original is not None and loaded is not None and original.name == loaded.name
        same_definition = same_name and describe_column(original) == describe_column(loaded)
        rows.append(
            [
                str(index + 1),
                name if same_name or loaded is None else f"{name} ≠ {loaded.name}",
                bq_types.get(name, "-"),
                describe_column(original),
                describe_column(loaded),
                STATUS_OK if same_definition else STATUS_DIVERGE,
            ]
        )
    return rows


def short_bound(high_value: str) -> str:
    """Encurta o limite de uma partição por data para exibição.

    :param high_value: Limite no texto do dicionário.
    :returns: ``< AAAA-MM-DD HH:MI:SS`` para limites ``TO_DATE``; o texto
        original nos demais casos.
    """
    match = DATE_BOUND_PATTERN.match(high_value)
    return f"< {match.group(1)}" if match else high_value


def describe_partition(partition: TablePartition | None) -> str:
    """Descreve limite e tablespace de uma partição.

    :param partition: Partição, ou ``None`` se ausente.
    :returns: Por exemplo ``< 2026-02-01 00:00:00 em DFEN_BIG_DATA``, ou ``(ausente)``.
    """
    if partition is None:
        return "(ausente)"
    return short_bound(partition.high_value) + (f" em {partition.tablespace}" if partition.tablespace else "")


def compare_partitions(expected: Partitioning | None, actual: Partitioning | None) -> list[list[str]]:
    """Compara o particionamento da tabela original com o da tabela carregada.

    A primeira linha compara tipo, chave e intervalo; as seguintes, cada partição
    declarada, pelo nome, na ordem da original. Partições criadas pelo
    ``INTERVAL`` não entram.

    :param expected: Particionamento da original, ou ``None``.
    :param actual: Particionamento da carregada, ou ``None``.
    :returns: Linhas ``[#, partição, original, carregada, status]``.
    """
    expected_summary = expected.summary if expected else "sem partição"
    actual_summary = actual.summary if actual else "sem partição"
    status = STATUS_OK if expected_summary == actual_summary else STATUS_DIVERGE
    rows = [["-", "(particionamento)", expected_summary, actual_summary, status]]
    wanted = list(expected.partitions) if expected else []
    found = {partition.name: partition for partition in actual.partitions} if actual else {}
    for position, partition in enumerate(wanted, start=1):
        loaded = found.pop(partition.name, None)
        status = STATUS_OK if loaded == partition else STATUS_DIVERGE
        rows.append([str(position), partition.name, describe_partition(partition), describe_partition(loaded), status])
    rows += [
        ["+", name, "(ausente)", describe_partition(partition), STATUS_DIVERGE] for name, partition in found.items()
    ]
    return rows


def compare_indexes(expected: list[IndexDefinition], actual: list[IndexDefinition]) -> list[list[str]]:
    """Compara os índices esperados (os da original, renomeados) com os da tabela carregada.

    Além da definição, o índice precisa estar utilizável e sem grau de
    paralelismo gravado.

    :param expected: Índices esperados, já com o nome da tabela carregada.
    :param actual: Índices da tabela carregada.
    :returns: Linhas ``[índice, esperado, carregado, paralelismo, status do
        índice, resultado]``.
    """
    found = {index.name: index for index in actual}
    rows = []
    for index in expected:
        loaded = found.pop(index.name, None)
        healthy = loaded is not None and loaded.degree == "1" and loaded.status in USABLE_INDEX_STATUSES
        rows.append(
            [
                index.name,
                index.description,
                loaded.description if loaded else "(ausente)",
                loaded.degree if loaded else "-",
                loaded.status if loaded else "-",
                STATUS_OK if healthy and loaded == index else STATUS_DIVERGE,
            ]
        )
    rows += [
        [name, "(não esperado)", index.description, index.degree, index.status, STATUS_DIVERGE]
        for name, index in found.items()
    ]
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

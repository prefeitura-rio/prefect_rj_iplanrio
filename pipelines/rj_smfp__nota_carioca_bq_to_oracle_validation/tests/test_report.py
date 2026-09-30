from decimal import Decimal

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.utils.report import (
    compare_columns,
    compare_metrics,
    count_divergences,
    describe_column,
    format_text_table,
    metric_specs,
    normalize_metric,
)


def field(name: str, type_: str, mode: str = "NULLABLE") -> dict[str, str]:
    return {"name": name, "type": type_, "mode": mode}


def ora(name: str, data_type: str, length=None, char_used=None, precision=None, scale=None, nullable=False):
    return OracleColumn(name, data_type, length, char_used, precision, scale, nullable)


ORIGINAL = [
    ora("DATA_COMPETENCIA_MUNICIPIO", "DATE", 7),
    ora("CPF_CNPJ_RESPONSAVEL", "VARCHAR2", 14, "B"),
    ora("NOTA_FISCAL", "NUMBER", 22, precision=15, scale=0),
]
BQ_TYPES = {
    "DATA_COMPETENCIA_MUNICIPIO": "STRING (NULLABLE)",
    "CPF_CNPJ_RESPONSAVEL": "STRING (NULLABLE)",
    "NOTA_FISCAL": "NUMERIC (NULLABLE)",
}


def test_describe_column_shows_type_and_not_null():
    assert describe_column(ORIGINAL[0]) == "DATE NOT NULL"
    assert describe_column(ora("NOME", "VARCHAR2", 300, "C", nullable=True)) == "VARCHAR2(300 CHAR)"
    assert describe_column(ora("DOC", "CLOB", 4000)) == "CLOB NOT NULL"
    assert describe_column(None) == "(ausente)"


def test_compare_columns_accepts_identical_copy():
    rows = compare_columns(ORIGINAL, list(ORIGINAL), BQ_TYPES)
    assert [row[-1] for row in rows] == ["OK", "OK", "OK"]
    assert rows[0][2:5] == ["STRING (NULLABLE)", "DATE NOT NULL", "DATE NOT NULL"]


def test_compare_columns_flags_type_nullability_and_missing_columns():
    loaded = [
        ora("DATA_COMPETENCIA_MUNICIPIO", "VARCHAR2", 4000, "B", nullable=True),
        ora("CPF_CNPJ_RESPONSAVEL", "VARCHAR2", 14, "B", nullable=True),
    ]
    rows = compare_columns(ORIGINAL, loaded, BQ_TYPES)
    assert [row[-1] for row in rows] == ["DIVERGE", "DIVERGE", "DIVERGE"]
    assert rows[0][3:5] == ["DATE NOT NULL", "VARCHAR2(4000 BYTE)"]
    assert rows[1][3:5] == ["VARCHAR2(14 BYTE) NOT NULL", "VARCHAR2(14 BYTE)"]
    assert rows[2][4] == "(ausente)"
    assert count_divergences(rows) == 3


def test_compare_columns_flags_renamed_column():
    rows = compare_columns(ORIGINAL[:1], [ora("DATA", "DATE", 7)], BQ_TYPES)
    assert rows[0][1] == "DATA_COMPETENCIA_MUNICIPIO ≠ DATA"
    assert rows[0][-1] == "DIVERGE"


def test_metric_specs_only_loaded_columns_and_convert_text_dates():
    specs = metric_specs(
        [
            field("_bigquery_uid", "STRING"),
            field("data_competencia_municipio", "STRING"),
            field("nome", "STRING"),
            field("valor", "NUMERIC"),
        ],
        {"DATA_COMPETENCIA_MUNICIPIO": "DATE", "NOME": "VARCHAR2", "VALOR": "NUMBER"},
    )
    assert [(spec.column, spec.kind) for spec in specs] == [
        ("data_competencia_municipio", "nao_nulos"),
        ("data_competencia_municipio", "minimo"),
        ("data_competencia_municipio", "maximo"),
        ("nome", "nao_nulos"),
        ("nome", "comprimento_total"),
        ("nome", "comprimento_maximo"),
        ("valor", "nao_nulos"),
        ("valor", "soma"),
        ("valor", "minimo_num"),
        ("valor", "maximo_num"),
    ]
    assert specs[1].bigquery_expression == (
        "FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', MIN(SAFE_CAST(REPLACE(SUBSTR(`data_competencia_municipio`, 1, 19), "
        "'T', ' ') AS DATETIME)))"
    )
    assert specs[1].oracle_expression == "TO_CHAR(MIN(\"DATA_COMPETENCIA_MUNICIPIO\"), 'YYYY-MM-DD HH24:MI:SS')"
    assert specs[3].bigquery_expression == "COUNTIF(`nome` IS NOT NULL AND `nome` != '')"


def test_normalize_metric_treats_equivalent_decimals_as_equal():
    assert normalize_metric("soma", "1.50") == normalize_metric("soma", Decimal("1.5")) == "1.5"
    assert normalize_metric("soma", ".25") == "0.25"
    assert normalize_metric("soma", 1000) == "1000"
    assert normalize_metric("soma", None) is None
    assert normalize_metric("minimo", "2026-01-01 00:00:00") == "2026-01-01 00:00:00"


def test_compare_metrics_reports_divergences_with_readable_labels():
    specs = metric_specs([field("valor", "NUMERIC")], {"VALOR": "NUMBER"})
    rows = compare_metrics(specs, [10, "12.50", "1", "9"], [10, "12.5", "1", "8"])
    assert [row[-1] for row in rows] == ["OK", "OK", "OK", "DIVERGE"]
    assert rows[3][:2] == ["VALOR", "máximo"]


def test_format_text_table_aligns_columns():
    text = format_text_table(["A", "Coluna"], [["1", "x"], ["22", "yy"]])
    assert text.splitlines() == ["A  | Coluna", "---+-------", "1  | x", "22 | yy"]

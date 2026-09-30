from decimal import Decimal

from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.utils.report import (
    compare_columns,
    compare_metrics,
    count_divergences,
    format_oracle_type,
    format_text_table,
    metric_specs,
    normalize_metric,
)


def field(name: str, type_: str, mode: str = "NULLABLE") -> dict[str, str]:
    return {"name": name, "type": type_, "mode": mode}


def column(name: str, data_type: str, length: int = 22, precision=None, scale=None, char_used=None) -> dict:
    return {
        "column_name": name,
        "data_type": data_type,
        "data_length": length,
        "data_precision": precision,
        "data_scale": scale,
        "char_used": char_used,
        "nullable": "Y",
    }


def test_format_oracle_type_matches_create_table_syntax():
    assert format_oracle_type(column("A", "VARCHAR2", 4000, char_used="B")) == "VARCHAR2(4000 BYTE)"
    assert format_oracle_type(column("A", "VARCHAR2", 100, char_used="C")) == "VARCHAR2(100 CHAR)"
    assert format_oracle_type(column("A", "NUMBER")) == "NUMBER"
    assert format_oracle_type(column("A", "NUMBER", precision=19, scale=0)) == "NUMBER(19)"
    assert format_oracle_type(column("A", "NUMBER", precision=10, scale=2)) == "NUMBER(10,2)"
    assert format_oracle_type(column("A", "TIMESTAMP(6) WITH TIME ZONE")) == "TIMESTAMP(6) WITH TIME ZONE"


def test_compare_columns_flags_type_name_and_missing_columns():
    fields = [field("id", "INTEGER"), field("nome", "STRING"), field("valor", "NUMERIC")]
    expected = ["NUMBER(19)", "VARCHAR2(4000 BYTE)", "NUMBER"]
    oracle = [
        column("ID", "NUMBER", precision=19, scale=0),
        column("NOME", "VARCHAR2", 100, char_used="B"),
    ]
    rows = compare_columns(fields, expected, oracle)
    assert [row[-1] for row in rows] == ["OK", "DIVERGE", "DIVERGE"]
    assert rows[1][4] == "VARCHAR2(100 BYTE)"
    assert rows[2][4] == "(ausente)"
    assert count_divergences(rows) == 2


def test_compare_columns_flags_renamed_column():
    rows = compare_columns([field("id", "INTEGER")], ["NUMBER(19)"], [column("CODIGO", "NUMBER", precision=19, scale=0)])
    assert rows[0][1] == "ID ≠ CODIGO"
    assert rows[0][-1] == "DIVERGE"


def test_metric_specs_cover_each_type_without_exposing_text_values():
    specs = metric_specs(
        [field("valor", "NUMERIC"), field("nome", "STRING"), field("criado", "TIMESTAMP"), field("meta", "JSON")]
    )
    assert [(spec.column, spec.kind) for spec in specs] == [
        ("valor", "nao_nulos"),
        ("valor", "soma"),
        ("valor", "minimo_num"),
        ("valor", "maximo_num"),
        ("nome", "nao_nulos"),
        ("nome", "comprimento_total"),
        ("nome", "comprimento_maximo"),
        ("criado", "nao_nulos"),
        ("criado", "minimo"),
        ("criado", "maximo"),
        ("meta", "nao_nulos"),
        ("meta", "comprimento_total"),
    ]
    assert specs[4].bigquery_expression == "COUNTIF(`nome` IS NOT NULL AND `nome` != '')"
    assert specs[4].oracle_expression == 'COUNT("NOME")'


def test_normalize_metric_treats_equivalent_decimals_as_equal():
    assert normalize_metric("soma", "1.50") == normalize_metric("soma", Decimal("1.5")) == "1.5"
    assert normalize_metric("soma", ".25") == "0.25"
    assert normalize_metric("soma", 1000) == "1000"
    assert normalize_metric("soma", None) is None
    assert normalize_metric("minimo", "2026-01-01 00:00:00.000000") == "2026-01-01 00:00:00.000000"


def test_compare_metrics_reports_divergences():
    specs = metric_specs([field("valor", "NUMERIC")])
    rows = compare_metrics(specs, [10, "12.50", "1", "9"], [10, "12.5", "1", "8"])
    assert [row[-1] for row in rows] == ["OK", "OK", "OK", "DIVERGE"]
    assert rows[3][:2] == ["VALOR", "máximo"]


def test_format_text_table_aligns_columns():
    text = format_text_table(["A", "Coluna"], [["1", "x"], ["22", "yy"]])
    assert text.splitlines() == ["A  | Coluna", "---+-------", "1  | x", "22 | yy"]

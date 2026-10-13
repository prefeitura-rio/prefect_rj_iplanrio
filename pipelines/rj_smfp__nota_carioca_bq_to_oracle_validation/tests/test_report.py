from decimal import Decimal

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.structure import IndexDefinition, Partitioning, TablePartition
from pipelines.rj_smfp__nota_carioca_bq_to_oracle_validation.utils.report import (
    compare_columns,
    compare_grants,
    compare_indexes,
    compare_metrics,
    compare_partitions,
    compare_synonyms,
    count_divergences,
    describe_column,
    format_text_table,
    metric_specs,
    normalize_metric,
    short_bound,
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


def test_metric_specs_decode_raw_stored_as_base64_and_compare_bytes():
    specs = metric_specs([field("pessoa_emitente", "STRING")], {"PESSOA_EMITENTE": "RAW"})
    assert [(spec.kind, spec.oracle_expression) for spec in specs] == [
        ("nao_nulos", 'COUNT("PESSOA_EMITENTE")'),
        ("comprimento_total", 'SUM(UTL_RAW.LENGTH("PESSOA_EMITENTE"))'),
        ("minimo", 'RAWTOHEX(MIN("PESSOA_EMITENTE"))'),
        ("maximo", 'RAWTOHEX(MAX("PESSOA_EMITENTE"))'),
    ]
    assert specs[1].bigquery_expression == "SUM(BYTE_LENGTH(FROM_BASE64(NULLIF(`pessoa_emitente`, ''))))"
    assert specs[2].bigquery_expression == "UPPER(TO_HEX(MIN(FROM_BASE64(NULLIF(`pessoa_emitente`, '')))))"
    assert describe_column(ora("PESSOA_EMITENTE", "RAW", 16)) == "RAW(16) NOT NULL"


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


JAN = "TO_DATE(' 2026-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
FEB = "TO_DATE(' 2026-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
MONTHLY = Partitioning(
    "RANGE",
    ("DATA_COMPETENCIA_MUNICIPIO",),
    None,
    (TablePartition("P_INITIAL", JAN, "DFEN_BIG_DATA"), TablePartition("P_202601", FEB, "DFEN_BIG_DATA")),
)


def test_short_bound_shows_only_the_date():
    assert short_bound(JAN) == "< 2026-01-01 00:00:00"
    assert short_bound("MAXVALUE") == "MAXVALUE"


def test_compare_partitions_accepts_identical_copy():
    rows = compare_partitions(MONTHLY, MONTHLY)
    assert [row[-1] for row in rows] == ["OK", "OK", "OK"]
    assert rows[1] == [
        "1",
        "P_INITIAL",
        "< 2026-01-01 00:00:00 em DFEN_BIG_DATA",
        "< 2026-01-01 00:00:00 em DFEN_BIG_DATA",
        "OK",
    ]


def test_compare_partitions_flags_unpartitioned_missing_and_extra_partitions():
    rows = compare_partitions(MONTHLY, None)
    assert rows[0] == ["-", "(particionamento)", "RANGE (DATA_COMPETENCIA_MUNICIPIO)", "sem partição", "DIVERGE"]
    assert count_divergences(rows) == 3
    other = Partitioning(
        "RANGE", ("DATA_COMPETENCIA_MUNICIPIO",), None, (MONTHLY.partitions[0], TablePartition("P_X", FEB, "USERS"))
    )
    rows = compare_partitions(MONTHLY, other)
    assert [row[-1] for row in rows] == ["OK", "OK", "DIVERGE", "DIVERGE"]
    assert rows[2][3] == "(ausente)"
    assert rows[3][:3] == ["+", "P_X", "(ausente)"]
    assert compare_partitions(None, None) == [["-", "(particionamento)", "sem partição", "sem partição", "OK"]]


def index(name, degree="1", status="N/A", columns=("A", "B")):
    return IndexDefinition(name, "NORMAL", False, columns, "LOCAL", "DFEN_BIG_IDX", degree=degree, status=status)


def test_compare_indexes_requires_same_definition_usable_and_without_parallel_degree():
    expected = [index("BQLOAD_IX1"), index("BQLOAD_IX2"), index("BQLOAD_IX3"), index("BQLOAD_IX4")]
    actual = [
        index("BQLOAD_IX1"),
        index("BQLOAD_IX2", degree="4"),
        index("BQLOAD_IX3", status="UNUSABLE"),
        index("BQLOAD_EXTRA"),
    ]
    rows = compare_indexes(expected, actual)
    assert [row[-1] for row in rows] == ["OK", "DIVERGE", "DIVERGE", "DIVERGE", "DIVERGE"]
    assert rows[0][1] == "LOCAL NORMAL (A, B) em DFEN_BIG_IDX"
    assert rows[3][2] == "(ausente)"
    assert rows[4][:2] == ["BQLOAD_EXTRA", "(não esperado)"]
    assert compare_indexes([index("BQLOAD_IX1")], [index("BQLOAD_IX1", columns=("B", "A"))])[0][-1] == "DIVERGE"


def test_compare_synonyms_requires_all_owners_on_active_table():
    owners = ("DFEN", "NFSE_SIGA", "NFSE_USER")
    targets = {"DFEN": "DFEN.BQLOAD_X_B", "NFSE_SIGA": "DFEN.BQLOAD_X_B", "NFSE_USER": "DFEN.BQLOAD_X_A"}
    rows = compare_synonyms(owners, targets, "DFEN.BQLOAD_X_B")
    assert rows == [
        ["DFEN", "DFEN.BQLOAD_X_B", "OK"],
        ["NFSE_SIGA", "DFEN.BQLOAD_X_B", "OK"],
        ["NFSE_USER", "DFEN.BQLOAD_X_A", "DIVERGE"],
    ]
    assert compare_synonyms(("NFSE_SIGA",), {}, "DFEN.BQLOAD_X_A") == [["NFSE_SIGA", "(ausente)", "DIVERGE"]]


def test_compare_grants_accepts_extra_privileges_and_flags_missing_ones():
    expected = (("RL_NFSE", "SELECT"), ("NFSE_OWNER", "SELECT, ALTER, DELETE"))
    found = [("RL_NFSE", "SELECT"), ("RL_NFSE", "INSERT"), ("NFSE_OWNER", "SELECT"), ("NFSE_OWNER", "DELETE")]
    assert compare_grants(expected, found) == [
        ["RL_NFSE", "SELECT", "INSERT, SELECT", "OK"],
        ["NFSE_OWNER", "ALTER, DELETE, SELECT", "DELETE, SELECT", "DIVERGE"],
    ]
    assert compare_grants((("RL_NFSE", "SELECT"),), []) == [["RL_NFSE", "SELECT", "(nenhum)", "DIVERGE"]]


def test_metric_specs_decode_raw_stored_as_hex_when_the_load_used_hex():
    specs = metric_specs([field("pessoa_emitente", "STRING")], {"PESSOA_EMITENTE": "RAW"}, "hex")
    assert [(spec.kind, spec.oracle_expression) for spec in specs] == [
        ("nao_nulos", 'COUNT("PESSOA_EMITENTE")'),
        ("comprimento_total", 'SUM(UTL_RAW.LENGTH("PESSOA_EMITENTE"))'),
        ("minimo", 'RAWTOHEX(MIN("PESSOA_EMITENTE"))'),
        ("maximo", 'RAWTOHEX(MAX("PESSOA_EMITENTE"))'),
    ]
    assert specs[1].bigquery_expression == "SUM(BYTE_LENGTH(FROM_HEX(NULLIF(`pessoa_emitente`, ''))))"
    assert specs[2].bigquery_expression == "UPPER(TO_HEX(MIN(FROM_HEX(NULLIF(`pessoa_emitente`, '')))))"
    assert specs[3].bigquery_expression == "UPPER(TO_HEX(MAX(FROM_HEX(NULLIF(`pessoa_emitente`, '')))))"


def test_metric_specs_keep_base64_decoding_by_default_and_ignore_the_encoding_for_other_types():
    raw = [field("pessoa_emitente", "STRING")]
    assert metric_specs(raw, {"PESSOA_EMITENTE": "RAW"}) == metric_specs(raw, {"PESSOA_EMITENTE": "RAW"}, "base64")
    text = [field("nome", "STRING")]
    assert metric_specs(text, {"NOME": "VARCHAR2"}, "hex") == metric_specs(text, {"NOME": "VARCHAR2"}, "base64")

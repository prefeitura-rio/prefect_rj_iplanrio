import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import (
    LoaderField,
    OracleColumn,
    build_load_plan,
    loader_spec,
    oracle_column,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import (
    CROSS_SCHEMA_PRIVILEGES,
    MANAGED_TABLE_MARKER,
    assert_managed_table,
    column_definitions,
    definition_differences,
    missing_privileges,
    oracle_table_name,
    secret_env_key,
    validate_identifier,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.sqlldr import build_control_file, split_round_robin


def field(name: str, type_: str, mode: str = "NULLABLE") -> dict[str, str]:
    return {"name": name, "type": type_, "mode": mode}


def ora(name: str, data_type: str, length=None, char_used=None, precision=None, scale=None, nullable=False):
    return OracleColumn(name, data_type, length, char_used, precision, scale, nullable)


TEMPLATE = [
    ora("CPF_CNPJ_RESPONSAVEL", "VARCHAR2", 14, "B"),
    ora("DATA_COMPETENCIA_MUNICIPIO", "DATE", 7),
    ora("NOTA_FISCAL", "NUMBER", 22, precision=15, scale=0),
    ora("VALOR", "NUMBER", 22),
    ora("NOME", "VARCHAR2", 300, "C", nullable=True),
]


def test_ddl_matches_original_column_definitions():
    assert [column.definition for column in TEMPLATE] == [
        '"CPF_CNPJ_RESPONSAVEL" VARCHAR2(14 BYTE) NOT NULL',
        '"DATA_COMPETENCIA_MUNICIPIO" DATE NOT NULL',
        '"NOTA_FISCAL" NUMBER(15,0) NOT NULL',
        '"VALOR" NUMBER NOT NULL',
        '"NOME" VARCHAR2(300 CHAR)',
    ]
    assert ora("QTD", "NUMBER", 22, scale=0).ddl_type == "NUMBER(*,0)"
    assert ora("TS", "TIMESTAMP(6) WITH TIME ZONE", 13).ddl_type == "TIMESTAMP(6) WITH TIME ZONE"


def test_oracle_column_parses_dictionary_row():
    row = {
        "column_name": "NOTA_FISCAL",
        "data_type": "NUMBER",
        "data_length": 22,
        "char_used": None,
        "data_precision": 15,
        "data_scale": 0,
        "nullable": "N",
    }
    assert oracle_column(row) == ora("NOTA_FISCAL", "NUMBER", 22, precision=15, scale=0)


def test_build_load_plan_follows_original_and_ignores_extra_bigquery_columns():
    bq = [
        field("_bigquery_uid", "STRING"),
        field("cpf_cnpj_responsavel", "STRING"),
        field("data_competencia_municipio", "STRING"),
        field("nota_fiscal", "NUMERIC"),
        field("valor", "NUMERIC"),
        field("nome", "STRING"),
        field("_bigquery_particao_data", "DATE"),
    ]
    plan = build_load_plan(bq, TEMPLATE)
    assert plan.columns == TEMPLATE
    assert plan.ignored == ["_BIGQUERY_UID", "_BIGQUERY_PARTICAO_DATA"]
    assert [(f.name, f.spec) for f in plan.fields] == [
        ("_BIGQUERY_UID", "FILLER CHAR(4000)"),
        ("CPF_CNPJ_RESPONSAVEL", "CHAR(4000)"),
        (
            "DATA_COMPETENCIA_MUNICIPIO",
            "CHAR(64) \"TO_DATE(SUBSTR(REPLACE(:DATA_COMPETENCIA_MUNICIPIO, 'T', ' '), 1, 19), "
            "'YYYY-MM-DD HH24:MI:SS')\"",
        ),
        ("NOTA_FISCAL", "CHAR(64)"),
        ("VALOR", "CHAR(64)"),
        ("NOME", "CHAR(4000)"),
        ("_BIGQUERY_PARTICAO_DATA", "FILLER CHAR(4000)"),
    ]


def test_build_load_plan_requires_every_original_column():
    with pytest.raises(ValueError, match="ausentes no BigQuery"):
        build_load_plan([field("cpf_cnpj_responsavel", "STRING")], TEMPLATE)


def test_build_load_plan_excludes_rowid_columns_missing_in_bigquery():
    template = [*TEMPLATE[:2], ora("NN_ROWID", "ROWID", 10), ora("PC_ROWID", "UROWID", 4000)]
    bq = [field("cpf_cnpj_responsavel", "STRING"), field("data_competencia_municipio", "STRING")]
    plan = build_load_plan(bq, template)
    assert plan.excluded == ["NN_ROWID", "PC_ROWID"]
    assert [column.name for column in plan.columns] == ["CPF_CNPJ_RESPONSAVEL", "DATA_COMPETENCIA_MUNICIPIO"]


def test_build_load_plan_excludes_requested_columns_and_ignores_them_if_in_bigquery():
    template = [*TEMPLATE[:2], ora("DPS_ROWID", "VARCHAR2", 18, "B")]
    bq = [field("cpf_cnpj_responsavel", "STRING"), field("data_competencia_municipio", "STRING")]
    with pytest.raises(ValueError, match="excluded_template_columns"):
        build_load_plan(bq, template)
    plan = build_load_plan(bq, template, ["dps_rowid"])
    assert plan.excluded == ["DPS_ROWID"]
    assert [column.name for column in plan.columns] == ["CPF_CNPJ_RESPONSAVEL", "DATA_COMPETENCIA_MUNICIPIO"]
    with_extra = build_load_plan([*bq, field("dps_rowid", "STRING")], template, ["DPS_ROWID"])
    assert with_extra.ignored == ["DPS_ROWID"]


@pytest.mark.parametrize(
    ("column", "bq_type"),
    [
        (ora("DATA", "DATE", 7), "INTEGER"),
        (ora("VALOR", "NUMBER", 22), "STRING"),
        (ora("TEXTO", "CLOB", 4000), "STRING"),
        (ora("_DATA", "DATE", 7), "STRING"),
    ],
)
def test_loader_spec_rejects_unsupported_combinations(column, bq_type):
    with pytest.raises(NotImplementedError):
        loader_spec(column, bq_type)


def test_build_load_plan_rejects_nested_and_invalid_names():
    with pytest.raises(NotImplementedError):
        build_load_plan([field("itens", "STRING", "REPEATED")], [])
    with pytest.raises(ValueError, match="Nome de coluna"):
        build_load_plan([field("coluna com espaço", "STRING")], [])


def test_oracle_table_name_always_has_prefix():
    assert oracle_table_name("MVT_NOTAS_NACIONAIS_EXIGIVEIS") == "BQLOAD_MVT_NOTAS_NACIONAIS_EXIGIVEIS"
    assert oracle_table_name("tabela") == "BQLOAD_TABELA"


def test_validate_identifier_rejects_injection():
    with pytest.raises(ValueError, match="Identificador"):
        validate_identifier('X"; DROP TABLE Y; --')


def test_assert_managed_table_accepts_only_marked_prefixed_tables():
    assert_managed_table("BQLOAD_X", f"{MANAGED_TABLE_MARKER}: carga a partir de a.b.c")
    with pytest.raises(PermissionError, match="não foi criada"):
        assert_managed_table("BQLOAD_X", None)
    with pytest.raises(PermissionError, match="não foi criada"):
        assert_managed_table("BQLOAD_X", "tabela do sistema legado")
    with pytest.raises(PermissionError, match="não começa"):
        assert_managed_table("NOTAS", f"{MANAGED_TABLE_MARKER}: carga")


def test_missing_privileges_skips_schema_owner():
    assert missing_privileges("DFEN", "DFEN", set()) == []


def test_missing_privileges_lists_what_other_users_lack():
    current = {"CREATE ANY TABLE", "DROP ANY TABLE", "INSERT ANY TABLE", "SELECT ANY TABLE", "UNLIMITED TABLESPACE"}
    assert missing_privileges("26234793", "DFEN", current) == ["COMMENT ANY TABLE", "LOCK ANY TABLE"]
    assert missing_privileges("26234793", "DFEN", set(CROSS_SCHEMA_PRIVILEGES)) == []


def test_secret_env_key_follows_iplanrio_convention():
    assert secret_env_key("/db-oracle-nota-fiscal", "DB_HOST") == "DB_ORACLE_NOTA_FISCAL__DB_HOST"


def test_definition_differences_lists_changed_missing_and_extra_columns():
    expected = ['"A" DATE NOT NULL', '"B" VARCHAR2(14 BYTE) NOT NULL']
    assert definition_differences(expected, expected) == []
    assert definition_differences(['"A" VARCHAR2(4000 BYTE)'], expected) == [
        '"A" VARCHAR2(4000 BYTE) → "A" DATE NOT NULL',
        '(ausente) → "B" VARCHAR2(14 BYTE) NOT NULL',
    ]
    assert definition_differences([*expected, '"C" NUMBER'], expected) == ['"C" NUMBER → (ausente)']


def test_column_definitions_keep_types_and_not_null():
    assert column_definitions(TEMPLATE[:2]) == (
        '  "CPF_CNPJ_RESPONSAVEL" VARCHAR2(14 BYTE) NOT NULL,\n  "DATA_COMPETENCIA_MUNICIPIO" DATE NOT NULL'
    )


def test_build_control_file_handles_embedded_newlines_and_blanks():
    control = build_control_file(
        "DESTINO", "BQLOAD_X", [LoaderField("ID", "CHAR(32)"), LoaderField("_EXTRA", "FILLER CHAR(4000)")]
    )
    lines = control.splitlines()
    assert lines[:5] == ["LOAD DATA", "CHARACTERSET AL32UTF8", "APPEND", "PRESERVE BLANKS", 'INTO TABLE "DESTINO"."BQLOAD_X"']
    assert "FIELDS CSV WITH EMBEDDED TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'" in lines
    assert '  "ID" CHAR(32),' in lines
    assert '  "_EXTRA" FILLER CHAR(4000)' in lines


def test_split_round_robin_skips_empty_groups():
    assert split_round_robin(["a", "b", "c"], 2) == [["a", "c"], ["b"]]
    assert split_round_robin(["a"], 4) == [["a"]]
    assert split_round_robin([], 2) == []

import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import Column, map_columns
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import (
    MANAGED_TABLE_MARKER,
    assert_managed_table,
    column_definitions,
    oracle_table_name,
    secret_env_key,
    validate_identifier,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.sqlldr import build_control_file, split_round_robin


def field(name: str, type_: str, mode: str = "NULLABLE") -> dict[str, str]:
    return {"name": name, "type": type_, "mode": mode}


def test_map_columns_keeps_order_and_maps_types():
    columns = map_columns(
        [
            field("_airbyte_raw_id", "STRING", "REQUIRED"),
            field("valor", "NUMERIC"),
            field("qtd", "INTEGER"),
            field("criado_em", "TIMESTAMP"),
            field("meta", "JSON"),
        ]
    )
    assert [column.name for column in columns] == ["_AIRBYTE_RAW_ID", "VALOR", "QTD", "CRIADO_EM", "META"]
    assert [column.oracle_type for column in columns] == [
        "VARCHAR2(4000 BYTE)",
        "NUMBER",
        "NUMBER(19)",
        "TIMESTAMP(6) WITH TIME ZONE",
        "VARCHAR2(4000 BYTE)",
    ]
    assert columns[3].loader_field == 'TIMESTAMP WITH TIME ZONE "YYYY-MM-DD HH24:MI:SS.FF TZR"'


@pytest.mark.parametrize(
    "unsupported",
    [field("dia", "DATE"), field("itens", "STRING", "REPEATED"), field("endereco", "RECORD")],
)
def test_map_columns_rejects_unsupported_types(unsupported):
    with pytest.raises(NotImplementedError):
        map_columns([field("id", "STRING"), unsupported])


def test_map_columns_rejects_invalid_column_name():
    with pytest.raises(ValueError, match="Nome de coluna"):
        map_columns([field("coluna com espaço", "STRING")])


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


def test_secret_env_key_follows_iplanrio_convention():
    assert secret_env_key("/db-oracle-nota-fiscal", "DB_HOST") == "DB_ORACLE_NOTA_FISCAL__DB_HOST"


def test_column_definitions_quotes_names():
    columns = [Column("ID", "NUMBER(19)", "CHAR(32)"), Column("NOME", "VARCHAR2(4000 BYTE)", "CHAR(4000)")]
    assert column_definitions(columns) == '  "ID" NUMBER(19),\n  "NOME" VARCHAR2(4000 BYTE)'


def test_build_control_file_handles_embedded_newlines_and_blanks():
    control = build_control_file("DESTINO", "BQLOAD_X", [Column("ID", "NUMBER(19)", "CHAR(32)")])
    lines = control.splitlines()
    assert lines[:5] == ["LOAD DATA", "CHARACTERSET AL32UTF8", "APPEND", "PRESERVE BLANKS", 'INTO TABLE "DESTINO"."BQLOAD_X"']
    assert "FIELDS CSV WITH EMBEDDED TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'" in lines
    assert '  "ID" CHAR(32)' in lines


def test_split_round_robin_skips_empty_groups():
    assert split_round_robin(["a", "b", "c"], 2) == [["a", "c"], ["b"]]
    assert split_round_robin(["a"], 4) == [["a"]]
    assert split_round_robin([], 2) == []

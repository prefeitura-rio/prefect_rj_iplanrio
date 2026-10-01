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
    QUERIES_ANCHOR,
    assert_managed_table,
    column_definitions,
    definition_differences,
    foreign_indexes,
    missing_privileges,
    oracle_table_name,
    secret_env_key,
    validate_identifier,
)
import io
from datetime import UTC, datetime, timedelta

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils import bigquery
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.bigquery import changed_tables, quiet_wait_seconds
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.inmemory import InMemoryStatus
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.slots import (
    CONSUMER_GRANTS,
    SlotPlan,
    SynonymTarget,
    choose_slots,
    load_comment,
    parse_load_comment,
    synonym_owners,
    synonyms_to_realign,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.structure import (
    IndexDefinition,
    Partitioning,
    TableLayout,
    TablePartition,
    index_from_dictionary,
    index_statement_parts,
    inmemory_from_dictionary,
    layout_differences,
    partitioning_from_dictionary,
    plan_structure,
    slot_indexes,
    storage_clause,
)
from prefect_rj_iplanrio.sql import load_query

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.sqlldr import (
    CountingReader,
    ProgressSnapshot,
    SessionProgress,
    build_control_file,
    format_count,
    format_duration,
    format_progress,
    format_size,
    progress_percent,
    split_round_robin,
)


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


def test_missing_privileges_owner_needs_only_synonyms_in_other_schemas():
    assert missing_privileges("DFEN", "DFEN", set()) == ["CREATE ANY SYNONYM"]
    assert missing_privileges("DFEN", "DFEN", {"CREATE ANY SYNONYM"}) == []


def test_missing_privileges_lists_what_other_users_lack():
    current = {"CREATE ANY TABLE", "DROP ANY TABLE", "INSERT ANY TABLE", "SELECT ANY TABLE", "UNLIMITED TABLESPACE"}
    assert missing_privileges("26234793", "DFEN", current) == [
        "ALTER ANY INDEX",
        "ANALYZE ANY",
        "COMMENT ANY TABLE",
        "CREATE ANY INDEX",
        "CREATE ANY SYNONYM",
        "DROP ANY INDEX",
        "GRANT ANY OBJECT PRIVILEGE",
        "LOCK ANY TABLE",
    ]
    assert missing_privileges("26234793", "DFEN", set(CROSS_SCHEMA_PRIVILEGES)) == []


def test_grant_and_synonym_statements_for_the_physical_table():
    assert load_query(
        QUERIES_ANCHOR, "grant_on_table", privileges="SELECT, ALTER, DELETE", schema="DFEN", table="BQLOAD_X_A",
        grantee="NFSE_OWNER",
    ) == 'GRANT SELECT, ALTER, DELETE ON "DFEN"."BQLOAD_X_A" TO "NFSE_OWNER"\n'
    assert load_query(
        QUERIES_ANCHOR, "create_synonym", owner="NFSE_SIGA", synonym="BQLOAD_X", schema="DFEN", table="BQLOAD_X_B"
    ) == 'CREATE OR REPLACE SYNONYM "NFSE_SIGA"."BQLOAD_X" FOR "DFEN"."BQLOAD_X_B"\n'
    assert CONSUMER_GRANTS == (
        ("RL_NFSE", "SELECT"),
        ("RL_NFSE_SIGA", "SELECT"),
        ("RL_NFSEOWNER_DRL", "SELECT"),
        ("NFSE_OWNER", "SELECT, ALTER, DELETE"),
    )
    assert synonym_owners("DFEN") == ("DFEN", "NFSE_SIGA", "NFSE_USER")


def test_inmemory_queries_keep_the_dollar_of_the_views():
    assert "gv$inmemory_area" in load_query(QUERIES_ANCHOR, "get_inmemory_area")
    assert "gv$im_segments" in load_query(QUERIES_ANCHOR, "get_inmemory_segments")


def syn(owner, table_name, table_owner="DFEN"):
    return SynonymTarget(owner, table_owner, table_name)


def test_choose_slots_first_load_goes_to_a_even_with_legacy_synonyms():
    plan = choose_slots("BQLOAD_X", "DFEN", [])
    assert (plan.active, plan.inactive, plan.slot) == (None, "BQLOAD_X_A", "A")
    legacy = choose_slots("BQLOAD_X", "DFEN", [syn("NFSE_SIGA", "BQLOAD_X"), syn("NFSE_USER", "BQLOAD_X")])
    assert (legacy.active, legacy.inactive) == (None, "BQLOAD_X_A")


def test_choose_slots_alternates_between_a_and_b():
    on_a = [syn("DFEN", "BQLOAD_X_A"), syn("NFSE_SIGA", "BQLOAD_X_A"), syn("NFSE_USER", "BQLOAD_X_A")]
    assert choose_slots("BQLOAD_X", "DFEN", on_a) == SlotPlan("BQLOAD_X", "BQLOAD_X_A", "BQLOAD_X_B")
    on_b = [syn("DFEN", "BQLOAD_X_B")]
    assert choose_slots("BQLOAD_X", "DFEN", on_b) == SlotPlan("BQLOAD_X", "BQLOAD_X_B", "BQLOAD_X_A")


def test_choose_slots_after_interrupted_swap_trusts_schema_then_consumers():
    partial = [syn("DFEN", "BQLOAD_X_A"), syn("NFSE_SIGA", "BQLOAD_X_B"), syn("NFSE_USER", "BQLOAD_X_B")]
    plan = choose_slots("BQLOAD_X", "DFEN", partial)
    assert (plan.active, plan.inactive) == ("BQLOAD_X_A", "BQLOAD_X_B")
    assert synonyms_to_realign(plan, partial) == ["NFSE_SIGA", "NFSE_USER"]
    first_swap_interrupted = [syn("NFSE_SIGA", "BQLOAD_X_A"), syn("NFSE_USER", "BQLOAD_X")]
    plan = choose_slots("BQLOAD_X", "DFEN", first_swap_interrupted)
    assert (plan.active, plan.inactive) == ("BQLOAD_X_A", "BQLOAD_X_B")
    assert synonyms_to_realign(plan, first_swap_interrupted) == []


def test_choose_slots_refuses_synonyms_pointing_to_unmanaged_objects():
    with pytest.raises(PermissionError, match="não gerencia"):
        choose_slots("BQLOAD_X", "DFEN", [syn("NFSE_SIGA", "MVT_X")])
    with pytest.raises(PermissionError, match="não gerencia"):
        choose_slots("BQLOAD_X", "DFEN", [syn("DFEN", "BQLOAD_X_A", table_owner="OUTRO")])
    other_owner = choose_slots("BQLOAD_X", "DFEN", [syn("APP_QUALQUER", "MVT_X")])
    assert other_owner.active is None


def test_load_comment_round_trip_keeps_pipeline_marker():
    snapshot = datetime(2026, 10, 1, 1, 50, 47, 630000, tzinfo=UTC)
    loaded_at = datetime(2026, 10, 1, 3, 4, 5, tzinfo=UTC)
    comment = load_comment("rj-iplanrio-dia.nota_carioca.T", snapshot, loaded_at)
    assert comment == (
        f"{MANAGED_TABLE_MARKER}: carga a partir de rj-iplanrio-dia.nota_carioca.T; "
        "snapshot do BigQuery de 2026-10-01 01:50:47 UTC; carregada em 2026-10-01 03:04:05 UTC"
    )
    assert parse_load_comment(comment) == ("2026-10-01 01:50:47", "2026-10-01 03:04:05")
    assert parse_load_comment(f"{MANAGED_TABLE_MARKER}: carga a partir de x") is None
    assert parse_load_comment(None) is None
    assert_managed_table("BQLOAD_X_A", comment)


def test_slot_indexes_suffix_each_physical_table():
    renamed = slot_indexes((index("BQLOAD_IX_A", ["A"]), index("BQLOAD_IX_B", ["B"])), "B")
    assert [item.name for item in renamed] == ["BQLOAD_IX_A_B", "BQLOAD_IX_B_B"]
    with pytest.raises(ValueError, match="inválido"):
        slot_indexes((index("I" * 127, ["A"]),), "A")


T0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)


def test_quiet_wait_and_changed_tables():
    assert quiet_wait_seconds([T0 - timedelta(minutes=2), T0 - timedelta(hours=1)], T0, 300) == 180
    assert quiet_wait_seconds([T0 - timedelta(minutes=10)], T0, 300) == 0
    assert quiet_wait_seconds([], T0, 300) == 0
    assert changed_tables({"A": T0, "B": T0}, {"A": T0, "B": T0 + timedelta(seconds=1)}) == ["B"]


def snapshot_request(**extra):
    return bigquery.SnapshotRequest(
        project="p", dataset_id="d", table_ids=["DET", "EXI"], bucket="b", prefix="d/run", quiet_seconds=300, **extra
    )


def fake_bigquery(monkeypatch, modified_reads):
    reads = iter(modified_reads)
    extracted, deleted = [], []
    monkeypatch.setattr(bigquery, "get_last_modified", lambda project, dataset_id, table_id: next(reads)[table_id])
    monkeypatch.setattr(
        bigquery, "get_table_schema", lambda project, dataset_id, table_id: {"fields": [], "num_rows": 1}
    )

    def extract(project, dataset_id, table_id, bucket, prefix):
        extracted.append(prefix)
        return [bigquery.ExportedFile(name=f"{prefix}/part-0.csv.gz", size=1)]

    monkeypatch.setattr(bigquery, "extract_table_to_gcs", extract)
    monkeypatch.setattr(bigquery, "delete_blobs", lambda project, bucket, blob_names: deleted.extend(blob_names))
    monkeypatch.setattr(bigquery, "datetime", type("FrozenDatetime", (), {"now": staticmethod(lambda tz: T0)}))
    return extracted, deleted


def test_export_consistent_snapshot_waits_for_quiet_period_and_retries_on_change(monkeypatch):
    old, recent, newer = T0 - timedelta(hours=1), T0 - timedelta(minutes=1), T0 - timedelta(seconds=30)
    reads = [
        {"DET": recent}, {"EXI": old},
        {"DET": old}, {"EXI": old}, {"DET": old}, {"EXI": newer},
        {"DET": old}, {"EXI": old}, {"DET": old}, {"EXI": old},
    ]
    waits, messages = [], []
    extracted, deleted = fake_bigquery(monkeypatch, reads)
    snapshot = bigquery.export_consistent_snapshot(snapshot_request(), report=messages.append, sleep=waits.append)
    assert waits == [240]
    assert extracted == ["d/run/tentativa-1/DET", "d/run/tentativa-1/EXI", "d/run/tentativa-2/DET", "d/run/tentativa-2/EXI"]
    assert deleted == ["d/run/tentativa-1/DET/part-0.csv.gz", "d/run/tentativa-1/EXI/part-0.csv.gz"]
    assert "mudou durante o extract em ['EXI']" in messages[1]
    assert snapshot["DET"].last_modified == old
    assert snapshot["EXI"].files == [bigquery.ExportedFile("d/run/tentativa-2/EXI/part-0.csv.gz", 1)]


def test_export_consistent_snapshot_gives_up(monkeypatch):
    recent = T0 - timedelta(seconds=10)
    fake_bigquery(monkeypatch, [{"DET": recent}, {"EXI": recent}])
    with pytest.raises(RuntimeError, match="sem alterações"):
        bigquery.export_consistent_snapshot(
            snapshot_request(max_wait_seconds=60), report=lambda message: None, sleep=lambda seconds: None
        )
    old = T0 - timedelta(hours=1)
    fake_bigquery(monkeypatch, [{"DET": old}, {"EXI": old}, {"DET": T0}, {"EXI": old}])
    with pytest.raises(RuntimeError, match="1 extracts seguidos"):
        bigquery.export_consistent_snapshot(
            snapshot_request(max_attempts=1), report=lambda message: None, sleep=lambda seconds: None
        )


def test_inmemory_status_complete_and_description():
    done = InMemoryStatus("BQLOAD_X_A", 6, 6, 0, 0)
    assert done.complete
    assert done.description == "BQLOAD_X_A: 6/6 segmento(s) no In-Memory, 0,00 GB por popular"
    assert not InMemoryStatus("T", 6, 5, 0, 0).complete
    assert not InMemoryStatus("T", 6, 6, 1024**3, 1).complete
    assert InMemoryStatus("T", 0, 0, 0, 0).complete


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


GB = 1024**3


def test_format_helpers():
    assert format_count(67987860) == "67.987.860"
    assert format_size(int(1.5 * GB)) == "1,50 GB"
    assert format_duration(250) == "4m10s"
    assert format_duration(3725) == "1h02m05s"


def test_progress_percent_and_message_with_estimate():
    snapshot = ProgressSnapshot(files_done=30, files_total=120, bytes_done=GB, bytes_total=4 * GB, elapsed_seconds=300)
    assert progress_percent(snapshot) == 25.0
    assert format_progress("BQLOAD_X", snapshot) == (
        "Carga de BQLOAD_X: 30/120 arquivos, 1,00 GB de 4,00 GB (25,0%), 5m00s decorridos, término estimado em ~15m00s"
    )


def test_progress_message_before_start_and_after_all_files_sent():
    start = ProgressSnapshot(files_done=0, files_total=10, bytes_done=0, bytes_total=GB, elapsed_seconds=1)
    assert format_progress("T", start).endswith("(0,0%), 0m01s decorridos, calculando estimativa")
    done = ProgressSnapshot(files_done=10, files_total=10, bytes_done=GB, bytes_total=GB, elapsed_seconds=60)
    assert progress_percent(done) == 100.0
    assert format_progress("T", done).endswith("aguardando o SQL*Loader concluir")
    empty = ProgressSnapshot(files_done=0, files_total=0, bytes_done=0, bytes_total=0, elapsed_seconds=0)
    assert progress_percent(empty) == 100.0


def test_counting_reader_tracks_bytes_read():
    progress = SessionProgress()
    reader = CountingReader(io.BytesIO(b"x" * 10), progress)
    assert reader.read(4) == b"xxxx"
    assert reader.read() == b"xxxxxx"
    assert reader.read() == b""
    progress.add_file()
    assert (progress.bytes_done, progress.files_done) == (10, 1)


JAN = "TO_DATE(' 2026-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
FEB = "TO_DATE(' 2026-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
RANGE_BY_MONTH = Partitioning(
    kind="RANGE",
    key_columns=("DATA_COMPETENCIA_MUNICIPIO",),
    interval=None,
    partitions=(TablePartition("P_INITIAL", JAN, "DFEN_BIG_DATA"), TablePartition("P_202601", FEB, "DFEN_BIG_DATA")),
)


def index(name, columns, locality="LOCAL", index_type="NORMAL", unique=False, tablespace="DFEN_BIG_IDX", **extra):
    return IndexDefinition(name, index_type, unique, tuple(columns), locality, tablespace, **extra)


def test_partitioning_from_dictionary_keeps_only_declared_partitions():
    partitioning = partitioning_from_dictionary(
        {"partitioning_type": "RANGE", "subpartitioning_type": "NONE", "interval": "NUMTOYMINTERVAL(1, 'MONTH') "},
        ["DATA_COMPETENCIA_MUNICIPIO"],
        [
            {"partition_name": "P_INITIAL", "high_value": f" {JAN} ", "tablespace_name": "DFEN_BIG_DATA", "interval": "NO"},
            {"partition_name": "SYS_P101", "high_value": FEB, "tablespace_name": "DFEN_BIG_DATA", "interval": "YES"},
        ],
    )
    assert partitioning == Partitioning(
        "RANGE",
        ("DATA_COMPETENCIA_MUNICIPIO",),
        "NUMTOYMINTERVAL(1, 'MONTH')",
        (TablePartition("P_INITIAL", JAN, "DFEN_BIG_DATA"),),
    )
    composite = partitioning_from_dictionary(
        {"partitioning_type": "RANGE", "subpartitioning_type": "HASH", "interval": None}, ["D"], []
    )
    assert composite.kind == "RANGE-HASH"


def test_index_from_dictionary_ignores_degree_and_status_in_comparison():
    row = {
        "owner": "DFEN",
        "index_name": "IX",
        "index_type": "NORMAL",
        "uniqueness": "NONUNIQUE",
        "locality": "LOCAL",
        "tablespace_name": "DFEN_BIG_IDX",
        "degree": "  4",
        "status": "N/A",
    }
    parsed = index_from_dictionary(row, ["A", "B"])
    assert (parsed.degree, parsed.status, parsed.owner) == ("4", "N/A", "DFEN")
    assert parsed == index("IX", ["A", "B"])


def test_plan_structure_renames_indexes_and_skips_materialized_view_snapshot_index():
    template = TableLayout(
        "DFEN_BIG_DATA",
        RANGE_BY_MONTH,
        (
            index("I_SNAP$_MVT_NOTAS", ["SYS_NC00015$"], index_type="FUNCTION-BASED NORMAL", tablespace=None),
            index("IX_MVT_NNEX_DET_CPFR_DCM_NN", ["CPF_CNPJ_RESPONSAVEL", "DATA_COMPETENCIA_MUNICIPIO"]),
        ),
    )
    plan = plan_structure(template, ["CPF_CNPJ_RESPONSAVEL", "DATA_COMPETENCIA_MUNICIPIO"], "BQLOAD_")
    assert plan.skipped_indexes == ("I_SNAP$_MVT_NOTAS",)
    assert plan.layout == TableLayout(
        "DFEN_BIG_DATA",
        RANGE_BY_MONTH,
        (index("BQLOAD_IX_MVT_NNEX_DET_CPFR_DCM_NN", ["CPF_CNPJ_RESPONSAVEL", "DATA_COMPETENCIA_MUNICIPIO"]),),
        inmemory=INMEMORY_HIGH,
    )


@pytest.mark.parametrize(
    ("template", "error"),
    [
        (TableLayout(None, None, (index("IX", ["A"], locality="GLOBAL"),)), NotImplementedError),
        (TableLayout(None, None, (index("IX", ["SYS_NC1$"], index_type="FUNCTION-BASED NORMAL"),)), NotImplementedError),
        (TableLayout(None, Partitioning("HASH", ("A",), None, ()), ()), NotImplementedError),
        (TableLayout(None, None, (index("IX", ["NN_ROWID"]),)), ValueError),
        (TableLayout(None, Partitioning("RANGE", ("NN_ROWID",), None, ()), ()), ValueError),
        (TableLayout(None, None, (index("I" * 124, ["A"]),)), ValueError),
    ],
)
def test_plan_structure_rejects_what_the_load_cannot_replicate(template, error):
    with pytest.raises(error):
        plan_structure(template, ["A"], "BQLOAD_")


def test_storage_clause_copies_tablespace_and_partitions_without_inmemory():
    assert storage_clause(TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH)) == (
        'TABLESPACE "DFEN_BIG_DATA"\n'
        "NO INMEMORY\n"
        'PARTITION BY RANGE ("DATA_COMPETENCIA_MUNICIPIO") (\n'
        f'  PARTITION "P_INITIAL" VALUES LESS THAN ({JAN}) TABLESPACE "DFEN_BIG_DATA",\n'
        f'  PARTITION "P_202601" VALUES LESS THAN ({FEB}) TABLESPACE "DFEN_BIG_DATA"\n'
        ")"
    )
    interval = Partitioning("RANGE", ("D",), "NUMTOYMINTERVAL(1, 'MONTH')", (TablePartition("P", JAN, None),))
    assert storage_clause(TableLayout(None, interval)) == (
        f"NO INMEMORY\nPARTITION BY RANGE (\"D\") INTERVAL (NUMTOYMINTERVAL(1, 'MONTH')) (\n"
        f'  PARTITION "P" VALUES LESS THAN ({JAN})\n)'
    )
    listed = Partitioning("LIST", ("UF",), None, (TablePartition("P_RJ", "'RJ'", None),))
    assert "PARTITION \"P_RJ\" VALUES ('RJ')" in storage_clause(TableLayout(None, listed))
    assert storage_clause(TableLayout(None, None)) == "NO INMEMORY"


INMEMORY_HIGH = "INMEMORY PRIORITY HIGH MEMCOMPRESS FOR QUERY HIGH DISTRIBUTE AUTO NO DUPLICATE"


def inmemory_row(inmemory="ENABLED", priority="HIGH", compression="FOR QUERY HIGH", distribute="AUTO", duplicate="NO DUPLICATE"):
    return {
        "inmemory": inmemory,
        "inmemory_priority": priority,
        "inmemory_compression": compression,
        "inmemory_distribute": distribute,
        "inmemory_duplicate": duplicate,
    }


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        pytest.param(inmemory_row(), INMEMORY_HIGH, id="original_mvt"),
        pytest.param(
            inmemory_row(priority="NONE", compression="NO MEMCOMPRESS", distribute=None, duplicate=None),
            "INMEMORY PRIORITY NONE NO MEMCOMPRESS",
            id="no_compression_single_instance",
        ),
        pytest.param(
            inmemory_row(compression="FOR CAPACITY LOW", distribute="BY ROWID RANGE", duplicate="DUPLICATE ALL"),
            "INMEMORY PRIORITY HIGH MEMCOMPRESS FOR CAPACITY LOW DISTRIBUTE BY ROWID RANGE DUPLICATE ALL",
            id="rac_options",
        ),
        pytest.param(inmemory_row(inmemory="DISABLED"), None, id="disabled"),
        pytest.param(inmemory_row(inmemory="NONE"), None, id="partition_default_not_set"),
        pytest.param(inmemory_row(inmemory=None), None, id="no_value"),
    ],
)
def test_inmemory_from_dictionary_builds_the_create_table_clause(row, expected):
    assert inmemory_from_dictionary(row) == expected


@pytest.mark.parametrize(
    "row",
    [
        pytest.param(inmemory_row(priority="HIGH; DROP"), id="priority"),
        pytest.param(inmemory_row(compression="FOR QUERY"), id="compression"),
        pytest.param(inmemory_row(distribute="BY HASH"), id="distribute"),
        pytest.param(inmemory_row(duplicate="TRIPLICATE"), id="duplicate"),
    ],
)
def test_inmemory_from_dictionary_rejects_unknown_values(row):
    with pytest.raises(NotImplementedError, match="INMEMORY"):
        inmemory_from_dictionary(row)


@pytest.mark.parametrize(
    "original_inmemory",
    [
        pytest.param(None, id="original_not_in_memory"),
        pytest.param(INMEMORY_HIGH, id="original_in_memory"),
        pytest.param("INMEMORY PRIORITY LOW MEMCOMPRESS FOR DML", id="original_with_other_options"),
    ],
)
def test_plan_structure_puts_every_loaded_table_in_memory(original_inmemory):
    template = TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH, inmemory=original_inmemory)
    assert plan_structure(template, ["DATA_COMPETENCIA_MUNICIPIO"], "BQLOAD_").layout.inmemory == INMEMORY_HIGH


def test_storage_clause_uses_inmemory_of_original():
    assert storage_clause(TableLayout("DFEN_BIG_DATA", None, inmemory=INMEMORY_HIGH)) == (
        f'TABLESPACE "DFEN_BIG_DATA"\n{INMEMORY_HIGH}'
    )


def test_layout_differences_detect_changed_inmemory():
    expected = TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH, inmemory=INMEMORY_HIGH)
    assert layout_differences(expected, expected) == []
    assert layout_differences(TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH), expected) == [
        f"inmemory: NO INMEMORY → {INMEMORY_HIGH}"
    ]


def test_index_statement_parts():
    assert index_statement_parts(index("BQLOAD_IX", ["A", "B"]), 4) == {
        "kind": "",
        "columns": '"A", "B"',
        "options": 'TABLESPACE "DFEN_BIG_IDX" LOCAL PARALLEL 4',
    }
    unique = index("BQLOAD_UK", ["A"], locality=None, unique=True, tablespace=None)
    assert index_statement_parts(unique, 2) == {"kind": "UNIQUE", "columns": '"A"', "options": "PARALLEL 2"}
    assert index_statement_parts(index("BQLOAD_BM", ["A"], index_type="BITMAP"), 1)["kind"] == "BITMAP"


def test_layout_differences_detect_missing_partitioning_and_changed_partitions():
    expected = TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH)
    assert layout_differences(expected, expected) == []
    assert layout_differences(TableLayout("USERS", None), expected) == [
        "tablespace: USERS → DFEN_BIG_DATA",
        "particionamento: sem partição → RANGE (DATA_COMPETENCIA_MUNICIPIO) com 2 partição(ões) declarada(s)",
    ]
    fewer = Partitioning("RANGE", ("DATA_COMPETENCIA_MUNICIPIO",), None, RANGE_BY_MONTH.partitions[:1])
    assert layout_differences(TableLayout("DFEN_BIG_DATA", fewer), expected) == [
        f"partição ausente: P_202601 ({FEB}) em DFEN_BIG_DATA"
    ]
    with_interval = Partitioning("RANGE", ("DATA_COMPETENCIA_MUNICIPIO",), "NUMTOYMINTERVAL(1, 'MONTH')", RANGE_BY_MONTH.partitions)
    assert layout_differences(TableLayout("DFEN_BIG_DATA", with_interval), expected) == [
        "particionamento: RANGE (DATA_COMPETENCIA_MUNICIPIO) INTERVAL NUMTOYMINTERVAL(1, 'MONTH') → "
        "RANGE (DATA_COMPETENCIA_MUNICIPIO)"
    ]


def test_foreign_indexes_protects_indexes_the_pipeline_did_not_create():
    ours = index("BQLOAD_IX", ["A"], owner="DFEN")
    assert foreign_indexes([ours], "DFEN") == []
    assert foreign_indexes([ours, index("IX_MANUAL", ["A"], owner="DFEN"), index("BQLOAD_X", ["A"], owner="OUTRO")], "DFEN") == [
        "DFEN.IX_MANUAL",
        "OUTRO.BQLOAD_X",
    ]

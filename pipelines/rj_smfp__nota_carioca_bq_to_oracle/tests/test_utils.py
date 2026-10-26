import io
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import final

import pytest
from google.cloud import storage

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers import bigquery, oracle, oracle_ddl, slots_oracle
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.bigquery import changed_tables, quiet_wait_seconds
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import (
    LoaderField,
    OracleColumn,
    build_load_plan,
    loader_spec,
    oracle_column,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.inmemory import InMemoryStatus
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import (
    CROSS_SCHEMA_PRIVILEGES,
    MANAGED_TABLE_MARKER,
    OracleConfig,
    assert_managed_table,
    missing_privileges,
    oracle_table_name,
    validate_identifier,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_ddl import definition_differences, foreign_indexes
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_query import QUERIES_ANCHOR, column_definitions
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.slots import (
    CONSUMER_GRANTS,
    SlotPlan,
    SynonymTarget,
    choose_slots,
    load_comment,
    parse_load_comment,
    synonym_owners,
    synonyms_to_realign,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.sqlldr import build_control_file, split_round_robin
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.sqlldr_progress import (
    CountingReader,
    ProgressSnapshot,
    SessionProgress,
    format_count,
    format_duration,
    format_progress,
    format_size,
    progress_percent,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure import (
    index_from_dictionary,
    index_key,
    inmemory_from_dictionary,
    layout_differences,
    partitioning_from_dictionary,
    plan_structure,
    slot_indexes,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_ddl import index_statement_parts, storage_clause
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import (
    IndexDefinition,
    IndexExpression,
    Partitioning,
    TableLayout,
    TablePartition,
)
from utils.utils import render_query


def field(name: str, type_: str, mode: str = "NULLABLE") -> dict[str, str]:
    return {"name": name, "type": type_, "mode": mode}


def ora(
    name: str,
    data_type: str,
    length: int | None = None,
    char_used: str | None = None,
    precision: int | None = None,
    scale: int | None = None,
    nullable: bool = False,
) -> OracleColumn:
    return OracleColumn(name, data_type, length, char_used, precision, scale, nullable)


TEMPLATE = [
    ora("CPF_CNPJ_RESPONSAVEL", "VARCHAR2", 14, "B"),
    ora("DATA_COMPETENCIA_MUNICIPIO", "DATE", 7),
    ora("NOTA_FISCAL", "NUMBER", 22, precision=15, scale=0),
    ora("VALOR", "NUMBER", 22),
    ora("NOME", "VARCHAR2", 300, "C", nullable=True),
]


def test_ddl_matches_original_column_definitions() -> None:
    """Check that ddl matches original column definitions."""
    assert [column.definition for column in TEMPLATE] == [
        '"CPF_CNPJ_RESPONSAVEL" VARCHAR2(14 BYTE) NOT NULL',
        '"DATA_COMPETENCIA_MUNICIPIO" DATE NOT NULL',
        '"NOTA_FISCAL" NUMBER(15,0) NOT NULL',
        '"VALOR" NUMBER NOT NULL',
        '"NOME" VARCHAR2(300 CHAR)',
    ]
    assert ora("QTD", "NUMBER", 22, scale=0).ddl_type == "NUMBER(*,0)"
    assert ora("TS", "TIMESTAMP(6) WITH TIME ZONE", 13).ddl_type == "TIMESTAMP(6) WITH TIME ZONE"


def test_oracle_column_parses_dictionary_row() -> None:
    """Check that oracle column parses dictionary row."""
    row: dict[str, object] = {
        "column_name": "NOTA_FISCAL",
        "data_type": "NUMBER",
        "data_length": 22,
        "char_used": None,
        "data_precision": 15,
        "data_scale": 0,
        "nullable": "N",
    }
    assert oracle_column(row) == ora("NOTA_FISCAL", "NUMBER", 22, precision=15, scale=0)


def test_build_load_plan_follows_original_and_ignores_extra_bigquery_columns() -> None:
    """Check that build load plan follows original and ignores extra bigquery columns."""
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
        ("CPF_CNPJ_RESPONSAVEL", "CHAR(56)"),
        ("DATA_COMPETENCIA_MUNICIPIO", 'DATE "YYYY-MM-DD\\"T\\"HH24:MI:SS"'),
        ("NOTA_FISCAL", "CHAR(64)"),
        ("VALOR", "CHAR(64)"),
        ("NOME", "CHAR(1200)"),
        ("_BIGQUERY_PARTICAO_DATA", "FILLER CHAR(4000)"),
    ]


def test_build_load_plan_requires_every_original_column() -> None:
    """Check that build load plan requires every original column."""
    with pytest.raises(ValueError, match="ausentes no BigQuery"):
        build_load_plan([field("cpf_cnpj_responsavel", "STRING")], TEMPLATE)


def test_build_load_plan_excludes_rowid_columns_missing_in_bigquery() -> None:
    """Check that build load plan excludes rowid columns missing in bigquery."""
    template = [*TEMPLATE[:2], ora("NN_ROWID", "ROWID", 10), ora("PC_ROWID", "UROWID", 4000)]
    bq = [field("cpf_cnpj_responsavel", "STRING"), field("data_competencia_municipio", "STRING")]
    plan = build_load_plan(bq, template)
    assert plan.excluded == ["NN_ROWID", "PC_ROWID"]
    assert [column.name for column in plan.columns] == ["CPF_CNPJ_RESPONSAVEL", "DATA_COMPETENCIA_MUNICIPIO"]


def test_build_load_plan_excludes_requested_columns_and_ignores_them_if_in_bigquery() -> None:
    """Check that build load plan excludes requested columns and ignores them if in bigquery."""
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
    ],
)
def test_loader_spec_rejects_unsupported_combinations(column: OracleColumn, bq_type: str) -> None:
    """Check that loader spec rejects unsupported combinations."""
    with pytest.raises(NotImplementedError):
        loader_spec(column, bq_type)


def test_build_load_plan_rejects_nested_and_invalid_names() -> None:
    """Check that build load plan rejects nested and invalid names."""
    with pytest.raises(NotImplementedError):
        build_load_plan([field("itens", "STRING", "REPEATED")], [])
    with pytest.raises(ValueError, match="Nome de coluna"):
        build_load_plan([field("coluna com espaço", "STRING")], [])


def test_oracle_table_name_always_has_prefix() -> None:
    """Check that oracle table name always has prefix."""
    assert oracle_table_name("MVT_NOTAS_NACIONAIS_EXIGIVEIS") == "BQLOAD_MVT_NOTAS_NACIONAIS_EXIGIVEIS"
    assert oracle_table_name("tabela") == "BQLOAD_TABELA"


def test_validate_identifier_rejects_injection() -> None:
    """Check that validate identifier rejects injection."""
    with pytest.raises(ValueError, match="Identificador"):
        validate_identifier('X"; DROP TABLE Y; --')


def test_assert_managed_table_accepts_only_marked_prefixed_tables() -> None:
    """Check that assert managed table accepts only marked prefixed tables."""
    assert_managed_table("BQLOAD_X", f"{MANAGED_TABLE_MARKER}: carga a partir de a.b.c")
    with pytest.raises(PermissionError, match="não foi criada"):
        assert_managed_table("BQLOAD_X", None)
    with pytest.raises(PermissionError, match="não foi criada"):
        assert_managed_table("BQLOAD_X", "tabela do sistema legado")
    with pytest.raises(PermissionError, match="não começa"):
        assert_managed_table("NOTAS", f"{MANAGED_TABLE_MARKER}: carga")


def test_missing_privileges_owner_needs_only_synonyms_in_other_schemas() -> None:
    """Check that missing privileges owner needs only synonyms in other schemas."""
    assert missing_privileges("DFEN", "DFEN", set()) == ["CREATE ANY SYNONYM"]
    assert missing_privileges("DFEN", "DFEN", {"CREATE ANY SYNONYM"}) == []


def test_missing_privileges_lists_what_other_users_lack() -> None:
    """Check that missing privileges lists what other users lack."""
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


def test_grant_and_synonym_statements_for_the_physical_table() -> None:
    """Check that grant and synonym statements for the physical table."""
    assert (
        render_query(
            QUERIES_ANCHOR,
            "grant_on_table",
            privileges="SELECT, ALTER, DELETE",
            schema="DFEN",
            table="BQLOAD_X_A",
            grantee="NFSE_OWNER",
        )
        == 'GRANT SELECT, ALTER, DELETE ON "DFEN"."BQLOAD_X_A" TO "NFSE_OWNER"\n'
    )
    assert (
        render_query(
            QUERIES_ANCHOR, "create_synonym", owner="NFSE_SIGA", synonym="BQLOAD_X", schema="DFEN", table="BQLOAD_X_B"
        )
        == 'CREATE OR REPLACE SYNONYM "NFSE_SIGA"."BQLOAD_X" FOR "DFEN"."BQLOAD_X_B"\n'
    )
    assert CONSUMER_GRANTS == (
        ("RL_NFSE", "SELECT"),
        ("RL_NFSE_SIGA", "SELECT"),
        ("RL_NFSEOWNER_DRL", "SELECT"),
        ("NFSE_OWNER", "SELECT, ALTER, DELETE"),
    )
    assert synonym_owners("DFEN") == ("DFEN", "NFSE_SIGA", "NFSE_USER", "NFSE_OWNER")


def test_inmemory_queries_keep_the_dollar_of_the_views() -> None:
    """Check that inmemory queries keep the dollar of the views."""
    assert "gv$inmemory_area" in render_query(QUERIES_ANCHOR, "get_inmemory_area")
    assert "gv$im_segments" in render_query(QUERIES_ANCHOR, "get_inmemory_segments")


def syn(owner: str, table_name: str, table_owner: str = "DFEN") -> SynonymTarget:
    return SynonymTarget(owner, table_owner, table_name)


def test_choose_slots_first_load_goes_to_a_even_with_legacy_synonyms() -> None:
    """Check that choose slots first load goes to a even with legacy synonyms."""
    plan = choose_slots("BQLOAD_X", "DFEN", [])
    assert (plan.active, plan.inactive, plan.slot) == (None, "BQLOAD_X_A", "A")
    legacy = choose_slots("BQLOAD_X", "DFEN", [syn("NFSE_SIGA", "BQLOAD_X"), syn("NFSE_USER", "BQLOAD_X")])
    assert (legacy.active, legacy.inactive) == (None, "BQLOAD_X_A")


def test_choose_slots_alternates_between_a_and_b() -> None:
    """Check that choose slots alternates between a and b."""
    on_a = [syn("DFEN", "BQLOAD_X_A"), syn("NFSE_SIGA", "BQLOAD_X_A"), syn("NFSE_USER", "BQLOAD_X_A")]
    assert choose_slots("BQLOAD_X", "DFEN", on_a) == SlotPlan("BQLOAD_X", "BQLOAD_X_A", "BQLOAD_X_B")
    on_b = [syn("DFEN", "BQLOAD_X_B")]
    assert choose_slots("BQLOAD_X", "DFEN", on_b) == SlotPlan("BQLOAD_X", "BQLOAD_X_B", "BQLOAD_X_A")


def test_choose_slots_after_interrupted_swap_trusts_schema_then_consumers() -> None:
    """Check that choose slots after interrupted swap trusts schema then consumers."""
    partial = [syn("DFEN", "BQLOAD_X_A"), syn("NFSE_SIGA", "BQLOAD_X_B"), syn("NFSE_USER", "BQLOAD_X_B")]
    plan = choose_slots("BQLOAD_X", "DFEN", partial)
    assert (plan.active, plan.inactive) == ("BQLOAD_X_A", "BQLOAD_X_B")
    assert synonyms_to_realign(plan, partial) == ["NFSE_SIGA", "NFSE_USER"]
    first_swap_interrupted = [syn("NFSE_SIGA", "BQLOAD_X_A"), syn("NFSE_USER", "BQLOAD_X")]
    plan = choose_slots("BQLOAD_X", "DFEN", first_swap_interrupted)
    assert (plan.active, plan.inactive) == ("BQLOAD_X_A", "BQLOAD_X_B")
    assert synonyms_to_realign(plan, first_swap_interrupted) == []


def test_choose_slots_refuses_synonyms_pointing_to_unmanaged_objects() -> None:
    """Check that choose slots refuses synonyms pointing to unmanaged objects."""
    with pytest.raises(PermissionError, match="não gerencia"):
        choose_slots("BQLOAD_X", "DFEN", [syn("NFSE_SIGA", "MVT_X")])
    with pytest.raises(PermissionError, match="não gerencia"):
        choose_slots("BQLOAD_X", "DFEN", [syn("DFEN", "BQLOAD_X_A", table_owner="OUTRO")])
    other_owner = choose_slots("BQLOAD_X", "DFEN", [syn("APP_QUALQUER", "MVT_X")])
    assert other_owner.active is None


def test_load_comment_round_trip_keeps_pipeline_marker() -> None:
    """Check that load comment round trip keeps pipeline marker."""
    snapshot = datetime(2026, 10, 1, 1, 50, 47, 630000, tzinfo=UTC)
    loaded_at = datetime(2026, 10, 1, 3, 4, 5, tzinfo=UTC)
    comment = load_comment("rj-iplanrio-dia.nota_carioca.T", snapshot, loaded_at)
    assert comment == (
        f"{MANAGED_TABLE_MARKER}: carga a partir de rj-iplanrio-dia.nota_carioca.T; "
        + "snapshot do BigQuery de 2026-10-01 01:50:47 UTC; carregada em 2026-10-01 03:04:05 UTC"
    )
    assert parse_load_comment(comment) == ("2026-10-01 01:50:47", "2026-10-01 03:04:05")
    assert parse_load_comment(f"{MANAGED_TABLE_MARKER}: carga a partir de x") is None
    assert parse_load_comment(None) is None
    assert_managed_table("BQLOAD_X_A", comment)


def test_slot_indexes_suffix_each_physical_table() -> None:
    """Check that slot indexes suffix each physical table."""
    renamed = slot_indexes((index("BQLOAD_IX_A", ["A"]), index("BQLOAD_IX_B", ["B"])), "B")
    assert [item.name for item in renamed] == ["BQLOAD_IX_A_B", "BQLOAD_IX_B_B"]
    with pytest.raises(ValueError, match="inválido"):
        slot_indexes((index("I" * 127, ["A"]),), "A")


T0 = datetime(2026, 10, 1, 12, 0, 0, tzinfo=UTC)


def test_quiet_wait_and_changed_tables() -> None:
    """Check that quiet wait and changed tables."""
    assert quiet_wait_seconds([T0 - timedelta(minutes=2), T0 - timedelta(hours=1)], T0, 300) == 180
    assert quiet_wait_seconds([T0 - timedelta(minutes=10)], T0, 300) == 0
    assert quiet_wait_seconds([], T0, 300) == 0
    assert changed_tables({"A": T0, "B": T0}, {"A": T0, "B": T0 + timedelta(seconds=1)}) == ["B"]


def snapshot_request(max_wait_seconds: float = 3600, max_attempts: int = 3) -> bigquery.SnapshotRequest:
    return bigquery.SnapshotRequest(
        project="p",
        dataset_id="d",
        table_ids=["DET", "EXI"],
        bucket="b",
        prefix="d/run",
        quiet_seconds=300,
        max_wait_seconds=max_wait_seconds,
        max_attempts=max_attempts,
    )


@final
class FrozenDatetime:
    """Replaces `datetime` in the BigQuery helper, so that `now` is always `T0`."""

    @staticmethod
    def now(tz: object) -> datetime:
        del tz

        return T0


def ignore_message(message: str) -> None:
    del message


def ignore_seconds(seconds: float) -> None:
    del seconds


def fake_bigquery(
    monkeypatch: pytest.MonkeyPatch, modified_reads: list[dict[str, datetime]]
) -> tuple[list[str], list[str]]:
    reads = iter(modified_reads)
    extracted: list[str] = []
    deleted: list[str] = []

    def last_modified(project: str, dataset_id: str, table_id: str) -> datetime:
        del project, dataset_id

        return next(reads)[table_id]

    def schema(project: str, dataset_id: str, table_id: str) -> bigquery.TableSchema:
        del project, dataset_id, table_id

        return bigquery.TableSchema(fields=[], num_rows=1)

    def extract(project: str, dataset_id: str, table_id: str, bucket: str, prefix: str) -> list[bigquery.ExportedFile]:
        del project, dataset_id, table_id, bucket
        extracted.append(prefix)

        return [bigquery.ExportedFile(name=f"{prefix}/part-0.csv.gz", size=1)]

    def delete(project: str, bucket: str, blob_names: list[str]) -> None:
        del project, bucket
        deleted.extend(blob_names)

    monkeypatch.setattr(bigquery, "get_last_modified", last_modified)
    monkeypatch.setattr(bigquery, "get_table_schema", schema)
    monkeypatch.setattr(bigquery, "extract_table_to_gcs", extract)
    monkeypatch.setattr(bigquery, "delete_blobs", delete)
    monkeypatch.setattr(bigquery, "datetime", FrozenDatetime)

    return extracted, deleted


def test_export_consistent_snapshot_waits_for_quiet_period_and_retries_on_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Check that export consistent snapshot waits for quiet period and retries on change."""
    old, recent, newer = T0 - timedelta(hours=1), T0 - timedelta(minutes=1), T0 - timedelta(seconds=30)
    reads = [
        {"DET": recent},
        {"EXI": old},
        {"DET": old},
        {"EXI": old},
        {"DET": old},
        {"EXI": newer},
        {"DET": old},
        {"EXI": old},
        {"DET": old},
        {"EXI": old},
    ]
    waits: list[float] = []
    messages: list[str] = []
    extracted, deleted = fake_bigquery(monkeypatch, reads)
    snapshot = bigquery.export_consistent_snapshot(snapshot_request(), report=messages.append, sleep=waits.append)
    assert waits == [240]
    assert extracted == [
        "d/run/tentativa-1/DET",
        "d/run/tentativa-1/EXI",
        "d/run/tentativa-2/DET",
        "d/run/tentativa-2/EXI",
    ]
    assert deleted == ["d/run/tentativa-1/DET/part-0.csv.gz", "d/run/tentativa-1/EXI/part-0.csv.gz"]
    assert "mudou durante o extract em ['EXI']" in messages[1]
    assert snapshot["DET"].last_modified == old
    assert snapshot["EXI"].files == [bigquery.ExportedFile("d/run/tentativa-2/EXI/part-0.csv.gz", 1)]


def test_export_consistent_snapshot_gives_up(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check that export consistent snapshot gives up."""
    recent = T0 - timedelta(seconds=10)
    fake_bigquery(monkeypatch, [{"DET": recent}, {"EXI": recent}])
    with pytest.raises(RuntimeError, match="sem alterações"):
        bigquery.export_consistent_snapshot(
            snapshot_request(max_wait_seconds=60), report=ignore_message, sleep=ignore_seconds
        )
    old = T0 - timedelta(hours=1)
    fake_bigquery(monkeypatch, [{"DET": old}, {"EXI": old}, {"DET": T0}, {"EXI": old}])
    with pytest.raises(RuntimeError, match="1 extracts seguidos"):
        bigquery.export_consistent_snapshot(
            snapshot_request(max_attempts=1), report=ignore_message, sleep=ignore_seconds
        )


def test_inmemory_status_complete_and_description() -> None:
    """Check that inmemory status complete and description."""
    done = InMemoryStatus("BQLOAD_X_A", 6, 6, 0, 0)
    assert done.complete
    assert done.description == "BQLOAD_X_A: 6/6 segmento(s) no In-Memory, 0,00 GB por popular"
    assert not InMemoryStatus("T", 6, 5, 0, 0).complete
    assert not InMemoryStatus("T", 6, 6, 1024**3, 1).complete
    assert InMemoryStatus("T", 0, 0, 0, 0).complete


def test_definition_differences_lists_changed_missing_and_extra_columns() -> None:
    """Check that definition differences lists changed missing and extra columns."""
    expected = ['"A" DATE NOT NULL', '"B" VARCHAR2(14 BYTE) NOT NULL']
    assert definition_differences(expected, expected) == []
    assert definition_differences(['"A" VARCHAR2(4000 BYTE)'], expected) == [
        '"A" VARCHAR2(4000 BYTE) → "A" DATE NOT NULL',
        '(ausente) → "B" VARCHAR2(14 BYTE) NOT NULL',
    ]
    assert definition_differences([*expected, '"C" NUMBER'], expected) == ['"C" NUMBER → (ausente)']


def test_column_definitions_keep_types_and_not_null() -> None:
    """Check that column definitions keep types and not null."""
    assert column_definitions(TEMPLATE[:2]) == (
        '  "CPF_CNPJ_RESPONSAVEL" VARCHAR2(14 BYTE) NOT NULL,\n  "DATA_COMPETENCIA_MUNICIPIO" DATE NOT NULL'
    )


def test_build_control_file_handles_embedded_newlines_and_blanks() -> None:
    """Check that build control file handles embedded newlines and blanks."""
    control = build_control_file(
        "DESTINO", "BQLOAD_X", [LoaderField("ID", "CHAR(32)"), LoaderField("_EXTRA", "FILLER CHAR(4000)")]
    )
    lines = control.splitlines()
    assert lines[:5] == [
        "LOAD DATA",
        "CHARACTERSET AL32UTF8",
        "APPEND",
        "PRESERVE BLANKS",
        'INTO TABLE "DESTINO"."BQLOAD_X"',
    ]
    assert "FIELDS CSV WITH EMBEDDED TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'" in lines
    assert '  "ID" CHAR(32),' in lines
    assert '  "_EXTRA" FILLER CHAR(4000)' in lines


def test_split_round_robin_skips_empty_groups() -> None:
    """Check that split round robin skips empty groups."""
    assert split_round_robin(["a", "b", "c"], 2) == [["a", "c"], ["b"]]
    assert split_round_robin(["a"], 4) == [["a"]]
    assert split_round_robin([], 2) == []


GB = 1024**3


def test_format_helpers() -> None:
    """Check that format helpers."""
    assert format_count(67987860) == "67.987.860"
    assert format_size(int(1.5 * GB)) == "1,50 GB"
    assert format_duration(250) == "4m10s"
    assert format_duration(3725) == "1h02m05s"


def test_progress_percent_and_message_with_estimate() -> None:
    """Check that progress percent and message with estimate."""
    snapshot = ProgressSnapshot(files_done=30, files_total=120, bytes_done=GB, bytes_total=4 * GB, elapsed_seconds=300)
    assert progress_percent(snapshot) == 25.0
    assert format_progress("BQLOAD_X", snapshot) == (
        "Carga de BQLOAD_X: 30/120 arquivos, 1,00 GB de 4,00 GB (25,0%), 5m00s decorridos, término estimado em ~15m00s"
    )


def test_progress_message_before_start_and_after_all_files_sent() -> None:
    """Check that progress message before start and after all files sent."""
    start = ProgressSnapshot(files_done=0, files_total=10, bytes_done=0, bytes_total=GB, elapsed_seconds=1)
    assert format_progress("T", start).endswith("(0,0%), 0m01s decorridos, calculando estimativa")
    done = ProgressSnapshot(files_done=10, files_total=10, bytes_done=GB, bytes_total=GB, elapsed_seconds=60)
    assert progress_percent(done) == 100.0
    assert format_progress("T", done).endswith("aguardando o SQL*Loader concluir")
    empty = ProgressSnapshot(files_done=0, files_total=0, bytes_done=0, bytes_total=0, elapsed_seconds=0)
    assert progress_percent(empty) == 100.0


def test_counting_reader_tracks_bytes_read() -> None:
    """Check that counting reader tracks bytes read."""
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


def index(
    name: str,
    columns: Sequence[str],
    locality: str | None = "LOCAL",
    index_type: str = "NORMAL",
    unique: bool = False,
    tablespace: str | None = "DFEN_BIG_IDX",
    owner: str = "",
) -> IndexDefinition:
    return IndexDefinition(name, index_type, unique, tuple(columns), locality, tablespace, owner=owner)


def test_partitioning_from_dictionary_keeps_only_declared_partitions() -> None:
    """Check that partitioning from dictionary keeps only declared partitions."""
    partitioning = partitioning_from_dictionary(
        {"partitioning_type": "RANGE", "subpartitioning_type": "NONE", "interval": "NUMTOYMINTERVAL(1, 'MONTH') "},
        ["DATA_COMPETENCIA_MUNICIPIO"],
        [
            {
                "partition_name": "P_INITIAL",
                "high_value": f" {JAN} ",
                "tablespace_name": "DFEN_BIG_DATA",
                "interval": "NO",
            },
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


def test_index_from_dictionary_ignores_degree_and_status_in_comparison() -> None:
    """Check that index from dictionary ignores degree and status in comparison."""
    row: dict[str, object] = {
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


def test_plan_structure_renames_indexes_and_skips_materialized_view_snapshot_index() -> None:
    """Check that plan structure renames indexes and skips materialized view snapshot index."""
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
        (
            TableLayout(None, None, (index("IX", ["SYS_NC1$"], index_type="FUNCTION-BASED NORMAL"),)),
            NotImplementedError,
        ),
        (TableLayout(None, Partitioning("HASH", ("A",), None, ()), ()), NotImplementedError),
        (TableLayout(None, None, (index("IX", ["NN_ROWID"]),)), ValueError),
        (TableLayout(None, Partitioning("RANGE", ("NN_ROWID",), None, ()), ()), ValueError),
        (TableLayout(None, None, (index("I" * 124, ["A"]),)), ValueError),
    ],
)
def test_plan_structure_rejects_what_the_load_cannot_replicate(template: TableLayout, error: type[Exception]) -> None:
    """Check that plan structure rejects what the load cannot replicate."""
    with pytest.raises(error):
        plan_structure(template, ["A"], "BQLOAD_")


def test_storage_clause_copies_tablespace_and_partitions_without_inmemory() -> None:
    """Check that storage clause copies tablespace and partitions without inmemory."""
    assert storage_clause(TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH)) == (
        'TABLESPACE "DFEN_BIG_DATA"\n'
        + "NO INMEMORY\n"
        + 'PARTITION BY RANGE ("DATA_COMPETENCIA_MUNICIPIO") (\n'
        + f'  PARTITION "P_INITIAL" VALUES LESS THAN ({JAN}) TABLESPACE "DFEN_BIG_DATA",\n'
        + f'  PARTITION "P_202601" VALUES LESS THAN ({FEB}) TABLESPACE "DFEN_BIG_DATA"\n'
        + ")"
    )
    interval = Partitioning("RANGE", ("D",), "NUMTOYMINTERVAL(1, 'MONTH')", (TablePartition("P", JAN, None),))
    assert storage_clause(TableLayout(None, interval)) == (
        "NO INMEMORY\nPARTITION BY RANGE (\"D\") INTERVAL (NUMTOYMINTERVAL(1, 'MONTH')) (\n"
        + f'  PARTITION "P" VALUES LESS THAN ({JAN})\n)'
    )
    listed = Partitioning("LIST", ("UF",), None, (TablePartition("P_RJ", "'RJ'", None),))
    assert "PARTITION \"P_RJ\" VALUES ('RJ')" in storage_clause(TableLayout(None, listed))
    assert storage_clause(TableLayout(None, None)) == "NO INMEMORY"


INMEMORY_HIGH = "INMEMORY PRIORITY HIGH MEMCOMPRESS FOR QUERY HIGH DISTRIBUTE AUTO NO DUPLICATE"


def inmemory_row(
    inmemory: str | None = "ENABLED",
    priority: str | None = "HIGH",
    compression: str | None = "FOR QUERY HIGH",
    distribute: str | None = "AUTO",
    duplicate: str | None = "NO DUPLICATE",
) -> dict[str, object]:
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
def test_inmemory_from_dictionary_builds_the_create_table_clause(row: dict[str, object], expected: str | None) -> None:
    """Check that inmemory from dictionary builds the create table clause."""
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
def test_inmemory_from_dictionary_rejects_unknown_values(row: dict[str, object]) -> None:
    """Check that inmemory from dictionary rejects unknown values."""
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
def test_plan_structure_puts_every_loaded_table_in_memory(original_inmemory: str | None) -> None:
    """Check that plan structure puts every loaded table in memory."""
    template = TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH, inmemory=original_inmemory)
    assert plan_structure(template, ["DATA_COMPETENCIA_MUNICIPIO"], "BQLOAD_").layout.inmemory == INMEMORY_HIGH


def test_storage_clause_uses_inmemory_of_original() -> None:
    """Check that storage clause uses inmemory of original."""
    assert storage_clause(TableLayout("DFEN_BIG_DATA", None, inmemory=INMEMORY_HIGH)) == (
        f'TABLESPACE "DFEN_BIG_DATA"\n{INMEMORY_HIGH}'
    )


def test_layout_differences_detect_changed_inmemory() -> None:
    """Check that layout differences detect changed inmemory."""
    expected = TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH, inmemory=INMEMORY_HIGH)
    assert layout_differences(expected, expected) == []
    assert layout_differences(TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH), expected) == [
        f"inmemory: NO INMEMORY → {INMEMORY_HIGH}"
    ]


def test_index_statement_parts() -> None:
    """Check that index statement parts."""
    assert index_statement_parts(index("BQLOAD_IX", ["A", "B"]), 4) == {
        "kind": "",
        "columns": '"A", "B"',
        "options": 'TABLESPACE "DFEN_BIG_IDX" LOCAL PARALLEL 4',
    }
    unique = index("BQLOAD_UK", ["A"], locality=None, unique=True, tablespace=None)
    assert index_statement_parts(unique, 2) == {"kind": "UNIQUE", "columns": '"A"', "options": "PARALLEL 2"}
    assert index_statement_parts(index("BQLOAD_BM", ["A"], index_type="BITMAP"), 1)["kind"] == "BITMAP"


def test_layout_differences_detect_missing_partitioning_and_changed_partitions() -> None:
    """Check that layout differences detect missing partitioning and changed partitions."""
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
    with_interval = Partitioning(
        "RANGE", ("DATA_COMPETENCIA_MUNICIPIO",), "NUMTOYMINTERVAL(1, 'MONTH')", RANGE_BY_MONTH.partitions
    )
    assert layout_differences(TableLayout("DFEN_BIG_DATA", with_interval), expected) == [
        "particionamento: RANGE (DATA_COMPETENCIA_MUNICIPIO) INTERVAL NUMTOYMINTERVAL(1, 'MONTH') → "
        + "RANGE (DATA_COMPETENCIA_MUNICIPIO)"
    ]


def test_foreign_indexes_protects_indexes_the_pipeline_did_not_create() -> None:
    """Check that foreign indexes protects indexes the pipeline did not create."""
    ours = index("BQLOAD_IX", ["A"], owner="DFEN")
    assert foreign_indexes([ours], "DFEN") == []
    assert foreign_indexes(
        [ours, index("IX_MANUAL", ["A"], owner="DFEN"), index("BQLOAD_X", ["A"], owner="OUTRO")], "DFEN"
    ) == [
        "DFEN.IX_MANUAL",
        "OUTRO.BQLOAD_X",
    ]


MARKED = f"{MANAGED_TABLE_MARKER}: carga a partir de x"


type DbRow = tuple[object, ...]


@final
class FakeCursor:
    """Responde às consultas de dicionário usadas na troca e registra os comandos DDL."""

    def __init__(
        self,
        objects: dict[str, list[str]],
        comments: dict[str, str | None],
        synonyms: Sequence[tuple[str, str, str]] = (),
    ) -> None:
        """Store the arguments."""
        self.objects = objects
        self.comments = comments
        self.synonyms = synonyms
        self.ddl: list[str] = []
        self.rows: list[DbRow] = []
        self.description: list[tuple[str]] = []

    def __enter__(self) -> "FakeCursor":
        """Return the fake."""
        return self

    def __exit__(self, *args: object) -> bool:
        """Close the fake."""
        return False

    def execute(self, sql: str, binds: dict[str, str] | None = None) -> None:
        binds = binds or {}
        if "FROM all_objects" in sql:
            self.answer(["object_type"], [(kind,) for kind in self.objects.get(binds["object_name"], [])])
        elif "FROM all_tables t" in sql and "all_tab_comments" in sql:
            name = binds["table_name"]
            found: list[DbRow] = [(name, self.comments[name])] if name in self.comments else []
            self.answer(["table_name", "comments"], found)
        elif "session_privs" in sql:
            self.answer(["session_user", "privilege"], [("DFEN", "CREATE ANY SYNONYM")])
        elif "FROM all_synonyms" in sql:
            self.answer(["owner", "table_owner", "table_name"], list(self.synonyms))
        else:
            self.ddl.append(" ".join(sql.split()))

    def answer(self, names: list[str], rows: Sequence[DbRow]) -> None:
        self.description = [(name,) for name in names]
        self.rows = list(rows)

    def fetchone(self) -> DbRow | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[DbRow]:
        return self.rows


@final
class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        """Store the arguments."""
        self.fake_cursor = cursor

    def __enter__(self) -> "FakeConnection":
        """Return the fake."""
        return self

    def __exit__(self, *args: object) -> bool:
        """Close the fake."""
        return False

    def cursor(self) -> FakeCursor:
        return self.fake_cursor


def fake_database(
    monkeypatch: pytest.MonkeyPatch,
    objects: dict[str, list[str]],
    comments: dict[str, str | None],
    synonyms: Sequence[tuple[str, str, str]] = (),
) -> FakeCursor:
    cursor = FakeCursor(objects, comments, synonyms)

    def connect(config: object) -> FakeConnection:
        del config

        return FakeConnection(cursor)

    monkeypatch.setattr(slots_oracle, "connect", connect)

    return cursor


def slots_config() -> OracleConfig:
    return OracleConfig("DFEN", "x", "h", "1521", "S", "DFEN")


PLAN_A = SlotPlan("BQLOAD_X", None, "BQLOAD_X_A")


def test_swap_migrates_partitioned_legacy_table_after_repointing_consumers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check that swap migrates partitioned legacy table after repointing consumers."""
    cursor = fake_database(
        monkeypatch,
        objects={"BQLOAD_X": ["TABLE", "TABLE PARTITION"]},
        comments={"BQLOAD_X": MARKED, "BQLOAD_X_A": MARKED},
    )
    slots_oracle.swap_synonyms(slots_config(), PLAN_A)
    assert cursor.ddl == [
        'CREATE OR REPLACE SYNONYM "NFSE_SIGA"."BQLOAD_X" FOR "DFEN"."BQLOAD_X_A"',
        'CREATE OR REPLACE SYNONYM "NFSE_USER"."BQLOAD_X" FOR "DFEN"."BQLOAD_X_A"',
        'CREATE OR REPLACE SYNONYM "NFSE_OWNER"."BQLOAD_X" FOR "DFEN"."BQLOAD_X_A"',
        'DROP TABLE "DFEN"."BQLOAD_X" PURGE',
        'CREATE OR REPLACE SYNONYM "DFEN"."BQLOAD_X" FOR "DFEN"."BQLOAD_X_A"',
    ]


def test_swap_after_migration_only_repoints_synonyms(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check that swap after migration only repoints synonyms."""
    cursor = fake_database(monkeypatch, objects={"BQLOAD_X": ["SYNONYM"]}, comments={"BQLOAD_X_B": MARKED})
    slots_oracle.swap_synonyms(slots_config(), SlotPlan("BQLOAD_X", "BQLOAD_X_A", "BQLOAD_X_B"))
    assert [statement.split()[4] for statement in cursor.ddl] == [
        '"NFSE_SIGA"."BQLOAD_X"',
        '"NFSE_USER"."BQLOAD_X"',
        '"NFSE_OWNER"."BQLOAD_X"',
        '"DFEN"."BQLOAD_X"',
    ]


@pytest.mark.parametrize(
    ("objects", "comments", "message"),
    [
        ({"BQLOAD_X": ["TABLE"]}, {"BQLOAD_X": "tabela de outro sistema", "BQLOAD_X_A": MARKED}, "não foi criada"),
        ({"BQLOAD_X": ["VIEW"]}, {"BQLOAD_X_A": MARKED}, "impede criar o sinônimo"),
    ],
)
def test_swap_changes_nothing_when_the_name_cannot_become_a_synonym(
    monkeypatch: pytest.MonkeyPatch, objects: dict[str, list[str]], comments: dict[str, str | None], message: str
) -> None:
    """Check that swap changes nothing when the name cannot become a synonym."""
    cursor = fake_database(monkeypatch, objects=objects, comments=comments)
    with pytest.raises(PermissionError, match=message):
        slots_oracle.swap_synonyms(slots_config(), PLAN_A)
    assert cursor.ddl == []


def test_resolve_slots_stops_before_loading_when_legacy_table_is_not_managed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check that resolve slots stops before loading when legacy table is not managed."""
    cursor = fake_database(
        monkeypatch,
        objects={"BQLOAD_X": ["TABLE"]},
        comments={"BQLOAD_X": None},
        synonyms=[("NFSE_SIGA", "DFEN", "BQLOAD_X_B"), ("DFEN", "DFEN", "BQLOAD_X_A")],
    )
    with pytest.raises(PermissionError, match="não foi criada"):
        slots_oracle.resolve_slots(slots_config(), "BQLOAD_X")
    assert cursor.ddl == []


def test_refresh_synonyms_points_every_owner_to_the_active_table(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check that refresh synonyms points every owner to the active table."""
    cursor = fake_database(
        monkeypatch,
        objects={"BQLOAD_X": ["SYNONYM"]},
        comments={"BQLOAD_X_A": MARKED, "BQLOAD_X_B": MARKED},
        synonyms=[("DFEN", "DFEN", "BQLOAD_X_B"), ("NFSE_SIGA", "DFEN", "BQLOAD_X_B")],
    )
    actions = slots_oracle.refresh_synonyms(slots_config(), "BQLOAD_X")
    assert [statement for statement in cursor.ddl if statement.startswith("GRANT")] == [
        f'GRANT {privileges} ON "DFEN"."BQLOAD_X_B" TO "{grantee}"' for grantee, privileges in CONSUMER_GRANTS
    ]
    assert [statement for statement in cursor.ddl if statement.startswith("CREATE")] == [
        f'CREATE OR REPLACE SYNONYM "{owner}"."BQLOAD_X" FOR "DFEN"."BQLOAD_X_B"'
        for owner in ("NFSE_SIGA", "NFSE_USER", "NFSE_OWNER", "DFEN")
    ]
    assert actions[-1] == "DFEN.BQLOAD_X → BQLOAD_X_B"


@pytest.mark.parametrize(
    ("objects", "comments", "synonyms", "error"),
    [
        ({}, {}, [], LookupError),
        ({"BQLOAD_X": ["TABLE"]}, {"BQLOAD_X": MARKED}, [("NFSE_SIGA", "DFEN", "BQLOAD_X")], PermissionError),
    ],
)
def test_refresh_synonyms_changes_nothing_without_an_active_a_b_table(
    monkeypatch: pytest.MonkeyPatch,
    objects: dict[str, list[str]],
    comments: dict[str, str | None],
    synonyms: list[tuple[str, str, str]],
    error: type[Exception],
) -> None:
    """Check that refresh synonyms changes nothing without an active a b table."""
    cursor = fake_database(monkeypatch, objects=objects, comments=comments, synonyms=synonyms)
    with pytest.raises(error):
        slots_oracle.refresh_synonyms(slots_config(), "BQLOAD_X")
    assert cursor.ddl == []


NN_DETALHES_TEMPLATE = [
    ora("CHAVE_ACESSO", "VARCHAR2", 50, "B"),
    ora("PESSOA_EMITENTE", "RAW", 16),
    ora("PESSOA_PRESTADOR", "RAW", 16),
    ora("PESSOA_TOMADOR", "RAW", 16),
    ora("PESSOA_INTERMEDIARIO", "RAW", 16),
    ora("CPF_CNPJ_INTERMEDIARIO", "VARCHAR2", 14, "B", nullable=True),
    ora("DATA_VALIDACAO", "DATE", 7),
    ora("DATA_COMPETENCIA_MUNICIPIO", "DATE", 7),
    ora("STATUS_NOTA", "NUMBER", 22, precision=1, scale=0),
    ora("NOTA_NACIONAL", "NUMBER", 22, precision=15, scale=0),
    ora("NN_ROWID", "ROWID", 10, nullable=True),
    ora("DPS_ROWID", "ROWID", 10, nullable=True),
    ora("PE_ROWID", "ROWID", 10, nullable=True),
]
NN_DETALHES_BQ = [
    field("_bigquery_uid", "STRING"),
    field("chave_acesso", "STRING"),
    field("pessoa_emitente", "STRING"),
    field("pessoa_prestador", "STRING"),
    field("pessoa_tomador", "STRING"),
    field("pessoa_intermediario", "STRING"),
    field("cpf_cnpj_intermediario", "STRING"),
    field("data_validacao", "STRING"),
    field("data_competencia_municipio", "STRING"),
    field("status_nota", "NUMERIC"),
    field("nota_nacional", "NUMERIC"),
    field("_bigquery_particao_data", "DATE"),
    field("_bigquery_updated_at", "DATETIME"),
]
STATUS_EXPRESSIONS = (
    IndexExpression('CASE  WHEN "STATUS_NOTA"<>0 THEN "DATA_COMPETENCIA_MUNICIPIO" END '),
    IndexExpression('CASE  WHEN "STATUS_NOTA"<>0 THEN "NOTA_NACIONAL" END '),
)


def function_based_index(
    columns: Sequence[str] = STATUS_EXPRESSIONS, locality: str | None = "LOCAL"
) -> IndexDefinition:
    return index(
        "IX_MVT_NN_CANC_COMP_NN", columns, locality=locality, index_type="FUNCTION-BASED NORMAL", tablespace=None
    )


def test_raw_ddl_type_and_definition() -> None:
    """Check that raw ddl type and definition."""
    assert ora("PESSOA_EMITENTE", "RAW", 16).ddl_type == "RAW(16)"
    assert ora("PESSOA_EMITENTE", "RAW", 16).definition == '"PESSOA_EMITENTE" RAW(16) NOT NULL'
    assert ora("PESSOA_OPCIONAL", "RAW", 8, nullable=True).definition == '"PESSOA_OPCIONAL" RAW(8)'


def test_raw_loader_spec_decodes_base64_text() -> None:
    """Check that raw loader spec decodes base64 text."""
    assert loader_spec(ora("PESSOA_EMITENTE", "RAW", 16), "STRING") == (
        'CHAR(64) "CASE WHEN :PESSOA_EMITENTE IS NOT NULL THEN '
        + 'UTL_ENCODE.BASE64_DECODE(UTL_RAW.CAST_TO_RAW(:PESSOA_EMITENTE)) END"'
    )
    assert loader_spec(ora("BLOB_GRANDE", "RAW", 2000), "STRING").startswith("CHAR(2668) ")


@pytest.mark.parametrize(
    ("column", "bq_type"),
    [(ora("PESSOA", "RAW", 16), "BYTES"), (ora("PESSOA", "RAW", 16), "INTEGER"), (ora("_PESSOA", "RAW", 16), "STRING")],
)
def test_raw_loader_spec_rejects_other_sources_and_quoted_names(column: OracleColumn, bq_type: str) -> None:
    """Check that raw loader spec rejects other sources and quoted names."""
    with pytest.raises(NotImplementedError):
        loader_spec(column, bq_type)


def test_build_load_plan_for_notas_nacionais_detalhes_excludes_rowids_and_decodes_raw() -> None:
    """Check that build load plan for notas nacionais detalhes excludes rowids and decodes raw."""
    plan = build_load_plan(NN_DETALHES_BQ, NN_DETALHES_TEMPLATE)
    assert plan.excluded == ["NN_ROWID", "DPS_ROWID", "PE_ROWID"]
    assert plan.ignored == ["_BIGQUERY_UID", "_BIGQUERY_PARTICAO_DATA", "_BIGQUERY_UPDATED_AT"]
    specs = {loader.name: loader.spec for loader in plan.fields}
    assert specs["PESSOA_INTERMEDIARIO"] == (
        'CHAR(64) "CASE WHEN :PESSOA_INTERMEDIARIO IS NOT NULL THEN '
        + 'UTL_ENCODE.BASE64_DECODE(UTL_RAW.CAST_TO_RAW(:PESSOA_INTERMEDIARIO)) END"'
    )
    assert specs["DATA_VALIDACAO"] == 'DATE "YYYY-MM-DD\\"T\\"HH24:MI:SS"'
    assert specs["STATUS_NOTA"] == "CHAR(64)"
    assert specs["_BIGQUERY_UPDATED_AT"] == "FILLER CHAR(4000)"
    assert [column.definition for column in plan.columns][1] == '"PESSOA_EMITENTE" RAW(16) NOT NULL'
    control = build_control_file("DFEN", "BQLOAD_X_A", plan.fields)
    assert '  "PESSOA_TOMADOR" CHAR(64) "CASE WHEN :PESSOA_TOMADOR IS NOT NULL THEN ' in control
    assert '  "_BIGQUERY_PARTICAO_DATA" FILLER CHAR(4000),' in control


def test_index_key_reads_column_expression_and_descending_columns() -> None:
    """Check that index key reads column expression and descending columns."""
    assert index_key({"column_name": "A", "descend": "ASC", "column_expression": None}) == "A"
    row: dict[str, object] = {
        "column_name": "SYS_NC1$",
        "descend": "ASC",
        "column_expression": ' CASE WHEN "A"=1 THEN 1 END',
    }
    expression = index_key(row)
    assert isinstance(expression, IndexExpression)
    assert expression == 'CASE WHEN "A"=1 THEN 1 END'
    descending = index_key({"column_name": "SYS_NC2$", "descend": "DESC", "column_expression": '"B"'})
    assert isinstance(descending, IndexExpression)
    assert descending == '"B" DESC'


def test_index_from_dictionary_keeps_expressions_of_function_based_index() -> None:
    """Check that index from dictionary keeps expressions of function based index."""
    row: dict[str, object] = {
        "owner": "DFEN",
        "index_name": "IX_MVT_NN_CANC_COMP_NN",
        "index_type": "FUNCTION-BASED NORMAL",
        "uniqueness": "NONUNIQUE",
        "locality": "LOCAL",
        "tablespace_name": None,
        "degree": "1",
        "status": "N/A",
    }
    parsed = index_from_dictionary(row, list(STATUS_EXPRESSIONS))
    assert parsed == function_based_index()
    assert all(isinstance(key, IndexExpression) for key in parsed.columns)


LOADED = ["STATUS_NOTA", "DATA_COMPETENCIA_MUNICIPIO", "NOTA_NACIONAL", "CHAVE_ACESSO"]


def test_plan_structure_accepts_local_function_based_index_and_renames_it() -> None:
    """Check that plan structure accepts local function based index and renames it."""
    template = TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH, (function_based_index(),))
    plan = plan_structure(template, LOADED, "BQLOAD_")
    assert plan.layout.indexes == (replace(function_based_index(), name="BQLOAD_IX_MVT_NN_CANC_COMP_NN"),)
    (renamed,) = slot_indexes(plan.layout.indexes, "A")
    assert renamed.name == "BQLOAD_IX_MVT_NN_CANC_COMP_NN_A"
    assert all(isinstance(key, IndexExpression) for key in renamed.columns)
    long_name = "IX_MVTNND_RESPONSAVEL_DV_CA"
    long_template = TableLayout(None, None, (index(long_name, ["CHAVE_ACESSO"]),))
    (long_index,) = slot_indexes(plan_structure(long_template, LOADED, "BQLOAD_").layout.indexes, "B")
    assert long_index.name == "BQLOAD_IX_MVTNND_RESPONSAVEL_DV_CA_B"


def test_plan_structure_rejects_function_based_index_on_column_that_is_not_loaded() -> None:
    """Check that plan structure rejects function based index on column that is not loaded."""
    template = TableLayout("DFEN_BIG_DATA", RANGE_BY_MONTH, (function_based_index(),))
    with pytest.raises(ValueError, match="STATUS_NOTA"):
        plan_structure(template, ["DATA_COMPETENCIA_MUNICIPIO", "NOTA_NACIONAL"], "BQLOAD_")


@pytest.mark.parametrize(
    "unsupported",
    [
        function_based_index(locality="GLOBAL"),
        index("IX_BM", STATUS_EXPRESSIONS, index_type="FUNCTION-BASED BITMAP"),
        index("IX_DOM", ["A"], index_type="DOMAIN"),
        function_based_index(columns=("SYS_NC00001$",)),
    ],
)
def test_plan_structure_still_rejects_other_function_based_and_unknown_indexes(unsupported: IndexDefinition) -> None:
    """Check that plan structure still rejects other function based and unknown indexes."""
    with pytest.raises(NotImplementedError):
        plan_structure(TableLayout(None, None, (unsupported,)), LOADED, "BQLOAD_")


def test_index_statement_emits_expressions_verbatim_with_local_and_parallel() -> None:
    """Check that index statement emits expressions verbatim with local and parallel."""
    parts = index_statement_parts(function_based_index(), 4)
    assert parts == {
        "kind": "",
        "columns": 'CASE  WHEN "STATUS_NOTA"<>0 THEN "DATA_COMPETENCIA_MUNICIPIO" END , '
        + 'CASE  WHEN "STATUS_NOTA"<>0 THEN "NOTA_NACIONAL" END ',
        "options": "LOCAL PARALLEL 4",
    }
    statement = render_query(
        QUERIES_ANCHOR,
        "create_index",
        schema="DFEN",
        table="BQLOAD_MVT_NOTAS_NACIONAIS_DETALHES_A",
        index="BQLOAD_IX_MVT_NN_CANC_COMP_NN_A",
        **parts,
    )
    assert statement.startswith(
        'CREATE  INDEX "DFEN"."BQLOAD_IX_MVT_NN_CANC_COMP_NN_A" ON "DFEN"."BQLOAD_MVT_NOTAS_NACIONAIS_DETALHES_A" '
        + '(CASE  WHEN "STATUS_NOTA"<>0 THEN "DATA_COMPETENCIA_MUNICIPIO" END , '
        + 'CASE  WHEN "STATUS_NOTA"<>0 THEN "NOTA_NACIONAL" END )'
    )
    assert statement.rstrip().endswith("LOCAL PARALLEL 4")


def test_function_based_index_description_shows_expressions() -> None:
    """Check that function based index description shows expressions."""
    assert function_based_index().description.startswith('LOCAL FUNCTION-BASED NORMAL (CASE  WHEN "STATUS_NOTA"<>0')


@pytest.mark.parametrize(
    ("column", "expected"),
    [
        (ora("CPF", "VARCHAR2", 14, "B"), "CHAR(56)"),
        (ora("NOME", "VARCHAR2", 300, "C"), "CHAR(1200)"),
        (ora("GRANDE", "VARCHAR2", 1500, "B"), "CHAR(4000)"),
        (ora("MAXIMO", "VARCHAR2", 4000, "B"), "CHAR(4000)"),
        (ora("FIXO", "CHAR", 1, "B"), "CHAR(4)"),
        (ora("NACIONAL", "NVARCHAR2", 20, "C"), "CHAR(80)"),
    ],
)
def test_text_loader_spec_is_sized_from_the_column_with_utf8_headroom(column: OracleColumn, expected: str) -> None:
    """Check that text loader spec is sized from the column with utf8 headroom."""
    assert loader_spec(column, "STRING") == expected
    assert loader_spec(column, "JSON") == expected


def test_text_loader_spec_holds_a_max_length_multibyte_value_in_a_char_semantics_column() -> None:
    """Check that text loader spec holds a max length multibyte value in a char semantics column."""
    column = ora("NOME", "VARCHAR2", 1200, "C")  # VARCHAR2(300 CHAR) num banco AL32UTF8: 1200 bytes
    declared = int(loader_spec(column, "STRING").removeprefix("CHAR(").removesuffix(")"))
    assert declared >= len(("😀" * 300).encode()) == 1200


def test_date_loader_spec_is_native_without_sql_expression() -> None:
    """Check that date loader spec is native without sql expression."""
    assert loader_spec(ora("DATA", "DATE", 7), "STRING") == 'DATE "YYYY-MM-DD\\"T\\"HH24:MI:SS"'
    assert loader_spec(ora("_DATA", "DATE", 7), "STRING") == 'DATE "YYYY-MM-DD\\"T\\"HH24:MI:SS"'


@pytest.mark.parametrize(("length", "chars"), [(16, 32), (8, 16), (2000, 4000)])
def test_raw_hex_loader_spec_is_a_plain_char_field_sized_by_two_chars_per_byte(length: int, chars: int) -> None:
    """Check that raw hex loader spec is a plain char field sized by two chars per byte."""
    spec = loader_spec(ora("PESSOA", "RAW", length), "STRING", "hex")
    assert spec == f"CHAR({chars})"


def test_raw_hex_loader_spec_does_not_need_an_unquoted_name_and_base64_stays_the_default() -> None:
    """Check that raw hex loader spec does not need an unquoted name and base64 stays the default."""
    assert loader_spec(ora("_PESSOA", "RAW", 16), "STRING", "hex") == "CHAR(32)"
    assert loader_spec(ora("PESSOA", "RAW", 16), "STRING", "base64") == loader_spec(ora("PESSOA", "RAW", 16), "STRING")


def test_build_load_plan_propagates_raw_text_encoding_to_pessoa_columns_only() -> None:
    """Check that build load plan propagates raw text encoding to pessoa columns only."""
    hex_plan = build_load_plan(NN_DETALHES_BQ, NN_DETALHES_TEMPLATE, raw_text_encoding="hex")
    specs = {loader.name: loader.spec for loader in hex_plan.fields}
    assert [specs[name] for name in ("PESSOA_EMITENTE", "PESSOA_PRESTADOR", "PESSOA_TOMADOR")] == ["CHAR(32)"] * 3
    assert specs["PESSOA_INTERMEDIARIO"] == "CHAR(32)"
    assert specs["STATUS_NOTA"] == "CHAR(64)"
    control = build_control_file("DFEN", "BQLOAD_X_A", hex_plan.fields)
    assert "UTL_ENCODE" not in control
    assert '  "PESSOA_TOMADOR" CHAR(32),' in control


def test_count_rows_query_runs_parallel_with_the_requested_degree() -> None:
    """Check that count rows query runs parallel with the requested degree."""
    sql = render_query(QUERIES_ANCHOR, "count_rows", schema="DFEN", table="BQLOAD_X_A", degree=4)
    assert sql == 'SELECT /*+ PARALLEL(4) */ COUNT(*)\nFROM "DFEN"."BQLOAD_X_A"\n'


@final
class CountCursor:
    """A cursor that records the SQL and answers a count of 7."""

    def __init__(self, executed: list[str]) -> None:
        """Store the arguments."""
        self.executed = executed

    def __enter__(self) -> "CountCursor":
        """Return the fake."""
        return self

    def __exit__(self, *args: object) -> bool:
        """Close the fake."""
        return False

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchone(self) -> tuple[int]:
        return (7,)


@final
class CountConnection:
    """A connection with the cursor of `CountCursor`."""

    def __init__(self, executed: list[str]) -> None:
        """Store the arguments."""
        self.executed = executed

    def __enter__(self) -> "CountConnection":
        """Return the fake."""
        return self

    def __exit__(self, *args: object) -> bool:
        """Close the fake."""
        return False

    def cursor(self) -> CountCursor:
        return CountCursor(self.executed)


@pytest.mark.parametrize(("requested", "hinted"), [(4, 4), (1, 1), (0, 1), (-3, 1)])
def test_count_rows_sends_the_parallel_hint_to_oracle(
    monkeypatch: pytest.MonkeyPatch, requested: int, hinted: int
) -> None:
    """Check that count rows sends the parallel hint to oracle."""
    executed: list[str] = []

    def connect(config: object) -> CountConnection:
        del config

        return CountConnection(executed)

    config = oracle.OracleConfig("DFEN", "p", "h", "1", "s", "DFEN")
    monkeypatch.setattr(oracle_ddl, "connect", connect)
    assert oracle_ddl.count_rows(config, "BQLOAD_X_A", parallel_degree=requested) == 7
    assert executed == [f'SELECT /*+ PARALLEL({hinted}) */ COUNT(*)\nFROM "DFEN"."BQLOAD_X_A"\n']


@final
class FakeBatch:
    def __init__(self, log: list[str]) -> None:
        """Store the arguments."""
        self.log = log

    def __enter__(self) -> "FakeBatch":
        """Return the fake."""
        self.log.append("batch-open")
        return self

    def __exit__(self, *exc: object) -> bool:
        """Close the fake."""
        self.log.append("batch-close")
        return False


@final
class FakeBlob:
    def __init__(self, log: list[str], name: str) -> None:
        """Store the arguments."""
        self.log = log
        self.name = name

    def delete(self) -> None:
        self.log.append(self.name)


@final
class FakeBucket:
    def __init__(self, log: list[str]) -> None:
        """Store the arguments."""
        self.log = log

    def blob(self, blob_name: str) -> FakeBlob:
        return FakeBlob(self.log, blob_name)


@final
class FakeStorageClient:
    def __init__(self, log: list[str]) -> None:
        """Store the arguments."""
        self.log = log

    def batch(self, raise_exception: bool = True) -> FakeBatch:
        assert raise_exception is True
        return FakeBatch(self.log)

    def bucket(self, name: str) -> FakeBucket:
        del name

        return FakeBucket(self.log)


def fake_storage(monkeypatch: pytest.MonkeyPatch, log: list[str]) -> None:
    def client(project: str) -> FakeStorageClient:
        del project

        return FakeStorageClient(log)

    monkeypatch.setattr(storage, "Client", client)


def test_delete_blobs_removes_every_object_in_batches_of_at_most_100(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check that delete blobs removes every object in batches of at most 100."""
    log: list[str] = []
    fake_storage(monkeypatch, log)
    names = [f"p/part-{n}.csv.gz" for n in range(250)]
    bigquery.delete_blobs("proj", "bucket", names)
    batches: list[list[str]] = []
    current: list[str] = []
    for item in log:
        if item == "batch-open":
            current = []
        elif item == "batch-close":
            batches.append(current)
        else:
            current.append(item)
    assert [len(batch) for batch in batches] == [100, 100, 50]
    assert [name for batch in batches for name in batch] == names


def test_delete_blobs_with_no_objects_makes_no_request(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check that delete blobs with no objects makes no request."""
    log: list[str] = []
    fake_storage(monkeypatch, log)
    bigquery.delete_blobs("proj", "bucket", [])
    assert log == []

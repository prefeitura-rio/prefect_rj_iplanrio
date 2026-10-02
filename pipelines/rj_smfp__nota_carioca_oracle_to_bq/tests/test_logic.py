# ruff: noqa: PLR2004
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.chunks import Chunk, chunk_task_name
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import (
    ExtractOptions,
    ExtractRequest,
    build_jobs,
    collect_results,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.gcs import blob_prefix, delete_prefix
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import (
    MAX_JSON_SAFE_INTEGER,
    OracleConfig,
    Snapshot,
    secret_env_key,
    validate_identifier,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import (
    CountMismatchError,
    TablePlan,
    assert_counts_match,
    cluster_fields_for,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import (
    Progress,
    estimate_remaining_seconds,
    format_duration,
    format_progress,
    format_size,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.worker import ChunkResult
from prefect_rj_iplanrio.sql import load_query

SNAPSHOT = Snapshot(scn=123456789, taken_at=datetime(2026, 10, 1, tzinfo=UTC))
COLUMNS = (OracleColumn("DPS", "VARCHAR2", None, None), OracleColumn("DATA", "DATE", None, None))


def test_secret_env_key_follows_iplanrio_convention() -> None:
    assert secret_env_key("/db-oracle-nota-fiscal", "DB_HOST") == "DB_ORACLE_NOTA_FISCAL__DB_HOST"


@pytest.mark.parametrize("name", ["dps; drop table x", "1abc", "", "A" * 200])
def test_invalid_identifiers_are_rejected(name: str) -> None:
    with pytest.raises(ValueError, match="Identificador"):
        validate_identifier(name)


def test_sync_id_is_the_scn_and_rejects_values_json_cannot_hold() -> None:
    assert SNAPSHOT.sync_id == 123456789
    with pytest.raises(ValueError, match="SCN"):
        _ = Snapshot(scn=MAX_JSON_SAFE_INTEGER + 1, taken_at=SNAPSHOT.taken_at).sync_id


def test_chunk_task_name_is_unique_per_run_and_a_valid_identifier() -> None:
    first = chunk_task_name("DPS", "0b1f-aa")
    second = chunk_task_name("DPS", "0b1f-ab")

    assert first != second
    assert first == "O2BQ_DPS_0B1F_AA"
    assert validate_identifier(first) == first


def test_select_chunk_is_rendered_with_flashback_scn_and_rowid_binds() -> None:
    sql = load_query(QUERIES_ANCHOR, "select_chunk", columns='"DPS", "DATA"', schema="DFEN", table="DPS")

    assert 'SELECT "DPS", "DATA"' in sql
    assert "FROM DFEN.DPS AS OF SCN :scn" in sql
    assert "CHARTOROWID(:start_rowid) AND CHARTOROWID(:end_rowid)" in sql


def test_drop_default_ddl_targets_the_temp_table() -> None:
    sql = load_query(
        QUERIES_ANCHOR,
        "drop_column_default",
        project="p",
        dataset_id="d",
        table_id="T__oracle_to_bq_tmp",
        column="_airbyte_meta",
    )

    assert sql.strip() == "ALTER TABLE `p.d.T__oracle_to_bq_tmp` ALTER COLUMN _airbyte_meta DROP DEFAULT"


def test_cluster_fields_match_destination_contract() -> None:
    columns = (OracleColumn("NOTA_NACIONAL", "VARCHAR2", None, None),)

    assert cluster_fields_for("NOTAS_NACIONAIS", columns) == ("NOTA_NACIONAL", "_airbyte_extracted_at")
    with pytest.raises(ValueError, match="Sem cluster"):
        cluster_fields_for("OUTRA", columns)
    with pytest.raises(ValueError, match="não existe"):
        cluster_fields_for("DPS", columns)


def test_temp_table_never_collides_with_final_name() -> None:
    plan = TablePlan("DPS", "DFEN", COLUMNS, (), ("DPS",), None)

    assert plan.temp_id == "DPS__oracle_to_bq_tmp"


def test_count_check_requires_oracle_files_and_bigquery_to_agree() -> None:
    assert_counts_match("DPS", 10, 10, 10)
    for oracle, bq, extracted in [(10, 9, 10), (10, 10, 9), (11, 10, 10)]:
        with pytest.raises(CountMismatchError, match="não foi alterada"):
            assert_counts_match("DPS", oracle, bq, extracted)


def test_progress_math_and_formatting() -> None:
    progress = Progress(chunks_done=25, chunks_total=100, rows=500_000, bytes_written=3 * 1024**3, elapsed_seconds=100)

    assert estimate_remaining_seconds(progress) == 300
    line = format_progress("DPS", progress)
    assert "25/100 faixas (25.0%)" in line
    assert "5,000 linhas/s" in line
    assert "3.0 GB" in line
    assert "faltam ~5m00s" in line
    assert estimate_remaining_seconds(Progress(0, 10, 0, 0, 5)) is None
    assert "calculando" in format_progress("DPS", Progress(0, 10, 0, 0, 5))


def test_format_helpers() -> None:
    assert format_duration(3723) == "1h02m03s"
    assert format_duration(59) == "59s"
    assert format_size(512) == "512.0 B"
    assert format_size(2048) == "2.0 KB"


def test_build_jobs_name_one_parquet_per_chunk_inside_the_run_prefix() -> None:
    request = ExtractRequest(
        config=OracleConfig("u", "p", "h", "1521", "s", "DFEN"),
        schema="DFEN",
        table="DPS",
        columns=COLUMNS,
        snapshot=SNAPSHOT,
        project="proj",
        bucket="bucket",
        run_id="run-1",
        options=ExtractOptions(batch_rows=1234),
    )

    jobs = build_jobs(request, [Chunk(1, "AAA", "BBB"), Chunk(12, "CCC", "DDD")])

    assert [job.blob_name for job in jobs] == [
        "oracle_to_bq/DPS/run-1/chunk-000001.parquet",
        "oracle_to_bq/DPS/run-1/chunk-000012.parquet",
    ]
    assert {job.scn for job in jobs} == {SNAPSHOT.scn}
    assert {job.batch_rows for job in jobs} == {1234}
    assert 'SELECT "DPS", "DATA"' in jobs[0].sql


def test_delete_prefix_refuses_prefixes_wider_than_one_run() -> None:
    assert blob_prefix("oracle_to_bq", "DPS", "run-1") == "oracle_to_bq/DPS/run-1"
    with pytest.raises(ValueError, match="largo demais"):
        delete_prefix("proj", "bucket", "oracle_to_bq")


def request_with_interval(seconds: int) -> ExtractRequest:
    return ExtractRequest(
        config=OracleConfig("u", "p", "h", "1521", "s", "DFEN"),
        schema="DFEN",
        table="DPS",
        columns=COLUMNS,
        snapshot=SNAPSHOT,
        project="p",
        bucket="b",
        run_id="r",
        options=ExtractOptions(progress_interval_seconds=seconds),
    )


def test_collect_results_reports_progress_and_returns_every_chunk() -> None:
    lines: list[str] = []

    def work(index: int) -> ChunkResult:
        time.sleep(0.05)
        return ChunkResult(rows=index, bytes_written=index * 10)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(work, index) for index in range(1, 6)]
        results = collect_results(request_with_interval(0), futures, lines.append)

    assert sum(result.rows for result in results) == 15
    assert lines
    assert all(line.startswith("DPS: ") for line in lines)


def test_collect_results_raises_first_worker_failure() -> None:
    def work(index: int) -> ChunkResult:
        if index == 2:
            raise RuntimeError("ORA-01555")
        time.sleep(0.05)
        return ChunkResult(rows=1, bytes_written=1)

    with ThreadPoolExecutor(max_workers=1) as pool:
        futures = [pool.submit(work, index) for index in range(1, 5)]
        with pytest.raises(RuntimeError, match="ORA-01555"):
            collect_results(request_with_interval(60), futures, lambda _: None)

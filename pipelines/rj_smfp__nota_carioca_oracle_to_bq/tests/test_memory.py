# ruff: noqa: PLR2004
import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.memory import (
    MAIN_BASE_MB,
    MIB,
    MIN_BATCH_ROWS,
    TEXT_BYTES_PER_ROW,
    WORKER_BASE_MB,
    MemoryBudgetError,
    check_pod_budget,
    column_fetch_bytes,
    plan_worker_memory,
    row_fetch_bytes,
    worker_estimate_mb,
)


def varchar(length: int) -> OracleColumn:
    return OracleColumn("C", "VARCHAR2", None, None, data_length=length)


def dps_like_columns() -> tuple[OracleColumn, ...]:
    """Mesma composição de tipos da DFEN.DPS: 19 VARCHAR2 (2 de 2000), 5 RAW, 44 NUMBER e 3 DATE."""
    texts = [varchar(2000)] * 2 + [varchar(255)] * 2 + [varchar(50)] * 15
    raws = [OracleColumn("R", "RAW", None, None, data_length=16)] * 5
    numbers = [OracleColumn("N", "NUMBER", 17, 2, data_length=22)] * 44
    dates = [OracleColumn("D", "DATE", None, None, data_length=7)] * 3
    return (*texts, *raws, *numbers, *dates)


def test_text_and_raw_cost_the_same_floor_whatever_their_declared_size() -> None:
    assert column_fetch_bytes(varchar(2)) == TEXT_BYTES_PER_ROW
    assert column_fetch_bytes(varchar(2000)) == TEXT_BYTES_PER_ROW
    assert column_fetch_bytes(OracleColumn("R", "RAW", None, None, data_length=16)) == TEXT_BYTES_PER_ROW


def test_text_declared_wider_than_the_floor_costs_its_declared_bytes() -> None:
    assert column_fetch_bytes(varchar(32767)) == 32767


def test_number_and_date_are_cheaper_than_text() -> None:
    number = column_fetch_bytes(OracleColumn("N", "NUMBER", 17, 2))
    date = column_fetch_bytes(OracleColumn("D", "DATE", None, None))

    assert 0 < date < number < TEXT_BYTES_PER_ROW


def test_unsupported_type_is_refused_instead_of_estimated() -> None:
    with pytest.raises(NotImplementedError):
        column_fetch_bytes(OracleColumn("B", "BLOB", None, None))


def test_batch_is_the_largest_that_fits_the_worker_budget() -> None:
    columns = dps_like_columns()
    row_bytes = row_fetch_bytes(columns)

    memory = plan_worker_memory(columns, worker_memory_mb=1536, max_batch_rows=50_000)

    assert memory.row_bytes == row_bytes
    assert memory.batch_rows == (1536 - WORKER_BASE_MB) * MIB // row_bytes
    assert memory.worker_mb <= 1536
    assert worker_estimate_mb(row_bytes, memory.batch_rows + 1000) > 1536


def test_narrow_table_is_capped_by_max_batch_rows() -> None:
    columns = (OracleColumn("N", "NUMBER", 7, 0),)

    assert plan_worker_memory(columns, worker_memory_mb=1536, max_batch_rows=20_000).batch_rows == 20_000


def test_budget_below_the_worker_baseline_still_reads_the_minimum_batch() -> None:
    memory = plan_worker_memory(dps_like_columns(), worker_memory_mb=100, max_batch_rows=50_000)

    assert memory.batch_rows == MIN_BATCH_ROWS


def test_pod_budget_accepts_what_fits_and_reports_the_heaviest_table_when_it_does_not() -> None:
    assert check_pod_budget({"A": 1000}, workers=2, pod_memory_mb=MAIN_BASE_MB + 2000) == MAIN_BASE_MB + 2000
    with pytest.raises(MemoryBudgetError, match="PESSOAS"):
        check_pod_budget({"DPS": 1000, "PESSOAS": 1500}, workers=2, pod_memory_mb=MAIN_BASE_MB + 2999)


def test_new_defaults_fit_the_pod_for_the_widest_table() -> None:
    options = ExtractOptions()
    memory = plan_worker_memory(dps_like_columns(), options.worker_memory_mb, options.batch_rows)

    total = check_pod_budget({"DPS": memory.worker_mb}, options.workers, options.pod_memory_mb)

    assert total <= options.pod_memory_mb


def test_first_staging_configuration_would_have_been_refused_before_extraction() -> None:
    row_bytes = row_fetch_bytes(dps_like_columns())
    old_worker_mb = worker_estimate_mb(row_bytes, batch_rows=50_000)

    with pytest.raises(MemoryBudgetError):
        check_pod_budget({"DPS": old_worker_mb}, workers=8, pod_memory_mb=7168)


def test_defaults_fit_the_two_gib_pod_request_with_headroom_and_are_accepted() -> None:
    options = ExtractOptions()

    total = check_pod_budget({"DPS": options.worker_memory_mb}, options.workers, options.pod_memory_mb)

    assert (options.workers, options.worker_memory_mb, options.pod_memory_mb) == (2, 640, 1792)
    assert options.pod_memory_mb == 2048 - 256
    assert options.upload_concurrency == 2
    assert total == MAIN_BASE_MB + 2 * 640 == 1792


def test_three_workers_with_the_default_worker_memory_are_refused() -> None:
    options = ExtractOptions()

    with pytest.raises(MemoryBudgetError, match="request"):
        check_pod_budget({"PESSOAS": options.worker_memory_mb}, workers=3, pod_memory_mb=options.pod_memory_mb)


def sized_columns(texts: int, numbers: int, dates: int) -> tuple[OracleColumn, ...]:
    return (
        *[varchar(50)] * texts,
        *[OracleColumn("N", "NUMBER", 17, 2, data_length=22)] * numbers,
        *[OracleColumn("D", "DATE", None, None, data_length=7)] * dates,
    )


def test_real_table_row_sizes_get_a_batch_above_the_minimum_within_the_pod_budget() -> None:
    options = ExtractOptions()
    tables = {
        "DPS": (sized_columns(24, 44, 3), 128_256),
        "NOTAS_NACIONAIS": (sized_columns(20, 14, 6), 99_072),
    }

    estimates: dict[str, int] = {}
    for name, (columns, row_bytes) in tables.items():
        memory = plan_worker_memory(columns, options.worker_memory_mb, options.batch_rows)
        assert memory.row_bytes == row_bytes
        assert MIN_BATCH_ROWS < memory.batch_rows < 4000
        assert memory.worker_mb <= options.worker_memory_mb
        estimates[name] = memory.worker_mb

    assert check_pod_budget(estimates, options.workers, options.pod_memory_mb) <= options.pod_memory_mb

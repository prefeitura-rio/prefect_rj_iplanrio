import pytest

from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.measure import ChunkStages, TableBenchmark, VariantWrite
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.rates import (
    BenchTotals,
    TableRates,
    bottleneck,
    build_scenarios,
    estimate_seconds,
    fetch_efficiency,
    table_rates,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.scaling import ScalingRun, skip_reason
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.work import StageTiming


def timing(wall: float, cpu: float = 0.0) -> StageTiming:
    return StageTiming(wall, cpu)


def chunk(rows: int, fetch_wall: float, fetch_cpu: float, convert: float = 1.0, write: float = 1.0) -> ChunkStages:
    return ChunkStages(
        chunk_id=1,
        rows=rows,
        fetched_bytes=rows * 100,
        fetch=timing(fetch_wall, fetch_cpu),
        convert=timing(convert),
        converted_bytes=rows * 80,
        writes=(VariantWrite("zstd", timing(write), rows * 10),),
        upload_seconds=None,
        upload_bytes=0,
        peak_rss_mb=100.0,
    )


def totals(fetch_wall: float, fetch_cpu: float, convert: float, write: float, upload: float = 0.0) -> BenchTotals:
    return BenchTotals(1000, fetch_wall, fetch_cpu, convert, write, upload, 0, 0, 0)


def run(workers: int, mode: str, rows: int, work: float) -> ScalingRun:
    return ScalingRun(workers, mode, rows, rows * 10, work, work + 2, work * workers)


def test_bottleneck_is_client_cpu_when_fetch_dominates_with_high_cpu_ratio() -> None:
    label, evidence = bottleneck(totals(10, 9, 2, 2), None)

    assert label.startswith("CPU do cliente na leitura")
    assert "0.90" in evidence


def test_bottleneck_is_database_saturation_when_fetch_waits_and_does_not_scale() -> None:
    label, _ = bottleneck(totals(10, 1, 2, 2), 0.5)

    assert label.startswith("Capacidade do Oracle")


def test_bottleneck_is_connection_latency_when_fetch_waits_but_scales() -> None:
    label, _ = bottleneck(totals(10, 1, 2, 2), 0.95)

    assert label.startswith("Latência do Oracle")


@pytest.mark.parametrize(
    ("stages", "expected"),
    [
        (totals(1, 0.1, 5, 1), "conversão"),
        (totals(1, 0.1, 1, 5), "Parquet"),
        (totals(1, 0.1, 1, 1, 5), "GCS"),
    ],
)
def test_bottleneck_names_the_slowest_non_fetch_stage(stages: BenchTotals, expected: str) -> None:
    assert expected in bottleneck(stages, 1.0)[0]


def test_fetch_efficiency_is_parallel_rate_over_n_times_single_rate() -> None:
    benchmark = TableBenchmark("T", (chunk(1000, 10.0, 1.0),), ())
    from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.rates import bench_totals

    runs = [run(4, "só fetch", 4000, 20.0)]  # 200 rows/s with 4 workers; single is 100 rows/s

    assert fetch_efficiency(bench_totals(benchmark), runs) == pytest.approx(0.5)


def test_db_cap_applies_only_when_fetch_scaling_saturates() -> None:
    benchmark = TableBenchmark("T", (chunk(1000, 10.0, 1.0),), ())
    saturated = [run(1, "completo", 1000, 20.0), run(4, "só fetch", 4000, 20.0)]
    linear = [run(1, "completo", 1000, 20.0), run(4, "só fetch", 4000, 10.0)]

    assert table_rates("T", 1_000_000, benchmark, saturated).db_cap == pytest.approx(200.0)
    assert table_rates("T", 1_000_000, benchmark, linear).db_cap is None


def test_worker_rate_comes_from_the_smallest_complete_run() -> None:
    benchmark = TableBenchmark("T", (chunk(1000, 10.0, 1.0),), ())
    runs = [run(2, "completo", 2000, 10.0), run(1, "completo", 1000, 10.0)]

    rates = table_rates("T", 1_000_000, benchmark, runs)

    assert rates.worker_rate == pytest.approx(100.0)
    assert rates.bytes_per_row == pytest.approx(10.0)


def test_estimate_scales_linearly_and_respects_the_cap() -> None:
    free = TableRates("T", 1_000_000, 10.0, 100.0, None)
    capped = TableRates("T", 1_000_000, 10.0, 100.0, 300.0)

    assert estimate_seconds(free, 4) == pytest.approx(2500.0)
    assert estimate_seconds(capped, 8) == pytest.approx(1_000_000 / 300.0)


def test_estimate_is_unknown_without_rows_or_measured_rate() -> None:
    assert estimate_seconds(TableRates("T", None, 10.0, 100.0, None), 2) is None
    assert estimate_seconds(TableRates("T", 10, 10.0, None, None), 2) is None


def test_scenarios_use_max_for_parallel_pods_and_sum_for_sequential_tables() -> None:
    rates = [TableRates("A", 1000, 1.0, 10.0, None), TableRates("B", 3000, 1.0, 10.0, None)]

    scenarios = build_scenarios(rates)

    assert scenarios[0].total_seconds == pytest.approx(150.0)  # max(1000/20, 3000/20)
    assert scenarios[1].total_seconds == pytest.approx(100.0)  # 4000 / (4 * 10)
    assert [s.label.split(" ")[0] for s in scenarios[1:]] == ["4", "8", "16", "32"]


def test_scenario_total_is_unknown_when_a_table_cannot_be_estimated() -> None:
    rates = [TableRates("A", 1000, 1.0, 10.0, None), TableRates("B", None, None, None, None)]

    assert all(s.total_seconds is None for s in build_scenarios(rates))


def test_worker_count_is_skipped_when_estimate_exceeds_pod_budget() -> None:
    reason = skip_reason(4, 1700, 7680)

    assert reason is not None
    assert "512 + 4 x 1700 = 7,312" in reason
    assert skip_reason(2, 1700, 7680) is None
    assert skip_reason(64, 1700, None) is None

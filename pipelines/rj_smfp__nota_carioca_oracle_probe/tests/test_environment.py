import pytest

from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.environment import (
    effective_cpus,
    parse_cfs,
    parse_cpu_max,
    parse_memory_limit,
)


@pytest.mark.parametrize(
    ("text", "expected"),
    [("200000 100000\n", 2.0), ("150000 100000", 1.5), ("max 100000\n", None)],
)
def test_parse_cpu_max_v2(text: str, expected: float | None) -> None:
    assert parse_cpu_max(text) == expected


@pytest.mark.parametrize(("quota", "expected"), [("200000\n", 2.0), ("-1\n", None)])
def test_parse_cfs_v1(quota: str, expected: float | None) -> None:
    assert parse_cfs(quota, "100000\n") == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("8589934592\n", 8 * 1024**3),
        ("max\n", None),
        ("9223372036854771712\n", None),
    ],
)
def test_parse_memory_limit_handles_v1_v2_and_unlimited(text: str, expected: int | None) -> None:
    assert parse_memory_limit(text) == expected


def test_effective_cpus_is_the_smaller_of_affinity_and_quota() -> None:
    assert effective_cpus(16, 2.0) == 2.0
    assert effective_cpus(2, 4.0) == 2.0
    assert effective_cpus(4, None) == 4.0

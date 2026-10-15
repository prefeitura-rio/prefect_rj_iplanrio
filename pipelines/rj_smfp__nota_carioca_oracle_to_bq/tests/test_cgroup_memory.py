# ruff: noqa: PLR2004
from pathlib import Path

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.cgroup_memory import (
    UNLIMITED_BYTES,
    PodMemory,
    read_peak_bytes,
    read_pod_memory,
)

MIB = 1024 * 1024
GIB = 1024 * MIB


def write(root: Path, name: str, text: str) -> None:
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def v2(root: Path, current: int, limit: str, inactive_file: int | None = 0) -> None:
    write(root, "memory.current", f"{current}\n")
    write(root, "memory.max", f"{limit}\n")
    if inactive_file is not None:
        write(root, "memory.stat", f"anon 1\nfile 2\ninactive_file {inactive_file}\nactive_file 5\n")


def test_v2_usage_is_the_working_set_and_limit_comes_from_memory_max(tmp_path: Path) -> None:
    # Given a v2 cgroup using 4 GiB, of which 1 GiB is inactive file cache, under a 6 GiB limit
    v2(tmp_path, current=4 * GIB, limit=str(6 * GIB), inactive_file=1 * GIB)
    # When it is read
    memory = read_pod_memory(tmp_path)
    # Then the cache the kernel can drop is not counted as use
    assert memory == PodMemory(used_bytes=3 * GIB, limit_bytes=6 * GIB)
    assert memory.ratio == 0.5


def test_v2_max_means_no_limit(tmp_path: Path) -> None:
    # Given a v2 cgroup whose memory.max is "max"
    v2(tmp_path, current=2 * GIB, limit="max")
    # When it is read
    memory = read_pod_memory(tmp_path)
    # Then usage is known but there is no limit, so there is no ratio to compare
    assert memory == PodMemory(used_bytes=2 * GIB, limit_bytes=None)
    assert memory.ratio is None


def test_v2_without_memory_stat_counts_the_whole_cgroup_usage(tmp_path: Path) -> None:
    v2(tmp_path, current=2 * GIB, limit=str(4 * GIB), inactive_file=None)

    assert read_pod_memory(tmp_path) == PodMemory(2 * GIB, 4 * GIB)


def test_v2_inactive_file_larger_than_usage_never_yields_negative_use(tmp_path: Path) -> None:
    v2(tmp_path, current=1 * GIB, limit=str(4 * GIB), inactive_file=2 * GIB)

    assert read_pod_memory(tmp_path) == PodMemory(0, 4 * GIB)


def test_v1_reads_usage_limit_and_total_inactive_file(tmp_path: Path) -> None:
    # Given a v1 cgroup (memory controller in its own directory)
    write(tmp_path, "memory/memory.usage_in_bytes", f"{5 * GIB}\n")
    write(tmp_path, "memory/memory.limit_in_bytes", f"{8 * GIB}\n")
    write(tmp_path, "memory/memory.stat", f"cache 9\ninactive_file 1\ntotal_inactive_file {GIB}\n")
    # When it is read
    memory = read_pod_memory(tmp_path)
    # Then the hierarchical inactive cache is discounted
    assert memory == PodMemory(4 * GIB, 8 * GIB)


def test_v1_limit_near_two_to_the_sixty_third_means_no_limit(tmp_path: Path) -> None:
    # Given the value v1 reports for "no limit"
    write(tmp_path, "memory/memory.usage_in_bytes", f"{GIB}\n")
    write(tmp_path, "memory/memory.limit_in_bytes", "9223372036854771712\n")
    # When it is read
    memory = read_pod_memory(tmp_path)
    # Then it is treated as unlimited, like any absurdly large limit
    assert memory == PodMemory(GIB, None)
    write(tmp_path, "memory/memory.limit_in_bytes", f"{UNLIMITED_BYTES}\n")
    assert read_pod_memory(tmp_path) == PodMemory(GIB, None)


def test_v2_wins_when_both_layouts_exist(tmp_path: Path) -> None:
    v2(tmp_path, current=2 * GIB, limit=str(4 * GIB))
    write(tmp_path, "memory/memory.usage_in_bytes", f"{3 * GIB}\n")
    write(tmp_path, "memory/memory.limit_in_bytes", f"{9 * GIB}\n")

    assert read_pod_memory(tmp_path) == PodMemory(2 * GIB, 4 * GIB)


def test_missing_cgroup_files_mean_no_reading(tmp_path: Path) -> None:
    # Given a directory with no cgroup files (local development, or a nonexistent root)
    # Then there is nothing to read
    assert read_pod_memory(tmp_path) is None
    assert read_pod_memory(tmp_path / "does-not-exist") is None
    assert read_peak_bytes(tmp_path) is None


def test_garbage_in_the_cgroup_files_means_no_reading_or_no_limit(tmp_path: Path) -> None:
    write(tmp_path, "memory.current", "not a number\n")
    assert read_pod_memory(tmp_path) is None
    write(tmp_path, "memory.current", f"{GIB}\n")
    write(tmp_path, "memory.max", "banana\n")
    assert read_pod_memory(tmp_path) == PodMemory(GIB, None)


def test_peak_comes_from_memory_peak_in_v2_and_max_usage_in_v1(tmp_path: Path) -> None:
    v2_root = tmp_path / "v2"
    write(v2_root, "memory.peak", f"{3 * GIB}\n")
    v1_root = tmp_path / "v1"
    write(v1_root, "memory/memory.max_usage_in_bytes", f"{4 * GIB}\n")

    assert read_peak_bytes(v2_root) == 3 * GIB
    assert read_peak_bytes(v1_root) == 4 * GIB

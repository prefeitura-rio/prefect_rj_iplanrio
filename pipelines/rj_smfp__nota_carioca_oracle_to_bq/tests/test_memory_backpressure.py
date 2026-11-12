# ruff: noqa: ARG002, ARG005, PLR2004
import math
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime

import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import extract
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.cgroup_memory import PodMemory
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.chunks import Chunk, ChunkRequest
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions, ExtractRequest
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.memory_gate import GateSummary, MemoryGate
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig, Snapshot
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.scheduler import (
    PAUSED_POLL_SECONDS,
    ChunkRun,
    ChunkScheduler,
    Limits,
    run_chunks,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.worker import ChunkJob, ChunkResult

MIB = 1024 * 1024
LIMIT = 8192 * MIB
TAKEN_AT = datetime(2026, 10, 14, 4, 0, 0, tzinfo=UTC)
COLUMNS = (OracleColumn("DPS", "VARCHAR2", None, None),)


def make_job(chunk_id: int) -> ChunkJob:
    return ChunkJob(
        sql="select 1",
        chunk=Chunk(chunk_id, "A", "B"),
        scn=1,
        extracted_at=TAKEN_AT,
        columns=COLUMNS,
        blob_name=f"oracle_to_bq/DPS/run/chunk-{chunk_id:06d}.parquet",
        batch_rows=10,
    )


class Meter:
    """Leitor de memória falso: devolve o que o teste mandou, ou o que uma função do tempo decidir."""

    def __init__(self, ratio: float | None = None) -> None:
        self.ratio = ratio
        self.dynamic: Callable[[], float] | None = None

    def __call__(self) -> PodMemory | None:
        ratio = self.dynamic() if self.dynamic is not None else self.ratio
        return None if ratio is None else PodMemory(math.ceil(ratio * LIMIT), LIMIT)


class Manual:
    """Leituras que só terminam quando o teste manda: nenhuma thread, tudo determinístico."""

    def __init__(self) -> None:
        self.futures: dict[int, Future[ChunkResult]] = {}

    def submit(self, job: ChunkJob) -> Future[ChunkResult]:
        future: Future[ChunkResult] = Future()
        self.futures[job.chunk.chunk_id] = future
        return future

    def finish(self, scheduler: ChunkScheduler, chunk_id: int) -> None:
        self.futures[chunk_id].set_result(ChunkResult(rows=10, bytes_written=0))
        scheduler.handle(self.futures[chunk_id])


def make_scheduler(
    meter: Meter, manual: Manual, workers: int = 3, ratio: float | None = 0.85
) -> tuple[ChunkScheduler, list[str]]:
    lines: list[str] = []
    gate = MemoryGate(ratio, meter, lines.append)
    scheduler = ChunkScheduler(
        manual.submit, lambda job, result: None, Limits(workers, 1, 20, 30), ThreadPoolExecutor(max_workers=1), gate
    )
    return scheduler, lines


def test_no_new_chunk_starts_while_memory_is_high_and_the_queue_resumes_when_it_falls() -> None:
    # Given 3 workers busy on chunks 1-3, with 5 more chunks queued
    meter, manual = Meter(0.50), Manual()
    scheduler, lines = make_scheduler(meter, manual)
    queue = [make_job(index) for index in range(8, 0, -1)]
    scheduler.start_ready(queue)
    assert sorted(manual.futures) == [1, 2, 3]
    # When memory rises over the pause ratio and the first two reads finish
    meter.ratio = 0.90
    manual.finish(scheduler, 1)
    scheduler.start_ready(queue)
    manual.finish(scheduler, 2)
    scheduler.start_ready(queue)
    # Then no chunk was started although two workers are free
    assert sorted(manual.futures) == [1, 2, 3]
    assert scheduler.gate.paused
    # When the last read finishes with memory still high, nothing is in flight
    manual.finish(scheduler, 3)
    scheduler.start_ready(queue)
    # Then exactly one chunk is released so the extraction keeps moving
    assert sorted(manual.futures) == [1, 2, 3, 4]
    # When memory falls below the resume level
    meter.ratio = 0.70
    scheduler.start_ready(queue)
    # Then the workers are filled again
    assert sorted(manual.futures) == [1, 2, 3, 4, 5, 6]
    assert len(lines) == 2
    assert "pausando" in lines[0]
    assert "retomando" in lines[1]


def test_the_gate_only_gates_it_does_not_change_the_disk_and_worker_limits() -> None:
    # Given memory below the ratio, 2 workers and 5 queued chunks
    meter, manual = Meter(0.50), Manual()
    scheduler, _ = make_scheduler(meter, manual, workers=2)
    queue = [make_job(index) for index in range(5, 0, -1)]
    # When chunks are started
    scheduler.start_ready(queue)
    # Then it is the worker limit that holds the rest back
    assert sorted(manual.futures) == [1, 2]
    assert len(queue) == 3


def test_progress_carries_the_pod_memory_read_by_the_gate() -> None:
    # Given a pod at 4096 MiB of 8192 MiB
    meter = Meter(0.50)
    scheduler, _ = make_scheduler(meter, Manual())
    # When progress is built
    progress = scheduler.progress(total=10, elapsed=5.0)
    # Then it has the usage and the limit
    assert progress.memory_bytes == 4096 * MIB
    assert progress.memory_limit_bytes == LIMIT
    # And without a cgroup both are absent
    meter.ratio = None
    empty = scheduler.progress(total=10, elapsed=5.0)
    assert (empty.memory_bytes, empty.memory_limit_bytes) == (None, None)


def test_scheduler_polls_faster_than_the_progress_interval_only_while_paused() -> None:
    meter, manual = Meter(0.50), Manual()
    scheduler, _ = make_scheduler(meter, manual)
    assert scheduler.poll_seconds() == 30
    meter.ratio = 0.90
    scheduler.start_ready([])
    assert scheduler.poll_seconds() == PAUSED_POLL_SECONDS
    scheduler.limits = Limits(3, 1, 20, 0.5)
    assert scheduler.poll_seconds() == 0.5


class Reads:
    """Leituras em threads que medem quantas rodam ao mesmo tempo e se a memória estava alta ao iniciar."""

    def __init__(self, meter: Meter, seconds: float = 0.02) -> None:
        self.meter = meter
        self.seconds = seconds
        self.lock = threading.Lock()
        self.in_flight = 0
        self.max_in_flight = 0
        self.starts_while_high: list[int] = []
        self.pool = ThreadPoolExecutor(max_workers=6)

    def submit(self, job: ChunkJob) -> Future[ChunkResult]:
        with self.lock:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            if (reading := self.meter()) is not None and reading.ratio is not None and reading.ratio >= 0.85:
                self.starts_while_high.append(self.in_flight)
        return self.pool.submit(self.read, job)

    def read(self, job: ChunkJob) -> ChunkResult:
        time.sleep(self.seconds)
        with self.lock:
            self.in_flight -= 1
        return ChunkResult(rows=10, bytes_written=0)


def run_with(meter: Meter, reads: Reads, total: int, ratio: float | None = 0.85) -> tuple[ChunkRun, list[str]]:
    lines: list[str] = []
    run = run_chunks(
        [make_job(index) for index in range(1, total + 1)],
        reads.submit,
        lambda job, result: None,
        Limits(3, 1, 20, 0.01),
        lambda _: None,
        gate=MemoryGate(ratio, meter, lines.append),
    )
    reads.pool.shutdown()
    return run, lines


def test_extraction_never_deadlocks_when_memory_stays_high_it_runs_one_chunk_at_a_time() -> None:
    # Given memory that never goes down
    meter = Meter(0.95)
    reads = Reads(meter)
    # When 6 chunks are extracted with 3 workers
    run, lines = run_with(meter, reads, total=6)
    # Then every chunk is read, one at a time, and the pause was logged only once
    assert [result.rows for result in run.results] == [10] * 6
    assert reads.max_in_flight == 1
    assert run.memory.pauses == 1
    assert run.memory.forced_chunks == 6
    assert len(lines) == 1


def test_extraction_resumes_full_parallelism_when_memory_falls_during_the_run() -> None:
    # Given memory that is high for the first 0.1 s and then falls
    start = time.monotonic()
    meter = Meter()
    meter.dynamic = lambda: 0.95 if time.monotonic() - start < 0.1 else 0.40
    reads = Reads(meter)
    # When 30 chunks are extracted
    run, lines = run_with(meter, reads, total=30)
    # Then it paused once, was held to one chunk while high and ran in parallel afterwards
    assert len(run.results) == 30
    assert run.memory.pauses == 1
    assert run.memory.paused_seconds > 0.05
    assert set(reads.starts_while_high) == {1}
    assert reads.max_in_flight == 3
    assert [("pausando" in line, "retomando" in line) for line in lines] == [(True, False), (False, True)]


def test_without_a_gate_the_scheduler_neither_reads_memory_nor_pauses() -> None:
    reads = Reads(Meter(0.99))
    run = run_chunks(
        [make_job(index) for index in range(1, 7)],
        reads.submit,
        lambda job, result: None,
        Limits(3, 1, 20, 0.01),
        lambda _: None,
    )
    reads.pool.shutdown()

    assert run.memory == GateSummary()
    assert reads.max_in_flight == 3


def extract_request(memory_pause_ratio: float = 0.85) -> ExtractRequest:
    return ExtractRequest(
        config=OracleConfig("u", "p", "h", "1521", "s", "DFEN"),
        schema="DFEN",
        table="DPS",
        columns=COLUMNS,
        snapshot=Snapshot(scn=42, taken_at=TAKEN_AT),
        project="proj",
        bucket="bucket",
        run_id="run",
        options=ExtractOptions(workers=3, progress_interval_seconds=1, memory_pause_ratio=memory_pause_ratio),
    )


@pytest.fixture
def pool_of_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extract, "open_bucket", lambda _: object())
    monkeypatch.setattr(extract, "open_pool", lambda request, spool, jobs: ThreadPoolExecutor(max_workers=3))
    monkeypatch.setattr(extract, "process_chunk", lambda job: ChunkResult(rows=10, bytes_written=0))


@pytest.mark.usefixtures("pool_of_threads")
def test_run_extraction_gates_on_the_cgroup_read_with_the_option_and_names_the_table_in_the_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a pod at 90% and memory_pause_ratio 0.5
    monkeypatch.setattr(extract, "read_pod_memory", Meter(0.90))
    lines: list[str] = []
    # When the extraction runs
    run = extract.run_extraction(extract_request(memory_pause_ratio=0.5), [make_job(1), make_job(2)], lines.append)
    # Then it paused (and still finished), logging the table
    assert run.memory.pauses == 1
    assert len(run.results) == 2
    assert any(line.startswith("DPS: memória do pod em 7373 MiB de 8192 MiB (90%)") for line in lines)


@pytest.mark.usefixtures("pool_of_threads")
def test_run_extraction_with_ratio_zero_never_pauses_even_at_the_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(extract, "read_pod_memory", Meter(0.99))
    lines: list[str] = []

    run = extract.run_extraction(extract_request(memory_pause_ratio=0), [make_job(1), make_job(2)], lines.append)

    assert run.memory.pauses == 0
    assert not any("pausando" in line for line in lines)


def test_extraction_summary_reports_pauses_and_the_cgroup_peak(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given an extraction whose scheduler paused twice
    @contextmanager
    def fake_chunks(_: ChunkRequest) -> Iterator[list[Chunk]]:
        yield [Chunk(1, "A", "B")]

    summary = GateSummary(pauses=2, paused_seconds=12.0, forced_chunks=1, peak_used_bytes=5 * 1024**3)
    monkeypatch.setattr(extract, "rowid_chunks", fake_chunks)
    monkeypatch.setattr(
        extract, "run_extraction", lambda *args, **kwargs: ChunkRun([ChunkResult(10, 100)], 0, 0, summary)
    )
    monkeypatch.setattr(extract, "count_as_of_scn", lambda *args: 10)
    monkeypatch.setattr(extract, "read_peak_bytes", lambda: 6 * 1024**3)
    lines: list[str] = []
    # When the table is extracted
    extract.extract_table(extract_request(), lines.append)
    # Then the log has the memory summary with pauses and peaks
    assert (
        "DPS: memória do pod: pico de 5.0 GB, memory.peak 6.0 GB (com cache de arquivo); "
        "2 pausas por memória, 12 s no total, 1 faixas liberadas com a memória ainda alta"
    ) in lines

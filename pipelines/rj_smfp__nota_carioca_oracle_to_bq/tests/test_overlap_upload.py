# ruff: noqa: PLR2004
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import Executor, ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq import flow as flow_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import extract, load, oracle
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.checksum import ColumnChecksum
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.chunks import Chunk, ChunkRequest
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions, ExtractRequest, ExtractResult
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.load import Destination, validate_table
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig, Snapshot
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.overlap import BackgroundCount
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import CountMismatchError, TablePlan
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import Progress
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.scheduler import Limits, run_chunks
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import TableLayout
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.worker import ChunkJob, ChunkResult

TAKEN_AT = datetime(2026, 10, 7, 4, 0, 0, tzinfo=UTC)
SNAPSHOT = Snapshot(scn=42, taken_at=TAKEN_AT)
COLUMNS = (OracleColumn("DPS", "VARCHAR2", None, None), OracleColumn("DATA", "DATE", None, None))
ROWS_PER_CHUNK = 10


class FakeUploadError(RuntimeError):
    """Falha injetada no upload."""


class FakeReadError(RuntimeError):
    """Falha injetada na leitura."""


class FakeCountError(RuntimeError):
    """Falha injetada na contagem."""


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


class Spool:
    """Spool falso: lê criando arquivos, envia dormindo, e mede quantos arquivos coexistem no disco."""

    def __init__(self, directory: Path, upload_seconds: float = 0.0) -> None:
        self.directory = directory
        self.upload_seconds = upload_seconds
        self.lock = threading.Lock()
        self.max_on_disk = 0
        self.submitted: list[int] = []
        self.uploaded: list[int] = []
        self.fail_upload_at: int | None = None
        self.fail_read_at: int | None = None
        self.empty_chunks: frozenset[int] = frozenset()
        self.upload_gate: threading.Event | None = None

    def observe(self) -> None:
        with self.lock:
            self.max_on_disk = max(self.max_on_disk, len(list(self.directory.iterdir())))

    def read(self, job: ChunkJob) -> ChunkResult:
        with self.lock:
            self.submitted.append(job.chunk.chunk_id)
        time.sleep(0.01)
        if job.chunk.chunk_id == self.fail_read_at:
            raise FakeReadError(f"chunk {job.chunk.chunk_id}")
        if job.chunk.chunk_id in self.empty_chunks:
            return ChunkResult(rows=0, bytes_written=0)
        path = self.directory / f"chunk-{job.chunk.chunk_id:06d}.parquet"
        path.write_bytes(b"x" * 100)
        self.observe()
        return ChunkResult(rows=ROWS_PER_CHUNK, bytes_written=100, path=path)

    def send(self, job: ChunkJob, result: ChunkResult) -> None:
        if self.upload_gate is not None:
            assert self.upload_gate.wait(timeout=10)
        time.sleep(self.upload_seconds)
        with self.lock:
            number = len(self.uploaded) + 1
        if number == self.fail_upload_at:
            raise FakeUploadError(f"upload {number}")
        self.observe()
        with self.lock:
            self.uploaded.append(job.chunk.chunk_id)

    def upload(self, job: ChunkJob, result: ChunkResult) -> None:
        self.send(job, result)
        assert result.path is not None
        result.path.unlink()


def limits(workers: int = 2, uploads: int = 2, pending: int = 2, interval: float = 0.01) -> Limits:
    return Limits(workers, uploads, pending, interval)


def run_scheduler(
    spool: Spool, total: int, config: Limits, report: Callable[[Progress], None] | None = None
) -> extract.ChunkRun:
    with ThreadPoolExecutor(max_workers=config.workers) as readers:
        return run_chunks(
            [make_job(index) for index in range(1, total + 1)],
            lambda job: readers.submit(spool.read, job),
            spool.upload,
            config,
            report or (lambda _: None),
        )


def test_lazy_submission_never_exceeds_pending_limit_plus_workers_with_a_slow_uploader(tmp_path: Path) -> None:
    # Given a slow uploader, 3 workers and at most 2 local files
    spool = Spool(tmp_path, upload_seconds=0.05)
    # When 14 chunks are processed
    run = run_scheduler(spool, 14, limits(workers=3, uploads=1, pending=2))
    # Then the files on disk never passed the limit (nor the pending ones), everything was uploaded and the spool is empty
    assert run.max_pending_files <= 2
    assert run.max_local_files <= 2
    assert spool.max_on_disk <= 2
    assert sorted(spool.uploaded) == list(range(1, 15))
    assert list(tmp_path.iterdir()) == []
    assert [result.rows for result in run.results] == [ROWS_PER_CHUNK] * 14


def test_submission_stops_while_pending_uploads_reach_the_limit(tmp_path: Path) -> None:
    # Given uploads blocked, 2 workers and a limit of 2 local files
    spool = Spool(tmp_path)
    gate = threading.Event()
    spool.upload_gate = gate
    submitted_while_blocked: list[int] = []

    def report(progress: Progress) -> None:
        # the first report after both workers' files are waiting for upload: let the run keep going
        if progress.pending_files >= 1 and not gate.is_set():
            submitted_while_blocked.append(len(spool.submitted))
            gate.set()

    # When the 6 chunks are processed
    run_scheduler(spool, 6, limits(workers=2, uploads=1, pending=2), report)
    # Then only the first two chunks (one per worker) had been started while the upload was blocked
    assert submitted_while_blocked == [2]
    assert sorted(spool.uploaded) == [1, 2, 3, 4, 5, 6]


def test_chunk_counts_as_uploaded_only_after_its_upload_finishes(tmp_path: Path) -> None:
    # Given uploads that wait for the first progress report with a chunk already read
    spool = Spool(tmp_path)
    spool.upload_gate = threading.Event()
    seen: list[Progress] = []

    def report(progress: Progress) -> None:
        seen.append(progress)
        if progress.chunks_read >= 1:
            assert spool.upload_gate is not None
            spool.upload_gate.set()

    # When all 4 chunks are processed
    run_scheduler(spool, 4, limits(workers=2, uploads=2, pending=4), report)
    # Then the first report that sees a read chunk still shows none uploaded and a pending local file
    first_read = next(progress for progress in seen if progress.chunks_read >= 1)
    assert first_read.chunks_uploaded == 0
    assert first_read.pending_files >= 1
    assert first_read.bytes_uploaded == 0


def test_final_progress_state_counts_every_chunk_and_byte_uploaded(tmp_path: Path) -> None:
    # Given a normal run reported at every loop
    spool = Spool(tmp_path, upload_seconds=0.02)
    seen: list[Progress] = []
    # When it finishes
    run_scheduler(spool, 5, limits(workers=2, uploads=2, pending=3), seen.append)
    # Then uploads never outrun reads and the last report is consistent
    assert all(progress.chunks_uploaded <= progress.chunks_read for progress in seen)
    assert all(progress.bytes_uploaded == 100 * progress.chunks_uploaded for progress in seen)


def test_empty_chunk_produces_no_upload_and_counts_as_done(tmp_path: Path) -> None:
    # Given chunk 2 has no rows
    spool = Spool(tmp_path)
    spool.empty_chunks = frozenset({2})
    # When the run completes
    run = run_scheduler(spool, 3, limits())
    # Then only two files were uploaded and the empty chunk has no path
    assert sorted(spool.uploaded) == [1, 3]
    assert run.results[1].path is None
    assert [result.rows for result in run.results] == [ROWS_PER_CHUNK, 0, ROWS_PER_CHUNK]


def test_upload_failure_stops_submitting_and_propagates(tmp_path: Path) -> None:
    # Given the second upload fails, with a single worker and a single pending file
    spool = Spool(tmp_path, upload_seconds=0.01)
    spool.fail_upload_at = 2
    # When the run is executed
    with pytest.raises(FakeUploadError, match="upload 2"):
        run_scheduler(spool, 30, limits(workers=1, uploads=1, pending=1))
    # Then it did not keep reading the remaining chunks
    assert len(spool.submitted) < 10


def test_read_failure_propagates_after_threads_are_shut_down(tmp_path: Path) -> None:
    # Given chunk 3 fails to read
    spool = Spool(tmp_path)
    spool.fail_read_at = 3
    # When the run is executed
    with pytest.raises(FakeReadError, match="chunk 3"):
        run_scheduler(spool, 10, limits(workers=1, uploads=2, pending=2))
    # Then no upload thread is left running
    assert not [thread for thread in threading.enumerate() if thread.name.startswith("gcs-upload")]


class Env:
    """Ambiente falso de ``extract_table``: pool de threads no lugar de processos, bucket e Oracle falsos."""

    def __init__(self, spool_root: Path) -> None:
        self.spool_root = spool_root
        self.spool: Spool | None = None
        self.spool_dir: Path | None = None
        self.pool_closed = False
        self.count_finished = threading.Event()
        self.count_calls = 0
        self.count_rows = 3 * ROWS_PER_CHUNK
        self.count_fails = False
        self.count_seconds = 0.0
        self.uploads_before_failure: int | None = None

    def open_pool(self, request: ExtractRequest, spool_dir: Path, jobs: int) -> Executor:
        self.spool_dir = spool_dir
        self.spool = Spool(spool_dir)
        self.spool.fail_upload_at = self.uploads_before_failure
        env = self

        class ClosingPool(ThreadPoolExecutor):
            def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
                super().shutdown(wait=wait, cancel_futures=cancel_futures)
                env.pool_closed = True

        return ClosingPool(max_workers=min(request.options.workers, jobs))

    def process(self, job: ChunkJob) -> ChunkResult:
        assert self.spool is not None
        return self.spool.read(job)

    def upload(self, _: object, path: Path, blob_name: str) -> None:
        assert self.spool is not None
        job_id = int(blob_name.rsplit("-", 1)[1].split(".")[0])
        self.spool.send(make_job(job_id), ChunkResult(rows=1, bytes_written=1, path=path))

    def count(self, config: OracleConfig, schema: str, table: str, snapshot: Snapshot) -> int:
        self.count_calls += 1
        time.sleep(self.count_seconds)
        self.count_finished.set()
        if self.count_fails:
            raise FakeCountError("ORA-00942")
        return self.count_rows


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Env:
    fake = Env(tmp_path)

    @contextmanager
    def fake_chunks(_: ChunkRequest) -> Iterator[list[Chunk]]:
        yield [Chunk(1, "A", "B"), Chunk(2, "C", "D"), Chunk(3, "E", "F")]

    monkeypatch.setattr(extract, "rowid_chunks", fake_chunks)
    monkeypatch.setattr(extract, "open_pool", fake.open_pool)
    monkeypatch.setattr(extract, "open_bucket", lambda _: object())
    monkeypatch.setattr(extract, "process_chunk", fake.process)
    monkeypatch.setattr(extract, "upload_file", fake.upload)
    monkeypatch.setattr(extract, "count_as_of_scn", fake.count)
    return fake


def extract_request(**options: int) -> ExtractRequest:
    return ExtractRequest(
        config=OracleConfig("u", "p", "h", "1521", "s", "DFEN"),
        schema="DFEN",
        table="DPS",
        columns=COLUMNS,
        snapshot=SNAPSHOT,
        project="proj",
        bucket="bucket",
        run_id="run",
        options=ExtractOptions(progress_interval_seconds=1, **options),
    )


def test_extract_table_returns_totals_with_the_overlapped_oracle_count_and_empties_the_spool(env: Env) -> None:
    # Given a count that takes a while, in parallel with extraction
    env.count_seconds = 0.2
    lines: list[str] = []
    # When the table is extracted
    result = extract.extract_table(extract_request(workers=2), lines.append)
    # Then the totals cover files written and uploaded, plus the count from the background thread
    assert (result.rows, result.chunks, result.files, result.bytes_written) == (30, 3, 3, 300)
    assert result.oracle_rows == 30
    assert env.count_calls == 1
    assert env.spool_dir is not None
    assert not env.spool_dir.exists()
    assert any("contagem no Oracle concluída" in line for line in lines)
    assert any("contagem do Oracle levou" in line for line in lines)


def test_count_runs_while_the_chunks_are_still_being_processed(env: Env, monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a count that finishes quickly and chunks that are read slowly
    observed: list[bool] = []

    def slow_process(job: ChunkJob) -> ChunkResult:
        observed.append(env.count_finished.wait(timeout=5))
        return env.process(job)

    monkeypatch.setattr(extract, "process_chunk", slow_process)
    # When the table is extracted
    extract.extract_table(extract_request(workers=1), lambda _: None)
    # Then the count ended before the first chunk finished being read
    assert observed == [True, True, True]


def test_count_failure_is_raised_after_workers_shut_down_and_spool_is_gone(env: Env) -> None:
    # Given a count that fails
    env.count_fails = True
    # When the table is extracted
    with pytest.raises(FakeCountError, match="ORA-00942"):
        extract.extract_table(extract_request(), lambda _: None)
    # Then the pool was already closed and the spool removed
    assert env.pool_closed
    assert env.spool_dir is not None
    assert not env.spool_dir.exists()
    assert not [thread for thread in threading.enumerate() if thread.name.startswith("count-")]


def test_upload_failure_cleans_spool_waits_for_the_count_and_propagates(env: Env) -> None:
    # Given the second upload fails while the count is still running
    env.uploads_before_failure = 2
    env.count_seconds = 0.3
    # When the table is extracted
    with pytest.raises(FakeUploadError, match="upload 2"):
        extract.extract_table(extract_request(workers=1, max_pending_files=1, upload_concurrency=1), lambda _: None)
    # Then pool and count thread are done and the spool directory is gone
    assert env.pool_closed
    assert env.count_finished.is_set()
    assert env.spool_dir is not None
    assert not env.spool_dir.exists()


def test_extraction_failure_takes_precedence_over_a_count_failure(env: Env) -> None:
    # Given both the upload and the count fail
    env.uploads_before_failure = 1
    env.count_fails = True
    # When the table is extracted
    with pytest.raises(FakeUploadError):
        extract.extract_table(extract_request(), lambda _: None)


def test_background_count_returns_rows_and_duration_and_logs_when_done() -> None:
    # Given a count blocked until the caller proceeds, which proves it runs in its own thread
    release = threading.Event()
    lines: list[str] = []

    def count() -> int:
        assert release.wait(timeout=5)
        return 123

    background = BackgroundCount("DPS", count, lines.append)
    background.start()
    assert lines == []
    release.set()
    # When the result is collected
    outcome = background.result()
    # Then rows come through and the finish was logged with its duration
    assert outcome.rows == 123
    assert outcome.seconds >= 0
    assert len(lines) == 1
    assert "123" in lines[0]


def test_background_count_error_is_reraised_to_the_caller() -> None:
    def count() -> int:
        raise FakeCountError("ORA-01555")

    background = BackgroundCount("DPS", count, lambda _: None)
    background.start()
    with pytest.raises(FakeCountError, match="ORA-01555"):
        background.result()


def plan_for_dps() -> TablePlan:
    return TablePlan("DPS", "DFEN", COLUMNS, (), TableLayout("DAY", "_airbyte_extracted_at", ("DPS",)), None, ("N",))


def extracted_result(rows: int, oracle_rows: int) -> ExtractResult:
    return ExtractResult(
        table="DPS",
        rows=rows,
        bytes_written=1,
        chunks=1,
        files=1,
        prefix="oracle_to_bq/DPS/run",
        seconds=1.0,
        checksums={"N": ColumnChecksum(count=rows, total="5")},
        oracle_rows=oracle_rows,
        max_pending_files=0,
        max_local_files=0,
    )


class NoSecondOracleCall(RuntimeError):
    """O Oracle foi consultado de novo durante a validação."""


def refuse_oracle(*_: object) -> None:
    raise NoSecondOracleCall


@pytest.fixture
def bigquery_with(monkeypatch: pytest.MonkeyPatch) -> Callable[[int, ColumnChecksum], None]:
    def install(rows: int, checksum: ColumnChecksum) -> None:
        monkeypatch.setattr(load.bigquery, "count_rows", lambda *_: rows)
        monkeypatch.setattr(load.bigquery, "read_checksums", lambda *_: {"N": checksum})

    monkeypatch.setattr(oracle, "connect", refuse_oracle)
    monkeypatch.setattr(oracle, "count_as_of_scn", refuse_oracle)
    return install


def test_validate_table_uses_the_extracted_oracle_count_without_querying_oracle(
    bigquery_with: Callable[[int, ColumnChecksum], None],
) -> None:
    # Given a BigQuery table equal to the extraction and to the overlapped Oracle count
    bigquery_with(30, ColumnChecksum(count=30, total="5"))
    destination = Destination("proj", "ds", "bucket")
    # When it is validated
    rows = validate_table(destination, plan_for_dps(), extracted_result(rows=30, oracle_rows=30))
    # Then it passes and Oracle was never touched (any call would raise NoSecondOracleCall)
    assert rows == 30


def test_validate_table_still_fails_when_the_oracle_count_differs(
    bigquery_with: Callable[[int, ColumnChecksum], None],
) -> None:
    # Given the extraction and BigQuery agree but the Oracle count is higher
    bigquery_with(30, ColumnChecksum(count=30, total="5"))
    # When it is validated
    with pytest.raises(CountMismatchError):
        validate_table(Destination("proj", "ds", "bucket"), plan_for_dps(), extracted_result(rows=30, oracle_rows=31))


def test_max_pending_files_defaults_to_four_and_rejects_less_than_one() -> None:
    assert ExtractOptions().max_pending_files == 4
    for value in (0, -3):
        with pytest.raises(ValueError, match="max_pending_files"):
            ExtractOptions(max_pending_files=value)


def test_parent_passes_max_pending_files_to_children(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a parent flow with tasks replaced by fakes
    captured: dict[str, object] = {}

    class Stop(Exception):
        """Interrompe o flow quando os filhos seriam lançados."""

    def fake_launch(table_ids: list[str], snapshot: Snapshot, passthrough: dict[str, object]) -> None:
        captured.update(passthrough)
        raise Stop

    def ignore(**_: object) -> None:
        return None

    def fake_plan(**kwargs: object) -> object:
        return type("FakePlan", (), {"table_id": str(kwargs["table_id"])})()

    for name, fake in {
        "rename_current_flow_run_task": ignore,
        "inject_bd_credentials_task": ignore,
        "ensure_exclusive_task": ignore,
        "drop_leftover_chunk_tasks_task": ignore,
        "take_snapshot_task": lambda infisical_secret_path: SNAPSHOT,
        "plan_table_task": fake_plan,
        "check_memory_budget_task": lambda plans, options: options,
        "launch_children_task": fake_launch,
        "cleanup_task": ignore,
    }.items():
        monkeypatch.setattr(flow_module, name, fake)
    # When the parent runs up to the child launch
    with pytest.raises(Stop):
        flow_module.rj_smfp__nota_carioca_oracle_to_bq.fn(table_ids=["DPS"], max_pending_files=7)
    # Then the child parameters carry it
    assert captured["max_pending_files"] == 7

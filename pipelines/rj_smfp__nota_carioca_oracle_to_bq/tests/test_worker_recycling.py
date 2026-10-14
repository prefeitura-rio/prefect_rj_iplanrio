# ruff: noqa: ARG001, ARG005, PLR2004
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import oracledb
import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq import flow as flow_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import extract, worker
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.chunks import Chunk
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions, ExtractRequest
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig, Snapshot
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import TableRunContext
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.recycling import recycling_supported
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.worker import ChunkJob, ChunkResult, WorkerContext

TAKEN_AT = datetime(2026, 10, 14, 4, 0, 0, tzinfo=UTC)
SNAPSHOT = Snapshot(scn=42, taken_at=TAKEN_AT)
CONFIG = OracleConfig("u", "p", "h", "1521", "s", "DFEN")
COLUMNS = (OracleColumn("DPS", "VARCHAR2", None, None),)


def request(workers: int = 2, worker_max_tasks: int | None = 25) -> ExtractRequest:
    return ExtractRequest(
        config=CONFIG,
        schema="DFEN",
        table="DPS",
        columns=COLUMNS,
        snapshot=SNAPSHOT,
        project="proj",
        bucket="bucket",
        run_id="run",
        options=ExtractOptions(workers=workers, worker_max_tasks=worker_max_tasks),
    )


def make_job() -> ChunkJob:
    return ChunkJob("select 1", Chunk(1, "A", "B"), 1, TAKEN_AT, COLUMNS, "blob", 10)


# --- ExtractOptions -------------------------------------------------------------------------------------------------


def test_defaults_recycle_workers_every_25_chunks_and_pause_at_85_percent() -> None:
    options = ExtractOptions()

    assert options.worker_max_tasks == 25
    assert options.memory_pause_ratio == 0.85


@pytest.mark.parametrize("value", [-1, -25])
def test_negative_worker_max_tasks_is_rejected(value: int) -> None:
    with pytest.raises(ValueError, match="worker_max_tasks"):
        ExtractOptions(worker_max_tasks=value)


@pytest.mark.parametrize("value", [-0.1, 1.01, 85])
def test_memory_pause_ratio_outside_zero_to_one_is_rejected(value: float) -> None:
    with pytest.raises(ValueError, match="memory_pause_ratio"):
        ExtractOptions(memory_pause_ratio=value)


@pytest.mark.parametrize("value", [0, None])
def test_zero_and_none_are_valid_ways_to_turn_recycling_off(value: int | None) -> None:
    assert ExtractOptions(worker_max_tasks=value).worker_max_tasks == value


# --- open_pool ------------------------------------------------------------------------------------------------------


@dataclass
class RecordedPool:
    kwargs: dict[str, Any]


def record_pool(monkeypatch: pytest.MonkeyPatch) -> RecordedPool:
    recorded = RecordedPool({})

    def fake_executor(**kwargs: Any) -> object:
        recorded.kwargs = kwargs
        return object()

    monkeypatch.setattr(extract, "ProcessPoolExecutor", fake_executor)
    return recorded


def test_open_pool_recycles_workers_after_worker_max_tasks_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given the pool opened for an extraction with worker_max_tasks=7
    recorded = record_pool(monkeypatch)
    # When it is built
    extract.open_pool(request(workers=6, worker_max_tasks=7), Path("/spool"), jobs=100)
    # Then the executor recycles workers after 7 tasks, with spawn and the init that opens a connection per worker
    assert recorded.kwargs["max_tasks_per_child"] == 7
    assert recorded.kwargs["max_workers"] == 6
    assert recorded.kwargs["initializer"] is worker.init_worker
    assert recorded.kwargs["initargs"] == (CONFIG, Path("/spool"))
    assert recorded.kwargs["mp_context"].get_start_method() == "spawn"


@pytest.mark.parametrize("value", [0, None])
def test_open_pool_keeps_workers_for_the_whole_extraction_when_recycling_is_off(
    monkeypatch: pytest.MonkeyPatch, value: int | None
) -> None:
    recorded = record_pool(monkeypatch)

    extract.open_pool(request(worker_max_tasks=value), Path("/spool"), jobs=100)

    assert recorded.kwargs["max_tasks_per_child"] is None


# --- Python versions with the max_tasks_per_child deadlock (gh-115634) ------------------------------------------


@pytest.mark.parametrize(
    ("version", "supported"),
    [
        ((3, 12, 9), False),
        ((3, 13, 14), False),
        ((3, 13, 15), True),
        ((3, 13, 20), True),
        ((3, 14, 0), False),
        ((3, 15, 0), True),
    ],
)
def test_recycling_is_only_supported_on_pythons_with_the_deadlock_fix(
    version: tuple[int, int, int], supported: bool
) -> None:
    assert recycling_supported(version) is supported


def run_extraction_capturing_the_pool_request(
    monkeypatch: pytest.MonkeyPatch, supported: bool
) -> tuple[ExtractRequest, list[str]]:
    opened: list[ExtractRequest] = []

    def fake_open_pool(pool_request: ExtractRequest, spool: Path, jobs: int) -> ThreadPoolExecutor:
        opened.append(pool_request)
        return ThreadPoolExecutor(max_workers=1)

    monkeypatch.setattr(extract, "recycling_supported", lambda: supported)
    monkeypatch.setattr(extract, "open_bucket", lambda _: object())
    monkeypatch.setattr(extract, "open_pool", fake_open_pool)
    monkeypatch.setattr(extract, "process_chunk", lambda job: ChunkResult(rows=1, bytes_written=0))
    monkeypatch.setattr(extract, "read_pod_memory", lambda: None)
    lines: list[str] = []
    extract.run_extraction(request(worker_max_tasks=25), [make_job()], lines.append)
    return opened[0], lines


def test_extraction_on_a_python_with_the_deadlock_runs_without_recycling_and_says_so(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool_request, lines = run_extraction_capturing_the_pool_request(monkeypatch, supported=False)

    assert pool_request.options.worker_max_tasks is None
    assert any("worker_max_tasks=25 ignorado" in line and "gh-115634" in line for line in lines)


def test_extraction_on_a_fixed_python_keeps_recycling_without_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    pool_request, lines = run_extraction_capturing_the_pool_request(monkeypatch, supported=True)

    assert pool_request.options.worker_max_tasks == 25
    assert not any("ignorado" in line for line in lines)


# --- options through the flow ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class FakePlan:
    table_id: str


class StopFlowError(Exception):
    """Interrompe o flow assim que os filhos seriam lançados."""


def _ignore(**_: object) -> None:
    return None


def patch_flow(monkeypatch: pytest.MonkeyPatch, **replacements: Callable[..., object]) -> None:
    defaults: dict[str, Callable[..., object]] = {
        "rename_current_flow_run_task": _ignore,
        "inject_bd_credentials_task": _ignore,
        "ensure_exclusive_task": _ignore,
        "drop_leftover_chunk_tasks_task": _ignore,
        "take_snapshot_task": lambda infisical_secret_path: SNAPSHOT,
        "plan_table_task": lambda **kwargs: FakePlan(str(kwargs["table_id"])),
        "cleanup_task": _ignore,
    }
    for name, fake in {**defaults, **replacements}.items():
        monkeypatch.setattr(flow_module, name, fake)


def run_parent(
    monkeypatch: pytest.MonkeyPatch, worker_max_tasks: int | None = 25, memory_pause_ratio: float = 0.85
) -> tuple[dict[str, object], ExtractOptions]:
    captured: dict[str, object] = {}
    seen: list[ExtractOptions] = []

    def fake_launch(table_ids: list[str], snapshot: Snapshot, passthrough: dict[str, object]) -> None:
        captured.update(passthrough)
        raise StopFlowError

    def fake_budget(plans: list[FakePlan], options: ExtractOptions) -> ExtractOptions:
        seen.append(options)
        return options

    patch_flow(monkeypatch, launch_children_task=fake_launch, check_memory_budget_task=fake_budget)
    with pytest.raises(StopFlowError):
        flow_module.rj_smfp__nota_carioca_oracle_to_bq.fn(
            table_ids=["DPS"], worker_max_tasks=worker_max_tasks, memory_pause_ratio=memory_pause_ratio
        )
    return captured, seen[0]


def test_parent_passes_the_recycling_and_gate_parameters_to_the_children_and_its_own_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a parent run with custom recycling and gate parameters
    # When it launches the children
    passthrough, options = run_parent(monkeypatch, worker_max_tasks=9, memory_pause_ratio=0.7)
    # Then the children receive both, and the parent's own options carry them
    assert passthrough["worker_max_tasks"] == 9
    assert passthrough["memory_pause_ratio"] == 0.7
    assert (options.worker_max_tasks, options.memory_pause_ratio) == (9, 0.7)


def test_parent_passes_the_defaults_and_a_disabled_recycling_through(monkeypatch: pytest.MonkeyPatch) -> None:
    defaults, _ = run_parent(monkeypatch)
    disabled, options = run_parent(monkeypatch, worker_max_tasks=None)

    assert (defaults["worker_max_tasks"], defaults["memory_pause_ratio"]) == (25, 0.85)
    assert disabled["worker_max_tasks"] is None
    assert options.worker_max_tasks is None


def test_child_builds_its_extract_options_from_the_parameters_the_parent_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a child flow run launched with the parent's parameters
    contexts: list[TableRunContext] = []

    def fake_run_child(ctx: TableRunContext, source_schema: str | None, table_id: str, notify_progress: bool) -> None:
        contexts.append(ctx)

    patch_flow(monkeypatch, run_child=fake_run_child)
    # When the child runs
    flow_module.rj_smfp__nota_carioca_oracle_to_bq.fn(
        table_id="DPS",
        scn=42,
        snapshot_taken_at=TAKEN_AT.isoformat(),
        parent_run_id="parent",
        worker_max_tasks=3,
        memory_pause_ratio=0.7,
    )
    # Then the options used for extraction have them
    options = contexts[0].options
    assert (options.worker_max_tasks, options.memory_pause_ratio) == (3, 0.7)


# --- worker: closing the connection and releasing memory --------------------------------------------------------------


class SpyConnection:
    def __init__(self, error: oracledb.Error | None = None) -> None:
        self.error = error
        self.closed = 0

    def close(self) -> None:
        self.closed += 1
        if self.error is not None:
            raise self.error


@pytest.fixture
def fresh_worker(monkeypatch: pytest.MonkeyPatch) -> WorkerContext:
    context = WorkerContext()
    monkeypatch.setattr(worker, "WORKER", context)
    return context


def test_close_worker_closes_the_connection_once_and_forgets_it(
    monkeypatch: pytest.MonkeyPatch, fresh_worker: WorkerContext
) -> None:
    # Given a worker with an open connection
    connection = SpyConnection()
    monkeypatch.setattr(fresh_worker, "connection", connection)
    # When the worker closes twice (exit handlers may run more than once)
    worker.close_worker()
    worker.close_worker()
    # Then the connection was closed a single time
    assert connection.closed == 1
    assert fresh_worker.connection is None


def test_close_worker_swallows_a_failure_to_close_because_the_connection_is_already_lost(
    monkeypatch: pytest.MonkeyPatch, fresh_worker: WorkerContext
) -> None:
    connection = SpyConnection(oracledb.InterfaceError("DPY-1001: not connected"))
    monkeypatch.setattr(fresh_worker, "connection", connection)

    worker.close_worker()

    assert connection.closed == 1
    assert fresh_worker.connection is None


def test_init_worker_registers_the_close_hook_once_even_if_called_twice(
    monkeypatch: pytest.MonkeyPatch, fresh_worker: WorkerContext, tmp_path: Path
) -> None:
    # Given an init_worker that opens a fake connection and a spy on atexit
    registered: list[object] = []
    monkeypatch.setattr(worker, "connect", lambda config: SpyConnection())
    monkeypatch.setattr(worker.atexit, "register", registered.append)
    monkeypatch.setattr(
        worker.atexit, "unregister", lambda func: registered.remove(func) if func in registered else None
    )
    # When it runs twice in the same process
    worker.init_worker(CONFIG, tmp_path)
    worker.init_worker(CONFIG, tmp_path)
    # Then the close hook is registered once
    assert registered == [worker.close_worker]
    assert fresh_worker.spool_dir == tmp_path


class FakePool:
    def __init__(self, with_release: bool = True) -> None:
        self.released = 0
        if with_release:
            self.release_unused = self._release

    def _release(self) -> None:
        self.released += 1


def test_release_memory_asks_the_arrow_pool_to_return_its_free_blocks(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a spy on the Arrow memory pool
    pool = FakePool()
    monkeypatch.setattr(worker.pa, "default_memory_pool", lambda: pool)
    # When memory is released
    worker.release_memory()
    # Then the pool was asked once
    assert pool.released == 1


def test_release_memory_runs_on_the_real_arrow_pool() -> None:
    worker.release_memory()

    assert hasattr(worker.pa.default_memory_pool(), "release_unused")


def test_release_memory_tolerates_a_pyarrow_pool_without_release_unused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker.pa, "default_memory_pool", lambda: FakePool(with_release=False))

    worker.release_memory()


def test_process_chunk_releases_memory_after_each_chunk_and_returns_its_result(
    monkeypatch: pytest.MonkeyPatch, fresh_worker: WorkerContext, tmp_path: Path
) -> None:
    # Given an initialized worker whose chunk read is faked
    fresh_worker.config = CONFIG
    fresh_worker.spool_dir = tmp_path
    monkeypatch.setattr(fresh_worker, "connection", SpyConnection())
    events: list[str] = []
    expected = ChunkResult(rows=3, bytes_written=30)

    def fake_write(connection: object, spool_dir: Path, job: ChunkJob) -> ChunkResult:
        events.append("write")
        return expected

    monkeypatch.setattr(worker, "write_chunk", fake_write)
    monkeypatch.setattr(worker, "release_memory", lambda: events.append("release"))
    # When two chunks are processed
    first, second = worker.process_chunk(make_job()), worker.process_chunk(make_job())
    # Then memory is released after every chunk, once its result exists
    assert (first, second) == (expected, expected)
    assert events == ["write", "release", "write", "release"]


def test_process_chunk_releases_memory_once_after_a_reconnect(
    monkeypatch: pytest.MonkeyPatch, fresh_worker: WorkerContext, tmp_path: Path
) -> None:
    # Given a first read that loses the connection and a second that succeeds
    fresh_worker.config = CONFIG
    fresh_worker.spool_dir = tmp_path
    monkeypatch.setattr(fresh_worker, "connection", SpyConnection())
    monkeypatch.setattr(worker, "connect", lambda config: SpyConnection())
    events: list[str] = []
    outcomes: list[ChunkResult | oracledb.Error] = [oracledb.OperationalError("lost"), ChunkResult(1, 10)]

    def fake_write(connection: object, spool_dir: Path, job: ChunkJob) -> ChunkResult:
        outcome = outcomes.pop(0)
        events.append("write")
        if isinstance(outcome, oracledb.Error):
            raise outcome
        return outcome

    monkeypatch.setattr(worker, "write_chunk", fake_write)
    monkeypatch.setattr(worker, "release_memory", lambda: events.append("release"))
    # When the chunk is processed
    result = worker.process_chunk(make_job())
    # Then the retried read succeeded and memory was released after it only
    assert result == ChunkResult(1, 10)
    assert events == ["write", "write", "release"]

import re
from collections.abc import Callable
from datetime import UTC, datetime
from types import SimpleNamespace

import oracledb
import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq import dfen_task as dfen_task_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq import flow as flow_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import dfen as dfen_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.dfen import (
    DfenExtractionError,
    ExtractionStatus,
    RunningTask,
    check_no_extraction,
    classify_tasks,
    describe_blocking,
    describe_stale,
    read_running_tasks,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.discord_embed import RunStatus
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.notify import NotifierConfig, ParentNotifier
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.notify_view import ParentStage
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig, Snapshot
from prefect_rj_iplanrio.sql import load_query

HOUR = 3600
STARTED = datetime.fromisoformat("2026-10-20T12:00:00")  # DATE do Oracle: sem fuso
CUSTOM_STALE_HOURS = 2
SNAPSHOT = Snapshot(scn=1, taken_at=datetime(2026, 10, 20, 14, 0, 0, tzinfo=UTC))
NO_TABLE = "ORA-00942: table or view does not exist\nHelp: https://docs.oracle.com/error-help/db/ora-00942/"
NO_PRIVILEGE = "ORA-01031: insufficient privileges"


def task(task_id: int, kind: int = 46, running_seconds: int | None = HOUR) -> RunningTask:
    started = None if running_seconds is None else STARTED
    return RunningTask(task_id=task_id, kind=kind, started_at=started, running_seconds=running_seconds)


class Reports:
    """Coleta os avisos que a guarda publica."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def __call__(self, message: str) -> None:
        self.lines.append(message)


def failing_fetch(message: str) -> Callable[[], tuple[RunningTask, ...]]:
    def fetch() -> tuple[RunningTask, ...]:
        raise oracledb.DatabaseError(message)

    return fetch


# --- decisão: quem bloqueia e quem está travado --------------------------------------------------------------------


def test_recent_tasks_block_and_old_ones_are_stale() -> None:
    # Given a recent extraction, a post-processing and a task running for longer than the 6 h threshold
    recent, post, stuck = task(1, 46, HOUR), task(2, 48, 5 * HOUR), task(3, 46, 7 * HOUR)
    # When they are classified with the default threshold
    status = classify_tasks([recent, post, stuck], stale_hours=6)
    # Then the first two block the load and the old one is only stale
    assert status.blocking == (recent, post)
    assert status.stale == (stuck,)


def test_a_task_running_exactly_the_threshold_blocks_and_one_second_more_is_stale() -> None:
    # Given tasks running for exactly 6 h and for 6 h and 1 s
    on_limit, past_limit = task(1, running_seconds=6 * HOUR), task(2, running_seconds=6 * HOUR + 1)
    # When they are classified
    status = classify_tasks([on_limit, past_limit], stale_hours=6)
    # Then DATA_INICIO >= SYSDATE - 6/24 holds only for the first
    assert status.blocking == (on_limit,)
    assert status.stale == (past_limit,)


def test_a_larger_threshold_makes_the_same_task_block() -> None:
    # Given a task running for 8 h
    eight_hours = task(1, running_seconds=8 * HOUR)
    # When it is classified with 6 h and with 12 h
    # Then it is stale for the first and blocking for the second
    assert classify_tasks([eight_hours], stale_hours=6).blocking == ()
    assert classify_tasks([eight_hours], stale_hours=12).blocking == (eight_hours,)


def test_a_task_without_start_date_does_not_block() -> None:
    # Given a running task whose age cannot be known
    unknown = task(1, running_seconds=None)
    # When it is classified
    status = classify_tasks([unknown], stale_hours=6)
    # Then it is stale, like a row failing DATA_INICIO >= SYSDATE - 6/24
    assert (status.blocking, status.stale) == ((), (unknown,))


def test_no_running_tasks_means_nothing_to_report() -> None:
    assert classify_tasks([], stale_hours=6) == ExtractionStatus(blocking=(), stale=())


# --- mensagens ----------------------------------------------------------------------------------------------------


def test_refusal_message_names_each_task_its_kind_since_when_and_how_to_proceed() -> None:
    # Given an extraction (46) and a post-processing (48) in progress
    # When the refusal message is built
    message = describe_blocking([task(101, 46, HOUR + 2 * 60 + 3), task(102, 48, 90)])
    # Then it names both tasks, what each does, since when, and how to re-trigger or force
    assert "tarefa 101 (tipo 46, extração de documentos fiscais nacionais)" in message
    assert "tarefa 102 (tipo 48, pós-processamento das notas nacionais)" in message
    assert "desde 20/10/2026 12:00:00" in message
    assert "há 1h02m03s" in message
    assert "Rode a carga de novo depois que terminarem" in message
    assert "check_dfen_extraction=false" in message


def test_stale_warning_names_the_task_id_kind_and_start_time() -> None:
    # Given a task stuck for 30 h
    # When its warning is built
    warning = describe_stale(task(77, 48, 30 * HOUR), stale_hours=6)
    # Then it carries the id, the type and the start time, and says it was ignored
    assert "tarefa 77 (tipo 48" in warning
    assert "20/10/2026 12:00:00" in warning
    assert "ignorada" in warning


def test_task_without_start_date_is_described_without_a_time() -> None:
    assert "sem data de início" in describe_stale(task(5, running_seconds=None), stale_hours=6)


# --- a guarda: recusa, ignora travadas e falha aberta --------------------------------------------------------------


def test_guard_refuses_when_an_extraction_is_running() -> None:
    # Given a recent extraction in TAREFAS
    reports = Reports()
    # When the guard runs
    with pytest.raises(DfenExtractionError) as excinfo:
        check_no_extraction(lambda: (task(101, 46, 600),), 6, reports)
    # Then it refuses naming the task, before any snapshot
    assert "tarefa 101" in str(excinfo.value)
    assert reports.lines == []


def test_guard_lets_the_load_start_when_only_stale_tasks_exist_and_warns_about_each() -> None:
    # Given two stuck tasks and nothing recent
    reports = Reports()
    # When the guard runs
    stuck = (task(1, 46, 9 * HOUR), task(2, 48, 20 * HOUR))
    status = check_no_extraction(lambda: stuck, 6, reports)
    # Then it does not refuse, returns the verdict and warns once per stuck task
    assert status is not None
    assert status.blocking == ()
    assert len(reports.lines) == len(stuck)
    assert "tarefa 1 " in reports.lines[0]
    assert "tarefa 2 " in reports.lines[1]


def test_guard_warns_about_stale_tasks_even_when_it_refuses_for_another() -> None:
    # Given a stuck task and a recent one
    reports = Reports()
    # When the guard runs
    with pytest.raises(DfenExtractionError) as excinfo:
        check_no_extraction(lambda: (task(1, 46, 9 * HOUR), task(2, 48, 60)), 6, reports)
    # Then only the recent task is named in the refusal and the stuck one was warned about
    assert "tarefa 2" in str(excinfo.value)
    assert "tarefa 1 " not in str(excinfo.value)
    assert len(reports.lines) == 1
    assert "tarefa 1 " in reports.lines[0]


def test_guard_is_silent_and_passes_when_nothing_is_running() -> None:
    # Given an empty TAREFAS result
    reports = Reports()
    # When the guard runs
    status = check_no_extraction(lambda: (), 6, reports)
    # Then it passes without warnings
    assert status is not None
    assert (status.blocking, status.stale, reports.lines) == ((), (), [])


@pytest.mark.parametrize("message", [NO_TABLE, NO_PRIVILEGE])
def test_guard_fails_open_when_the_session_cannot_read_tarefas(message: str) -> None:
    # Given a session without privilege on NFSE_OWNER.TAREFAS
    reports = Reports()
    # When the guard runs
    status = check_no_extraction(failing_fetch(message), 6, reports)
    # Then it does not block, reports that it could not check, and tells who to ask for the GRANT
    assert status is None
    assert len(reports.lines) == 1
    assert message.splitlines()[0] in reports.lines[0]
    assert "GRANT SELECT ON NFSE_OWNER.TAREFAS" in reports.lines[0]


@pytest.mark.parametrize(
    "message", ["ORA-12541: TNS:no listener", "ORA-00060: deadlock detected", "DPY-6005: cannot connect"]
)
def test_guard_raises_any_other_oracle_error(message: str) -> None:
    # Given an Oracle error that is not a missing privilege
    reports = Reports()
    # When the guard runs, Then the error surfaces and nothing is silently skipped
    with pytest.raises(oracledb.DatabaseError, match=re.escape(message.partition(":")[0])):
        check_no_extraction(failing_fetch(message), 6, reports)
    assert reports.lines == []


def test_a_missing_privilege_code_in_a_later_line_is_not_a_fail_open() -> None:
    # Given an error whose first line is another ORA error that merely mentions ORA-00942 further down
    message = "ORA-04063: view has errors\nORA-00942: table or view does not exist"
    # When the guard runs, Then it raises instead of failing open
    with pytest.raises(oracledb.DatabaseError):
        check_no_extraction(failing_fetch(message), 6, Reports())


@pytest.mark.parametrize("hours", [0, -1])
def test_a_stale_threshold_below_one_hour_is_rejected_before_querying(hours: int) -> None:
    # Given an invalid threshold and a fetch that must not run
    def fetch() -> tuple[RunningTask, ...]:
        raise AssertionError("não deve consultar o Oracle")

    # When the guard runs, Then it is refused as a configuration error
    with pytest.raises(ValueError, match="dfen_extraction_stale_hours"):
        check_no_extraction(fetch, hours, Reports())


# --- leitura do Oracle ---------------------------------------------------------------------------------------------


class FakeCursor:
    """Cursor que registra a consulta e devolve as linhas de ``TAREFAS``."""

    description = tuple(
        SimpleNamespace(name=name.upper()) for name in ("tarefa", "tipo", "data_inicio", "segundos_em_execucao")
    )

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, dict[str, object]]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, sql: str, binds: dict[str, object]) -> None:
        self.executed.append((sql, binds))

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self._cursor


CONFIG = OracleConfig("u", "p", "h", "1521", "svc", "S")


def test_read_binds_every_variable_of_the_query_and_maps_the_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a session that returns an extraction with a start date and a task without one
    cursor = FakeCursor([(1001, 46, STARTED, 3723), (1002, 48, None, None)])
    monkeypatch.setattr(dfen_module, "connect", lambda _: FakeConnection(cursor))
    # When the running tasks are read
    tasks = read_running_tasks(CONFIG)
    # Then the rows become typed tasks
    assert tasks == (RunningTask(1001, 46, STARTED, 3723), RunningTask(1002, 48, None, None))
    # And the query received exactly the bind variables it declares (46 and 48, STATUS = 1), nothing interpolated
    (sql, binds), *_ = cursor.executed
    assert set(re.findall(r"(?<![\w:]):(\w+)", sql)) == set(binds)
    assert {binds["tipo_extracao"], binds["tipo_pos_processamento"]} == {46, 48}
    assert binds["status_em_execucao"] == 1
    assert sql == load_query(QUERIES_ANCHOR, "list_running_extractions")


# --- fiação no flow ------------------------------------------------------------------------------------------------


class LaunchStoppedError(Exception):
    """Interrompe o pai onde os filhos seriam lançados."""


class ParentHarness:
    """Troca as tasks do pai por fakes que registram a ordem das chamadas e o que receberam."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, guard_error: Exception | None = None) -> None:
        self.calls: list[str] = []
        self.guard_kwargs: dict[str, object] = {}
        self.child_passthrough: dict[str, object] = {}
        self.notifiers: list[ParentNotifier] = []
        self._guard_error = guard_error
        for name, fake in self._fakes().items():
            monkeypatch.setattr(flow_module, name, fake)
        original_create = ParentNotifier.create

        def recording_create(config: NotifierConfig) -> ParentNotifier:
            notifier = original_create(config)
            self.notifiers.append(notifier)
            return notifier

        monkeypatch.setattr(ParentNotifier, "create", staticmethod(recording_create))

    def _fakes(self) -> dict[str, object]:
        def record(name: str) -> object:
            def fake(**_: object) -> None:
                self.calls.append(name)

            return fake

        def guard(**kwargs: object) -> None:
            self.calls.append("check_dfen_extraction_task")
            self.guard_kwargs.update(kwargs)
            if self._guard_error is not None:
                raise self._guard_error

        def take_snapshot(**_: object) -> Snapshot:
            self.calls.append("take_snapshot_task")
            return SNAPSHOT

        def launch(*, passthrough: dict[str, object], **_: object) -> None:
            self.calls.append("launch_children_task")
            self.child_passthrough.update(passthrough)
            raise LaunchStoppedError

        return {
            "rename_current_flow_run_task": record("rename"),
            "inject_bd_credentials_task": record("inject"),
            "ensure_exclusive_task": record("ensure_exclusive_task"),
            "check_dfen_extraction_task": guard,
            "drop_leftover_chunk_tasks_task": record("drop_leftover_chunk_tasks_task"),
            "take_snapshot_task": take_snapshot,
            "plan_table_task": lambda **kwargs: SimpleNamespace(table_id=kwargs["table_id"]),
            "check_memory_budget_task": lambda **kwargs: kwargs["options"],
            "launch_children_task": launch,
            "cleanup_task": record("cleanup_task"),
        }


def run_parent(check_dfen_extraction: bool = True, dfen_extraction_stale_hours: int = 6) -> None:
    flow_module.rj_smfp__nota_carioca_oracle_to_bq.fn(
        table_ids=["DPS"],
        discord_notifications=False,
        check_dfen_extraction=check_dfen_extraction,
        dfen_extraction_stale_hours=dfen_extraction_stale_hours,
    )


def test_parent_checks_the_extraction_after_the_exclusion_check_and_before_any_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given a parent run with the default parameters
    harness = ParentHarness(monkeypatch)
    # When it runs up to the child launch
    with pytest.raises(LaunchStoppedError):
        run_parent()
    # Then the guard runs right after the mutual exclusion and before the leftover cleanup and the snapshot
    assert harness.calls == [
        "rename",
        "inject",
        "ensure_exclusive_task",
        "check_dfen_extraction_task",
        "drop_leftover_chunk_tasks_task",
        "take_snapshot_task",
        "launch_children_task",
        "cleanup_task",
    ]
    # And it is enabled with the 6 h threshold and the pipeline's own Oracle secret
    assert harness.guard_kwargs == {
        "infisical_secret_path": "/db-oracle-nota-fiscal",
        "enabled": True,
        "stale_hours": 6,
    }


def test_operator_can_bypass_the_guard_and_tune_the_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a parent run with the guard off and a custom threshold
    harness = ParentHarness(monkeypatch)
    # When it runs
    with pytest.raises(LaunchStoppedError):
        run_parent(check_dfen_extraction=False, dfen_extraction_stale_hours=CUSTOM_STALE_HOURS)
    # Then the guard task receives both parameters (it logs the bypass and skips the query)
    assert harness.guard_kwargs["enabled"] is False
    assert harness.guard_kwargs["stale_hours"] == CUSTOM_STALE_HOURS


def test_children_are_not_given_the_guard_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a parent run with custom guard parameters
    harness = ParentHarness(monkeypatch)
    # When it launches the children
    with pytest.raises(LaunchStoppedError):
        run_parent(check_dfen_extraction=False, dfen_extraction_stale_hours=CUSTOM_STALE_HOURS)
    # Then the pass-through parameters of the children carry neither of them
    assert "check_dfen_extraction" not in harness.child_passthrough
    assert "dfen_extraction_stale_hours" not in harness.child_passthrough


def test_a_child_never_runs_the_guard_even_if_the_parameter_is_given(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a child run (table_id with the parent's snapshot) that also received the guard parameters
    harness = ParentHarness(monkeypatch, guard_error=DfenExtractionError("não deveria rodar"))
    ran: list[str] = []
    monkeypatch.setattr(flow_module, "run_child", lambda *args: ran.append(str(args[2])))
    # When the flow runs in child mode
    flow_module.rj_smfp__nota_carioca_oracle_to_bq.fn(
        table_id="DPS",
        scn=1,
        snapshot_taken_at="2026-10-20T14:00:00+00:00",
        parent_run_id="parent",
        check_dfen_extraction=True,
        dfen_extraction_stale_hours=6,
    )
    # Then the table is processed and the guard was never called
    assert ran == ["DPS"]
    assert "check_dfen_extraction_task" not in harness.calls


def test_a_refused_load_fails_before_the_snapshot_and_is_reported_as_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given an extraction in progress
    harness = ParentHarness(monkeypatch, guard_error=DfenExtractionError("Extração das notas nacionais em andamento"))
    # When the parent runs
    with pytest.raises(DfenExtractionError):
        run_parent()
    # Then neither the leftover cleanup nor the snapshot nor any extraction happened
    assert harness.calls == ["rename", "inject", "ensure_exclusive_task", "check_dfen_extraction_task"]
    # And the notifier, as for any early failure, ended the run as failed at the snapshot stage with the reason
    (notifier,) = harness.notifiers
    assert notifier.state.status is RunStatus.FAILED
    assert notifier.state.failed_stage is ParentStage.SNAPSHOT
    assert notifier.state.error == "DfenExtractionError: Extração das notas nacionais em andamento"


# --- a task ---------------------------------------------------------------------------------------------------------


def test_task_with_the_guard_off_skips_the_query_and_logs_a_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given the guard turned off and an Oracle that must not be touched
    logged: list[tuple[str, str]] = []
    monkeypatch.setattr(dfen_task_module, "log", lambda message, level="info": logged.append((level, message)))

    def no_oracle(_: str) -> OracleConfig:
        raise AssertionError("não deve ler a configuração do Oracle")

    monkeypatch.setattr(dfen_task_module.oracle, "read_oracle_config", no_oracle)
    # When the task runs
    dfen_task_module.check_dfen_extraction_task.fn("/secret", False, 6)
    # Then it only warns that the check was bypassed
    assert [level for level, _ in logged] == ["warning"]
    assert "check_dfen_extraction=false" in logged[0][1]


def test_task_reads_with_the_pipeline_secret_and_raises_when_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a secret path resolved to a config and a running extraction
    monkeypatch.setattr(dfen_task_module, "log", lambda *_: None)
    secrets: list[str] = []

    def read_config(path: str) -> OracleConfig:
        secrets.append(path)
        return CONFIG

    monkeypatch.setattr(dfen_task_module.oracle, "read_oracle_config", read_config)
    monkeypatch.setattr(dfen_task_module.dfen, "read_running_tasks", lambda _: (task(9, 46, 120),))
    # When the task runs enabled
    with pytest.raises(DfenExtractionError, match="tarefa 9"):
        dfen_task_module.check_dfen_extraction_task.fn("/db-oracle-nota-fiscal", True, 6)
    # Then it used the same secret folder as the snapshot
    assert secrets == ["/db-oracle-nota-fiscal"]

"""Testes do passo opcional de dbt: parâmetros, espera pelo run filho, espera do BigQuery e posição no flow."""

import inspect
from datetime import UTC, datetime, timedelta

import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle import flow as flow_module
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import DBT_DEPLOYMENT, default_dbt_parameters
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils import bigquery
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.dbt import (
    DbtRunError,
    RunStatus,
    RunWaiter,
    resolve_dbt_parameters,
    should_run_dbt,
    skip_initial_quiet_wait,
)

T0 = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
RUN_ID = "run-1"


def status(state_type: str, name: str | None = None, message: str | None = None, end_time: datetime | None = None):
    return RunStatus(state_type=state_type, state_name=name or state_type.title(), message=message, end_time=end_time)


class FakeClock:
    """Relógio e sleep que avançam juntos, sem esperar de verdade."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class FakeRuns:
    """Devolve os estados em ordem (o último se repete) e registra os cancelamentos."""

    def __init__(self, statuses: list[RunStatus], error: BaseException | None = None) -> None:
        self.statuses = statuses
        self.error = error
        self.reads = 0
        self.cancelled: list[str] = []

    def read_status(self, run_id: str) -> RunStatus:
        assert run_id == RUN_ID
        if self.error is not None and self.reads == len(self.statuses):
            raise self.error
        current = self.statuses[min(self.reads, len(self.statuses) - 1)]
        self.reads += 1
        return current

    def cancel(self, run_id: str) -> None:
        self.cancelled.append(run_id)


def waiter(runs: FakeRuns, clock: FakeClock, messages: list[str], timeout_seconds: float = 600) -> RunWaiter:
    return RunWaiter(
        runs=runs,
        timeout_seconds=timeout_seconds,
        poll_seconds=30,
        report=messages.append,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )


def test_default_parameters_when_none_are_given():
    assert resolve_dbt_parameters(None) == {
        "command": "build",
        "select": "tag:nota_carioca",
        "send_discord_report": False,
        "github_repo": "https://github.com/prefeitura-rio/queries-rj-iplanrio.git",
        "bigquery_project": "rj-iplanrio",
        "target": "prod",
        "gcs_buckets": {"prod": "rj-iplanrio_dbt", "dev": "rj-iplanrio-dev_dbt"},
    }


def test_default_parameters_are_a_fresh_copy_each_time():
    buckets = default_dbt_parameters()["gcs_buckets"]
    assert isinstance(buckets, dict)
    buckets["prod"] = "outro"
    assert default_dbt_parameters()["gcs_buckets"] == {"prod": "rj-iplanrio_dbt", "dev": "rj-iplanrio-dev_dbt"}


def test_given_parameters_replace_the_defaults_without_merging():
    given: dict[str, object] = {"command": "run", "select": "tag:x"}
    assert resolve_dbt_parameters(given) == given


def test_flow_defaults_keep_dbt_off_and_point_to_the_prod_deployment():
    params = inspect.signature(flow_module.rj_smfp__nota_carioca_bq_to_oracle.fn).parameters
    assert params["run_dbt"].default is False
    assert params["dbt_deployment"].default == "rj-iplanrio--run-dbt/rj-iplanrio--run_dbt--prod" == DBT_DEPLOYMENT
    assert params["dbt_parameters"].default is None
    assert params["dbt_timeout_minutes"].default == 120  # noqa: PLR2004


@pytest.mark.parametrize(
    ("run_dbt", "mode", "expected"),
    [(True, "full", True), (False, "full", False), (True, "synonyms_only", False), (False, "synonyms_only", False)],
)
def test_dbt_runs_only_when_requested_and_not_in_synonyms_only(run_dbt, mode, expected):
    assert should_run_dbt(run_dbt, mode) is expected


def test_completed_run_returns_its_end_time_after_polling_until_final():
    clock, messages = FakeClock(), []
    runs = FakeRuns([status("RUNNING"), status("RUNNING"), status("COMPLETED", end_time=T0)])
    assert waiter(runs, clock, messages).wait(RUN_ID) == T0
    assert clock.sleeps == [30, 30]
    assert runs.cancelled == []
    assert messages[0] == "dbt Running há 0.0 min"
    assert messages[-1] == "dbt Completed há 1.0 min"


@pytest.mark.parametrize("state_type", ["FAILED", "CRASHED", "CANCELLED"])
def test_non_completed_final_states_raise_without_cancelling(state_type):
    runs = FakeRuns([status("RUNNING"), status(state_type, message="dbt build executed with errors")])
    with pytest.raises(DbtRunError, match=f"terminou em {state_type.title()} .*executed with errors"):
        waiter(runs, FakeClock(), []).wait(RUN_ID)
    assert runs.cancelled == []


def test_timeout_cancels_the_child_and_raises():
    clock, messages = FakeClock(), []
    runs = FakeRuns([status("RUNNING")])
    with pytest.raises(DbtRunError, match="não terminou em 1 min"):
        waiter(runs, clock, messages, timeout_seconds=60).wait(RUN_ID)
    assert runs.cancelled == [RUN_ID]
    assert clock.now == 60  # noqa: PLR2004
    assert any("cancelando o run de dbt" in message for message in messages)


def test_run_finishing_exactly_at_the_deadline_still_counts():
    runs = FakeRuns([status("RUNNING"), status("RUNNING"), status("COMPLETED", end_time=T0)])
    assert waiter(runs, FakeClock(), [], timeout_seconds=60).wait(RUN_ID) == T0
    assert runs.cancelled == []


def test_interrupt_while_waiting_cancels_the_child_and_reraises():
    runs = FakeRuns([status("RUNNING")], error=KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        waiter(runs, FakeClock(), []).wait(RUN_ID)
    assert runs.cancelled == [RUN_ID]


@pytest.mark.parametrize(
    ("offsets", "dbt_end_offset", "expected"),
    [
        ([-10, -5], 0, True),
        ([-10, 0], 0, True),
        ([-10, 1], 0, False),
        ([5], 0, False),
        ([-10, -5], None, False),
        ([], 0, True),
    ],
)
def test_skip_initial_quiet_wait_only_if_nothing_changed_after_dbt(offsets, dbt_end_offset, expected):
    modified = [T0 + timedelta(seconds=offset) for offset in offsets]
    dbt_end = None if dbt_end_offset is None else T0 + timedelta(seconds=dbt_end_offset)
    assert skip_initial_quiet_wait(modified, dbt_end) is expected


def fake_bigquery(monkeypatch, modified_reads):
    """Cada item de ``modified_reads`` é uma rodada de leitura das duas tabelas (A e B)."""
    reads = iter([modified for modified in modified_reads for _ in range(2)])  # uma leitura por tabela
    extracted: list[str] = []
    monkeypatch.setattr(bigquery, "get_last_modified", lambda *args: next(reads)[args[2]])
    monkeypatch.setattr(bigquery, "get_table_schema", lambda *_: {"fields": [], "num_rows": 1})

    def extract(*args):
        extracted.append(args[4])
        return [bigquery.ExportedFile(name=f"{args[4]}/part-0.csv.gz", size=1)]

    monkeypatch.setattr(bigquery, "extract_table_to_gcs", extract)
    monkeypatch.setattr(bigquery, "delete_blobs", lambda *_: None)
    monkeypatch.setattr(bigquery, "datetime", type("FrozenDatetime", (), {"now": staticmethod(lambda _tz: T0)}))
    return extracted


def request(dbt_finished_at):
    return bigquery.SnapshotRequest(
        project="p",
        dataset_id="d",
        table_ids=["A", "B"],
        bucket="b",
        prefix="d/run",
        quiet_seconds=300,
        dbt_finished_at=dbt_finished_at,
    )


def test_export_skips_the_initial_wait_when_tables_changed_only_before_dbt_ended(monkeypatch):
    recent = T0 - timedelta(seconds=30)
    reads = [{"A": recent, "B": recent}, {"A": recent, "B": recent}]
    extracted = fake_bigquery(monkeypatch, reads)
    waits: list[float] = []
    messages: list[str] = []
    bigquery.export_consistent_snapshot(request(T0 - timedelta(seconds=10)), messages.append, waits.append)
    assert waits == []
    assert extracted == ["d/run/tentativa-1/A", "d/run/tentativa-1/B"]
    assert "dispensando a espera" in messages[0]


def test_export_keeps_the_wait_when_a_table_changed_after_dbt_ended(monkeypatch):
    recent = T0 - timedelta(seconds=30)
    quiet = T0 - timedelta(hours=1)
    reads = [{"A": recent, "B": T0 - timedelta(seconds=5)}, {"A": quiet, "B": quiet}, {"A": quiet, "B": quiet}]
    fake_bigquery(monkeypatch, reads)
    waits: list[float] = []
    bigquery.export_consistent_snapshot(request(T0 - timedelta(seconds=10)), lambda _m: None, waits.append)
    assert waits == [295]


def test_export_without_dbt_keeps_the_normal_wait(monkeypatch):
    recent = T0 - timedelta(seconds=30)
    quiet = T0 - timedelta(hours=1)
    fake_bigquery(monkeypatch, [{"A": recent, "B": recent}, {"A": quiet, "B": quiet}, {"A": quiet, "B": quiet}])
    waits: list[float] = []
    bigquery.export_consistent_snapshot(request(None), lambda _m: None, waits.append)
    assert waits == [270]


def test_export_still_discards_and_repeats_when_tables_change_during_the_extract_after_a_skip(monkeypatch):
    recent = T0 - timedelta(seconds=30)
    quiet = T0 - timedelta(hours=1)
    changed = {"A": recent, "B": T0}
    reads = [{"A": recent, "B": recent}, changed, changed, {"A": quiet, "B": quiet}, {"A": quiet, "B": quiet}]
    extracted = fake_bigquery(monkeypatch, reads)
    waits: list[float] = []
    bigquery.export_consistent_snapshot(request(T0 - timedelta(seconds=10)), lambda _m: None, waits.append)
    assert extracted[-2:] == ["d/run/tentativa-2/A", "d/run/tentativa-2/B"]
    assert waits == [300]


class FlowCalls:
    """Troca as tasks do flow por funções simples que registram a ordem das chamadas."""

    def __init__(self, monkeypatch) -> None:
        self.calls: list[str] = []
        self.dbt_arguments: dict[str, object] = {}
        self.export_arguments: dict[str, object] = {}
        names = (
            "rename_current_flow_run_task",
            "inject_bd_credentials_task",
            "refresh_synonyms_task",
            "wait_inmemory_task",
            "swap_synonyms_task",
        )
        for name in names:
            monkeypatch.setattr(flow_module, name, self.recorder(name))
        monkeypatch.setattr(flow_module, "list_tables_task", lambda **_: self.record("list_tables_task", []))
        monkeypatch.setattr(flow_module, "run_dbt_task", self.run_dbt)
        monkeypatch.setattr(flow_module, "export_snapshot_task", self.export)

    def record(self, name, value=None):
        self.calls.append(name)
        return value

    def recorder(self, name):
        return lambda **_: self.record(name)

    def run_dbt(self, **arguments):
        self.dbt_arguments = arguments
        return self.record("run_dbt_task", T0)

    def export(self, **arguments):
        self.export_arguments = arguments
        return self.record("export_snapshot_task", {})


def run_flow(**arguments):
    flow_module.rj_smfp__nota_carioca_bq_to_oracle.fn(**arguments)


def test_flow_does_not_run_dbt_by_default(monkeypatch):
    flow = FlowCalls(monkeypatch)
    run_flow()
    assert "run_dbt_task" not in flow.calls
    assert flow.export_arguments["dbt_finished_at"] is None


def test_flow_does_not_run_dbt_in_synonyms_only_mode(monkeypatch):
    flow = FlowCalls(monkeypatch)
    run_flow(run_dbt=True, mode="synonyms_only")
    assert flow.calls == [
        "rename_current_flow_run_task",
        "inject_bd_credentials_task",
        "list_tables_task",
        "refresh_synonyms_task",
    ]


def test_flow_runs_dbt_first_after_credentials_and_hands_its_end_to_the_export(monkeypatch):
    flow = FlowCalls(monkeypatch)
    run_flow(run_dbt=True, dbt_timeout_minutes=7)
    assert flow.calls[:4] == [
        "rename_current_flow_run_task",
        "inject_bd_credentials_task",
        "run_dbt_task",
        "list_tables_task",
    ]
    assert flow.calls[4] == "export_snapshot_task"
    assert flow.dbt_arguments == {
        "deployment": DBT_DEPLOYMENT,
        "parameters": default_dbt_parameters(),
        "timeout_minutes": 7,
    }
    assert flow.export_arguments["dbt_finished_at"] == T0


def test_flow_passes_overridden_deployment_and_parameters_to_dbt(monkeypatch):
    flow = FlowCalls(monkeypatch)
    run_flow(run_dbt=True, dbt_deployment="f/d", dbt_parameters={"fail": True})
    assert flow.dbt_arguments["deployment"] == "f/d"
    assert flow.dbt_arguments["parameters"] == {"fail": True}

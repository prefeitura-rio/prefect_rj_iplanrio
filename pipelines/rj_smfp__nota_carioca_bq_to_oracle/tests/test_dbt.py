"""Testes do passo opcional de dbt: parâmetros, espera pelo run filho, espera do BigQuery e posição no flow."""

from datetime import UTC, datetime, timedelta
from typing import Literal, final

import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import default_dbt_parameters
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers import bigquery
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.dbt import (
    DbtRunError,
    RunWaiter,
    resolve_dbt_parameters,
    should_run_dbt,
    skip_initial_quiet_wait,
)
from utils.runs import RunStatus

T0 = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
RUN_ID = "run-1"


def status(
    state_type: str, name: str | None = None, message: str | None = None, end_time: datetime | None = None
) -> RunStatus:
    return RunStatus(state_type=state_type, state_name=name or state_type.title(), message=message, end_time=end_time)


@final
class FakeClock:
    """Relógio e sleep que avançam juntos, sem esperar de verdade."""

    def __init__(self) -> None:
        """Store the arguments."""
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@final
class FakeRuns:
    """Devolve os estados em ordem (o último se repete) e registra os cancelamentos."""

    def __init__(self, statuses: list[RunStatus], error: BaseException | None = None) -> None:
        """Store the arguments."""
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


def test_default_parameters_when_none_are_given() -> None:
    """Check that default parameters when none are given."""
    assert resolve_dbt_parameters(None) == {
        "command": "build",
        "select": "tag:nota_carioca",
        "send_discord_report": False,
        "github_repo": "https://github.com/prefeitura-rio/queries-rj-iplanrio.git",
        "bigquery_project": "rj-iplanrio",
        "target": "prod",
        "gcs_buckets": {"prod": "rj-iplanrio_dbt", "dev": "rj-iplanrio-dev_dbt"},
    }


def test_default_parameters_are_a_fresh_copy_each_time() -> None:
    """Check that default parameters are a fresh copy each time."""
    buckets = default_dbt_parameters()["gcs_buckets"]
    assert isinstance(buckets, dict)
    buckets["prod"] = "outro"
    assert default_dbt_parameters()["gcs_buckets"] == {"prod": "rj-iplanrio_dbt", "dev": "rj-iplanrio-dev_dbt"}


def test_given_parameters_replace_the_defaults_without_merging() -> None:
    """Check that given parameters replace the defaults without merging."""
    given: dict[str, object] = {"command": "run", "select": "tag:x"}
    assert resolve_dbt_parameters(given) == given


@pytest.mark.parametrize(
    ("run_dbt", "mode", "expected"),
    [(True, "full", True), (False, "full", False), (True, "synonyms_only", False), (False, "synonyms_only", False)],
)
def test_dbt_runs_only_when_requested_and_not_in_synonyms_only(
    run_dbt: bool, mode: Literal["full", "synonyms_only"], expected: bool
) -> None:
    """Check that dbt runs only when requested and not in synonyms only."""
    assert should_run_dbt(run_dbt, mode) is expected


def test_completed_run_returns_its_end_time_after_polling_until_final() -> None:
    """Check that completed run returns its end time after polling until final."""
    clock, messages = FakeClock(), []
    runs = FakeRuns([status("RUNNING"), status("RUNNING"), status("COMPLETED", end_time=T0)])
    assert waiter(runs, clock, messages).wait(RUN_ID) == T0
    assert clock.sleeps == [30, 30]
    assert runs.cancelled == []
    assert messages[0] == "dbt Running há 0.0 min"
    assert messages[-1] == "dbt Completed há 1.0 min"


@pytest.mark.parametrize("state_type", ["FAILED", "CRASHED", "CANCELLED"])
def test_non_completed_final_states_raise_without_cancelling(state_type: str) -> None:
    """Check that non completed final states raise without cancelling."""
    runs = FakeRuns([status("RUNNING"), status(state_type, message="dbt build executed with errors")])
    with pytest.raises(DbtRunError, match=f"terminou em {state_type.title()} .*executed with errors"):
        waiter(runs, FakeClock(), []).wait(RUN_ID)
    assert runs.cancelled == []


def test_timeout_cancels_the_child_and_raises() -> None:
    """Check that timeout cancels the child and raises."""
    clock, messages = FakeClock(), []
    runs = FakeRuns([status("RUNNING")])
    with pytest.raises(DbtRunError, match="não terminou em 1 min"):
        waiter(runs, clock, messages, timeout_seconds=60).wait(RUN_ID)
    assert runs.cancelled == [RUN_ID]
    assert clock.now == 60
    assert any("cancelando o run de dbt" in message for message in messages)


def test_run_finishing_exactly_at_the_deadline_still_counts() -> None:
    """Check that run finishing exactly at the deadline still counts."""
    runs = FakeRuns([status("RUNNING"), status("RUNNING"), status("COMPLETED", end_time=T0)])
    assert waiter(runs, FakeClock(), [], timeout_seconds=60).wait(RUN_ID) == T0
    assert runs.cancelled == []


def test_interrupt_while_waiting_cancels_the_child_and_reraises() -> None:
    """Check that interrupt while waiting cancels the child and reraises."""
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
def test_skip_initial_quiet_wait_only_if_nothing_changed_after_dbt(
    offsets: list[int], dbt_end_offset: int | None, expected: bool
) -> None:
    """Check that skip initial quiet wait only if nothing changed after dbt."""
    modified = [T0 + timedelta(seconds=offset) for offset in offsets]
    dbt_end = None if dbt_end_offset is None else T0 + timedelta(seconds=dbt_end_offset)
    assert skip_initial_quiet_wait(modified, dbt_end) is expected


@final
class FrozenDatetime:
    """Replaces `datetime` in the BigQuery helper, so that `now` is always `T0`."""

    @staticmethod
    def now(tz: object) -> datetime:
        del tz

        return T0


def ignore(message: str) -> None:
    del message


def fake_bigquery(monkeypatch: pytest.MonkeyPatch, modified_reads: list[dict[str, datetime]]) -> list[str]:
    """Each item of `modified_reads` is a round of reads of the two tables (A and B)."""
    reads = iter([modified for modified in modified_reads for _ in range(2)])  # one read for each table
    extracted: list[str] = []

    def last_modified(project: str, dataset_id: str, table_id: str) -> datetime:
        del project, dataset_id

        return next(reads)[table_id]

    def schema(project: str, dataset_id: str, table_id: str) -> bigquery.TableSchema:
        del project, dataset_id, table_id

        return bigquery.TableSchema(fields=[], num_rows=1)

    def extract(project: str, dataset_id: str, table_id: str, bucket: str, prefix: str) -> list[bigquery.ExportedFile]:
        del project, dataset_id, table_id, bucket
        extracted.append(prefix)

        return [bigquery.ExportedFile(name=f"{prefix}/part-0.csv.gz", size=1)]

    def delete(project: str, bucket: str, blob_names: list[str]) -> None:
        del project, bucket, blob_names

    monkeypatch.setattr(bigquery, "get_last_modified", last_modified)
    monkeypatch.setattr(bigquery, "get_table_schema", schema)
    monkeypatch.setattr(bigquery, "extract_table_to_gcs", extract)
    monkeypatch.setattr(bigquery, "delete_blobs", delete)
    monkeypatch.setattr(bigquery, "datetime", FrozenDatetime)

    return extracted


def request(dbt_finished_at: datetime | None) -> bigquery.SnapshotRequest:
    return bigquery.SnapshotRequest(
        project="p",
        dataset_id="d",
        table_ids=["A", "B"],
        bucket="b",
        prefix="d/run",
        quiet_seconds=300,
        dbt_finished_at=dbt_finished_at,
    )


def test_export_skips_the_initial_wait_when_tables_changed_only_before_dbt_ended(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Check that export skips the initial wait when tables changed only before dbt ended."""
    recent = T0 - timedelta(seconds=30)
    reads = [{"A": recent, "B": recent}, {"A": recent, "B": recent}]
    extracted = fake_bigquery(monkeypatch, reads)
    waits: list[float] = []
    messages: list[str] = []
    bigquery.export_consistent_snapshot(request(T0 - timedelta(seconds=10)), messages.append, waits.append)
    assert waits == []
    assert extracted == ["d/run/tentativa-1/A", "d/run/tentativa-1/B"]
    assert "dispensando a espera" in messages[0]


def test_export_keeps_the_wait_when_a_table_changed_after_dbt_ended(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check that export keeps the wait when a table changed after dbt ended."""
    recent = T0 - timedelta(seconds=30)
    quiet = T0 - timedelta(hours=1)
    reads = [{"A": recent, "B": T0 - timedelta(seconds=5)}, {"A": quiet, "B": quiet}, {"A": quiet, "B": quiet}]
    fake_bigquery(monkeypatch, reads)
    waits: list[float] = []
    bigquery.export_consistent_snapshot(request(T0 - timedelta(seconds=10)), ignore, waits.append)
    assert waits == [295]


def test_export_without_dbt_keeps_the_normal_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    """Check that export without dbt keeps the normal wait."""
    recent = T0 - timedelta(seconds=30)
    quiet = T0 - timedelta(hours=1)
    fake_bigquery(monkeypatch, [{"A": recent, "B": recent}, {"A": quiet, "B": quiet}, {"A": quiet, "B": quiet}])
    waits: list[float] = []
    bigquery.export_consistent_snapshot(request(None), ignore, waits.append)
    assert waits == [270]


def test_export_still_discards_and_repeats_when_tables_change_during_the_extract_after_a_skip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Check that export still discards and repeats when tables change during the extract after a skip."""
    recent = T0 - timedelta(seconds=30)
    quiet = T0 - timedelta(hours=1)
    changed = {"A": recent, "B": T0}
    reads = [{"A": recent, "B": recent}, changed, changed, {"A": quiet, "B": quiet}, {"A": quiet, "B": quiet}]
    extracted = fake_bigquery(monkeypatch, reads)
    waits: list[float] = []
    bigquery.export_consistent_snapshot(request(T0 - timedelta(seconds=10)), ignore, waits.append)
    assert extracted[-2:] == ["d/run/tentativa-2/A", "d/run/tentativa-2/B"]
    assert waits == [300]

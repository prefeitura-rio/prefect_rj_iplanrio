from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from prefect.exceptions import TerminationSignal

from pipelines.rj_smfp__nota_carioca_oracle_to_bq import tasks as tasks_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import LAUNCH_RETRIES, LAUNCH_RETRY_DELAYS_SECONDS
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import runs as runs_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import Snapshot
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import is_final_attempt, launch_key, will_retry
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.runs import ChildLaunch, launch_children

TABLES = ("DPS", "NOTAS_NACIONAIS", "PESSOAS_NACIONAIS")
ALL_CHILDREN = len(TABLES)
PARENT = "0a1b2c3d-1111-4222-8333-444455556666"
SNAPSHOT = Snapshot(scn=987654321, taken_at=datetime(2026, 10, 20, 12, 0, 0, tzinfo=UTC))
PREFIX = launch_key(PARENT, SNAPSHOT)


class FakePrefect:
    """``run_deployment`` e ``cancel_runs`` falsos que imitam o servidor: a mesma chave devolve o mesmo flow run."""

    def __init__(self) -> None:
        self.by_key: dict[str, str] = {}
        self.creations = 0
        self.calls: list[str] = []
        self.cancelled: list[str] = []
        self.fail_calls: dict[int, BaseException] = {}
        self.lose_response_calls: dict[int, BaseException] = {}
        self.fail_cancel_for: set[str] = set()

    def run_deployment(self, **kwargs: object) -> SimpleNamespace:
        self.calls.append(str(kwargs["idempotency_key"]))
        failure = self.fail_calls.get(len(self.calls))
        if failure is not None:
            raise failure
        key = str(kwargs["idempotency_key"])
        if key not in self.by_key:
            self.creations += 1
            self.by_key[key] = str(uuid4())
        lost = self.lose_response_calls.get(len(self.calls))
        if lost is not None:
            raise lost
        return SimpleNamespace(id=self.by_key[key])

    def cancel_runs(self, run_ids: Sequence[str]) -> None:
        for run_id in run_ids:
            if run_id in self.fail_cancel_for:
                raise ConnectionError(f"API fora do ar ao cancelar {run_id}")
            self.cancelled.append(run_id)

    def id_of(self, table_id: str, prefix: str = PREFIX) -> str:
        return self.by_key[f"{prefix}:{table_id}"]


@pytest.fixture
def api(monkeypatch: pytest.MonkeyPatch) -> FakePrefect:
    fake = FakePrefect()
    monkeypatch.setattr(runs_module, "run_deployment", fake.run_deployment)
    monkeypatch.setattr(runs_module, "cancel_runs", fake.cancel_runs)
    return fake


def request(prefix: str = PREFIX) -> ChildLaunch:
    parameters: Mapping[str, Mapping[str, object]] = {table_id: {"table_id": table_id} for table_id in TABLES}
    return ChildLaunch("flow/deployment", prefix, parameters)


def attempt(final: bool, prefix: str = PREFIX, report: Callable[[str], None] | None = None) -> dict[str, str]:
    return launch_children(request(prefix), final, report or (lambda _: None))


# --- as tentativas repetem sem duplicar -------------------------------------------------------------------------------


def test_every_child_is_created_with_a_key_made_of_the_parent_run_and_the_table(api: FakePrefect) -> None:
    # Given a launch that succeeds on the first attempt
    # When the children are launched
    children = attempt(final=False)
    # Then each table got its own key built from the parent run, the snapshot and the table, in table order
    assert api.calls == [f"{PARENT}:{SNAPSHOT.scn}:{table_id}" for table_id in TABLES]
    assert list(children) == list(TABLES)
    assert len(set(children.values())) == ALL_CHILDREN


def test_a_retry_after_a_partial_launch_reuses_the_created_children_and_creates_no_duplicate(
    api: FakePrefect,
) -> None:
    # Given a first attempt that creates DPS and then loses the Prefect API on the next call
    api.fail_calls = {2: ConnectionError("ConnectTimeout")}
    with pytest.raises(ConnectionError):
        attempt(final=False)
    first_dps = api.id_of("DPS")
    # When the retry runs and the API is back
    children = attempt(final=False)
    # Then DPS is the very same flow run, the other two were created once, and there are exactly three children
    assert children["DPS"] == first_dps
    assert api.creations == ALL_CHILDREN
    assert set(children.values()) == set(api.by_key.values())


def test_a_retry_after_losing_the_response_of_a_creation_does_not_duplicate_the_child(api: FakePrefect) -> None:
    # Given a creation that the server completed but whose response never reached the parent
    api.lose_response_calls = {1: TimeoutError("resposta perdida")}
    with pytest.raises(TimeoutError):
        attempt(final=False)
    assert api.creations == 1
    # When the retry asks for the same table again
    children = attempt(final=False)
    # Then the server returned the child it already had
    assert api.creations == ALL_CHILDREN
    assert children["DPS"] == api.id_of("DPS")


def test_a_failure_that_will_be_retried_cancels_nothing(api: FakePrefect) -> None:
    # Given an attempt that created DPS and NOTAS_NACIONAIS and failed on the third table, with retries left
    api.fail_calls = {3: ConnectionError("ConnectTimeout")}
    # When it fails
    with pytest.raises(ConnectionError):
        attempt(final=False)
    # Then no child is cancelled: the retry will pick them up by key
    assert api.cancelled == []
    assert api.creations == ALL_CHILDREN - 1


# --- só a última tentativa cancela ------------------------------------------------------------------------------------


def test_the_final_failure_cancels_every_known_child_including_those_of_earlier_attempts(
    api: FakePrefect,
) -> None:
    # Given a first attempt that created DPS and NOTAS_NACIONAIS and failed on PESSOAS_NACIONAIS
    api.fail_calls = {3: ConnectionError("ConnectTimeout")}
    with pytest.raises(ConnectionError):
        attempt(final=False)
    # And a final attempt that already fails when asking for NOTAS_NACIONAIS again
    api.fail_calls = {5: ConnectionError("ConnectTimeout")}
    # When the final attempt fails
    with pytest.raises(ConnectionError, match="ConnectTimeout"):
        attempt(final=True)
    # Then both children created along the way are cancelled, the one the last attempt never reached included
    assert sorted(api.cancelled) == sorted([api.id_of("DPS"), api.id_of("NOTAS_NACIONAIS")])


def test_a_final_failure_before_any_child_exists_cancels_nothing(api: FakePrefect) -> None:
    # Given an outage that stops even the first creation of the final attempt
    api.fail_calls = {1: ConnectionError("ConnectTimeout")}
    # When it fails
    with pytest.raises(ConnectionError):
        attempt(final=True)
    # Then there is nothing to cancel
    assert api.cancelled == []


def test_cancel_failures_are_reported_and_never_replace_the_original_error(api: FakePrefect) -> None:
    # Given two created children, one of which cannot be cancelled, and a final attempt that fails
    api.fail_calls = {3: ConnectionError("ConnectTimeout")}
    with pytest.raises(ConnectionError):
        attempt(final=False)
    api.fail_cancel_for = {api.id_of("DPS")}
    api.fail_calls = {4: RuntimeError("falha original")}
    reports: list[str] = []
    # When the final attempt fails
    with pytest.raises(RuntimeError, match="falha original"):
        attempt(final=True, report=reports.append)
    # Then the original error surfaced, the failure was logged naming the child, and the other one was still cancelled
    assert len(reports) == 1
    assert api.id_of("DPS") in reports[0]
    assert "ConnectionError" in reports[0]
    assert api.cancelled == [api.id_of("NOTAS_NACIONAIS")]


@pytest.mark.parametrize("interruption", [KeyboardInterrupt(), TerminationSignal(signal=15)])
def test_an_interruption_cancels_at_once_because_the_prefect_does_not_retry_it(
    api: FakePrefect, interruption: BaseException
) -> None:
    # Given a parent that is terminated (SIGTERM, Ctrl+C) after creating DPS, on a non-final attempt
    api.fail_calls = {2: interruption}
    # When the launch is interrupted
    with pytest.raises(type(interruption)):
        attempt(final=False)
    # Then the child already created is cancelled, as before the retries existed
    assert api.cancelled == [api.id_of("DPS")]


def test_a_finished_launch_forgets_its_children_and_other_launches_are_independent(api: FakePrefect) -> None:
    # Given a launch of another parent run that already succeeded
    attempt(final=False, prefix=launch_key("another-parent", SNAPSHOT))
    # When this parent's launch fails for good after creating DPS
    api.fail_calls = {5: ConnectionError("ConnectTimeout")}
    with pytest.raises(ConnectionError):
        attempt(final=True)
    # Then only that launch's own child is cancelled, never the finished launch's children
    assert api.cancelled == [api.id_of("DPS")]


# --- decisões puras ---------------------------------------------------------------------------------------------------


def test_the_key_changes_with_the_snapshot_so_a_rerun_of_the_parent_never_gets_old_children() -> None:
    # Given the same parent run with a new snapshot (a retried flow run takes another SCN)
    newer = Snapshot(scn=SNAPSHOT.scn + 1, taken_at=SNAPSHOT.taken_at)
    # Then the key prefix differs, and is stable for the same inputs
    assert launch_key(PARENT, SNAPSHOT) == launch_key(PARENT, SNAPSHOT)
    assert launch_key(PARENT, SNAPSHOT) != launch_key(PARENT, newer)
    assert PARENT in launch_key(PARENT, SNAPSHOT)


@pytest.mark.parametrize(
    ("run_count", "expected"),
    [(1, False), (LAUNCH_RETRIES, False), (LAUNCH_RETRIES + 1, True), (LAUNCH_RETRIES + 2, True)],
)
def test_the_last_attempt_is_the_one_after_all_retries(run_count: int, expected: bool) -> None:
    assert is_final_attempt(run_count, LAUNCH_RETRIES) is expected


def test_only_an_ordinary_failure_with_attempts_left_is_retried() -> None:
    assert will_retry(ConnectionError("x"), final_attempt=False) is True
    assert will_retry(ConnectionError("x"), final_attempt=True) is False
    assert will_retry(KeyboardInterrupt(), final_attempt=False) is False


# --- fiação da task ---------------------------------------------------------------------------------------------------


def test_the_task_retries_with_the_shared_constants() -> None:
    # Given the launch task as declared
    # Then the Prefect retries are the shared constant and the waits grow up to five minutes
    assert tasks_module.launch_children_task.retries == LAUNCH_RETRIES
    assert tasks_module.launch_children_task.retry_delay_seconds == list(LAUNCH_RETRY_DELAYS_SECONDS)
    assert LAUNCH_RETRIES == len(LAUNCH_RETRY_DELAYS_SECONDS)


@pytest.mark.parametrize(("run_count", "final"), [(1, False), (LAUNCH_RETRIES, False), (LAUNCH_RETRIES + 1, True)])
def test_the_task_tells_the_launcher_whether_this_is_the_last_attempt(
    monkeypatch: pytest.MonkeyPatch, run_count: int, final: bool
) -> None:
    # Given the task running as the Nth attempt of a deployment run
    seen: dict[str, object] = {}

    def fake_launch(launch: ChildLaunch, final_attempt: bool, _report: Callable[[str], None]) -> dict[str, str]:
        seen.update(launch=launch, final_attempt=final_attempt)
        return {table_id: f"child-{table_id}" for table_id in launch.parameters_by_table}

    monkeypatch.setattr(tasks_module, "log", lambda *_: None)
    monkeypatch.setattr(tasks_module, "task_run", SimpleNamespace(run_count=run_count))
    monkeypatch.setattr(tasks_module, "deployment", SimpleNamespace(name="dep"))
    monkeypatch.setattr(tasks_module, "flow_run", SimpleNamespace(flow_name="flow", id=PARENT))
    monkeypatch.setattr(tasks_module.runs, "launch_children", fake_launch)
    # When it runs
    tasks_module.launch_children_task.fn(["DPS"], SNAPSHOT, {"dataset_id": "ds"})
    # Then the launcher is told whether this attempt is the last one, with the key prefix of this parent and snapshot
    assert seen["final_attempt"] is final
    launch = seen["launch"]
    assert isinstance(launch, ChildLaunch)
    assert launch.key_prefix == launch_key(PARENT, SNAPSHOT)
    assert launch.deployment_name == "flow/dep"

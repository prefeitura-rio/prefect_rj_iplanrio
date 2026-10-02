# ruff: noqa: PLR2004
from collections.abc import Sequence
from datetime import UTC, datetime

import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import CHILD_TAG, LABEL_ROWS, LABEL_RUN_ID, LABEL_SCN
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import Snapshot
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import (
    ChildContext,
    ParallelRunError,
    RunInfo,
    TableProof,
    Verdict,
    build_child_parameters,
    decide,
    describe_conflicts,
    encode_label_value,
    encode_proof,
    find_conflicts,
    parse_child_context,
    runs_to_cancel,
    verify_proof,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.supervise import Supervision, supervise

RUN_ID = "0a1b2c3d-1111-4222-8333-444455556666"
TAKEN_AT = datetime(2026, 10, 2, 12, 30, 5, 123456, tzinfo=UTC)
SNAPSHOT = Snapshot(scn=987654321, taken_at=TAKEN_AT)


def run(state: str, run_id: str = "r", tags: tuple[str, ...] = (), **parameters: object) -> RunInfo:
    return RunInfo(run_id, f"name-{run_id}", state, state.title(), tags, parameters)


def test_child_parameters_carry_snapshot_parent_run_and_passthrough() -> None:
    # Given a snapshot and the parent's pass-through parameters
    passthrough = {"dataset_id": "ds", "workers": 2}
    # When the child parameters are built
    params = build_child_parameters("DPS", SNAPSHOT, RUN_ID, passthrough)
    # Then the child gets its table, the exact SCN, the ISO time and the parent run id besides the pass-through
    assert params == {
        "dataset_id": "ds",
        "workers": 2,
        "table_id": "DPS",
        "scn": 987654321,
        "snapshot_taken_at": "2026-10-02T12:30:05.123456+00:00",
        "parent_run_id": RUN_ID,
    }


def test_child_parameters_round_trip_to_the_same_context() -> None:
    # Given parameters built by the parent
    params = build_child_parameters("DPS", SNAPSHOT, RUN_ID, {})
    # When the child parses them
    context = parse_child_context(
        str(params["table_id"]), int(str(params["scn"])), str(params["snapshot_taken_at"]), str(params["parent_run_id"])
    )
    # Then the snapshot is rebuilt without loss
    assert context == ChildContext("DPS", SNAPSHOT, RUN_ID)


def test_mode_is_parent_when_no_child_parameter_is_given() -> None:
    assert parse_child_context(None, None, None, None) is None


@pytest.mark.parametrize(
    ("scn", "taken_at", "parent"),
    [(None, "2026-01-01T00:00:00", "p"), (1, None, "p"), (1, "2026-01-01T00:00:00", None)],
)
def test_child_mode_without_all_parameters_is_refused(
    scn: int | None, taken_at: str | None, parent: str | None
) -> None:
    with pytest.raises(ValueError, match="exige"):
        parse_child_context("DPS", scn, taken_at, parent)


def test_child_parameters_without_table_id_are_refused() -> None:
    with pytest.raises(ValueError, match="só fazem sentido"):
        parse_child_context(None, 1, None, None)


def test_naive_snapshot_time_is_read_as_utc() -> None:
    context = parse_child_context("DPS", 1, "2026-01-01T00:00:00", "p")
    assert context is not None
    assert context.snapshot.taken_at.tzinfo is UTC


@pytest.mark.parametrize(
    ("states", "expected"),
    [
        (["COMPLETED", "COMPLETED", "COMPLETED"], Verdict.ALL_COMPLETED),
        (["COMPLETED", "RUNNING", "PENDING"], Verdict.RUNNING),
        (["SCHEDULED", "RUNNING", "RUNNING"], Verdict.RUNNING),
        (["COMPLETED", "FAILED", "RUNNING"], Verdict.FAILED),
        (["CRASHED", "RUNNING", "RUNNING"], Verdict.FAILED),
        (["COMPLETED", "CANCELLED", "COMPLETED"], Verdict.FAILED),
        (["CANCELLING", "RUNNING", "RUNNING"], Verdict.FAILED),
    ],
)
def test_children_states_are_aggregated_into_a_decision(states: list[str], expected: Verdict) -> None:
    assert decide([run(state, str(index)) for index, state in enumerate(states)]) is expected


def test_only_children_still_active_are_cancelled() -> None:
    states = ["FAILED", "RUNNING", "PENDING", "CANCELLING", "COMPLETED"]
    children = [run(state, name) for state, name in zip(states, "abcde", strict=True)]
    assert [child.run_id for child in runs_to_cancel(children)] == ["b", "c"]


def test_conflicts_are_other_active_parents_and_ignore_self_and_finished_runs() -> None:
    runs = [
        run("RUNNING", "me"),
        run("RUNNING", "other"),
        run("COMPLETED", "old"),
        run("FAILED", "dead"),
        run("SCHEDULED", "queued"),
    ]
    assert [conflict.run_id for conflict in find_conflicts(runs, "me")] == ["other"]


def test_children_are_recognised_by_tag_or_table_id_and_listed_after_parents() -> None:
    runs = [
        run("RUNNING", "tagged", tags=(CHILD_TAG,)),
        run("PENDING", "manual", table_id="DPS"),
        run("RUNNING", "parent", table_id=None),
    ]
    conflicts = find_conflicts(runs, "me")
    assert [conflict.run_id for conflict in conflicts] == ["parent", "tagged", "manual"]
    assert [conflict.is_child for conflict in conflicts] == [False, True, True]


def test_conflict_message_names_the_run_to_cancel() -> None:
    message = describe_conflicts([run("RUNNING", "zombie-id")])
    assert "pai 'name-zombie-id' (zombie-id, Running)" in message
    assert "cancele" in message


def test_no_conflict_when_only_self_is_active() -> None:
    assert find_conflicts([run("RUNNING", "me")], "me") == []


def test_label_value_is_sanitised_to_the_bigquery_charset() -> None:
    assert encode_label_value("Ab:C/d.E-1_2") == "ab_c_d_e-1_2"
    assert encode_label_value("x" * 100) == "x" * 63


def test_proof_labels_hold_run_id_scn_and_rows() -> None:
    assert encode_proof(RUN_ID, 987654321, 1500) == {LABEL_RUN_ID: RUN_ID, LABEL_SCN: "987654321", LABEL_ROWS: "1500"}


def proof_for(run_id: str = RUN_ID, scn: int = 987654321, label_rows: int = 1500, num_rows: int = 1500) -> TableProof:
    return TableProof(labels=encode_proof(run_id, scn, label_rows), num_rows=num_rows)


def test_matching_proof_is_accepted() -> None:
    verify_proof("DPS", proof_for(), RUN_ID, 987654321)


@pytest.mark.parametrize(
    ("proof", "run_id", "scn", "fragment"),
    [
        (None, RUN_ID, 987654321, "não existe"),
        (TableProof({}, 1500), RUN_ID, 987654321, LABEL_RUN_ID),
        (proof_for(run_id="another-run"), RUN_ID, 987654321, LABEL_RUN_ID),
        (proof_for(scn=111), RUN_ID, 987654321, LABEL_SCN),
        (proof_for(label_rows=1499), RUN_ID, 987654321, LABEL_ROWS),
        (proof_for(num_rows=1501), RUN_ID, 987654321, LABEL_ROWS),
    ],
)
def test_wrong_or_missing_proof_refuses_to_publish(
    proof: TableProof | None, run_id: str, scn: int, fragment: str
) -> None:
    with pytest.raises(ParallelRunError, match=fragment):
        verify_proof("DPS", proof, run_id, scn)


class FakeRuns:
    """Estado dos filhos que avança a cada leitura, conforme um roteiro de estados por filho."""

    def __init__(self, script: dict[str, list[str]]) -> None:
        self.script = script
        self.cancelled: list[str] = []
        self.reports: list[str] = []
        self.reads = 0

    def read(self, run_ids: Sequence[str]) -> list[RunInfo]:
        self.reads += 1
        out = []
        for run_id in run_ids:
            states = self.script[run_id]
            state = "CANCELLED" if run_id in self.cancelled else states[min(self.reads - 1, len(states) - 1)]
            out.append(run(state, run_id))
        return out

    def cancel(self, run_ids: Sequence[str]) -> None:
        self.cancelled.extend(run_ids)

    def supervision(self) -> Supervision:
        return Supervision(
            read=self.read, cancel=self.cancel, report=self.reports.append, sleep=lambda _seconds: None
        )


CHILDREN = {"DPS": "a", "NOTAS": "b", "PESSOAS": "c"}


def test_supervision_returns_when_all_children_complete_and_logs_each_child() -> None:
    fake = FakeRuns({"a": ["RUNNING", "COMPLETED"], "b": ["RUNNING", "COMPLETED"], "c": ["COMPLETED"]})
    supervise(CHILDREN, fake.supervision())
    assert fake.cancelled == []
    assert any(line.startswith("DPS: filho 'name-a' (a)") for line in fake.reports)
    assert fake.reads == 2


def test_supervision_cancels_running_siblings_and_fails_when_a_child_fails() -> None:
    fake = FakeRuns({"a": ["RUNNING", "FAILED"], "b": ["RUNNING"], "c": ["RUNNING"]})
    with pytest.raises(ParallelRunError, match="DPS") as failure:
        supervise(CHILDREN, fake.supervision())
    assert fake.cancelled == ["b", "c"]
    assert "NOTAS" not in str(failure.value)


def test_supervision_cancels_children_when_the_parent_is_interrupted() -> None:
    fake = FakeRuns({"a": ["RUNNING"], "b": ["RUNNING"], "c": ["RUNNING"]})
    supervision = fake.supervision()

    def interrupt(_seconds: float) -> None:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        supervise(CHILDREN, Supervision(supervision.read, supervision.cancel, supervision.report, sleep=interrupt))
    assert fake.cancelled == ["a", "b", "c"]

from datetime import UTC, datetime, timedelta

import pytest

from pipelines.rj_smfp__nota_carioca_carga_completa.helpers.chain import (
    build_run_tags,
    format_duration,
    is_done,
    item_tag,
    missing_positions,
    not_triggered,
    position_of,
    unfinished,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.helpers.guard import blocking_runs, describe_blocking
from pipelines.rj_smfp__nota_carioca_carga_completa.types import ActiveRun, ChildRun

NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)


def active(state_type: str, name: str = "r", state_name: str | None = None, start: datetime | None = None) -> ActiveRun:
    """Return an active run in a state."""
    return ActiveRun(f"id-{name}", name, state_type, state_name or state_type.title(), start)


@pytest.mark.parametrize(
    ("run", "blocks"),
    [
        pytest.param(active("RUNNING"), True, id="running"),
        pytest.param(active("PENDING"), True, id="pending"),
        pytest.param(active("CANCELLING"), True, id="cancelling"),
        pytest.param(active("SCHEDULED", start=NOW + timedelta(minutes=5)), True, id="scheduled_soon"),
        pytest.param(active("SCHEDULED", start=NOW + timedelta(minutes=10)), True, id="scheduled_at_the_limit"),
        pytest.param(active("SCHEDULED", start=NOW + timedelta(minutes=11)), False, id="scheduled_later"),
        pytest.param(active("SCHEDULED", start=NOW - timedelta(minutes=3)), True, id="late"),
        pytest.param(active("SCHEDULED"), True, id="scheduled_with_no_time"),
        pytest.param(active("COMPLETED"), False, id="completed"),
    ],
)
def test_a_run_blocks_the_chain_when_it_is_active_or_about_to_start(run: ActiveRun, blocks: bool) -> None:
    """Block for a run that is active, or scheduled within 10 minutes, or late."""
    assert bool(blocking_runs([run], NOW)) is blocks


def test_the_blocking_runs_are_described_with_their_links() -> None:
    """List three runs with their link, and count the rest."""
    runs = [active("RUNNING", name) for name in "abcde"]

    text = describe_blocking(runs, lambda run_id: f"<{run_id}>")

    assert text.startswith("5 execuções ativas ou agendadas: a (Running) <id-a>; b")
    assert text.endswith("; e mais 2")
    assert describe_blocking(runs[:1], lambda _: None) == "1 execução ativa ou agendada: a (Running)"


@pytest.mark.parametrize(
    ("current", "expected"),
    [
        pytest.param([], ["disparo:carga-completa"], id="no_tags"),
        pytest.param(
            ["disparo:discord", "discord-user:9", "other", "disparo:carga-completa"],
            ["disparo:carga-completa", "disparo:discord", "discord-user:9"],
            id="audit_tags_without_repeats",
        ),
    ],
)
def test_the_child_runs_carry_the_audit_tags_of_the_chain(current: list[str], expected: list[str]) -> None:
    """Keep the tags of the chain and the audit ones, and drop the others."""
    assert build_run_tags(current) == expected


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        pytest.param(7, "7s", id="seconds"),
        pytest.param(303, "5min 03s", id="minutes"),
        pytest.param(7500, "2h05min", id="hours"),
    ],
)
def test_a_duration_is_written_in_its_largest_units(seconds: int, expected: str) -> None:
    """Write hours, minutes or seconds."""
    assert format_duration(seconds) == expected


def child(state_type: str, name: str = "t", state_name: str | None = None) -> ChildRun:
    """Return a child run in a state."""
    return ChildRun(f"id-{name}", name, state_type, state_name or state_type.title())


def test_a_step_is_unfinished_only_if_a_child_run_did_not_complete() -> None:
    """Give nothing when every child run completed."""
    assert (
        unfinished("Oracle → BigQuery", [child("COMPLETED"), child("COMPLETED", "u")], 3600, ["BigQuery → Oracle"])
        is None
    )


def test_a_child_run_that_failed_or_is_still_going_stops_the_step() -> None:
    """Say how each bad child run ended, and that the next step did not start."""
    reason = unfinished(
        "Oracle → BigQuery",
        [child("COMPLETED", "ok"), child("FAILED", "bad"), child("RUNNING", "slow")],
        3600,
        ["BigQuery → Oracle"],
    )

    assert reason == (
        "Oracle → BigQuery: bad terminou em Failed; slow ainda estava em Running após o limite de 1h00min; "
        "BigQuery → Oracle não foi disparado"
    )
    assert "não foi disparado" not in str(unfinished("BigQuery → Oracle", [child("CRASHED")], 60, []))


@pytest.mark.parametrize(
    ("tags", "expected"),
    [
        pytest.param([item_tag(3), "disparo:discord"], 3, id="the_tag_of_the_position"),
        pytest.param(["disparo:discord"], 0, id="no_position_tag"),
        pytest.param(["carga-completa-item:x"], 0, id="a_position_that_is_not_a_number"),
    ],
)
def test_the_position_of_a_child_run_is_read_from_its_tags(tags: list[str], expected: int) -> None:
    """Read the number after the tag prefix, and give 0 when there is none."""
    assert position_of(tags) == expected


def positioned(position: int, state_type: str = "COMPLETED") -> ChildRun:
    """Return a child run with a position, in a state."""
    return ChildRun(f"id{position}", f"t{position}", state_type, state_type.title(), position)


@pytest.mark.parametrize(
    ("existing", "total", "missing", "done"),
    [
        pytest.param([], 2, [1, 2], False, id="nothing_started"),
        pytest.param([positioned(1)], 3, [2, 3], False, id="started_half_way"),
        pytest.param([positioned(1), positioned(2, "RUNNING")], 2, [], False, id="one_still_runs"),
        pytest.param([positioned(1), positioned(2)], 2, [], True, id="all_completed"),
        pytest.param([positioned(1, "FAILED")], 1, [], False, id="one_failed"),
    ],
)
def test_a_restart_starts_only_the_runs_that_are_missing(
    existing: list[ChildRun], total: int, missing: list[int], done: bool
) -> None:
    """Find the positions with no child run, and tell whether the step is done."""
    assert missing_positions(existing, total) == missing
    assert is_done(existing, total) is done


@pytest.mark.parametrize(
    ("labels", "expected"),
    [
        pytest.param([], "", id="nothing_skipped"),
        pytest.param(["A"], "; A não foi disparado", id="one_step"),
        pytest.param(["A", "B", "C"], "; A, B e C não foram disparados", id="three_steps"),
    ],
)
def test_the_steps_that_do_not_start_are_listed(labels: list[str], expected: str) -> None:
    """Write the skipped steps in the singular or the plural."""
    assert not_triggered(labels) == expected

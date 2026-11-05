"""The words and the tags of the chain: what the Discord lines and the errors say."""

from collections.abc import Iterable, Sequence

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    COMPLETED_STATE,
    DISPATCH_TAG,
    FINISHED_STATES,
    INHERITED_TAG_PREFIXES,
    ITEM_TAG_PREFIX,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.types import ChildRun


def build_run_tags(current_tags: Iterable[str]) -> list[str]:
    """Return the tags of the child runs: the one of the chain and the audit ones.

    The bot puts `disparo:discord` and `discord-user:<id>` on the run. Each tag
    appears once.
    """
    inherited = [tag for tag in current_tags if tag.startswith(INHERITED_TAG_PREFIXES)]

    return list(dict.fromkeys([DISPATCH_TAG, *inherited]))


def item_tag(position: int) -> str:
    """Return the tag that names the position of a child run among its step."""
    return f"{ITEM_TAG_PREFIX}{position}"


def position_of(tags: Iterable[str]) -> int:
    """Read the position of a child run from its tags, or 0 if it has none."""
    for tag in tags:
        if tag.startswith(ITEM_TAG_PREFIX) and tag[len(ITEM_TAG_PREFIX) :].isdigit():
            return int(tag[len(ITEM_TAG_PREFIX) :])

    return 0


def missing_positions(existing: Sequence[ChildRun], total: int) -> list[int]:
    """Return the positions, from 1, of the sets of parameters with no child run yet.

    A run that the chain started before a restart has its position in a tag, so a step
    that stopped half way starts only the missing ones.
    """
    have = {run.position for run in existing}

    return [position for position in range(1, total + 1) if position not in have]


def is_done(existing: Sequence[ChildRun], total: int) -> bool:
    """Tell whether the child runs of a step started before a restart all completed."""
    return not missing_positions(existing, total) and all(run.state_type == COMPLETED_STATE for run in existing)


def not_triggered(labels: Sequence[str]) -> str:
    """Write `; X não foi disparado` or `; X e Y não foram disparados`, or nothing."""
    if not labels:
        return ""

    if len(labels) == 1:
        return f"; {labels[0]} não foi disparado"

    return f"; {', '.join(labels[:-1])} e {labels[-1]} não foram disparados"


def format_duration(seconds: float) -> str:
    """Write a duration as `2h05min`, `5min 03s` or `7s`."""
    total = int(seconds)
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)

    if hours:
        return f"{hours}h{minutes:02d}min"

    return f"{minutes}min {secs:02d}s" if minutes else f"{secs}s"


def unfinished(label: str, runs: Sequence[ChildRun], timeout_seconds: int, skipped: Sequence[str]) -> str | None:
    """Say why a step did not complete, or give nothing if every child run completed.

    A child run that is still going at the timeout stays active. `skipped` are the
    steps that do not start because of this one.
    """
    bad = [run for run in runs if run.state_type != COMPLETED_STATE]

    if not bad:
        return None

    outcomes = [
        f"{run.name} terminou em {run.state_name}"
        if run.state_type in FINISHED_STATES
        else f"{run.name} ainda estava em {run.state_name} após o limite de {format_duration(timeout_seconds)}"
        for run in bad
    ]
    return f"{label}: {'; '.join(outcomes)}{not_triggered(skipped)}"

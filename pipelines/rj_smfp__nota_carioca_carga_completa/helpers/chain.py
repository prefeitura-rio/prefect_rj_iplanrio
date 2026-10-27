"""The words and the tags of the chain: what the Discord lines and the errors say."""

from collections.abc import Iterable, Sequence

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    COMPLETED_STATE,
    DISPATCH_TAG,
    FINISHED_STATES,
    INHERITED_TAG_PREFIXES,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.types import ChildRun

NOT_STARTED = "Carga completa não iniciada"


def build_run_tags(current_tags: Iterable[str]) -> list[str]:
    """Return the tags of the child runs: the one of the chain and the audit ones.

    The bot puts `disparo:discord` and `discord-user:<id>` on the run. Each tag
    appears once.
    """
    inherited = [tag for tag in current_tags if tag.startswith(INHERITED_TAG_PREFIXES)]

    return list(dict.fromkeys([DISPATCH_TAG, *inherited]))


def format_duration(seconds: float) -> str:
    """Write a duration as `2h05min`, `5min 03s` or `7s`."""
    total = int(seconds)
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)

    if hours:
        return f"{hours}h{minutes:02d}min"

    return f"{minutes}min {secs:02d}s" if minutes else f"{secs}s"


def unfinished(label: str, runs: Sequence[ChildRun], timeout_seconds: int, skipped: str | None) -> str | None:
    """Say why a step did not complete, or give nothing if every child run completed.

    A child run that is still going at the timeout stays active. `skipped` is the step
    that is not started because of this one.
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
    tail = f"; {skipped} não foi disparado" if skipped else ""

    return f"{label}: {'; '.join(outcomes)}{tail}"

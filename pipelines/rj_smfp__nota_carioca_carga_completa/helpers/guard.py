"""Decide whether a chained deployment is busy: a run that is active or about to start.

The deployments refuse to run in parallel on their own, but a manual run from the UI
does not go through the bot. The guard keeps the chain from starting, or going on, on
top of it. Reading the API is in `helpers/runs.py`.
"""

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    BUSY_STATES,
    MAX_LISTED_RUNS,
    SCHEDULED_STATE,
    SCHEDULED_WINDOW_MINUTES,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.types import ActiveRun


def blocking_runs(runs: Sequence[ActiveRun], now: datetime) -> list[ActiveRun]:
    """Keep the runs that stop the chain: active, or scheduled within 10 minutes.

    A scheduled run whose start has passed (`Late`) stops it too, and so does one with
    no expected start, to be safe.
    """
    horizon = now + timedelta(minutes=SCHEDULED_WINDOW_MINUTES)

    return [
        run
        for run in runs
        if run.state_type in BUSY_STATES
        or (
            run.state_type == SCHEDULED_STATE
            and (run.expected_start_time is None or run.expected_start_time <= horizon)
        )
    ]


def describe_blocking(runs: Sequence[ActiveRun], link: Callable[[str], str | None]) -> str:
    """Describe the runs that stop the chain, for the error and the log.

    ```text
    2 execuções ativas ou agendadas: abc (Running) <link>; def (Late)
    ```
    """
    listed: list[str] = []

    for run in runs[:MAX_LISTED_RUNS]:
        url = link(run.run_id)
        listed.append(f"{run.name} ({run.state_name}){' ' + url if url else ''}")

    extra = len(runs) - MAX_LISTED_RUNS
    more = f"; e mais {extra}" if extra > 0 else ""
    noun = "execução ativa ou agendada" if len(runs) == 1 else "execuções ativas ou agendadas"

    return f"{len(runs)} {noun}: {'; '.join(listed)}{more}"

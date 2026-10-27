"""Types of rj_smfp__nota_carioca_carga_completa."""

from dataclasses import dataclass
from datetime import datetime

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import BQ_TO_ORACLE_DEPLOYMENT, ORACLE_TO_BQ_DEPLOYMENT


@dataclass(frozen=True)
class Chain:
    """The two deployments to chain, with the waits between and around them.

    The defaults are the deployments of production, and the deployment of staging gives
    the ones of staging. `pause_seconds` is the wait between the two loads, and the
    timeouts are the hours to wait for each load before giving up. A child run that is
    still going at the timeout is not cancelled.
    """

    oracle_to_bq_deployment: str = ORACLE_TO_BQ_DEPLOYMENT
    bq_to_oracle_deployment: str = BQ_TO_ORACLE_DEPLOYMENT
    pause_seconds: int = 30
    oracle_to_bq_timeout_hours: int = 4
    bq_to_oracle_timeout_hours: int = 8


@dataclass(frozen=True)
class Step:
    """One load of the chain: its name, its deployment and its timeout."""

    label: str
    deployment: str
    timeout_seconds: int


@dataclass(frozen=True)
class ActiveRun:
    """What the guard needs of a flow run: its id, name and state.

    `state_name` is the name in the UI (`Late`, `AwaitingRetry`...), and
    `expected_start_time` matters only for a scheduled run.
    """

    run_id: str
    name: str
    state_type: str
    state_name: str
    expected_start_time: datetime | None


@dataclass(frozen=True)
class ChildRun:
    """A child run, as it was in the last state that was read."""

    run_id: str
    name: str
    state_type: str
    state_name: str


class ActiveRunsError(RuntimeError):
    """The deployment has a run that is active or about to start.

    `runs` are the runs that stop the chain.
    """

    def __init__(self, message: str, runs: tuple[ActiveRun, ...]) -> None:
        """Keep the runs, with the message."""
        super().__init__(message)
        self.runs: tuple[ActiveRun, ...] = runs


class ChainStoppedError(RuntimeError):
    """The chain stopped before it ended: a deployment is busy, or a child run failed.

    `announcement` is the line for Discord.
    """

    def __init__(self, message: str, announcement: str) -> None:
        """Keep the announcement, with the message."""
        super().__init__(message)
        self.announcement: str = announcement

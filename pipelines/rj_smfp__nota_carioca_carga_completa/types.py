"""Types of rj_smfp__nota_carioca_carga_completa."""

from dataclasses import dataclass
from datetime import datetime

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    BQ_TO_ORACLE_DEPLOYMENT,
    EXTRACTION_DEPLOYMENT,
    ORACLE_TO_BQ_DEPLOYMENT,
)


@dataclass(frozen=True)
class Chain:
    """The deployments to chain, with the waits between and around them.

    The XML extraction comes first, unless `include_extraction` is false, for example if
    it ran by hand. The defaults are the deployments of production, and the deployment
    of staging gives the ones of staging. `pause_seconds` is the wait between two steps,
    and the timeouts are the hours to wait for each step before giving up. A child run
    that is still going at the timeout is not cancelled. A stop mentions the roles of
    `alert_role_ids`, the numbers of the Discord roles.
    """

    extraction_deployment: str = EXTRACTION_DEPLOYMENT
    oracle_to_bq_deployment: str = ORACLE_TO_BQ_DEPLOYMENT
    bq_to_oracle_deployment: str = BQ_TO_ORACLE_DEPLOYMENT
    include_extraction: bool = True
    pause_seconds: int = 30
    extraction_timeout_hours: int = 9
    oracle_to_bq_timeout_hours: int = 4
    bq_to_oracle_timeout_hours: int = 8
    alert_role_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class Step:
    """One step of the chain: its name, deployment, timeout and word for done.

    A step with `per_schedule` starts one child run for each schedule of its deployment,
    and the others start one with the defaults of the deployment.
    """

    label: str
    deployment: str
    timeout_seconds: int
    done: str = "concluído"
    per_schedule: bool = False


@dataclass(frozen=True)
class Plan:
    """What the chain runs: the steps in order, the tags of the children and the waits.

    `parent_run_id` is the flow run of the chain, to find the children that it started
    before a restart. `alert_roles` are the Discord roles that a stop mentions.
    """

    steps: tuple[Step, ...]
    tags: tuple[str, ...]
    pause_seconds: int
    parent_run_id: str | None
    alert_roles: tuple[str, ...] = ()


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
    """A child run, as it was in the last state that was read.

    `position` counts from 1 among the sets of parameters of its step, and it is 0 for
    a run with no such tag.
    """

    run_id: str
    name: str
    state_type: str
    state_name: str
    position: int = 0


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

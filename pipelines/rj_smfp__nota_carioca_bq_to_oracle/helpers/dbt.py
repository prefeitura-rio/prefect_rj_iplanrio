"""The rules of the optional dbt step: its parameters and the waits.

The waits are for the child run and for BigQuery.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import default_dbt_parameters
from utils.runs import RunStatus

COMPLETED = "COMPLETED"
FINAL_STATE_TYPES = frozenset({COMPLETED, "FAILED", "CRASHED", "CANCELLED"})


class DbtRunError(RuntimeError):
    """O run de dbt não terminou com sucesso."""


class RunClient(Protocol):
    """Operações sobre o run filho de dbt."""

    def read_status(self, run_id: str) -> RunStatus:
        """Lê o estado atual do run."""
        ...

    def cancel(self, run_id: str) -> None:
        """Pede o cancelamento do run."""
        ...


def should_run_dbt(run_dbt: bool, mode: Literal["full", "synonyms_only"]) -> bool:
    """Decide whether dbt runs: if asked, and not in the synonyms-only mode."""
    return run_dbt and mode == "full"


def resolve_dbt_parameters(parameters: dict[str, object] | None) -> dict[str, object]:
    """Choose the parameters of the dbt run.

    `None` gives the defaults of the pipeline. Parameters that are given replace the
    defaults and are not merged with them.
    """
    return default_dbt_parameters() if parameters is None else parameters


def skip_initial_quiet_wait(last_modified: list[datetime], dbt_finished_at: datetime | None) -> bool:
    """Decide whether the first wait for no changes in BigQuery can be skipped.

    It can only if dbt ran here and no table changed after it ended: the changes seen
    are those of dbt itself, which has finished. `dbt_finished_at` is in UTC and `None`
    if dbt did not run.
    """
    return dbt_finished_at is not None and all(modified <= dbt_finished_at for modified in last_modified)


@dataclass(frozen=True)
class RunWaiter:
    """Wait for the child dbt run to end, and poll its state from time to time.

    `runs` gives access to the state and the cancellation of the run. After
    `timeout_seconds` the run is cancelled. `poll_seconds` is the time between polls,
    `report` gets the progress messages, `sleep` waits for a number of seconds, and
    `monotonic` is the monotonic clock.
    """

    runs: RunClient
    timeout_seconds: float
    poll_seconds: float
    report: Callable[[str], None]
    sleep: Callable[[float], None] = time.sleep
    monotonic: Callable[[], float] = time.monotonic

    def wait(self, run_id: str) -> datetime:
        """Wait for the run to end with success, and return its end in UTC.

        If the wait is interrupted (cancel of the parent flow, a termination signal) the
        child run is cancelled before the interruption goes on. Raise `DbtRunError` if
        the run ends without success or runs out of time.
        """
        started = self.monotonic()
        try:
            status = self.poll(run_id, started)
        except BaseException:
            self.cancel_run(run_id, "espera interrompida; cancelando o run de dbt")
            raise
        elapsed = self.monotonic() - started
        if status is None:
            self.cancel_run(
                run_id, f"tempo limite de {self.timeout_seconds / 60:.0f} min excedido; cancelando o run de dbt"
            )
            raise DbtRunError(f"O dbt não terminou em {self.timeout_seconds / 60:.0f} min; o run foi cancelado.")
        if status.state_type != COMPLETED:
            detail = f": {status.message}" if status.message else ""
            raise DbtRunError(f"O dbt terminou em {status.state_name} após {elapsed / 60:.1f} min{detail}")
        return status.end_time or datetime.now(UTC)

    def poll(self, run_id: str, started: float) -> RunStatus | None:
        """Read the run state until it ends, and return it, or nothing on a timeout."""
        while True:
            status = self.runs.read_status(run_id)
            elapsed = self.monotonic() - started
            self.report(f"dbt {status.state_name} há {elapsed / 60:.1f} min")
            if status.state_type in FINAL_STATE_TYPES:
                return status
            if elapsed >= self.timeout_seconds:
                return None
            self.sleep(min(self.poll_seconds, self.timeout_seconds - elapsed))

    def cancel_run(self, run_id: str, message: str) -> None:
        """Report the message and cancel the run."""
        self.report(message)
        self.runs.cancel(run_id)

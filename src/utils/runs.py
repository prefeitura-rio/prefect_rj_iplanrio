"""Prefect calls that start, read and cancel a run of a deployment."""

from dataclasses import dataclass
from datetime import datetime
from typing import cast
from uuid import UUID

from prefect import get_client
from prefect.client.schemas.objects import FlowRun
from prefect.deployments import run_deployment
from prefect.settings import PREFECT_UI_URL
from prefect.states import Cancelling


@dataclass(frozen=True)
class RunStatus:
    """The state of a flow run.

    :param state_type: The type of the state (`COMPLETED`, `FAILED`, `RUNNING`).
    :param state_name: The name of the state, as the UI shows it.
    :param message: The message of the state, if any.
    :param end_time: The end of the run in UTC, once it has ended.
    """

    state_type: str
    state_name: str
    message: str | None
    end_time: datetime | None


@dataclass(frozen=True)
class StartedRun:
    """A flow run that was created for a deployment.

    `url` is the link of the run in the UI, if `PREFECT_UI_URL` is set.
    """

    id: str
    name: str
    url: str | None


def start_deployment_run(deployment: str, parameters: dict[str, object]) -> StartedRun:
    """Create a run of the deployment `<flow>/<deployment>` as a subflow of the current run, and do not wait for it."""
    # run_deployment is sync compatible: it gives the FlowRun at once when it is called from sync code.
    flow_run = cast("FlowRun", run_deployment(name=deployment, parameters=parameters, timeout=0, as_subflow=True))
    ui_url = cast("str | None", PREFECT_UI_URL.value())
    url = f"{ui_url.rstrip('/')}/runs/flow-run/{flow_run.id}" if ui_url else None

    return StartedRun(id=str(flow_run.id), name=flow_run.name, url=url)


class PrefectRuns:
    """Read the state of flow runs and cancel them through the Prefect API."""

    def read_status(self, run_id: str) -> RunStatus:
        """Read the current state of a flow run. Raise `ValueError` if it has none."""
        with get_client(sync_client=True) as client:
            flow_run = client.read_flow_run(UUID(run_id))

        state = flow_run.state

        if state is None:
            raise ValueError(f"The flow run {run_id} has no state")

        return RunStatus(
            state_type=state.type.value,
            state_name=state.name or state.type.value,
            message=state.message,
            end_time=flow_run.end_time,
        )

    def cancel(self, run_id: str) -> None:
        """Move a flow run to `Cancelling`, which the worker turns into a cancellation."""
        with get_client(sync_client=True) as client:
            client.set_flow_run_state(UUID(run_id), Cancelling(), force=True)

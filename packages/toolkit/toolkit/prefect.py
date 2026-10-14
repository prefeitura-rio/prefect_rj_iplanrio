"""Prefect run helpers."""

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta

import yaml
from prefect.client.orchestration import get_client
from prefect.context import TaskRunContext, get_run_context
from pydantic import JsonValue

from . import log


async def rename_current_flow_run(new_name: str) -> None:
    """Rename the flow run that contains the current task.

    :param new_name: The new flow run name.
    """
    context = get_run_context()
    match context:
        case TaskRunContext():
            flow_run_id = context.task_run.flow_run_id
        case _:
            flow_run_id = None
    if flow_run_id is None:
        raise RuntimeError("Rename the flow run from a task that has a flow run")
    async with get_client() as client:
        await client.update_flow_run(flow_run_id=flow_run_id, name=new_name)
    log.info("Flow run renamed", {"new_name": new_name})


def schedules_yaml(
    parameters: Sequence[Mapping[str, JsonValue]],
    interval_seconds: int,
    anchor_date: str,
    stagger_minutes: int,
    timezone: str,
    slug_field: str = "table_id",
) -> str:
    """Return the ``schedules`` block of a ``prefect.yaml``, one schedule per parameter set.

    Each schedule starts ``stagger_minutes`` after the previous one, so the runs do not
    start together.

    :param parameters: The flow parameters of each schedule.
    :param interval_seconds: Time between two runs of one schedule.
    :param anchor_date: ISO date and time of the first run of the first schedule.
    :param stagger_minutes: Delay between the start of one schedule and the next.
    :param timezone: IANA timezone of the schedules.
    :param slug_field: The parameter that names each schedule.
    :raises ValueError: If a parameter set has no ``slug_field``.
    """
    first = datetime.fromisoformat(anchor_date)
    schedules: list[dict[str, JsonValue]] = []
    for index, values in enumerate(parameters):
        if slug_field not in values:
            raise ValueError(
                f"Missing {slug_field!r} in the parameters at index {index}"
            )
        start = first + timedelta(minutes=stagger_minutes * index)
        schedules.append(
            {
                "interval": interval_seconds,
                "anchor_date": start.isoformat(),
                "timezone": timezone,
                "slug": values[slug_field],
                "parameters": dict(values),
            }
        )
    return yaml.dump({"schedules": schedules}, sort_keys=False, indent=2, width=120)

"""Prefect run helpers."""

from prefect.client.orchestration import get_client
from prefect.context import TaskRunContext, get_run_context

from .log import logger


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
    logger.info("Flow run renamed: name=%s", new_name)

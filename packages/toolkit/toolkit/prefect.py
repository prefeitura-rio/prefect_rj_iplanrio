"""Prefect run helpers."""

from prefect.client.orchestration import get_client
from prefect.context import get_run_context

from .log import logger


async def rename_current_flow_run(new_name: str) -> None:
    """Rename the flow run that contains the current task.

    :param new_name: The new flow run name.
    """
    context = get_run_context()
    flow_run_id = context.task_run.flow_run_id  # pyright: ignore[reportAttributeAccessIssue]
    if flow_run_id is None:
        raise RuntimeError("The current task has no flow run")
    async with get_client() as client:
        await client.update_flow_run(flow_run_id=flow_run_id, name=new_name)
    logger.info("Flow run renamed: name=%s", new_name)

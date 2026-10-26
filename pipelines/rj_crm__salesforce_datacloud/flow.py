"""Flow for rj_crm__salesforce_datacloud."""

from prefect import Flow, flow
from prefect.client.schemas.objects import FlowRun, State

from pipelines.rj_crm__salesforce_datacloud.constants import DEFAULT_DATASET_ID, DEFAULT_PROJECT_ID
from pipelines.rj_crm__salesforce_datacloud.helpers.messages import build_run_failure
from pipelines.rj_crm__salesforce_datacloud.tasks import (
    create_tables_task,
    finish_run_task,
    log_in_task,
    notify_task,
    plan_run_task,
    run_feed_task,
)
from pipelines.rj_crm__salesforce_datacloud.types import Window


async def notify_failure(flow: Flow[..., object], flow_run: FlowRun, state: State[object]) -> None:
    """Post a failed run to the channel of the errors."""
    await notify_task(build_run_failure(flow.name, flow_run.name, state.message), errors=True)


@flow(log_prints=True, flow_run_name="salesforce-datacloud", on_failure=[notify_failure])
async def rj_crm__salesforce_datacloud(
    *,
    project_id: str = DEFAULT_PROJECT_ID,
    dataset_id: str = DEFAULT_DATASET_ID,
    window: Window | None = None,
    tables: list[str] | None = None,
) -> dict[str, dict[str, int]]:
    """Extract the Salesforce tables (Data Cloud and the CRM API) to BigQuery.

    It loads them with a MERGE.

    Each table loads into its staging table first.

    - `window` says what to extract: the last hour (every 15 minutes), a whole day (once
      a day), or each day of a reprocess. The MERGE does not duplicate, so a run can
      repeat.
    - `tables` picks some of `FEEDS`, and all of them if empty.
    - A critical table (the Agentforce ones) that fails stops the run. Any other one
      warns Discord and lets the run go on.
    - A day run and a reprocess post a summary.

    Return the rows inserted and updated, by group and table, summed over the days.
    """
    plan = await plan_run_task(window or {}, tables)
    await create_tables_task(project_id, dataset_id)
    session = await log_in_task()
    outcomes = [
        await run_feed_task(feed, session, span, project_id, dataset_id) for span in plan.spans for feed in plan.feeds
    ]

    return await finish_run_task(plan, outcomes)

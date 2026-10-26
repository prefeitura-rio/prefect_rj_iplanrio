"""Flow for rj_crm__salesforce_datacloud."""

from datetime import UTC, datetime
from time import monotonic

from prefect import Flow, flow
from prefect.client.schemas.objects import FlowRun, State
from toolkit.telemetry import logger

from pipelines.rj_crm__salesforce_datacloud.constants import DEFAULT_DATASET_ID, DEFAULT_PROJECT_ID, FEEDS
from pipelines.rj_crm__salesforce_datacloud.helpers.messages import build_feed_failure, build_run_failure, build_summary
from pipelines.rj_crm__salesforce_datacloud.helpers.window import format_span, plan_spans
from pipelines.rj_crm__salesforce_datacloud.tasks import (
    create_tables_task,
    extract_and_load_task,
    log_in_task,
    merge_task,
    notify_task,
)
from pipelines.rj_crm__salesforce_datacloud.types import Totals, Window


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
    """Extract the tables of Salesforce (Data Cloud and the CRM REST API) to BigQuery with a MERGE.

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
    window = window or {}
    unknown = set(tables or []) - {feed.name for feed in FEEDS}

    if unknown:
        raise ValueError(f"Unknown tables: {sorted(unknown)}")

    selected = [feed for feed in FEEDS if not tables or feed.name in tables]
    spans = plan_spans(window, datetime.now(UTC))
    reprocess = "reprocess_from" in window
    label = (
        f"reprocessamento {spans[0].partition_date} a {spans[-1].partition_date} ({len(spans)} dia(s))"
        if reprocess
        else f"modo='{window.get('mode', 'hour')}'"
    )
    started = monotonic()
    totals = Totals()
    logger.info("%s, tables: %s", label, [feed.name for feed in selected])
    await create_tables_task(project_id, dataset_id)
    session = await log_in_task()

    for span in spans:
        for feed in selected:
            try:
                await extract_and_load_task(feed, session, span, project_id, dataset_id)
                merged = await merge_task(feed, project_id, dataset_id)
                count = merged["inserted"] + merged["updated"]
                logger.info(
                    "Feed done: name=%s window=%s inserted=%d updated=%d",
                    feed.name,
                    format_span(span),
                    merged["inserted"],
                    merged["updated"],
                )
            except Exception as error:
                if feed.critical:
                    raise

                logger.warning("A feed failed and the run goes on: name=%s error=%s", feed.name, error)
                await notify_task(build_feed_failure(feed.name, feed.group, format_span(span), str(error)), errors=True)
                totals.failed += [] if feed.name in totals.failed else [feed.name]
                count = 0

            group = totals.rows.setdefault(feed.group, {})
            group[feed.name] = group.get(feed.name, 0) + count

    minutes = (monotonic() - started) / 60

    if reprocess or window.get("mode") == "day":
        await notify_task(build_summary(label, totals.rows, totals.failed, minutes), errors=False)

    logger.info("Done in %.1f minutes, failed: %s", minutes, totals.failed)

    return totals.rows

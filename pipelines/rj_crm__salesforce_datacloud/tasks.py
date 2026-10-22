"""Tasks of rj_crm__salesforce_datacloud."""

from asyncio import to_thread
from datetime import UTC, datetime

from prefect import task
from toolkit.telemetry import logger

from pipelines.rj_crm__salesforce_datacloud.constants import RETRIES, RETRY_DELAYS
from pipelines.rj_crm__salesforce_datacloud.helpers.bq import create_tables, load_staging, read_columns, run_merge
from pipelines.rj_crm__salesforce_datacloud.helpers.salesforce import log_in, query_crm, query_data_cloud
from pipelines.rj_crm__salesforce_datacloud.helpers.transform import build_rows, transform_rows
from pipelines.rj_crm__salesforce_datacloud.helpers.window import to_utc_window
from pipelines.rj_crm__salesforce_datacloud.settings import Settings
from pipelines.rj_crm__salesforce_datacloud.types import Feed, Session, Span
from utils.hooks import retry_on_transient_error
from utils.settings import NoticeSettings
from utils.tasks import notify_discord
from utils.utils import render_query


@task(retries=RETRIES, retry_delay_seconds=30, retry_condition_fn=retry_on_transient_error)
async def log_in_task() -> Session:
    """Log in to Salesforce with the client credentials of the settings, for Data Cloud and the CRM REST API."""
    settings = Settings()

    return await log_in(
        settings.SF_DC_INSTANCE_URL.rstrip("/"),
        settings.SF_DC_CLIENT_ID,
        settings.SF_DC_CLIENT_SECRET.get_secret_value(),
        settings.SF_DC_DATASPACE,
    )


@task
async def create_tables_task(project: str, dataset: str) -> None:
    """Create the tables of the pipeline that do not exist yet."""
    await to_thread(create_tables, project, dataset)


@task(retries=RETRIES, retry_delay_seconds=list(RETRY_DELAYS), retry_condition_fn=retry_on_transient_error)
async def extract_and_load_task(feed: Feed, session: Session, span: Span, project: str, dataset: str) -> int:
    """Extract a feed in a window, transform it, add it to its staging table, and return how many rows came.

    A server error, a 429 and a network error run again, and a 4xx fails at once.
    """
    window, partitions = to_utc_window(span) if feed.utc_window else (span, {})
    query = render_query(__file__, feed.query, start=window.start, end=window.end, **partitions)

    if feed.source == "data_cloud":
        names, values = await query_data_cloud(session, query, feed.order_by, feed.name)
        rows = build_rows(names, values, data_cloud=True)
    else:
        rows = await query_crm(session, query)

    if not rows:
        return 0

    prepared = transform_rows(rows, feed, span.partition_date, datetime.now(UTC))
    await to_thread(load_staging, project, dataset, feed.staging, prepared)
    logger.info("Feed extracted: name=%s rows=%d", feed.name, len(prepared))

    return len(prepared)


@task(retries=2, retry_delay_seconds=60)
async def merge_task(feed: Feed, project: str, dataset: str) -> dict[str, int]:
    """Merge the staging table of a feed into its table by key and day, and return the rows inserted and updated.

    The staging keeps the latest row of each key and day first, because the source sometimes repeats an id. It
    is cleared only after a MERGE that worked.
    """
    target = f"{project}.{dataset}.{feed.name}"
    staging = f"{project}.{dataset}.{feed.staging}"
    columns = [column for column in await to_thread(read_columns, project, staging) if column != feed.primary_key]
    merge_sql = render_query(
        __file__,
        "merge_staging",
        target=target,
        staging=staging,
        primary_key=feed.primary_key,
        partition_field="data_particao",
        set_clause=", ".join(f"t.{column} = s.{column}" for column in columns),
        insert_cols=", ".join([feed.primary_key, *columns]),
        insert_vals=", ".join(f"s.{name}" for name in [feed.primary_key, *columns]),
    )

    return await to_thread(run_merge, project, merge_sql, render_query(__file__, "truncate_staging", staging=staging))


@task
async def notify_task(message: str, *, errors: bool) -> None:
    """Post a message to the channel of the errors or of the summaries, or skip it if that channel is not set.

    A notice never fails the run.
    """
    notices = NoticeSettings()
    webhook = (notices.DISCORD_ERRORS_WEBHOOK_URL if errors else notices.DISCORD_WEBHOOK_URL).get_secret_value()

    if not webhook:
        logger.warning("The webhook is not set, so the notice is skipped")
        return

    try:
        await notify_discord.fn(message, webhook=webhook)
    except Exception as error:
        logger.warning("The notice to Discord failed: %s", error)

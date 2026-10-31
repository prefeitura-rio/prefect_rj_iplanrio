"""Flow for rj_iplanrio__cor_alerts_aggregator."""

from datetime import datetime
from json import dumps
from uuid import uuid4

from prefect import flow
from prefect.concurrency.asyncio import concurrency
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import (
    AGGREGATED_TABLE_ID,
    CONCURRENCY_LIMIT,
    DEFAULT_IMMEDIATE_THRESHOLD,
    DEFAULT_RADIUS_METERS,
    DEFAULT_WINDOW_MINUTES,
    SENT_TABLE_ID,
    SP_TZ,
)
from pipelines.rj_iplanrio__cor_alerts_aggregator.helpers.clusters import build_event, group_alerts, should_send_cluster
from pipelines.rj_iplanrio__cor_alerts_aggregator.helpers.rows import build_cluster_row, build_sent_rows
from pipelines.rj_iplanrio__cor_alerts_aggregator.tasks import (
    fetch_alerts_task,
    load_rows_task,
    open_event_task,
    write_sheet_task,
)
from pipelines.rj_iplanrio__cor_alerts_aggregator.types import Destination, Environment, Row, Rules, Sheet
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.types import Cloud


@flow(log_prints=True, flow_run_name="cor-alerts-aggregator-{environment}", on_failure=[notify_failure])
async def rj_iplanrio__cor_alerts_aggregator(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    environment: Environment = "staging",
    destination: Destination = "google_sheets",
    rules: Rules | None = None,
    sheet: Sheet | None = None,
) -> None:
    """Group the pending COR alerts by place, and send each group that is ready.

    The alerts of one type within the radius of each other (500 meters) form a cluster.
    A cluster goes out when it has 5 alerts, or when its oldest alert is 7 minutes old,
    and waits until then. `destination` is `cor_api` (an event in the COR API),
    `google_sheets` (a row in the aggregated table and in the sheet) or `dry_run` (only
    a log). Each sent alert enters the sent table, so no alert goes out twice. Only one
    run works at a time.
    """
    rules = rules or {}
    threshold = rules.get("immediate_threshold", DEFAULT_IMMEDIATE_THRESHOLD)
    window = rules.get("window_minutes", DEFAULT_WINDOW_MINUTES)

    async with concurrency(CONCURRENCY_LIMIT, occupy=1):
        alerts = await fetch_alerts_task(cloud, window)
        clusters = group_alerts(alerts, rules.get("radius_meters", DEFAULT_RADIUS_METERS))
        now = datetime.now(SP_TZ)
        ready = []

        for cluster in clusters:
            send, reason = should_send_cluster(cluster, threshold, window, now)
            logger.info(
                "Cluster %d (%s): %d alerts, %s", cluster.cluster_id, cluster.alert_type, cluster.alert_count, reason
            )

            if send:
                ready.append(cluster)

        sent: list[Row] = []
        cluster_rows: list[Row] = []

        for cluster in ready:
            if destination == "dry_run":
                logger.info(
                    "Dry run: event=%s alerts=%s", dumps(build_event(cluster, str(uuid4()), now)), cluster.alert_ids
                )
            elif destination == "cor_api":
                group_id = await open_event_task(cluster)

                if group_id:
                    sent += build_sent_rows(cluster.alert_ids, group_id=group_id, environment=environment, now=now)
            else:
                group_id = str(uuid4())
                cluster_rows.append(
                    build_cluster_row(
                        cluster, group_id=group_id, event_id=str(uuid4()), environment=environment, now=now
                    )
                )
                sent += build_sent_rows(cluster.alert_ids, group_id=group_id, environment=environment, now=now)

        if cluster_rows:
            await load_rows_task(cluster_rows, AGGREGATED_TABLE_ID, cloud)

            if sheet:
                await write_sheet_task(cluster_rows, sheet)

        if sent:
            await load_rows_task(sent, SENT_TABLE_ID, cloud)

        logger.info(
            "Done: pending=%d clusters=%d ready=%d sent_alerts=%d", len(alerts), len(clusters), len(ready), len(sent)
        )

"""Flow for rj_iplanrio__alertario_previsao_24h."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_iplanrio__alertario_previsao_24h.constants import ALERT_LOG_TABLE, PERIODS_TABLE
from pipelines.rj_iplanrio__alertario_previsao_24h.helpers import find_alerts
from pipelines.rj_iplanrio__alertario_previsao_24h.tasks import alert_task, build_tables_task, fetch_forecast_task
from pipelines.rj_iplanrio__alertario_previsao_24h.types import Alerts
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_records
from utils.types import Cloud, DumpMode, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="alertario-previsao-24h-{dataset_id}", on_failure=[notify_failure])
async def rj_iplanrio__alertario_previsao_24h(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    dataset_id: str = "brutos_alertario",
    dump_mode: DumpMode = "append",
    alerts: Alerts | None = None,
) -> None:
    """Load the AlertaRio 24-hour forecast to BigQuery, and warn of heavy rain.

    The XML gives four tables that share one execution id: `dim_quadro_sinotico` (one
    row for the run), `dim_previsao_periodo` (a forecast for each period),
    `dim_temperatura_zona` (a zone) and `dim_mares` (a tide). With `alerts["send"]`, a
    message goes to Discord when a period has heavy rain, unless the same message went
    out today or the daily and interval limits stop it. Each sent message adds rows to
    `alertario_precipitacao_alerts_log`.
    """
    rules = alerts or {}
    forecast = build_tables_task(await fetch_forecast_task())
    tables = dict(forecast["tables"])
    heavy_rain = find_alerts(tables.get(PERIODS_TABLE, []))

    if not rules.get("send"):
        logger.warning("The alert on Discord is off: periods with heavy rain=%d", len(heavy_rain))
    elif heavy_rain:
        log_rows = await alert_task(heavy_rain, forecast, project=cloud["project"], dataset_id=dataset_id, rules=rules)

        if log_rows:
            tables[ALERT_LOG_TABLE] = log_rows
    else:
        logger.info("No period with heavy rain")

    for table_id, rows in tables.items():
        table = Table(dataset_id=dataset_id, table_id=table_id)

        with TemporaryDirectory() as directory:
            folder = await write_records(rows, Path(directory))
            await upload_files_to_gcs(
                folder,
                table,
                bucket=cloud["bucket"],
                dump_mode=dump_mode,
                source_format="json",
                options=capture_options(dump_mode),
            )

        await publish_table_in_bq(table, cloud)

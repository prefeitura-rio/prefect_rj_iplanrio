"""Flow for rj_crm__geolocalizacao_residencia."""

from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__geolocalizacao_residencia.constants import DEFAULT_MAX_CONCURRENT
from pipelines.rj_crm__geolocalizacao_residencia.tasks import (
    geocode_fallback_task,
    geocode_geoapify_task,
    geocode_nominatim_task,
    read_addresses_task,
)
from pipelines.rj_crm__geolocalizacao_residencia.types import Source, Strategy
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs, write_parquet_records
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="geo-{strategy}-{table[table_id]}", on_failure=[notify_failure])
async def rj_crm__geolocalizacao_residencia(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table: Table,
    strategy: Strategy = "fallback",
    max_concurrent: int = DEFAULT_MAX_CONCURRENT,
    source: Source = "citizens",
) -> None:
    """Find the coordinates of citizen home addresses that have none, and load them.

    The load goes to BigQuery.

    The flow reads new addresses at random from the `source` and geocodes them by
    `strategy`. The source `citizens` gives up to 10,000 addresses of citizens, and
    `chamados` up to 50,000 addresses of the calls to 1746, which the schedule
    `chamados` loads to the project and the bucket of its own `cloud`.

    - `nominatim`: the Nominatim server of the settings, fast and with less precision,
      `max_concurrent` at one time.
    - `fallback`: Waze, MapTiler, Geocode.xyz, LocationIQ and OpenCage in turn, each
      taking what the last did not find.
    - `geoapify_batch`: the batch API of Geoapify, medium speed and good precision.

    Every row that got a point is loaded with its geocode and its plus code, all as
    text.
    """
    addresses = await read_addresses_task(source, cloud["project"])

    if not addresses:
        logger.info("No address to geocode")
        return

    if strategy == "nominatim":
        rows = await geocode_nominatim_task(addresses, max_concurrent)
    elif strategy == "fallback":
        rows = await geocode_fallback_task(addresses)
    else:
        rows = await geocode_geoapify_task(addresses)

    if not rows:
        logger.info("No address was geocoded by the strategy %s, so nothing is loaded", strategy)
        return

    with TemporaryDirectory() as directory:
        folder = Path(directory)
        await write_parquet_records(rows, folder)
        await upload_files_to_gcs(
            folder,
            table,
            bucket=cloud["bucket"],
            dump_mode="append",
            source_format="parquet",
            options=capture_options("append"),
        )

    await publish_table_in_bq(table, cloud)

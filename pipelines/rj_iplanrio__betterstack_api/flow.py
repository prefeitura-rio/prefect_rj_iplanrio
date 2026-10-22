from pathlib import Path

from prefect import flow

from pipelines.rj_iplanrio__betterstack_api.constants import BetterStackConstants
from pipelines.rj_iplanrio__betterstack_api.helpers.tasks import create_date_partitions
from pipelines.rj_iplanrio__betterstack_api.tasks import (
    calculate_date_range,
    fetch_incidents,
    get_betterstack_credentials,
    get_betterstack_monitor_id,
    transform_incidents,
)
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True)
async def rj_iplanrio__betterstack_api(
    *,
    cloud: Cloud,
    from_date: str | None = None,
    to_date: str | None = None,
    dataset_id: str | None = None,
    billing_project_id: str | None = None,
):
    """
    Flow para extrair dados da BetterStack API (Incidents)
    e carregar no BigQuery.
    """

    # 0. Setup
    billing_project_id = billing_project_id or BetterStackConstants.BILLING_PROJECT_ID.value
    dataset_id = dataset_id or BetterStackConstants.DATASET_ID.value

    # 0.1 Inject BD Credentials

    # 1. Credentials
    token = get_betterstack_credentials()
    monitor_id = get_betterstack_monitor_id()


    # 2. Date Logic
    # For high-availability, we always fetch the last 24h.
    # The 'date' parameters remain for manual backfills if needed.
    date_range = calculate_date_range(from_date=from_date, to_date=to_date)

    # --- TABLE: Incidents ---
    raw_incidents = fetch_incidents(token=token, monitor_id=monitor_id, date_range=date_range)

    df_incidents = transform_incidents(raw_incidents)

    if not df_incidents.empty:
        path_incidents = create_date_partitions(
            dataframe=df_incidents,
            partition_column=BetterStackConstants.PARTITION_COLUMN.value,
            file_format=BetterStackConstants.FILE_FORMAT.value,
            root_folder=f"{BetterStackConstants.ROOT_FOLDER.value}/{BetterStackConstants.TABLE_ID_INCIDENTS.value}"
        )

        table = Table(dataset_id=dataset_id, table_id=BetterStackConstants.TABLE_ID_INCIDENTS.value)
        await upload_files_to_gcs(
            Path(path_incidents),
            table,
            bucket=cloud["bucket"],
            dump_mode=BetterStackConstants.DUMP_MODE.value,
            source_format="parquet",
            options=capture_options(BetterStackConstants.DUMP_MODE.value),
        )
        await publish_table_in_bq(table, cloud)
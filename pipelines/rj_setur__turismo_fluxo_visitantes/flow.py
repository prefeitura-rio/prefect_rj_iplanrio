"""Flow for rj_setur__turismo_fluxo_visitantes."""

from prefect import flow

from pipelines.rj_setur__turismo_fluxo_visitantes.tasks import (
    extract_range_to_csv,
    get_google_credentials,
    list_spreadsheets_in_folder,
    plan_extraction_jobs,
)
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, DumpMode, Table


@flow(log_prints=True, flow_run_name="{dataset_id}_geodata_batch", on_failure=[notify_failure])
async def rj_setur__turismo_fluxo_visitantes(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    folder_id: str = "1d_SxiMsXQd2JH1Ttik3OQDRJy7wLtX6S",
    dataset_id: str = "brutos_turismo_fluxo_visitante",
    dump_mode: DumpMode = "overwrite",
) -> None:
    """Read the tourism visitor flow of several Sheets and load it to BigQuery."""
    credentials = get_google_credentials()
    spreadsheets = list_spreadsheets_in_folder(folder_id=folder_id, credentials=credentials)
    extraction_jobs = plan_extraction_jobs(
        spreadsheets=spreadsheets, credentials=credentials, dataset_id=dataset_id, dump_mode=dump_mode
    )

    for job in extraction_jobs:
        folder, table_id = extract_range_to_csv(job, credentials)
        table = Table(dataset_id=job["dataset_id"], table_id=table_id)
        await upload_files_to_gcs(
            folder, table, bucket=cloud["bucket"], dump_mode=job["dump_mode"], source_format="csv"
        )
        await publish_table_in_bq(table, cloud)

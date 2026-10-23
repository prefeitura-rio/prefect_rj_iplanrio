"""Flow for rj_setur__turismo_fluxo_visitantes."""

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_setur__turismo_fluxo_visitantes.helpers import get_worksheet_ranges_config
from pipelines.rj_setur__turismo_fluxo_visitantes.tasks import (
    extract_range_to_csv,
    get_google_credentials,
    get_worksheet_info,
    list_spreadsheets_in_folder,
)
from pipelines.rj_setur__turismo_fluxo_visitantes.types import ExtractionJob
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
    """Read tourism visitor flow data from multiple Google Sheets and upload to BigQuery."""
    credentials = get_google_credentials()
    worksheet_ranges_config = get_worksheet_ranges_config()
    spreadsheets = list_spreadsheets_in_folder(folder_id=folder_id, credentials=credentials)

    if not spreadsheets:
        logger.info("No spreadsheets found in folder")
        return

    extraction_jobs: list[ExtractionJob] = []

    for spreadsheet in spreadsheets:
        spreadsheet_url = spreadsheet["url"]
        spreadsheet_name = spreadsheet["name"]
        spreadsheet_year = spreadsheet["year"]
        worksheets = get_worksheet_info(spreadsheet_url=spreadsheet_url, credentials=credentials)

        for worksheet in worksheets:
            worksheet_title = str(worksheet["title"])
            worksheet_index = int(worksheet["index"])

            if worksheet_title not in worksheet_ranges_config:
                continue

            ranges_config = worksheet_ranges_config[worksheet_title]

            for cell_range, table_suffix in ranges_config:
                job: ExtractionJob = {
                    "spreadsheet_url": spreadsheet_url,
                    "spreadsheet_name": spreadsheet_name,
                    "spreadsheet_year": spreadsheet_year,
                    "worksheet_index": worksheet_index,
                    "worksheet_title": worksheet_title,
                    "cell_range": cell_range,
                    "table_suffix": table_suffix,
                    "dataset_id": dataset_id,
                    "dump_mode": dump_mode,
                }
                table_name = f"{worksheet_title}{'_' + table_suffix if table_suffix else ''}_{spreadsheet_year}"
                logger.info(f"  Queued: {table_name} from {cell_range}")
                extraction_jobs.append(job)

    logger.info(f"\nPrepared {len(extraction_jobs)} extraction tasks")
    logger.info("Starting sequential extraction...\n")

    for job in extraction_jobs:
        folder, table_id = extract_range_to_csv(job, credentials)
        table = Table(dataset_id=job["dataset_id"], table_id=table_id)
        await upload_files_to_gcs(
            folder, table, bucket=cloud["bucket"], dump_mode=job["dump_mode"], source_format="csv"
        )
        await publish_table_in_bq(table, cloud)

    logger.info(f"\n✓ All {len(extraction_jobs)} extractions completed successfully")

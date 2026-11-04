"""Flow for rj_smfp__crf."""

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_smfp__crf.helpers import parse_date
from pipelines.rj_smfp__crf.settings import Settings
from pipelines.rj_smfp__crf.tasks import process_zip_files_task, read_watermark_task
from pipelines.rj_smfp__crf.types import CrfTable
from utils.constants import DEFAULT_CLOUD
from utils.hooks import notify_failure
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="{dataset_id}.{table_id}", on_failure=[notify_failure])
async def rj_smfp__crf(
    *,
    cloud: Cloud = DEFAULT_CLOUD,
    table_id: CrfTable,
    dataset_id: str = "brutos_crf",
    data_inicio: date | str | None = None,
    data_fim: date | str | None = None,
) -> None:
    """Load one table of the CRF from the ZIP files of a bucket to BigQuery.

    The window of the ZIP files is `(data_inicio, data_fim]`, by the date in the name of
    each file. With neither limit, `data_inicio` is the latest `data_particao` of the
    table, so only the new files load. `data_fim` needs `data_inicio`. Each ZIP file
    gives the rows of its day, in the `data_particao` column, and they are added to the
    table. Run it again for the same window and nothing repeats.
    """
    if data_fim is not None and data_inicio is None:
        raise ValueError(
            "data_fim não pode ser fornecido sem data_inicio. Forneça data_inicio para o início do intervalo."
        )

    table = Table(dataset_id=dataset_id, table_id=table_id)
    start = parse_date(data_inicio, "data_inicio")

    if data_inicio is None and data_fim is None:
        start = await read_watermark_task(f"{cloud['project']}.{dataset_id}_staging.{table_id}", cloud["project"])

    settings = Settings()

    with TemporaryDirectory() as directory:
        folder = Path(directory)
        files = await process_zip_files_task(
            settings.CRF_BUCKET_NAME,
            settings.CRF_FOLDER_PREFIX_PERIODOS_EVENTOS,
            table_id,
            folder,
            (start, parse_date(data_fim, "data_fim")),
        )

        if not files:
            logger.info("Nenhum arquivo ZIP no intervalo: flow encerrado sem carga")
            return

        await upload_files_to_gcs(
            folder, table, bucket=cloud["bucket"], dump_mode="append", options=capture_options("append")
        )

    await publish_table_in_bq(table, cloud)

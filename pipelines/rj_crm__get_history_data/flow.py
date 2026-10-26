"""Flow for rj_crm__get_history_data."""

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__get_history_data.constants import DEFAULT_WINDOW_DAYS
from pipelines.rj_crm__get_history_data.helpers.history import build_rows
from pipelines.rj_crm__get_history_data.settings import Settings
from pipelines.rj_crm__get_history_data.tasks import authenticate_task, extract_task, list_extensions_task, load_task
from utils.hooks import notify_failure


@flow(log_prints=True, flow_run_name="get-history-data", on_failure=[notify_failure])
async def rj_crm__get_history_data(
    *,
    rest_uri: str | None = None,
    soap_uri: str | None = None,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> None:
    """Load the history Data Extensions of SFMC to BigQuery.

    The flow logs in to SFMC, lists the Data Extensions whose name ends in `historico`,
    and reads the records of each. One row goes out for each record, with the phone, the
    primary key, the entry date and the whole record as JSON. The rows whose entry date
    is in the last `window_days` days (30 by default) replace the tmp table
    `historico_sfmc_tmp` and are MERGEd into `historico_sfmc` by name, phone and entry
    date. A row with no entry date is always kept. The addresses come from the settings
    unless `rest_uri` and `soap_uri` are given. A Data Extension that fails is logged
    and the others still load.
    """
    settings = Settings()
    rest = rest_uri or settings.SFMC_REST_BASE_URL
    soap = soap_uri or settings.SFMC_SOAP_BASE_URL
    token = await authenticate_task()
    extensions = await list_extensions_task(token, soap)

    if not extensions:
        logger.info("No Data Extension ends in historico")
        return

    results = [await extract_task(token, soap, rest, extension) for extension in extensions]
    failed = [result.name for result in results if result.error is not None]
    logger.info("Extraction done: extensions=%d failed=%s", len(results), failed)
    await load_task(build_rows(results), window_days)

"""Flow for rj_crm__relatorio_engajamento_hsm."""

from datetime import date, datetime

from prefect import flow
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import DRIVE_ROOT_ID, SP_TZ
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks import (
    classify_task,
    extract_task,
    judge_task,
    publish_task,
    read_dispatches_task,
    report_exists_task,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Limits, Report
from utils.constants import BIFROST_MODEL
from utils.hooks import notify_failure


@flow(log_prints=True, flow_run_name="relatorio-engajamento-hsm", on_failure=[notify_failure])
async def rj_crm__relatorio_engajamento_hsm(
    *,
    reference_date: str | None = None,
    drive_folder_id: str = DRIVE_ROOT_ID,
    model: str = BIFROST_MODEL,
    limits: Limits | None = None,
) -> None:
    """Publish the engagement report of the HSMs that asked for one on a day.

    The flow reads `disparos_ativos` for the rows whose `relatorio_engajamento_data_geracao` is the
    reference date (today in São Paulo by default) and that have a campaign and a dispatch date. For each
    HSM it extracts the conversations since the dispatch date, discovers the categories and classifies the
    conversations with the model, has a second call judge a sample, and publishes a Word report with two
    CSV files (`;` separated) in the folder of the HSM. A report that exists for the HSM and the day is
    skipped, so a repeat on the same day adds nothing. `limits` sets the sizes of the batches and samples.
    """
    reference = date.fromisoformat(reference_date) if reference_date else datetime.now(SP_TZ).date()
    dispatches = await read_dispatches_task(reference)

    if not dispatches:
        logger.info("No dispatch asked for a report on %s", reference)
        return

    for dispatch in dispatches:
        if await report_exists_task(dispatch.hsm_name, reference, drive_folder_id):
            logger.info("The report of %s for %s already exists, skipping", dispatch.hsm_name, reference)
            continue

        extraction = await extract_task(dispatch)

        if not extraction.conversations:
            logger.info("No engaged session of %s since %s", dispatch.hsm_name, dispatch.sent_on)
            continue

        items, catalog = await classify_task(extraction.conversations, model, limits or {})

        if not items:
            logger.info("No conversation of %s was classified, so there is no report", dispatch.hsm_name)
            continue

        judged = await judge_task(items, catalog, model, limits or {})
        report = Report(dispatch, reference, judged, catalog, extraction.total_dispatches, extraction.engaged)
        await publish_task(report, drive_folder_id, (limits or {}).get("examples_per_category", 5))

    logger.info("Done")

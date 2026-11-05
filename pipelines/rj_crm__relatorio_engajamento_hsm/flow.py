"""Flow for rj_crm__relatorio_engajamento_hsm."""

from datetime import date, datetime

from prefect import flow
from prefect.runtime import flow_run
from toolkit.telemetry import logger

from pipelines.rj_crm__relatorio_engajamento_hsm.constants import DRIVE_ROOT_ID, SP_TZ
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks import (
    mark_status_task,
    process_request_task,
    read_dispatch_jobs_task,
    read_form_jobs_task,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Limits, Run, Source
from utils.constants import BIFROST_MODEL
from utils.hooks import notify_failure


@flow(log_prints=True, flow_run_name="relatorio-engajamento-hsm", on_failure=[notify_failure])
async def rj_crm__relatorio_engajamento_hsm(
    *,
    source: Source = "form",
    reference_date: str | None = None,
    released_campaigns: list[str] | None = None,
    limits: Limits | None = None,
    drive_folder_id: str = DRIVE_ROOT_ID,
) -> None:
    """Publish the engagement reports that were asked for, to the Drive of the campaign.

    `source` picks the trigger. With `form` (the default), the flow reads the pending
    requests of the Google Form response sheet, makes each one on its own, and writes
    its status and link back to the row. A request is a validation material, an
    engagement report with the catalog that the secretariat validated, or a quick report
    with an automatic catalog. With `dispatches`, the flow reads `disparos_ativos` for
    the HSMs whose report day is the reference day, and makes a quick report of each.
    It has no sheet, so the status only goes to the log.

    `reference_date` is the day to compare with the generation date (today in São Paulo
    by default). `released_campaigns` lets a campaign that waits above the ceiling run
    in this run only. `limits` sets the ceilings, the sizes of the samples and the
    batches, the concurrency and the number of examples. A request with an unexpected
    error gets its status, and the flow fails after all the others finish, so the alert
    still fires.
    """
    now = datetime.now(SP_TZ)
    today = date.fromisoformat(reference_date) if reference_date else now.date()
    run_id = flow_run.get_id()

    if source == "form":
        jobs = await read_form_jobs_task(today, released_campaigns or [], run_id, now)
    else:
        jobs = await read_dispatch_jobs_task(today)

    if not jobs:
        logger.info("Nothing to make on %s", today)
        return

    run = Run(drive_folder_id, now, BIFROST_MODEL, limits or {}, run_id)
    unexpected: list[str] = []

    for job in jobs:
        outcome = await process_request_task(job, run)
        logger.info("Request ended: campaign=%s status=%s", job.campaign, outcome.status)

        if job.key:
            await mark_status_task(job.key, outcome.status, outcome.link, now)

        if outcome.unexpected:
            unexpected.append(outcome.unexpected)

    if unexpected:
        raise RuntimeError(f"{len(unexpected)} request(s) failed with an unexpected error:\n" + "\n".join(unexpected))

    logger.info("Done")

"""Flow for rj_smfp__nota_carioca_carga_completa."""

from typing import cast

from prefect import flow
from prefect.runtime import flow_run
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    BQ_TO_ORACLE_LABEL,
    ORACLE_TO_BQ_LABEL,
    SECONDS_PER_HOUR,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.helpers.chain import build_run_tags
from pipelines.rj_smfp__nota_carioca_carga_completa.helpers.steps import guard, run_step
from pipelines.rj_smfp__nota_carioca_carga_completa.tasks import notify_task, pause_task, read_parameter_sets_task
from pipelines.rj_smfp__nota_carioca_carga_completa.types import Chain, Step

DEFAULT_CHAIN = Chain()


@flow(log_prints=True, flow_run_name="carga-completa")
async def rj_smfp__nota_carioca_carga_completa(
    *, chain: Chain = DEFAULT_CHAIN, discord_notifications: bool = True
) -> None:
    """Load Oracle to BigQuery and, only if that completes, BigQuery to Oracle.

    Each load is a set of child runs of its deployment, linked to this run as subflows:
    one for each schedule of the Oracle to BigQuery deployment (one table each, with its
    parameters), and one for BigQuery to Oracle with its own defaults. The pod only
    waits, and the load runs in the pods of the children. Before the loads a guard
    refuses to start if a deployment has a run that is active or about to start. If a
    child run does not complete, this run fails and the next load is not started. A
    child run that is still going at the timeout is not cancelled. Discord gets one line
    for the start, the move to the second load, the end, or the stop.
    """
    enabled = discord_notifications
    tags = build_run_tags(cast("list[str]", flow_run.tags))
    first = Step(ORACLE_TO_BQ_LABEL, chain.oracle_to_bq_deployment, chain.oracle_to_bq_timeout_hours * SECONDS_PER_HOUR)
    second = Step(
        BQ_TO_ORACLE_LABEL, chain.bq_to_oracle_deployment, chain.bq_to_oracle_timeout_hours * SECONDS_PER_HOUR
    )

    await guard(first, enabled=enabled)
    await guard(second, enabled=enabled)
    logger.info("Carga completa: %s, depois %s.", first.label, second.label)
    await notify_task(f"🔗 Carga completa iniciada: {first.label}, depois {second.label}", enabled=enabled)
    await run_step(first, await read_parameter_sets_task(first.deployment), tags, second, enabled=enabled)
    await notify_task(f"⏭️ {first.label} concluído; {second.label} começa em {chain.pause_seconds} s", enabled=enabled)
    await pause_task(chain.pause_seconds)
    await guard(second, enabled=enabled)
    await run_step(second, [{}], tags, None, enabled=enabled)
    await notify_task("✅ Carga completa concluída", enabled=enabled)

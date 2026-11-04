"""Flow for rj_smfp__nota_carioca_carga_completa."""

from typing import cast

from prefect import flow
from prefect.runtime import flow_run

from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    BQ_TO_ORACLE_LABEL,
    EXTRACTION_LABEL,
    ORACLE_TO_BQ_LABEL,
    SECONDS_PER_HOUR,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.helpers.chain import build_run_tags
from pipelines.rj_smfp__nota_carioca_carga_completa.helpers.steps import run_chain
from pipelines.rj_smfp__nota_carioca_carga_completa.types import Chain, Plan, Step

DEFAULT_CHAIN = Chain()


def build_plan(chain: Chain, tags: tuple[str, ...], parent_run_id: str | None) -> Plan:
    """Return the steps in order: the extraction, if it is on, and the two loads."""
    extraction = (
        Step(
            EXTRACTION_LABEL,
            chain.extraction_deployment,
            chain.extraction_timeout_hours * SECONDS_PER_HOUR,
            done="concluída",
        ),
    )
    loads = (
        Step(
            ORACLE_TO_BQ_LABEL,
            chain.oracle_to_bq_deployment,
            chain.oracle_to_bq_timeout_hours * SECONDS_PER_HOUR,
            per_schedule=True,
        ),
        Step(BQ_TO_ORACLE_LABEL, chain.bq_to_oracle_deployment, chain.bq_to_oracle_timeout_hours * SECONDS_PER_HOUR),
    )

    return Plan(
        (*(extraction if chain.include_extraction else ()), *loads),
        tags,
        chain.pause_seconds,
        parent_run_id,
        chain.alert_role_ids,
    )


@flow(log_prints=True, flow_run_name="carga-completa")
async def rj_smfp__nota_carioca_carga_completa(
    *, chain: Chain = DEFAULT_CHAIN, discord_notifications: bool = True
) -> None:
    """Run the XML extraction, Oracle to BigQuery and BigQuery to Oracle, in this order.

    Each step is a set of child runs of its deployment, linked to this run as subflows:
    one for each schedule of the Oracle to BigQuery deployment (one table each, with its
    parameters), and one for each of the others, with their own defaults. The pod only
    waits, and the work runs in the pods of the children. Each step starts only if the
    last one completed. Before the steps a guard refuses to start if a deployment has a
    run that is active or about to start. If a child run does not complete, this run
    fails and the next steps are not started. A child run that is still going at the
    timeout is not cancelled.

    If the pod stops, Prefect sends this flow run again. It finds the child runs that it
    started, does not repeat the steps that completed, and waits for those that still
    run. Discord gets one line for the start, each move to the next step, the end, or
    the stop. A stop mentions the roles of `chain.alert_role_ids`, and
    `chain.include_extraction` off skips the extraction, for example if it ran by hand.
    """
    tags = tuple(build_run_tags(cast("list[str]", flow_run.tags)))
    run_id = cast("str | None", flow_run.id)
    parent = run_id or None

    await run_chain(build_plan(chain, tags, parent), enabled=discord_notifications)

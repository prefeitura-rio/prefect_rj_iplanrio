"""The steps of the chain: guard a deployment, and run a load and check its end."""

from collections.abc import Mapping, Sequence

from pipelines.rj_smfp__nota_carioca_carga_completa.helpers.chain import NOT_STARTED, unfinished
from pipelines.rj_smfp__nota_carioca_carga_completa.tasks import ensure_idle_task, notify_task, run_children_task
from pipelines.rj_smfp__nota_carioca_carga_completa.types import ActiveRunsError, ChainStoppedError, Step


async def stop(message: str, announcement: str, *, enabled: bool) -> None:
    """Tell Discord why the chain stopped, and raise the error."""
    await notify_task(announcement, enabled=enabled)

    raise ChainStoppedError(message, announcement)


async def guard(step: Step, *, enabled: bool) -> None:
    """Stop the chain if the deployment of the step is busy with a run."""
    try:
        await ensure_idle_task(step.deployment)
    except ActiveRunsError as error:
        await stop(
            f"{NOT_STARTED}: {step.label} ({step.deployment}) tem {error}",
            f"⛔ {NOT_STARTED}: {step.label} já tem execução ativa",
            enabled=enabled,
        )


async def run_step(
    step: Step,
    parameter_sets: Sequence[Mapping[str, object]],
    tags: Sequence[str],
    skipped: Step | None,
    *,
    enabled: bool,
) -> None:
    """Run the child runs of a step, wait, and stop the chain if one did not complete.

    `skipped` is the step that does not start because of this one, none for the last.
    """
    children = await run_children_task(step.deployment, parameter_sets, step.timeout_seconds, tags)
    reason = unfinished(step.label, children, step.timeout_seconds, skipped.label if skipped else None)

    if reason:
        await stop(
            f"Carga completa interrompida: {reason}.", f"⛔ Carga completa interrompida: {reason}", enabled=enabled
        )

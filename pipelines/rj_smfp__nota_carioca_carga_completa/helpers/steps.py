"""The steps of the chain: guard a deployment, run a step, walk through the chain."""

from collections.abc import Mapping, Sequence
from time import monotonic

from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_carga_completa.helpers.chain import (
    NOT_STARTED,
    format_duration,
    is_done,
    unfinished,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.tasks import (
    ensure_idle_task,
    find_children_task,
    notify_task,
    pause_task,
    read_parameter_sets_task,
    run_children_task,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.types import (
    ActiveRunsError,
    ChainStoppedError,
    ChildRun,
    Plan,
    Step,
)


async def stop(message: str, announcement: str, plan: Plan, *, enabled: bool) -> None:
    """Tell Discord why the chain stopped, with the alert roles, and raise the error."""
    await notify_task(announcement, enabled=enabled, roles=plan.alert_roles)

    raise ChainStoppedError(message, announcement)


async def guard(step: Step, headline: str, plan: Plan, *, enabled: bool) -> None:
    """Stop the chain if the deployment of the step is busy with a run."""
    try:
        await ensure_idle_task(step.deployment)
    except ActiveRunsError as error:
        who = ", ".join(f"{run.name} ({run.state_name})" for run in error.runs)
        await stop(
            f"{headline}: {step.label} ({step.deployment}) tem {error}",
            f"⛔ {headline}: {step.label} já tem execução ativa ({who})",
            plan,
            enabled=enabled,
        )


async def run_step(
    step: Step,
    plan: Plan,
    parameter_sets: Sequence[Mapping[str, object]],
    existing: Sequence[ChildRun],
    *,
    enabled: bool,
) -> None:
    """Run the child runs of a step, wait, and stop the chain if one did not complete.

    `existing` are the child runs that the chain started before a restart: they are
    waited for and not started again. The steps after this one do not start if it fails.
    """
    children = await run_children_task(step, parameter_sets, plan.tags, existing)
    later = plan.steps[plan.steps.index(step) + 1 :]
    reason = unfinished(step.label, children, step.timeout_seconds, [item.label for item in later])

    if reason:
        await stop(
            f"Carga completa interrompida: {reason}.",
            f"⛔ Carga completa interrompida: {reason}",
            plan,
            enabled=enabled,
        )


async def read_sets(step: Step) -> list[dict[str, object]]:
    """Read the parameters of each schedule of a step, or one set with the defaults."""
    return await read_parameter_sets_task(step.deployment) if step.per_schedule else [{}]


async def announce_start(plan: Plan, own: Mapping[str, Sequence[ChildRun]], *, enabled: bool) -> None:
    """Tell Discord that the chain starts, or that it goes on after a restart."""
    sequence = ", depois ".join(step.label for step in plan.steps)
    logger.info("Carga completa: %s.", sequence)
    found = ", ".join(
        f"{step.label} ({len(own[step.deployment])} filhos)" for step in plan.steps if own[step.deployment]
    )

    if found:
        logger.info("Retomada depois de um reinício do orquestrador; filhos já disparados: %s.", found)
        line = f"🔁 Carga completa retomada depois de um reinício do orquestrador: {found}"
        await notify_task(f"{line}; os passos concluídos não rodam de novo", enabled=enabled)
    else:
        await notify_task(f"🔗 Carga completa iniciada: {sequence}", enabled=enabled)


async def advance(step: Step, previous: Step | None, plan: Plan, *, enabled: bool) -> None:
    """Wait after the previous step, then check that this deployment is idle."""
    if previous is None:
        return

    line = f"{previous.label} {previous.done}; {step.label} começa em {plan.pause_seconds} s"
    logger.info(line)
    await notify_task(f"⏭️ {line}", enabled=enabled)
    await pause_task(plan.pause_seconds)
    await guard(step, f"Carga completa interrompida ({previous.label} {previous.done})", plan, enabled=enabled)


async def run_chain(plan: Plan, *, enabled: bool) -> None:
    """Run the steps in order, and stop at the first one that does not complete.

    After a restart of the pod Prefect sends the same flow run again. The steps whose
    child runs all completed do not run again, the ones still going are waited for, and
    only the steps that never started pass the guard and start.
    """
    started = monotonic()
    own = {step.deployment: await find_children_task(step, plan.parent_run_id) for step in plan.steps}

    for step in plan.steps:
        if not own[step.deployment]:
            await guard(step, NOT_STARTED, plan, enabled=enabled)

    await announce_start(plan, own, enabled=enabled)
    previous: Step | None = None

    for step in plan.steps:
        existing = own[step.deployment]
        sets = await read_sets(step)

        if existing and is_done(existing, len(sets)):
            logger.info("%s: já %s antes do reinício.", step.label, step.done)
        else:
            await advance(step, previous if not existing else None, plan, enabled=enabled)
            await run_step(step, plan, sets, existing, enabled=enabled)

        previous = step

    took = format_duration(monotonic() - started)
    logger.info("Carga completa concluída em %s.", took)
    await notify_task(f"✅ Carga completa concluída em {took}", enabled=enabled)

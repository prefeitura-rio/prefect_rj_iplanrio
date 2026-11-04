"""The extraction from start to end: call, follow the task, wait for the release.

It imports neither Prefect nor the network: `tasks.py` injects the actions, and the
tests inject fakes. `Conflict`, another extraction that runs, is no error: the one that
runs is followed to its end, and the load goes on if it succeeds.
"""

from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import CONFLICT_ATTEMPTS, LABEL, SECONDS_PER_HOUR
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.format import format_duration
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.soap import decide
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.wait import wait_for_task, wait_until_idle
from pipelines.rj_smfp__nota_carioca_extracao_xml.types import (
    Decision,
    ExtractionFailedError,
    ExtractionRefusedError,
    ExtractionServices,
    ExtractionTimedOutError,
    Limits,
    Outcome,
    WebserviceError,
)


async def start_extraction(services: ExtractionServices, limits: Limits, deadline: float) -> Decision:
    """Call the web service and return the task to follow.

    On `Conflict`, the task is the extraction that already runs.

    Raise `ExtractionRefusedError` if the service refuses, or says `Conflict` with no
    extraction in the table for `CONFLICT_ATTEMPTS` calls.
    """
    for attempt in range(1, CONFLICT_ATTEMPTS + 1):
        response = await services.call()
        logger.info(
            "Webservice: %s, TarefaId %d, mensagem %r", response.status_code, response.task_id, response.message
        )
        decision = decide(response)

        match decision.outcome:
            case Outcome.STARTED:
                await services.notify(f"📥 {LABEL} iniciada: tarefa {decision.task_id}")

                return decision
            case Outcome.NOTHING_PENDING:
                return decision
            case Outcome.ALREADY_RUNNING:
                running = await services.store.extraction_in_progress()

                if running is not None:
                    await services.notify(
                        f"⏳ {LABEL} já em execução por outro disparo ({running.describe()}); "
                        + "aguardando terminar para seguir"
                    )

                    return Decision(Outcome.ALREADY_RUNNING, running.task_id)

        logger.warning(
            "Webservice respondeu Conflict, mas nenhuma extração aparece em NFSE_OWNER.TAREFAS: attempt=%d of=%d",
            attempt,
            CONFLICT_ATTEMPTS,
        )

        if services.clock.now() + limits.poll_seconds > deadline:
            break

        await services.clock.sleep(limits.poll_seconds)

    raise ExtractionRefusedError(
        f"O webservice respondeu Conflict ('já em execução') {CONFLICT_ATTEMPTS} vezes sem extração visível em "
        + "NFSE_OWNER.TAREFAS."
    )


async def run_extraction(services: ExtractionServices, limits: Limits) -> str:
    """Run the extraction, and tell Discord each step.

    Start it, follow the task and wait for the release. Return the summary of what
    happened. Raise `WebserviceError` if the service cannot
    be called or answers something unreadable, `ExtractionRefusedError` if it refuses,
    `ExtractionFailedError` if the task ends with no success, and
    `ExtractionTimedOutError` if the task or the release passes the limit.
    """
    clock = services.clock
    started = clock.now()
    deadline = started + limits.timeout_hours * SECONDS_PER_HOUR

    try:
        decision = await start_extraction(services, limits, deadline)

        if decision.outcome is Outcome.NOTHING_PENDING:
            await services.notify(f"📥 {LABEL}: nada pendente para extrair; conferindo se não há rotina em execução")
            await wait_until_idle(services, limits, deadline)
            summary = f"{LABEL}: nada pendente para extrair"
        else:
            row = await wait_for_task(services, decision.task_id, limits, deadline)
            await wait_until_idle(services, limits, deadline)
            summary = f"{LABEL} concluída: tarefa {row.task_id} em {format_duration(clock.now() - started)}"
    except (ExtractionFailedError, ExtractionTimedOutError) as error:
        await services.notify(error.announcement)
        raise
    except (ExtractionRefusedError, WebserviceError) as error:
        await services.notify(f"🚨 {LABEL} não iniciada: {error}")
        raise

    logger.info(summary)
    await services.notify(f"✅ {summary}")

    return summary

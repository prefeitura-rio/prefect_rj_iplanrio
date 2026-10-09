"""Tasks do Prefect da pipeline rj_iplanrio__nf_agent."""

from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.client.orchestration import get_client
from prefect.runtime import deployment

from .utils.bifrost import build_client
from .utils.observability import get_logger
from .utils.poll import PollSummary, poll_sessions
from .utils.settings import inject_gcp_credentials, load_settings
from .utils.submit import SubmitRequest, SubmitSummary, resolve_origem, submit_pending
from .utils.tracking import active_sessions

logger = get_logger(__name__)


@task
def inject_credentials_task() -> None:
    """Configura as credenciais GCP a partir do Infisical."""
    inject_gcp_credentials()


@task(cache_policy=NO_CACHE, retries=3, retry_delay_seconds=[30, 60, 120])
def submit_task(
    origem: str | None, mes_envio: str | None, max_paginas: int | None, versao_processamento: str | None
) -> SubmitSummary:
    """Submete todos os PDFs pendentes da origem (ou da base padrão + mes_envio).

    Reativar o agendamento e disparar o acompanhamento é feito à parte, por
    ``activate_schedule_task``, que o flow chama mesmo se esta task falhar.
    Uma falha transitória do Bifrost (ex.: ``no healthy
    upstream``) no meio da submissão de centenas de PDFs derrubava a task
    inteira sem deixar rastro em ``nf_batch_jobs`` — as sessões já criadas
    até ali ficavam registradas, mas o resto nunca era submetido. As
    tentativas automáticas reexecutam a task inteira, o que é seguro: cada
    tentativa recalcula os pendentes do zero e ``in_flight_pdf_names``
    exclui corretamente o que já foi submetido nas tentativas anteriores.
    """
    settings = load_settings()
    resolved_origem = resolve_origem(origem, mes_envio, settings)
    request = SubmitRequest(input_uri=resolved_origem, max_pages=max_paginas, processing_version=versao_processamento)
    return submit_pending(build_client(), settings, request)


@task(cache_policy=NO_CACHE, retries=3, retry_delay_seconds=[30, 60, 120])
def poll_task() -> PollSummary:
    """Avança as sessões ativas que terminaram.

    As tentativas automáticas em caso de falha transitória (Bifrost/GCS) são
    seguras pelo mesmo motivo de ``submit_task``: cada tentativa reconsulta
    ``active_sessions`` do zero, sem depender de estado da tentativa anterior.
    """
    settings = load_settings()
    return poll_sessions(build_client(), settings)


@task(cache_policy=NO_CACHE, retries=3, retry_delay_seconds=[30, 60, 120])
def activate_schedule_task() -> None:
    """Reativa o agendamento e dispara um acompanhamento se houver sessão ativa.

    Decide por ``active_sessions`` (o mesmo critério com que ``pause_schedule_if_idle_task``
    pausa), não pelo resumo da submissão: numa repetição da submissão, ``submit_pending``
    considera tudo "em voo" e devolveria zero sessões novas, deixando as já criadas sem
    acompanhamento. O flow chama esta task em ``finally``, então ela roda também quando a
    submissão falha depois de criar sessões. Sem efeito fora de um deployment.
    """
    settings = load_settings()
    if deployment.id is None or not active_sessions(settings.nf_batch_jobs_table):
        return
    with get_client(sync_client=True) as client:
        for schedule in client.read_deployment_schedules(deployment.id):
            if not schedule.active:
                client.update_deployment_schedule(deployment.id, schedule.id, active=True)
                logger.info("Agendamento %s: reativado", schedule.id)
        # Evita esperar até o próximo tick do agendamento (até 15 min) depois de uma submissão;
        # o novo run entra na fila e roda assim que este terminar (``concurrency_limit: 1``).
        client.create_flow_run_from_deployment(deployment.id, parameters={"acao": "acompanhar"})


@task(cache_policy=NO_CACHE, retries=3, retry_delay_seconds=[30, 60, 120])
def pause_schedule_if_idle_task() -> None:
    """Pausa o agendamento do próprio deployment se não sobrar sessão ativa.

    Só ``activate_schedule_task`` reativa, ao encontrar sessão ativa depois de uma
    submissão. Sem efeito fora de um deployment.
    """
    settings = load_settings()
    if deployment.id is None or active_sessions(settings.nf_batch_jobs_table):
        return
    with get_client(sync_client=True) as client:
        for schedule in client.read_deployment_schedules(deployment.id):
            if schedule.active:
                client.update_deployment_schedule(deployment.id, schedule.id, active=False)
                logger.info("Agendamento %s: pausado", schedule.id)

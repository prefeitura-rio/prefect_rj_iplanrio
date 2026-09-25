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


def set_own_schedule_active(active: bool) -> None:
    """Ativa ou pausa o agendamento do deployment que está rodando este flow.

    Sem efeito fora de um deployment (execução local, CLI, testes), onde
    ``deployment.id`` é ``None``. Usado para o agendamento só ficar ativo
    enquanto há sessão em andamento — ``submit_task`` reativa ao criar
    sessão nova, ``poll_task`` pausa quando não sobra nenhuma.

    :param active: ``True`` para reativar, ``False`` para pausar.
    """
    if deployment.id is None:
        return
    with get_client(sync_client=True) as client:
        for schedule in client.read_deployment_schedules(deployment.id):
            if schedule.active != active:
                client.update_deployment_schedule(deployment.id, schedule.id, active=active)
                logger.info("Agendamento %s: %s", schedule.id, "reativado" if active else "pausado")


def trigger_immediate_poll() -> None:
    """Dispara agora um novo run do próprio deployment (acao=acompanhar).

    Evita esperar até o próximo tick do agendamento (até 1h) depois de uma
    submissão — o novo run entra na fila e roda assim que este terminar
    (``concurrency_limit: 1``). Sem efeito fora de um deployment.
    """
    if deployment.id is None:
        return
    with get_client(sync_client=True) as client:
        client.create_flow_run_from_deployment(deployment.id, parameters={"acao": "acompanhar"})


@task
def inject_credentials_task() -> None:
    """Configura as credenciais GCP a partir do Infisical."""
    inject_gcp_credentials()


@task(cache_policy=NO_CACHE)
def submit_task(
    origem: str | None, mes_envio: str | None, max_paginas: int | None, versao_processamento: str | None
) -> SubmitSummary:
    """Submete todos os PDFs pendentes da origem (ou da base padrão + mes_envio).

    Reativa o agendamento e dispara um acompanhamento imediato se alguma
    sessão foi criada.
    """
    settings = load_settings()
    resolved_origem = resolve_origem(origem, mes_envio, settings)
    request = SubmitRequest(input_uri=resolved_origem, max_pages=max_paginas, processing_version=versao_processamento)
    summary = submit_pending(build_client(), settings, request)
    if summary.session_ids:
        set_own_schedule_active(True)
        trigger_immediate_poll()
    return summary


@task(cache_policy=NO_CACHE)
def poll_task() -> PollSummary:
    """Avança as sessões ativas que terminaram.

    Pausa o próprio agendamento se não sobrar nenhuma sessão ativa depois
    de avançar — só ``submit_task`` reativa, ao criar sessão nova.
    """
    settings = load_settings()
    summary = poll_sessions(build_client(), settings)
    if not active_sessions(settings.nf_batch_jobs_table):
        set_own_schedule_active(False)
    return summary

"""A extração do começo ao fim: chamar o webservice, acompanhar a tarefa e esperar a liberação das rotinas 46/48.

Não importa o Prefect nem fala com a rede ou o Oracle diretamente: ``tasks.py`` injeta as ações e os testes injetam
fakes. ``Conflict`` (outra extração em execução) não é erro: a extração em curso é acompanhada até o fim e a carga
segue se ela terminar com sucesso.
"""

from collections.abc import Callable
from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import LABEL
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.discord_format import format_duration
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.soap import (
    Decision,
    ExtractionRefusedError,
    Outcome,
    RoutineResponse,
    WebserviceError,
    decide,
)
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.tasks_db import TaskStore
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.wait import (
    Clock,
    ExtractionFailedError,
    ExtractionTimedOutError,
    Limits,
    wait_for_task,
    wait_until_idle,
)

# Chamadas ao webservice quando ele responde Conflict mas nenhuma extração aparece em TAREFAS (corrida no término).
CONFLICT_ATTEMPTS = 3


@dataclass(frozen=True)
class ExtractionServices:
    """O que a extração faz no mundo; cada ação é injetada.

    :param call: Chama o webservice e devolve a resposta lida.
    :param store: Leitura de ``NFSE_OWNER.TAREFAS``.
    :param notify: Posta uma linha avulsa no Discord; nunca levanta exceção.
    :param log: Escreve no log do Prefect.
    :param clock: Relógio e espera.
    """

    call: Callable[[], RoutineResponse]
    store: TaskStore
    notify: Callable[[str], None]
    log: Callable[[str], None]
    clock: Clock


def _start(services: ExtractionServices, limits: Limits, deadline: float) -> Decision:
    """Chama o webservice e devolve o que acompanhar; em ``Conflict``, a extração já em curso."""
    for attempt in range(1, CONFLICT_ATTEMPTS + 1):
        response = services.call()
        services.log(f"Webservice: {response.status_code}, TarefaId {response.task_id}, mensagem {response.message!r}.")
        decision = decide(response)
        match decision.outcome:
            case Outcome.STARTED:
                services.notify(f"📥 {LABEL} iniciada: tarefa {decision.task_id}")
                return decision
            case Outcome.NOTHING_PENDING:
                return decision
            case Outcome.ALREADY_RUNNING:
                running = services.store.extraction_in_progress()
                if running is not None:
                    services.notify(
                        f"⏳ {LABEL} já em execução por outro disparo ({running.describe()}); aguardando terminar "
                        "para seguir"
                    )
                    return Decision(Outcome.ALREADY_RUNNING, running.task_id)
        services.log(
            f"Webservice respondeu Conflict, mas nenhuma extração aparece em NFSE_OWNER.TAREFAS (tentativa {attempt} "
            f"de {CONFLICT_ATTEMPTS}); nova chamada em {limits.poll_seconds:.0f} s."
        )
        if services.clock.now() + limits.poll_seconds > deadline:
            break
        services.clock.sleep(limits.poll_seconds)
    raise ExtractionRefusedError(
        f"O webservice respondeu Conflict ('já em execução') {CONFLICT_ATTEMPTS} vezes sem extração visível em "
        "NFSE_OWNER.TAREFAS."
    )


def run_extraction(services: ExtractionServices, limits: Limits) -> str:
    """Dispara a extração, acompanha a tarefa e espera a liberação; avisa o Discord em cada passo.

    :param services: Ações do mundo.
    :param limits: Ritmo e limites.
    :returns: Resumo do que aconteceu.
    :raises WebserviceError: Se o webservice não puder ser chamado ou responder algo ilegível.
    :raises ExtractionRefusedError: Se o webservice recusar ou não iniciar a extração.
    :raises ExtractionFailedError: Se a tarefa terminar sem sucesso.
    :raises ExtractionTimedOutError: Se a tarefa ou a liberação passar do prazo.
    """
    clock = services.clock
    started = clock.now()
    deadline = started + limits.deadline_seconds
    try:
        decision = _start(services, limits, deadline)
        if decision.outcome is Outcome.NOTHING_PENDING:
            services.notify(f"📥 {LABEL}: nada pendente para extrair; conferindo se não há rotina em execução")
            wait_until_idle(services.store, limits, clock, services.notify, services.log, deadline)
            summary = f"{LABEL}: nada pendente para extrair"
        else:
            row = wait_for_task(
                services.store, decision.task_id, limits, clock, services.notify, services.log, deadline
            )
            wait_until_idle(services.store, limits, clock, services.notify, services.log, deadline)
            summary = f"{LABEL} concluída: tarefa {row.task_id} em {format_duration(clock.now() - started)}"
    except (ExtractionFailedError, ExtractionTimedOutError) as error:
        services.notify(error.announcement)
        raise
    except (ExtractionRefusedError, WebserviceError) as error:
        services.notify(f"🚨 {LABEL} não iniciada: {error}")
        raise
    services.log(summary)
    services.notify(f"✅ {summary}")
    return summary

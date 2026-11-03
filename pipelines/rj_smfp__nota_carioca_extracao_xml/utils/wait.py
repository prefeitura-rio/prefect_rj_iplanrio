"""Espera pela tarefa da extração e pela liberação das rotinas 46/48 em ``NFSE_OWNER.TAREFAS``.

Regra de término (a mesma usada à mão): ``DATA_TERMINO`` preenchida e ``STATUS = 2`` é sucesso; ``DATA_TERMINO``
preenchida com outro status, ou status 3 (erro) ou 5 (interrompida), é falha. Depois do sucesso, a carga só segue quando
nenhuma tarefa 46 ou 48 está em execução em ``idle_checks`` consultas seguidas: é a mesma condição da guarda do
``oracle_to_bq``, que recusa começar com extração ou pós-processamento rodando. Uma tarefa em execução há mais de
``stale_hours`` horas é tratada como travada (não bloqueia), também como na guarda. Nada aqui altera o Oracle.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import (
    LABEL,
    SECONDS_PER_HOUR,
    STATUS_FAILED,
    STATUS_INTERRUPTED,
    STATUS_SUCCESS,
)
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.discord_format import format_duration
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.tasks_db import TaskRow, TaskStore

SUMMARY_LIMIT = 500


@dataclass(frozen=True)
class Clock:
    """Relógio monotônico e espera (injetados; os testes usam um relógio falso).

    :param now: Segundos monotônicos.
    :param sleep: Espera os segundos dados.
    """

    now: Callable[[], float]
    sleep: Callable[[float], None]


@dataclass(frozen=True)
class Limits:
    """Ritmo e limites do acompanhamento.

    :param poll_seconds: Intervalo entre consultas ao Oracle.
    :param deadline_seconds: Tempo máximo total, da chamada ao webservice até a liberação.
    :param stale_hours: Horas em execução a partir das quais uma tarefa 46/48 é considerada travada.
    :param idle_checks: Consultas seguidas sem 46/48 em execução para considerar liberado.
    :param progress_every_seconds: Intervalo entre os avisos de progresso no Discord.
    """

    poll_seconds: float
    deadline_seconds: float
    stale_hours: int
    idle_checks: int
    progress_every_seconds: float

    def __post_init__(self) -> None:
        if self.poll_seconds <= 0 or self.deadline_seconds <= 0 or self.progress_every_seconds <= 0:
            raise ValueError("Intervalo, prazo e aviso de progresso precisam ser positivos.")
        if self.stale_hours <= 0 or self.idle_checks < 1:
            raise ValueError("stale_hours precisa ser positivo e idle_checks pelo menos 1.")


class ExtractionFailedError(RuntimeError):
    """A tarefa da extração terminou sem sucesso.

    :param message: Texto do erro do flow.
    :param announcement: Linha para o Discord.
    """

    def __init__(self, message: str, announcement: str) -> None:
        super().__init__(message)
        self.announcement = announcement


class ExtractionTimedOutError(RuntimeError):
    """A extração (ou a liberação das rotinas 46/48) passou do prazo; nada é cancelado no Oracle.

    :param message: Texto do erro do flow.
    :param announcement: Linha para o Discord.
    """

    def __init__(self, message: str, announcement: str) -> None:
        super().__init__(message)
        self.announcement = announcement


def finished_successfully(row: TaskRow) -> bool:
    """``STATUS = 2`` com ``DATA_TERMINO`` preenchida."""
    return row.status == STATUS_SUCCESS and row.finished_at is not None


def finished_without_success(row: TaskRow) -> bool:
    """``DATA_TERMINO`` preenchida com status diferente de 2, ou status 3 (erro) ou 5 (interrompida)."""
    return row.status in {STATUS_FAILED, STATUS_INTERRUPTED} or (
        row.finished_at is not None and row.status != STATUS_SUCCESS
    )


def _failure(row: TaskRow) -> ExtractionFailedError:
    end = f", término {row.finished_at:%d/%m/%Y %H:%M:%S} (horário do banco)" if row.finished_at else ""
    summary = f": {row.summary[:SUMMARY_LIMIT]}" if row.summary else ""
    text = f"{LABEL} falhou: tarefa {row.task_id} terminou com {row.status_description}{end}{summary}"
    return ExtractionFailedError(text, f"🚨 {text}")


def wait_for_task(  # noqa: PLR0913, PLR0917
    store: TaskStore,
    task_id: int,
    limits: Limits,
    clock: Clock,
    notify: Callable[[str], None],
    log: Callable[[str], None],
    deadline: float,
) -> TaskRow:
    """Consulta a tarefa a cada ``poll_seconds`` até terminar, avisando o progresso a cada ``progress_every_seconds``.

    :param store: Leitura de ``NFSE_OWNER.TAREFAS``.
    :param task_id: Tarefa a acompanhar.
    :param limits: Ritmo e limites.
    :param clock: Relógio e espera.
    :param notify: Linha avulsa no Discord.
    :param log: Log do Prefect.
    :param deadline: Instante (no relógio ``clock.now``) a partir do qual desiste de esperar.
    :returns: A tarefa concluída com sucesso.
    :raises ExtractionFailedError: Se a tarefa terminar sem sucesso.
    :raises ExtractionTimedOutError: Se o prazo acabar antes do término.
    """
    last_progress = clock.now()
    while True:
        row = store.get(task_id)
        if row is None:
            log(
                f"Tarefa {task_id} ainda não aparece em NFSE_OWNER.TAREFAS; "
                f"nova consulta em {limits.poll_seconds:.0f} s."
            )
        elif finished_successfully(row):
            return row
        elif finished_without_success(row):
            raise _failure(row)
        else:
            log(
                f"{row.describe()}: {row.status_description}, "
                f"progresso {row.progress if row.progress is not None else '?'}%."
            )
            if clock.now() - last_progress >= limits.progress_every_seconds:
                progress = f" · progresso {row.progress}%" if row.progress is not None else ""
                notify(f"⏳ {LABEL} em andamento: {row.describe()}{progress}")
                last_progress = clock.now()
        if clock.now() + limits.poll_seconds > deadline:
            limit = format_duration(limits.deadline_seconds)
            text = f"{LABEL}: tarefa {task_id} não terminou dentro do limite de {limit}"
            raise ExtractionTimedOutError(text, f"🚨 {text}; a tarefa segue no Oracle e não foi interrompida")
        clock.sleep(limits.poll_seconds)


def _blocking(rows: Sequence[TaskRow], stale_seconds: int) -> list[TaskRow]:
    return [row for row in rows if row.running_seconds is not None and row.running_seconds <= stale_seconds]


def wait_until_idle(  # noqa: PLR0913, PLR0917
    store: TaskStore,
    limits: Limits,
    clock: Clock,
    notify: Callable[[str], None],
    log: Callable[[str], None],
    deadline: float,
) -> None:
    """Espera não haver extração (46) nem pós-processamento (48) em execução em ``idle_checks`` consultas seguidas.

    :param store: Leitura de ``NFSE_OWNER.TAREFAS``.
    :param limits: Ritmo e limites.
    :param clock: Relógio e espera.
    :param notify: Linha avulsa no Discord (um aviso quando começa a esperar o pós-processamento).
    :param log: Log do Prefect.
    :param deadline: Instante (no relógio ``clock.now``) a partir do qual desiste de esperar.
    :raises ExtractionTimedOutError: Se o prazo acabar com 46/48 ainda em execução.
    """
    stale_seconds = limits.stale_hours * SECONDS_PER_HOUR
    idle_in_a_row = 0
    announced = False
    blocking: list[TaskRow] = []
    while True:
        rows = store.running()
        blocking = _blocking(rows, stale_seconds)
        for row in rows:
            if row not in blocking:
                log(
                    f"{row.describe()} parece travada (STATUS = 1 há mais de {limits.stale_hours}h ou sem início); "
                    "não bloqueia, como na guarda do oracle_to_bq."
                )
        if blocking:
            idle_in_a_row = 0
            listed = "; ".join(row.describe() for row in blocking)
            log(f"Aguardando terminar: {listed}.")
            if not announced:
                notify(f"⏳ {LABEL} concluída; aguardando terminar: {listed}")
                announced = True
        else:
            idle_in_a_row += 1
            if idle_in_a_row >= limits.idle_checks:
                log(f"Nenhuma extração ou pós-processamento em execução em {idle_in_a_row} consultas seguidas.")
                return
        if clock.now() + limits.poll_seconds > deadline:
            listed = "; ".join(row.describe() for row in blocking) or "liberação não confirmada"
            limit = format_duration(limits.deadline_seconds)
            text = f"{LABEL}: rotinas da aplicação ainda em execução no limite de {limit}: {listed}"
            raise ExtractionTimedOutError(text, f"🚨 {text}")
        clock.sleep(limits.poll_seconds)

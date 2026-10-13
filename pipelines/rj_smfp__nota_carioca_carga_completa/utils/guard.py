"""Decide se um deployment encadeado está ocupado: execução ativa ou prestes a começar.

Não importa o Prefect: ``runs.py`` lê a API e entrega ``ActiveRun``; aqui só se decide e se descreve. Os próprios
deployments já se recusam a rodar em paralelo, mas um disparo manual pela UI não passa pelo bot; a trava impede que a
carga completa comece, ou continue, por cima dele.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

BUSY_STATES = frozenset({"RUNNING", "PENDING", "CANCELLING"})
SCHEDULED_STATE = "SCHEDULED"
SCHEDULED_WINDOW = timedelta(minutes=10)
MAX_LISTED_RUNS = 3


@dataclass(frozen=True)
class ActiveRun:
    """Visão mínima de um flow run para a trava.

    :param run_id: Id do flow run.
    :param name: Nome do flow run.
    :param state_type: Tipo do estado (``RUNNING``, ``SCHEDULED``...).
    :param state_name: Nome do estado como aparece na UI (``Late``, ``AwaitingRetry``...).
    :param expected_start_time: Início previsto; só importa para ``SCHEDULED``.
    """

    run_id: str
    name: str
    state_type: str
    state_name: str
    expected_start_time: datetime | None


class ActiveRunsError(RuntimeError):
    """O deployment tem execução ativa ou prestes a começar.

    :param message: Descrição legível das execuções, com links.
    :param runs: As execuções que impedem o início.
    """

    def __init__(self, message: str, runs: Sequence[ActiveRun]) -> None:
        super().__init__(message)
        self.runs = tuple(runs)


def blocking_runs(runs: Sequence[ActiveRun], now: datetime) -> list[ActiveRun]:
    """Filtra as execuções que impedem a carga: ativas, ou agendadas para daqui a no máximo 10 minutos.

    Um agendado com início já passado (``Late``) também impede; sem horário previsto, por cautela, também.

    :param runs: Execuções lidas do Prefect.
    :param now: Hora de agora, com fuso.
    :returns: As que impedem, na mesma ordem.
    """
    horizon = now + SCHEDULED_WINDOW
    return [
        run
        for run in runs
        if run.state_type in BUSY_STATES
        or (
            run.state_type == SCHEDULED_STATE
            and (run.expected_start_time is None or run.expected_start_time <= horizon)
        )
    ]


def describe_blocking(runs: Sequence[ActiveRun], link: Callable[[str], str | None]) -> str:
    """Descreve as execuções que impedem o início, para o erro e o log.

    :param runs: Execuções que impedem (não vazia).
    :param link: Monta o link de um flow run na UI do Prefect, ou ``None`` se a URL não estiver configurada.
    :returns: Texto como ``2 execuções ativas ou agendadas: abc (Running) <link>; ...``.
    """
    listed = []
    for run in runs[:MAX_LISTED_RUNS]:
        url = link(run.run_id)
        listed.append(f"{run.name} ({run.state_name}){' ' + url if url else ''}")
    extra = len(runs) - MAX_LISTED_RUNS
    more = f"; e mais {extra}" if extra > 0 else ""
    noun = "execução ativa ou agendada" if len(runs) == 1 else "execuções ativas ou agendadas"
    return f"{len(runs)} {noun}: {'; '.join(listed)}{more}"

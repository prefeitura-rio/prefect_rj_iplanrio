"""Regra pura que decide quais flow runs estão presos em Pending."""

from dataclasses import dataclass
from datetime import datetime, timedelta

MIN_PENDING_MINUTES = 60


@dataclass(frozen=True)
class PendingRun:
    """Retrato de um flow run em Pending, sem dependência do Prefect.

    :param id: Id do flow run.
    :param name: Nome do flow run.
    :param deployment_id: Id do deployment, quando o run veio de um.
    :param state_name: Nome do estado atual (``Pending``, ``InfrastructurePending``...).
    :param state_type: Tipo do estado atual (``PENDING``, ``RUNNING``...).
    :param state_since: Momento em que o run entrou no estado atual, com fuso.
    :param started: Se o run já começou a executar alguma vez.
    :param infrastructure_pid: Identificador do Job criado pelo worker, quando existe.
    """

    id: str
    name: str
    deployment_id: str | None
    state_name: str
    state_type: str
    state_since: datetime
    started: bool
    infrastructure_pid: str | None


def validate_max_pending(max_pending: timedelta) -> None:
    """Recuse um limite curto o bastante para cancelar runs em Pending normal.

    Um pod que espera nó novo ou baixa uma imagem grande fica minutos em Pending;
    abaixo de :data:`MIN_PENDING_MINUTES` o flow passaria a cancelar runs saudáveis.

    :param max_pending: Tempo máximo aceito em Pending.
    :raises ValueError: Se ``max_pending`` for menor que :data:`MIN_PENDING_MINUTES`.
    """
    if max_pending < timedelta(minutes=MIN_PENDING_MINUTES):
        raise ValueError(f"max_pending precisa ser de pelo menos {MIN_PENDING_MINUTES} minutos, recebido {max_pending}")


def is_stuck(run: PendingRun, now: datetime, max_pending: timedelta) -> bool:
    """Diga se o run está em Pending, nunca começou e passou do tempo máximo.

    :param run: Retrato do flow run.
    :param now: Instante de referência, com fuso.
    :param max_pending: Tempo máximo aceito em Pending.
    :returns: ``True`` quando o run deve ser cancelado.
    :raises ValueError: Se ``now`` ou ``run.state_since`` não tiverem fuso, ou se
        ``max_pending`` for menor que :data:`MIN_PENDING_MINUTES`.
    """
    if now.tzinfo is None or run.state_since.tzinfo is None:
        raise ValueError("now e state_since precisam ter fuso horário")
    validate_max_pending(max_pending)
    if run.state_type != "PENDING" or run.started:
        return False
    return now - run.state_since > max_pending


def select_stuck_runs(runs: list[PendingRun], now: datetime, max_pending: timedelta) -> list[PendingRun]:
    """Filtre os runs presos em Pending, do mais antigo para o mais novo.

    :param runs: Runs candidatos.
    :param now: Instante de referência, com fuso.
    :param max_pending: Tempo máximo aceito em Pending.
    :returns: Os runs que passaram do tempo máximo, ordenados por ``state_since``.
    :raises ValueError: Nas mesmas condições de :func:`is_stuck`, mesmo sem runs.
    """
    validate_max_pending(max_pending)
    stuck = [run for run in runs if is_stuck(run, now=now, max_pending=max_pending)]
    return sorted(stuck, key=lambda run: run.state_since)


def describe_run(run: PendingRun, now: datetime) -> str:
    """Monte a linha de log que identifica o run e há quanto tempo está parado.

    :param run: Retrato do flow run.
    :param now: Instante de referência, com fuso.
    :returns: Texto curto com id, nome, estado, horas paradas e Job.
    """
    hours = (now - run.state_since).total_seconds() / 3600
    return (
        f"flow_run={run.id} name={run.name} deployment={run.deployment_id} "
        f"state={run.state_name} pending_hours={hours:.1f} job={run.infrastructure_pid}"
    )

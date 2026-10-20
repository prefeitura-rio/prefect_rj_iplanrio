"""Guarda contra a extração das notas nacionais: a carga não começa enquanto a aplicação Nota Carioca a executa.

A aplicação registra as suas tarefas em ``NFSE_OWNER.TAREFAS``: o tipo 46 extrai os documentos fiscais nacionais para
o schema DFEN e o 48 pós-processa as notas nacionais, em sequência. Uma tarefa em execução (``STATUS = 1``) há mais de
``stale_hours`` horas é tratada como travada: não bloqueia, mas é avisada. A decisão é pura; só
:func:`read_running_tasks` fala com o Oracle.
"""

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

import oracledb

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig, connect, fetch_rows, to_int
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import format_duration

TASK_EXTRACTION = 46
TASK_POST_PROCESSING = 48
TASK_RUNNING = 1
TASK_KINDS = {
    TASK_EXTRACTION: "extração de documentos fiscais nacionais",
    TASK_POST_PROCESSING: "pós-processamento das notas nacionais",
}
DEFAULT_STALE_HOURS = 6
SECONDS_PER_HOUR = 3600
# ORA-00942: tabela ou view inexistente (ou invisível ao usuário); ORA-01031: privilégios insuficientes.
NO_ACCESS = re.compile(r"ORA-(00942|01031)\b")
ACCESS_HINT = "Peça à DBA: GRANT SELECT ON NFSE_OWNER.TAREFAS ao usuário do segredo do Oracle"


class DfenExtractionError(RuntimeError):
    """A aplicação está extraindo notas nacionais para o schema DFEN; a carga não deve começar agora."""


@dataclass(frozen=True)
class RunningTask:
    """Tarefa de extração em execução (``STATUS = 1``) em ``NFSE_OWNER.TAREFAS``.

    :param task_id: Número da tarefa (coluna ``TAREFA``).
    :param kind: Tipo da tarefa: 46 (extração) ou 48 (pós-processamento).
    :param started_at: ``DATA_INICIO``, no relógio do banco; ``None`` se a tarefa não tem início registrado.
    :param running_seconds: Segundos desde o início até agora, contados pelo banco (``SYSDATE - DATA_INICIO``).
    """

    task_id: int
    kind: int
    started_at: datetime | None
    running_seconds: int | None


@dataclass(frozen=True)
class ExtractionStatus:
    """Resultado da conferência das tarefas em execução.

    :param blocking: Tarefas que começaram há no máximo ``stale_hours`` horas: a carga não pode começar.
    :param stale: Tarefas em execução há mais tempo (ou sem início registrado): travadas, não bloqueiam.
    """

    blocking: tuple[RunningTask, ...]
    stale: tuple[RunningTask, ...]


def classify_tasks(tasks: Sequence[RunningTask], stale_hours: int) -> ExtractionStatus:
    """Separa as tarefas em execução entre as que bloqueiam a carga e as travadas.

    Uma tarefa bloqueia se ``DATA_INICIO >= SYSDATE - stale_hours/24``, isto é, se está em execução há no máximo
    ``stale_hours`` horas. Sem ``DATA_INICIO`` não há como saber há quanto tempo roda: a tarefa é tratada como travada.

    :param tasks: Tarefas com ``STATUS = 1`` dos tipos 46 e 48.
    :param stale_hours: Horas em execução a partir das quais a tarefa é considerada travada.
    :returns: As tarefas que bloqueiam e as travadas, na ordem recebida.
    """
    limit = stale_hours * SECONDS_PER_HOUR
    blocking: list[RunningTask] = []
    stale: list[RunningTask] = []
    for task in tasks:
        is_recent = task.running_seconds is not None and task.running_seconds <= limit
        (blocking if is_recent else stale).append(task)
    return ExtractionStatus(blocking=tuple(blocking), stale=tuple(stale))


def describe_task(task: RunningTask) -> str:
    """Descreve uma tarefa: número, tipo, desde quando roda e há quanto tempo.

    :param task: Tarefa em execução.
    :returns: Texto como ``tarefa 12 (tipo 46, extração de documentos fiscais nacionais) desde 20/10/2026 12:00:00
        (horário do banco, há 1h02m03s)``.
    """
    kind = TASK_KINDS.get(task.kind, "tipo desconhecido")
    head = f"tarefa {task.task_id} (tipo {task.kind}, {kind})"
    if task.started_at is None or task.running_seconds is None:
        return f"{head} sem data de início registrada"
    elapsed = format_duration(max(task.running_seconds, 0))
    return f"{head} desde {task.started_at:%d/%m/%Y %H:%M:%S} (horário do banco, há {elapsed})"


def describe_blocking(blocking: Sequence[RunningTask]) -> str:
    """Monta a mensagem de recusa: o que está rodando, desde quando e como proceder.

    :param blocking: Tarefas que bloqueiam, não vazio.
    :returns: Mensagem com cada tarefa e a orientação de rodar de novo ou forçar.
    """
    listed = "; ".join(describe_task(task) for task in blocking)
    return (
        f"Extração das notas nacionais em andamento no Oracle (schema DFEN): {listed}. A carga foi recusada antes da "
        "foto porque essas tabelas estão sendo atualizadas. Rode a carga de novo depois que terminarem ou, para "
        "forçar, rode com check_dfen_extraction=false."
    )


def describe_stale(task: RunningTask, stale_hours: int) -> str:
    """Monta o aviso de uma tarefa travada, que não bloqueia a carga.

    :param task: Tarefa em execução há mais de ``stale_hours`` horas ou sem início registrado.
    :param stale_hours: Limite usado para considerá-la travada.
    :returns: Aviso com a tarefa e o motivo de ser ignorada.
    """
    return (
        f"Extração DFEN ignorada por parecer travada (STATUS = 1 há mais de {stale_hours}h ou sem início): "
        f"{describe_task(task)}"
    )


def optional_datetime(value: object) -> datetime | None:
    """Valida uma coluna ``DATE`` que pode ser nula.

    :param value: Valor lido do banco.
    :returns: O ``datetime``, ou ``None`` se a coluna for nula.
    :raises TypeError: Se o valor não for ``datetime`` nem ``None``.
    """
    if value is None or isinstance(value, datetime):
        return value
    raise TypeError(f"Data esperada, recebido {value!r}")


def read_running_tasks(config: OracleConfig) -> tuple[RunningTask, ...]:
    """Lê em ``NFSE_OWNER.TAREFAS`` as tarefas de extração (tipos 46 e 48) em execução, com o relógio do banco.

    :param config: Configuração da conexão.
    :returns: As tarefas com ``STATUS = 1``, da mais antiga para a mais nova.
    """
    binds: dict[str, object] = {
        "tipo_extracao": TASK_EXTRACTION,
        "tipo_pos_processamento": TASK_POST_PROCESSING,
        "status_em_execucao": TASK_RUNNING,
    }
    with connect(config) as connection, connection.cursor() as cursor:
        rows = fetch_rows(cursor, "list_running_extractions", binds)
    return tuple(
        RunningTask(
            task_id=to_int(row["tarefa"]),
            kind=to_int(row["tipo"]),
            started_at=optional_datetime(row["data_inicio"]),
            running_seconds=None if row["segundos_em_execucao"] is None else to_int(row["segundos_em_execucao"]),
        )
        for row in rows
    )


def check_no_extraction(
    fetch: Callable[[], tuple[RunningTask, ...]], stale_hours: int, report: Callable[[str], None]
) -> ExtractionStatus | None:
    """Recusa a carga se a aplicação estiver extraindo notas nacionais para o DFEN.

    As tarefas travadas são avisadas e ignoradas. Sem privilégio de leitura em ``NFSE_OWNER.TAREFAS`` (ORA-00942 ou
    ORA-01031) a guarda avisa e deixa a carga seguir, porque é uma proteção e não uma dependência; qualquer outro erro
    do Oracle sobe.

    :param fetch: Lê as tarefas em execução (:func:`read_running_tasks`).
    :param stale_hours: Horas em execução a partir das quais a tarefa é considerada travada; no mínimo 1.
    :param report: Publica um aviso de log.
    :returns: O resultado da conferência, ou ``None`` se não foi possível consultar a tabela.
    :raises ValueError: Se ``stale_hours`` for menor que 1.
    :raises DfenExtractionError: Se houver tarefa em execução que bloqueie, nomeando-a.
    :raises oracledb.DatabaseError: Se a consulta falhar por outro motivo que a falta de privilégio.
    """
    if stale_hours < 1:
        raise ValueError(f"dfen_extraction_stale_hours deve ser pelo menos 1, recebido {stale_hours}")
    try:
        tasks = fetch()
    except oracledb.DatabaseError as error:
        first_line = str(error).split("\n", 1)[0]
        if not NO_ACCESS.search(first_line):
            raise
        report(
            f"Não foi possível consultar NFSE_OWNER.TAREFAS ({first_line}); a carga segue sem esta conferência. "
            f"{ACCESS_HINT}"
        )
        return None
    status = classify_tasks(tasks, stale_hours)
    for task in status.stale:
        report(describe_stale(task, stale_hours))
    if status.blocking:
        raise DfenExtractionError(describe_blocking(status.blocking))
    return status

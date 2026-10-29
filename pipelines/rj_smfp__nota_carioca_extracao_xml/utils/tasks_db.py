"""Leitura de ``NFSE_OWNER.TAREFAS``: a tarefa da extração e as rotinas 46/48 em execução.

É a mesma tabela (e o mesmo segredo) que a guarda do ``oracle_to_bq`` consulta. Cada leitura abre uma conexão curta em
transação somente leitura: o acompanhamento dura horas e não segura sessão no banco entre as consultas.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import oracledb

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import (
    QUERIES_ANCHOR,
    STATUS_DESCRIPTIONS,
    TASK_EXTRACTION,
    TASK_KINDS,
    TASK_POST_PROCESSING,
)
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.config import OracleConfig
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.discord_format import format_duration
from prefect_rj_iplanrio.sql import load_query

# Uma consulta a TAREFAS é instantânea, mas a sonda evita que um firewall derrube a conexão num banco lento.
KEEPALIVE_MINUTES = 2


@dataclass(frozen=True)
class TaskRow:
    """Uma linha de ``NFSE_OWNER.TAREFAS``.

    :param task_id: ``TAREFA``.
    :param kind: ``TIPO`` (46 extração, 48 pós-processamento).
    :param status: ``STATUS`` (0 aguardando, 1 em execução, 2 sucesso, 3 erro, 4 interrupção solicitada,
        5 interrompida).
    :param started_at: ``DATA_INICIO`` no relógio do banco.
    :param finished_at: ``DATA_TERMINO``; ``None`` enquanto não termina.
    :param progress: ``EXECUCAO_PROGRESSO`` (percentual).
    :param summary: ``EXECUCAO_RESUMO``.
    :param running_seconds: ``SYSDATE - DATA_INICIO`` em segundos, contado pelo banco.
    """

    task_id: int
    kind: int
    status: int
    started_at: datetime | None
    finished_at: datetime | None
    progress: int | None
    summary: str | None
    running_seconds: int | None

    @property
    def status_description(self) -> str:
        """``status N (descrição)``."""
        return f"status {self.status} ({STATUS_DESCRIPTIONS.get(self.status, 'desconhecido')})"

    def describe(self) -> str:
        """``tarefa N (tipo 46, extração ...)``, com há quanto tempo roda quando o banco informa."""
        kind = TASK_KINDS.get(self.kind, "tipo desconhecido")
        text = f"tarefa {self.task_id} (tipo {self.kind}, {kind})"
        if self.running_seconds is not None and self.finished_at is None:
            text += f" há {format_duration(max(self.running_seconds, 0))}"
        return text


class TaskStore(Protocol):
    """O que o acompanhamento lê do Oracle."""

    def get(self, task_id: int) -> TaskRow | None:
        """A tarefa pelo número, ou ``None`` se ainda não estiver visível."""
        ...

    def running(self) -> tuple[TaskRow, ...]:
        """Tarefas dos tipos 46 e 48 com ``STATUS = 1``."""
        ...

    def extraction_in_progress(self) -> TaskRow | None:
        """A extração (tipo 46) mais recente aguardando, em execução ou com interrupção solicitada."""
        ...


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int | float | Decimal | str):
        return int(value)
    raise TypeError(f"Número esperado, recebido {value!r}")


def _optional_datetime(value: object) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    raise TypeError(f"Data esperada, recebido {value!r}")


def row_from_record(record: dict[str, object]) -> TaskRow:
    """Converte uma linha lida (colunas em minúsculas) em :class:`TaskRow`.

    :param record: Linha de uma das consultas de ``queries/``.
    :returns: A tarefa.
    :raises TypeError: Se uma coluna vier com tipo inesperado.
    """
    task_id = _optional_int(record["tarefa"])
    kind = _optional_int(record["tipo"])
    status = _optional_int(record["status"])
    if task_id is None or kind is None or status is None:
        raise TypeError(f"Tarefa sem número, tipo ou status: {record!r}")
    summary = record.get("execucao_resumo")
    return TaskRow(
        task_id=task_id,
        kind=kind,
        status=status,
        started_at=_optional_datetime(record.get("data_inicio")),
        finished_at=_optional_datetime(record.get("data_termino")),
        progress=_optional_int(record.get("execucao_progresso")),
        summary=None if summary is None else str(summary).strip() or None,
        running_seconds=_optional_int(record.get("segundos_em_execucao")),
    )


def connect(config: OracleConfig) -> oracledb.Connection:
    """Abre uma conexão em modo thick (Instant Client da imagem base; aceita verifier 10G, como o ``oracle_to_bq``).

    :param config: Configuração da conexão.
    :returns: Conexão aberta.
    """
    if oracledb.is_thin_mode():
        oracledb.init_oracle_client()
    oracledb.defaults.fetch_lobs = False
    return oracledb.connect(
        user=config.user, password=config.password, dsn=f"{config.dsn}?expire_time={KEEPALIVE_MINUTES}"
    )


class OracleTaskStore:
    """:class:`TaskStore` sobre o Oracle; uma conexão somente leitura por leitura.

    :param config: Configuração da conexão.
    :param open_connection: Abre a conexão (os testes não usam; o padrão é :func:`connect`).
    """

    def __init__(
        self, config: OracleConfig, open_connection: Callable[[OracleConfig], oracledb.Connection] = connect
    ) -> None:
        self._config = config
        self._open = open_connection

    def _fetch(self, query: str, binds: dict[str, object]) -> list[TaskRow]:
        with self._open(self._config) as connection, connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute(load_query(QUERIES_ANCHOR, query), binds)
            if cursor.description is None:
                raise LookupError(f"A consulta {query} não retornou colunas.")
            names = [column.name.lower() for column in cursor.description]
            rows = [row_from_record(dict(zip(names, values, strict=True))) for values in cursor.fetchall()]
            connection.rollback()
            return rows

    def get(self, task_id: int) -> TaskRow | None:
        """A tarefa pelo número, ou ``None`` se ainda não estiver visível."""
        rows = self._fetch("get_task", {"tarefa": task_id})
        return rows[0] if rows else None

    def running(self) -> tuple[TaskRow, ...]:
        """Tarefas dos tipos 46 e 48 com ``STATUS = 1``, pela data de início."""
        binds: dict[str, object] = {"tipo_extracao": TASK_EXTRACTION, "tipo_pos_processamento": TASK_POST_PROCESSING}
        return tuple(self._fetch("list_running", binds))

    def extraction_in_progress(self) -> TaskRow | None:
        """A extração (tipo 46) mais recente com ``STATUS`` 0, 1 ou 4."""
        rows = self._fetch("extraction_in_progress", {"tipo_extracao": TASK_EXTRACTION})
        return rows[0] if rows else None

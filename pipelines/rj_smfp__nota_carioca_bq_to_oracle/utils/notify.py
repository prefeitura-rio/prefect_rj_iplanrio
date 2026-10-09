"""Mensagem da carga BigQuery → Oracle no Discord: estado agregado, publicação e finalização.

Uma execução, um pod, tabelas em sequência: o próprio flow muda os passos (``enter``, ``table_step``...) e as tasks de
carga e de índices informam o andamento pelo notificador ativo (``current``), um ponteiro de módulo definido por
``LoadNotifier.guard``. As tasks usam ``NO_CACHE``; passar o notificador (com trava e cliente HTTP) como parâmetro
faria o Prefect percorrê-lo ao registrar as entradas, e há um flow run por processo, então o ponteiro é seguro.
Nada aqui levanta exceção para a carga.
"""

import asyncio
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Literal

from prefect.exceptions import TerminationSignal
from prefect.settings import PREFECT_API_URL, PREFECT_UI_URL

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord import (
    DiscordStatusMessage,
    Warner,
    never_raises,
    webhook_from_env,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord_embed import (
    RunStatus,
    build_announcement,
    build_payload,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord_format import flow_run_url
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.notify_view import (
    RunState,
    Step,
    TableState,
    TableStep,
    build_view,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.sqlldr import ProgressSnapshot

CANCELLATIONS = (KeyboardInterrupt, SystemExit, asyncio.CancelledError, TerminationSignal)


@dataclass(frozen=True)
class NotifierConfig:
    """Como criar o notificador.

    :param dataset_id: Dataset de origem.
    :param run_id: Flow run.
    :param run_name: Nome do flow run, no rodapé.
    :param table_ids: Tabelas da execução (vazio até a listagem, em ``synonyms_only`` também).
    :param sessions: Sessões do SQL*Loader pedidas.
    :param run_dbt: Se o dbt roda antes.
    :param mode: ``full`` ou ``synonyms_only``.
    :param enabled: ``False`` (``discord_notifications=false``) desliga o Discord.
    """

    dataset_id: str
    run_id: str
    run_name: str
    table_ids: tuple[str, ...]
    sessions: int
    run_dbt: bool
    mode: Literal["full", "synonyms_only"]
    enabled: bool


def is_cancellation(error: BaseException) -> bool:
    """Diz se a exceção é uma interrupção (SIGTERM do Prefect, Ctrl+C, cancelamento) e não uma falha de dados."""
    return isinstance(error, CANCELLATIONS)


class LoadNotifier:
    """Estado agregado da carga e a mensagem única do Discord; seguro para chamadas de qualquer thread.

    :param config: Execução.
    :param message: Cliente do webhook; ``None`` desliga o Discord.
    """

    def __init__(self, config: NotifierConfig, message: DiscordStatusMessage | None) -> None:
        self._message = message
        self._started = time.monotonic()
        self._table_started: dict[str, float] = {}
        self._lock = threading.RLock()
        self._finalized = False
        self._warn = Warner()
        self._state = RunState(
            dataset_id=config.dataset_id,
            run_name=config.run_name,
            url=flow_run_url(PREFECT_UI_URL.value(), PREFECT_API_URL.value(), config.run_id),
            status=RunStatus.RUNNING,
            mode=config.mode,
            run_dbt=config.run_dbt,
            step=Step.SWAP if config.mode == "synonyms_only" else Step.DBT if config.run_dbt else Step.EXPORT,
            tables={name: TableState(name) for name in config.table_ids},
            sessions=config.sessions,
            elapsed_seconds=0.0,
            updated_at=datetime.now(UTC),
        )

    @classmethod
    def create(cls, config: NotifierConfig) -> "LoadNotifier":
        """Cria o notificador; o webhook vem de ``DISCORD_WEBHOOK_URL_NOTA_CARIOCA`` (ausente = desligado)."""
        message = DiscordStatusMessage(webhook_from_env()) if config.enabled else None
        return cls(config, message if message is not None and message.enabled else None)

    @property
    def state(self) -> RunState:
        """Estado atual, com o tempo decorrido e a hora de agora."""
        with self._lock:
            return replace(self._state, elapsed_seconds=time.monotonic() - self._started, updated_at=datetime.now(UTC))

    def _push(self, force: bool) -> None:
        if self._message is None:
            return
        try:
            payload = build_payload(build_view(self.state))
        except Exception as error:
            self._warn(f"Não foi possível montar a mensagem do Discord: {type(error).__name__}: {error}")
            return
        self._message.publish(payload, force=force)

    def _change(self, force: bool = True, **changes: object) -> None:
        with self._lock:
            if self._finalized:
                return
            self._state = replace(self._state, **changes)
            self._push(force)

    def _change_table(self, name: str, force: bool = True, **changes: object) -> None:
        with self._lock:
            table = self._state.tables.get(name) or TableState(name)
            self._change(force, tables={**self._state.tables, name: replace(table, **changes)}, current_table=name)

    @never_raises
    def begin(self) -> None:
        """Cria a mensagem do Discord."""
        self._push(force=True)

    @never_raises
    def enter(self, step: Step) -> None:
        """Entra num passo (dbt, exportação, tabelas, espera do In-Memory, troca) e atualiza a mensagem na hora."""
        self._change(step=step)

    @never_raises
    def dbt_finished(self, seconds: float) -> None:
        """Guarda a duração do dbt, que aparece nos campos."""
        self._change(force=False, dbt_seconds=seconds)

    @never_raises
    def set_tables(self, sizes: Mapping[str, int]) -> None:
        """Registra as tabelas da execução e o peso de cada uma na barra geral.

        :param sizes: Bytes exportados por tabela, na ordem de carga; use 1 enquanto o volume for desconhecido.
        """
        with self._lock:
            known = self._state.tables
            tables = {
                name: replace(known.get(name) or TableState(name), weight=float(max(size, 1)))
                for name, size in sizes.items()
            }
            self._change(force=False, tables=tables)

    @never_raises
    def table_step(self, table: str, step: TableStep, slot: str | None = None) -> None:
        """Muda a etapa da tabela em processamento (atualiza na hora); ``slot`` só vem quando conhecido."""
        with self._lock:
            self._table_started.setdefault(table, time.monotonic())
            current = self._state.tables.get(table)
            self._change_table(table, step=step, slot=slot or (current.slot if current else None))

    @never_raises
    def table_done(self, table: str, rows: int) -> None:
        """Marca a tabela como concluída, com as linhas validadas e o tempo gasto nela."""
        seconds = time.monotonic() - self._table_started.get(table, self._started)
        self._change_table(table, step=TableStep.DONE, rows=rows, seconds=seconds)

    @never_raises
    def loading(self, snapshot: ProgressSnapshot) -> None:
        """Andamento do SQL*Loader da tabela em processamento (chamado pela task de carga, sem forçar a edição)."""
        table = self._state.current_table
        if table is not None:
            self._change_table(table, force=False, load=snapshot)

    @never_raises
    def indexes(self, done: int, total: int) -> None:
        """Andamento da criação de índices da tabela em processamento."""
        table = self._state.current_table
        if table is not None:
            self._change_table(table, force=False, indexes_done=done, indexes_total=total)

    @never_raises
    def succeed(self) -> None:
        """Finaliza em verde com o tempo total e o resumo por tabela, e envia a mensagem final avulsa."""
        with self._lock:
            if not self._finalized:
                self._state = replace(self._state, status=RunStatus.SUCCESS)
                self._finalize()

    @never_raises
    def fail(self, error: BaseException) -> None:
        """Finaliza como falha (ou cancelado, se for uma interrupção) no passo e na tabela em que estava."""
        with self._lock:
            if self._finalized:
                return
            cancelled = is_cancellation(error)
            tables = self._state.tables
            current = self._state.current_table
            if not cancelled and current is not None and current in tables and self._state.step is Step.TABLES:
                tables = {**tables, current: replace(tables[current], failed=True)}
            self._state = replace(
                self._state,
                tables=tables,
                status=RunStatus.CANCELLED if cancelled else RunStatus.FAILED,
                failed_step=self._state.step,
                error=None if cancelled else f"{type(error).__name__}: {error}",
            )
            self._finalize()

    def _finalize(self) -> None:
        self._finalized = True
        self._push(force=True)
        if self._message is not None:
            try:
                self._message.announce(build_announcement(build_view(self.state)))
            except Exception as error:
                self._warn(f"Não foi possível montar a mensagem final do Discord: {type(error).__name__}")

    @contextmanager
    def guard(self) -> Iterator["LoadNotifier"]:
        """Torna este o notificador ativo; ao sair, finaliza (sucesso, ou falha/cancelamento antes de repropagar)."""
        global _current  # noqa: PLW0603 - ponteiro de módulo restaurado ao sair
        previous, _current = _current, self
        try:
            yield self
        except BaseException as error:
            self.fail(error)
            raise
        else:
            self.succeed()
        finally:
            _current = previous


_current: LoadNotifier | None = None


def current() -> LoadNotifier | None:
    """Notificador em curso, ou ``None`` fora do flow (as tasks o encontram aqui, sem recebê-lo por parâmetro)."""
    return _current

"""Mensagem da carga BigQuery → Oracle no Discord: estado agregado, publicação e finalização.

Uma execução, um pod, tabelas em sequência: o próprio flow muda os passos (``enter``, ``table_step``...) e as tasks de
carga e de índices informam o andamento pelo notificador ativo (``current``), um ponteiro de módulo definido por
``LoadNotifier.guard``. As tasks usam ``NO_CACHE``; passar o notificador (com trava e cliente HTTP) como parâmetro
faria o Prefect percorrê-lo ao registrar as entradas, e há um flow run por processo, então o ponteiro é seguro.
Nada aqui levanta exceção para a carga.

Batimento (``heartbeat``): com o Discord ligado, ``begin`` inicia uma thread daemon que, a cada ``heartbeat_seconds``,
republica a visão atual (sem forçar; o intervalo mínimo do cliente continua valendo). Sem ele a mensagem parece
congelada nos passos que não informam andamento (``DROP`` de índices, ``TRUNCATE``, contagem, estatísticas, espera do
In-Memory): o "Decorrido" e o "há X" da etapa só avançam quando algo publica. A thread para quando a execução é
finalizada (``succeed``/``fail``) e quando o ``guard`` termina, e o batimento publica sob a mesma trava das demais
mudanças, então nunca sobrescreve a mensagem final.
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
    resolve_webhook,
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
    stage_key,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.sqlldr import ProgressSnapshot

CANCELLATIONS = (KeyboardInterrupt, SystemExit, asyncio.CancelledError, TerminationSignal)
DEFAULT_HEARTBEAT_SECONDS = 60.0
HEARTBEAT_JOIN_TIMEOUT_SECONDS = 2.0


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
    :param heartbeat_seconds: Intervalo do batimento que republica a mensagem; ``0`` ou menos desliga o batimento.
    """

    dataset_id: str
    run_id: str
    run_name: str
    table_ids: tuple[str, ...]
    sessions: int
    run_dbt: bool
    mode: Literal["full", "synonyms_only"]
    enabled: bool
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS


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
        self._step_started = self._started
        self._table_started: dict[str, float] = {}
        self._lock = threading.RLock()
        self._finalized = False
        self._warn = Warner()
        self._heartbeat_seconds = config.heartbeat_seconds
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
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
        """Cria o notificador; webhook da variável do Infisical ou do Secret block (sem nenhum = desligado)."""
        message = DiscordStatusMessage(resolve_webhook()) if config.enabled else None
        return cls(config, message if message is not None and message.enabled else None)

    @property
    def state(self) -> RunState:
        """Estado atual, com o tempo decorrido (da execução e da etapa) e a hora de agora."""
        with self._lock:
            now = time.monotonic()
            return replace(
                self._state,
                elapsed_seconds=now - self._started,
                step_elapsed_seconds=now - self._step_started,
                updated_at=datetime.now(UTC),
            )

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
            before = stage_key(self._state)
            self._state = replace(self._state, **changes)
            if stage_key(self._state) != before:
                self._step_started = time.monotonic()
            self._push(force)

    def _change_table(self, name: str, force: bool = True, **changes: object) -> None:
        with self._lock:
            table = self._state.tables.get(name) or TableState(name)
            self._change(force, tables={**self._state.tables, name: replace(table, **changes)}, current_table=name)

    @never_raises
    def begin(self) -> None:
        """Cria a mensagem do Discord e, com ele ligado, inicia o batimento que a mantém atualizada."""
        with self._lock:
            self._step_started = time.monotonic()
        self._push(force=True)
        self._start_heartbeat()

    def _start_heartbeat(self) -> None:
        """Inicia a thread do batimento (uma só); sem Discord, sem intervalo ou depois do fim, não faz nada."""
        with self._lock:
            if self._message is None or self._heartbeat_seconds <= 0 or self._finalized:
                return
            if self._heartbeat_thread is not None:
                return
            thread = threading.Thread(target=self._heartbeat_loop, name="discord-heartbeat", daemon=True)
            thread.start()
            self._heartbeat_thread = thread

    def _heartbeat_loop(self) -> None:
        while not self._heartbeat_stop.wait(self._heartbeat_seconds):
            self._beat()

    def _beat(self) -> None:
        """Republica a visão atual sem forçar; uma falha é avisada uma vez e o laço segue vivo."""
        with self._lock:
            if self._finalized or self._heartbeat_stop.is_set():
                return
            try:
                self._push(force=False)
            except Exception as error:
                self._warn(f"Batimento do Discord falhou: {type(error).__name__}: {error}")

    def _stop_heartbeat(self) -> None:
        """Para a thread do batimento e a espera um instante; chamar fora da trava, que a thread pode estar pedindo."""
        self._heartbeat_stop.set()
        thread = self._heartbeat_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=HEARTBEAT_JOIN_TIMEOUT_SECONDS)

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
    def set_bigquery_rows(self, rows: Mapping[str, object]) -> None:
        """Guarda as linhas de cada tabela na foto do BigQuery, o lado de origem da comparação.

        :param rows: Linhas por tabela, como vêm do schema da foto exportada (``num_rows``); a conversão acontece
            aqui, dentro do ``never_raises``, para um valor inesperado não chegar ao flow.
        """
        counts = {name: int(str(count)) for name, count in rows.items() if count is not None}
        with self._lock:
            known = self._state.tables
            tables = {
                **known,
                **{name: replace(known.get(name) or TableState(name), bq_rows=count) for name, count in counts.items()},
            }
            self._change(force=False, tables=tables)

    @never_raises
    def set_physical_table(self, table: str, physical_table: str) -> None:
        """Guarda a tabela física do Oracle (slot) que recebe a carga da tabela."""
        self._change_table(table, force=False, physical_table=physical_table)

    @never_raises
    def table_loaded(self, table: str, rows: int) -> None:
        """Guarda as linhas carregadas pelo SQL*Loader na tabela."""
        self._change_table(table, force=False, loaded_rows=rows)

    @never_raises
    def table_validated(self, table: str, rows: int) -> None:
        """Guarda as linhas contadas no Oracle na validação da tabela."""
        self._change_table(table, force=False, oracle_rows=rows)

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
        self._stop_heartbeat()

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
        self._stop_heartbeat()

    def _finalize(self) -> None:
        self._finalized = True
        self._heartbeat_stop.set()
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
            self._stop_heartbeat()


_current: LoadNotifier | None = None


def current() -> LoadNotifier | None:
    """Notificador em curso, ou ``None`` fora do flow (as tasks o encontram aqui, sem recebê-lo por parâmetro)."""
    return _current

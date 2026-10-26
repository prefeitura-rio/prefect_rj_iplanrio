"""Mensagem da carga BigQuery → Oracle no Discord: estado agregado, publicação e finalização.

Uma execução, um pod, tabelas em sequência: o próprio flow muda os passos (``enter``,
``table_step``...) e as tasks de carga e de índices informam o andamento pelo
notificador ativo (``current``), um ponteiro de módulo definido por
``LoadNotifier.guard``. As tasks usam ``NO_CACHE``; passar o notificador (com trava e
cliente HTTP) como parâmetro faria o Prefect percorrê-lo ao registrar as entradas, e há
um flow run por processo, então o ponteiro é seguro. Nada aqui levanta exceção para a
carga.

Batimento (``heartbeat``): com o Discord ligado, ``begin`` inicia uma thread daemon que,
a cada ``heartbeat_seconds``, republica a visão atual (sem forçar; o intervalo mínimo do
cliente continua valendo). Sem ele a mensagem parece congelada nos passos que não
informam andamento (``DROP`` de índices, ``TRUNCATE``, contagem, estatísticas, espera do
In-Memory): o "Decorrido" e o "há X" da etapa só avançam quando algo publica. A thread
para quando a execução é finalizada (``succeed``/``fail``) e quando o ``guard`` termina,
e o batimento publica sob a mesma trava das demais mudanças, então nunca sobrescreve a
mensagem final.
"""

import asyncio
import threading
import time
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Literal, final

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord import (
    DiscordStatusMessage,
    Warner,
    never_raises,
    resolve_webhook,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_embed import build_announcement, build_payload
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_view import RunStatus
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.notify_view import (
    RunState,
    TableState,
    build_view,
    stage_key,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.sqlldr_progress import ProgressSnapshot
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.stages import Step, TableStep

CANCELLATIONS = (KeyboardInterrupt, SystemExit, asyncio.CancelledError)
DEFAULT_HEARTBEAT_SECONDS = 60.0
HEARTBEAT_JOIN_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class NotifierConfig:
    """Como criar o notificador.

    :param dataset_id: Dataset de origem. :param url: Link do flow run, ou ``None`` sem
    URL do Prefect. :param run_name: Nome do flow run, no rodapé. :param table_ids:
    Tabelas da execução (vazio até a listagem, em ``synonyms_only`` também). :param
    sessions: Sessões do SQL*Loader pedidas. :param run_dbt: Se o dbt roda antes. :param
    mode: ``full`` ou ``synonyms_only``. :param enabled: ``False``
    (``discord_notifications=false``) desliga o Discord. :param heartbeat_seconds:
    Intervalo do batimento que republica a mensagem; ``0`` ou menos desliga o batimento.
    """

    dataset_id: str
    url: str | None
    run_name: str
    table_ids: tuple[str, ...]
    sessions: int
    run_dbt: bool
    mode: Literal["full", "synonyms_only"]
    enabled: bool
    heartbeat_seconds: float = DEFAULT_HEARTBEAT_SECONDS
    cancellations: tuple[type[BaseException], ...] = ()


def is_cancellation(error: BaseException, extra: tuple[type[BaseException], ...] = ()) -> bool:
    """Tell whether an exception is an interruption (Ctrl+C, a cancellation, or one of `extra`) and no data failure."""
    return isinstance(error, (*CANCELLATIONS, *extra))


@final
class LoadNotifier:
    """Estado agregado da carga e a mensagem única do Discord; seguro para chamadas de qualquer thread.

    :param config: Execução.
    :param message: Cliente do webhook; ``None`` desliga o Discord.
    """

    def __init__(self, config: NotifierConfig, message: DiscordStatusMessage | None) -> None:
        self.message = message
        self.started = time.monotonic()
        self.step_started = self.started
        self.table_started: dict[str, float] = {}
        self.lock = threading.RLock()
        self.finalized = False
        self.warn = Warner()
        self.cancellations = config.cancellations
        self.heartbeat_seconds = config.heartbeat_seconds
        self.heartbeat_stop = threading.Event()
        self.heartbeat_thread: threading.Thread | None = None
        self.run_state = RunState(
            dataset_id=config.dataset_id,
            run_name=config.run_name,
            url=config.url,
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
        with self.lock:
            now = time.monotonic()
            return replace(
                self.run_state,
                elapsed_seconds=now - self.started,
                step_elapsed_seconds=now - self.step_started,
                updated_at=datetime.now(UTC),
            )

    def push(self, force: bool) -> None:
        if self.message is None:
            return
        try:
            payload = build_payload(build_view(self.state))
        except Exception as error:
            self.warn(f"Não foi possível montar a mensagem do Discord: {type(error).__name__}: {error}")
            return
        self.message.publish(payload, force=force)

    def change(self, force: bool = True, **changes: object) -> None:
        with self.lock:
            if self.finalized:
                return
            before = stage_key(self.run_state)
            self.run_state = replace(self.run_state, **changes)
            if stage_key(self.run_state) != before:
                self.step_started = time.monotonic()
            self.push(force)

    def change_table(self, name: str, force: bool = True, **changes: object) -> None:
        with self.lock:
            table = self.run_state.tables.get(name) or TableState(name)
            self.change(force, tables={**self.run_state.tables, name: replace(table, **changes)}, current_table=name)

    @never_raises
    def begin(self) -> None:
        """Cria a mensagem do Discord e, com ele ligado, inicia o batimento que a mantém atualizada."""
        with self.lock:
            self.step_started = time.monotonic()
        self.push(force=True)
        self.start_heartbeat()

    def start_heartbeat(self) -> None:
        """Inicia a thread do batimento (uma só); sem Discord, sem intervalo ou depois do fim, não faz nada."""
        with self.lock:
            if self.message is None or self.heartbeat_seconds <= 0 or self.finalized:
                return
            if self.heartbeat_thread is not None:
                return
            thread = threading.Thread(target=self.heartbeat_loop, name="discord-heartbeat", daemon=True)
            thread.start()
            self.heartbeat_thread = thread

    def heartbeat_loop(self) -> None:
        while not self.heartbeat_stop.wait(self.heartbeat_seconds):
            self.beat()

    def beat(self) -> None:
        """Republica a visão atual sem forçar; uma falha é avisada uma vez e o laço segue vivo."""
        with self.lock:
            if self.finalized or self.heartbeat_stop.is_set():
                return
            try:
                self.push(force=False)
            except Exception as error:
                self.warn(f"Batimento do Discord falhou: {type(error).__name__}: {error}")

    def stop_heartbeat(self) -> None:
        """Para a thread do batimento e a espera um instante; chamar fora da trava, que a thread pode estar pedindo."""
        self.heartbeat_stop.set()
        thread = self.heartbeat_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=HEARTBEAT_JOIN_TIMEOUT_SECONDS)

    @never_raises
    def enter(self, step: Step) -> None:
        """Entra num passo (dbt, exportação, tabelas, espera do In-Memory, troca) e atualiza a mensagem na hora."""
        self.change(step=step)

    @never_raises
    def dbt_finished(self, seconds: float) -> None:
        """Guarda a duração do dbt, que aparece nos campos."""
        self.change(force=False, dbt_seconds=seconds)

    @never_raises
    def set_tables(self, sizes: Mapping[str, int]) -> None:
        """Registra as tabelas da execução e o peso de cada uma na barra geral.

        :param sizes: Bytes exportados por tabela, na ordem de carga; use 1 enquanto o
        volume for desconhecido.
        """
        with self.lock:
            known = self.run_state.tables
            tables = {
                name: replace(known.get(name) or TableState(name), weight=float(max(size, 1)))
                for name, size in sizes.items()
            }
            self.change(force=False, tables=tables)

    @never_raises
    def table_step(self, table: str, step: TableStep, slot: str | None = None) -> None:
        """Muda a etapa da tabela em processamento (atualiza na hora); ``slot`` só vem quando conhecido."""
        with self.lock:
            self.table_started.setdefault(table, time.monotonic())
            current = self.run_state.tables.get(table)
            self.change_table(table, step=step, slot=slot or (current.slot if current else None))

    @never_raises
    def exported(self, sizes: Mapping[str, int], rows: Mapping[str, object]) -> None:
        """Registra o fim da exportação: o peso de cada tabela e as linhas dela na foto do BigQuery."""
        self.set_tables(sizes)
        self.set_bigquery_rows(rows)

    def set_bigquery_rows(self, rows: Mapping[str, object]) -> None:
        """Guarda as linhas de cada tabela na foto do BigQuery, o lado de origem da comparação.

        :param rows: Linhas por tabela, como vêm do schema da foto exportada
        (``num_rows``); a conversão acontece
            aqui, dentro do ``never_raises``, para um valor inesperado não chegar ao
            flow.
        """
        counts = {name: int(str(count)) for name, count in rows.items() if count is not None}
        with self.lock:
            known = self.run_state.tables
            tables = {
                **known,
                **{name: replace(known.get(name) or TableState(name), bq_rows=count) for name, count in counts.items()},
            }
            self.change(force=False, tables=tables)

    @never_raises
    def set_physical_table(self, table: str, physical_table: str) -> None:
        """Guarda a tabela física do Oracle (slot) que recebe a carga da tabela."""
        self.change_table(table, force=False, physical_table=physical_table)

    @never_raises
    def table_loaded(self, table: str, rows: int) -> None:
        """Guarda as linhas carregadas pelo SQL*Loader na tabela."""
        self.change_table(table, force=False, loaded_rows=rows)

    @never_raises
    def table_validated(self, table: str, rows: int) -> None:
        """Guarda as linhas contadas no Oracle na validação da tabela."""
        self.change_table(table, force=False, oracle_rows=rows)

    @never_raises
    def table_done(self, table: str, rows: int) -> None:
        """Marca a tabela como concluída, com as linhas validadas e o tempo gasto nela."""
        seconds = time.monotonic() - self.table_started.get(table, self.started)
        self.change_table(table, step=TableStep.DONE, rows=rows, seconds=seconds)

    @never_raises
    def loading(self, snapshot: ProgressSnapshot) -> None:
        """Andamento do SQL*Loader da tabela em processamento (chamado pela task de carga, sem forçar a edição)."""
        table = self.run_state.current_table
        if table is not None:
            self.change_table(table, force=False, load=snapshot)

    @never_raises
    def indexes(self, done: int, total: int) -> None:
        """Andamento da criação de índices da tabela em processamento."""
        table = self.run_state.current_table
        if table is not None:
            self.change_table(table, force=False, indexes_done=done, indexes_total=total)

    @never_raises
    def succeed(self) -> None:
        """Finaliza em verde com o tempo total e o resumo por tabela, e envia a mensagem final avulsa."""
        with self.lock:
            if not self.finalized:
                self.run_state = replace(self.run_state, status=RunStatus.SUCCESS)
                self.finalize()
        self.stop_heartbeat()

    @never_raises
    def fail(self, error: BaseException) -> None:
        """Finaliza como falha (ou cancelado, se for uma interrupção) no passo e na tabela em que estava."""
        with self.lock:
            if self.finalized:
                return
            cancelled = is_cancellation(error, self.cancellations)
            tables = self.run_state.tables
            current = self.run_state.current_table
            if not cancelled and current is not None and current in tables and self.run_state.step is Step.TABLES:
                tables = {**tables, current: replace(tables[current], failed=True)}
            self.run_state = replace(
                self.run_state,
                tables=tables,
                status=RunStatus.CANCELLED if cancelled else RunStatus.FAILED,
                failed_step=self.run_state.step,
                error=None if cancelled else f"{type(error).__name__}: {error}",
            )
            self.finalize()
        self.stop_heartbeat()

    def finalize(self) -> None:
        self.finalized = True
        self.heartbeat_stop.set()
        self.push(force=True)
        if self.message is not None:
            try:
                self.message.announce(build_announcement(build_view(self.state)))
            except Exception as error:
                self.warn(f"Não foi possível montar a mensagem final do Discord: {type(error).__name__}")

    @contextmanager
    def guard(self) -> Generator["LoadNotifier"]:
        """Torna este o notificador ativo; ao sair, finaliza (sucesso, ou falha/cancelamento antes de repropagar)."""
        global active  # noqa: PLW0603 - ponteiro de módulo restaurado ao sair
        previous, active = active, self
        try:
            yield self
        except BaseException as error:
            self.fail(error)
            raise
        else:
            self.succeed()
        finally:
            active = previous
            self.stop_heartbeat()


active: LoadNotifier | None = None


def current() -> LoadNotifier | None:
    """Notificador em curso, ou ``None`` fora do flow (as tasks o encontram aqui, sem recebê-lo por parâmetro)."""
    return active

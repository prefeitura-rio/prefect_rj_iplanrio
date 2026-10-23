"""Mensagem da publicação BigQuery → Postgres no Discord: o mesmo formato das cargas irmãs.

Uma execução, um pod: o flow muda as etapas e as tasks informam as tabelas pelo notificador ativo (``current``), um
ponteiro de módulo definido por ``PublishNotifier.guard``. Nada aqui levanta exceção para a carga. A mensagem é criada
uma vez e editada a cada etapa; no fim sai uma mensagem avulsa (a que notifica as pessoas), com o erro na falha.
Em staging o webhook é o de staging e o título e a mensagem final levam ``🧪 STAGING · ``.
"""

import asyncio
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime

from prefect.exceptions import TerminationSignal
from prefect.settings import PREFECT_API_URL, PREFECT_UI_URL

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.discord import (
    DiscordStatusMessage,
    Environment,
    Warner,
    never_raises,
    resolve_webhook,
)
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.discord_embed import (
    RunStatus,
    build_announcement,
    build_payload,
)
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.discord_format import flow_run_url
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.notify_view import (
    NotifierConfig,
    RunState,
    Step,
    TableState,
    build_view,
)

CANCELLATIONS = (KeyboardInterrupt, SystemExit, asyncio.CancelledError, TerminationSignal)


def is_cancellation(error: BaseException) -> bool:
    """Diz se a exceção é uma interrupção (SIGTERM do Prefect, Ctrl+C, cancelamento) e não uma falha de dados."""
    return isinstance(error, CANCELLATIONS)


def make_message(environment: Environment) -> DiscordStatusMessage | None:
    """Cliente do webhook do ambiente (variável do Infisical ou Secret block); ``None`` sem webhook configurado."""
    message = DiscordStatusMessage(resolve_webhook(environment=environment))
    return message if message.enabled else None


class PublishNotifier:
    """Estado agregado da publicação e a mensagem do Discord; seguro para chamadas de qualquer thread.

    :param config: Execução.
    :param message: Cliente do webhook; ``None`` desliga o Discord.
    """

    def __init__(self, config: NotifierConfig, message: DiscordStatusMessage | None) -> None:
        self._message = message
        self._started = time.monotonic()
        self._lock = threading.RLock()
        self._warn = Warner()
        self._finalized = False
        self._state = RunState(
            config=config,
            url=flow_run_url(PREFECT_UI_URL.value(), PREFECT_API_URL.value(), config.run_id),
            status=RunStatus.RUNNING,
            step=Step.MANIFEST,
            tables={},
            elapsed_seconds=0.0,
            updated_at=datetime.now(UTC),
        )

    @classmethod
    def create(cls, config: NotifierConfig) -> "PublishNotifier":
        """Cria o notificador com o webhook do ambiente; sem webhook ou com ``enabled=False``, fica desligado."""
        return cls(config, make_message(config.environment) if config.enabled else None)

    @classmethod
    def disabled(cls) -> "PublishNotifier":
        """Notificador desligado, para as tasks chamadas fora de um flow."""
        return cls(NotifierConfig(dataset_id="-", run_id="-", run_name="-", enabled=False), None)

    @property
    def state(self) -> RunState:
        """Estado atual, com o tempo decorrido e a hora de agora."""
        with self._lock:
            return replace(self._state, elapsed_seconds=time.monotonic() - self._started, updated_at=datetime.now(UTC))

    def _push(self) -> None:
        if self._message is None:
            return
        try:
            payload = build_payload(build_view(self.state))
        except Exception as error:
            self._warn(f"Não foi possível montar a mensagem do Discord: {type(error).__name__}: {error}")
            return
        self._message.publish(payload, force=True)

    def _change(self, **changes: object) -> None:
        with self._lock:
            if not self._finalized:
                self._state = replace(self._state, **changes)
                self._push()

    @never_raises
    def begin(self) -> None:
        """Cria a mensagem do Discord."""
        self._push()

    @never_raises
    def enter(self, step: Step) -> None:
        """Entra num passo e atualiza a mensagem na hora."""
        self._change(step=step)

    @never_raises
    def set_tables(self, expected_rows: Mapping[str, int]) -> None:
        """Registra as tabelas da publicação e as linhas que o manifesto promete para cada uma."""
        self._change(tables={name: TableState(name, expected=rows) for name, rows in expected_rows.items()})

    @never_raises
    def table_loaded(self, name: str, rows: int) -> None:
        """Guarda as linhas carregadas no slot para a tabela."""
        known = self._state.tables.get(name) or TableState(name)
        self._change(tables={**self._state.tables, name: replace(known, loaded=rows)})

    @never_raises
    def published(self, slot: str, checksum: str, snapshot: datetime) -> None:
        """Guarda o slot publicado, o checksum e a foto do BigQuery que a publicação registrou."""
        self._change(slot=slot, checksum=checksum, snapshot=snapshot)

    @never_raises
    def succeed(self) -> None:
        """Finaliza em verde e envia a mensagem final avulsa."""
        with self._lock:
            if not self._finalized:
                self._state = replace(self._state, status=RunStatus.SUCCESS)
                self._finalize()

    @never_raises
    def fail(self, error: BaseException) -> None:
        """Finaliza como falha (ou cancelado, se for uma interrupção) no passo em que estava."""
        with self._lock:
            if self._finalized:
                return
            cancelled = is_cancellation(error)
            self._state = replace(
                self._state,
                status=RunStatus.CANCELLED if cancelled else RunStatus.FAILED,
                failed_step=self._state.step,
                error=None if cancelled else f"{type(error).__name__}: {error}",
            )
            self._finalize()

    def _finalize(self) -> None:
        self._finalized = True
        self._push()
        if self._message is not None:
            try:
                self._message.announce(build_announcement(build_view(self.state)))
            except Exception as error:
                self._warn(f"Não foi possível montar a mensagem final do Discord: {type(error).__name__}")

    @contextmanager
    def guard(self) -> Iterator["PublishNotifier"]:
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


_current: PublishNotifier | None = None


def current() -> PublishNotifier:
    """Notificador em curso; fora do flow, um desligado (as tasks o encontram aqui, sem recebê-lo por parâmetro)."""
    return _current if _current is not None else PublishNotifier.disabled()

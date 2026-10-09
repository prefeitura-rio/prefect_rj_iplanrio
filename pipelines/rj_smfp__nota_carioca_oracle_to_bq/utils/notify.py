"""Mensagem do pai no Discord: estado agregado, publicação e finalização em sucesso, falha ou cancelamento.

O pai é dono da mensagem. Os filhos só gravam JSONs no GCS (``table_progress``); a cada poll da supervisão o pai os lê
(:meth:`ParentNotifier.observe`) e edita a mensagem. No modo sequencial o repórter da tabela chama
:meth:`ParentNotifier.update_table` direto, em memória. Nada aqui levanta exceção para a carga: o construtor do
embed e o cliente HTTP são isolados.
"""

import asyncio
import threading
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from prefect.exceptions import TerminationSignal
from prefect.settings import PREFECT_API_URL, PREFECT_UI_URL

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.discord import (
    DiscordStatusMessage,
    Warner,
    never_raises,
    resolve_webhook,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.discord_embed import (
    RunStatus,
    build_announcement,
    build_payload,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.discord_format import flow_run_url
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.notify_view import (
    ParentStage,
    ParentState,
    build_view,
    current_stage,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import Snapshot
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import FAILED_STATES, RunInfo
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.table_progress import ProgressStore, TableProgress, TableStage

CANCELLATIONS = (KeyboardInterrupt, SystemExit, asyncio.CancelledError, TerminationSignal)
SETTLED_TABLE_STAGES = (TableStage.VALIDATED, TableStage.FAILED)


@dataclass(frozen=True)
class NotifierConfig:
    """Como criar o notificador do pai.

    :param dataset_id: Dataset de destino.
    :param run_id: Flow run do pai.
    :param run_name: Nome do flow run, no rodapé.
    :param table_names: Tabelas da execução.
    :param workers: Processos de leitura por pod.
    :param enabled: ``False`` (``discord_notifications=false``) desliga o Discord e a leitura dos JSONs.
    :param project: Projeto do GCS onde os filhos gravam o progresso.
    :param bucket: Bucket dos arquivos da carga.
    """

    dataset_id: str
    run_id: str
    run_name: str
    table_names: tuple[str, ...]
    workers: int
    enabled: bool
    project: str
    bucket: str


def is_cancellation(error: BaseException) -> bool:
    """Diz se a exceção é uma interrupção (SIGTERM do Prefect, Ctrl+C, cancelamento) e não uma falha de dados."""
    return isinstance(error, CANCELLATIONS)


class ParentNotifier:
    """Estado agregado do pai e a mensagem única do Discord; seguro para as threads da supervisão e da extração.

    :param config: Execução e destino.
    :param message: Cliente do webhook; ``None`` desliga o Discord.
    :param store: Leitor dos JSONs dos filhos; ``None`` no modo sequencial ou com as notificações desligadas.
    """

    def __init__(
        self, config: NotifierConfig, message: DiscordStatusMessage | None, store: ProgressStore | None
    ) -> None:
        self._message = message
        self._store = store
        self._started = time.monotonic()
        self._lock = threading.RLock()
        self._finalized = False
        self._warn = Warner()
        self._state = ParentState(
            dataset_id=config.dataset_id,
            run_name=config.run_name,
            url=flow_run_url(PREFECT_UI_URL.value(), PREFECT_API_URL.value(), config.run_id),
            status=RunStatus.RUNNING,
            stage=ParentStage.SNAPSHOT,
            table_names=config.table_names,
            tables={},
            workers=config.workers,
            elapsed_seconds=0.0,
            updated_at=datetime.now(UTC),
        )

    @classmethod
    def create(cls, config: NotifierConfig) -> "ParentNotifier":
        """Cria o notificador; webhook da variável do Infisical ou do Secret block (sem nenhum = desligado)."""
        if not config.enabled:
            return cls(config, None, None)
        message = DiscordStatusMessage(resolve_webhook())
        store = ProgressStore(config.project, config.bucket, config.run_id) if message.enabled else None
        return cls(config, message if message.enabled else None, store)

    @property
    def enabled(self) -> bool:
        """Indica se o Discord está ligado (webhook presente e ``discord_notifications`` verdadeiro)."""
        return self._message is not None

    @property
    def state(self) -> ParentState:
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

    @never_raises
    def begin(self) -> None:
        """Cria a mensagem do Discord (estado inicial, etapa da foto)."""
        self._push(force=True)

    @never_raises
    def set_stage(self, stage: ParentStage) -> None:
        """Muda a etapa do pai e atualiza a mensagem na hora."""
        with self._lock:
            if self._finalized:
                return
            self._state = replace(self._state, stage=stage)
            self._push(force=True)

    @never_raises
    def set_snapshot(self, snapshot: Snapshot) -> None:
        """Guarda o SCN e o horário da foto, que aparecem nos campos da mensagem."""
        with self._lock:
            self._state = replace(self._state, scn=snapshot.scn, snapshot_taken_at=snapshot.taken_at)

    @never_raises
    def update_table(self, progress: TableProgress) -> None:
        """Atualiza uma tabela (modo sequencial, em memória); a mudança de etapa publica na hora."""
        self._merge({progress.table: progress})

    @never_raises
    def observe(self, runs: Mapping[str, RunInfo]) -> None:
        """Gancho da supervisão, chamado a cada poll: lê os JSONs dos filhos e atualiza a mensagem.

        :param runs: Flow run de cada filho, por tabela; um filho que falhou sem avisar vira tabela falha.
        """
        found = self._store.read_all(list(runs)) if self._store is not None else {}
        merged = {}
        for table, run in runs.items():
            progress = found.get(table) or self._state.tables.get(table)
            if run.state_type in FAILED_STATES and (progress is None or progress.stage not in SETTLED_TABLE_STAGES):
                base = progress or TableProgress(table=table, stage=TableStage.WAITING)
                progress = replace(
                    base,
                    stage=TableStage.FAILED,
                    failed_stage=base.stage,
                    error=base.error or f"flow run do filho em {run.state_name}",
                )
            if progress is not None:
                merged[table] = progress
        self._merge(merged)

    def _merge(self, updates: Mapping[str, TableProgress]) -> None:
        with self._lock:
            if self._finalized:
                return
            stage_changed = any(
                (old := self._state.tables.get(name)) is None or old.stage is not new.stage
                for name, new in updates.items()
            )
            self._state = replace(self._state, tables={**self._state.tables, **updates})
            self._push(force=stage_changed)

    @never_raises
    def succeed(self) -> None:
        """Finaliza em verde com o tempo total e o resumo por tabela, e envia a mensagem final avulsa."""
        with self._lock:
            if self._finalized:
                return
            self._state = replace(self._state, status=RunStatus.SUCCESS, stage=ParentStage.CLEANUP)
            self._finalize()

    @never_raises
    def fail(self, error: BaseException) -> None:
        """Finaliza como falha (ou cancelado, se for uma interrupção) na etapa em que o pai estava."""
        with self._lock:
            if self._finalized:
                return
            cancelled = is_cancellation(error)
            self._state = replace(
                self._state,
                status=RunStatus.CANCELLED if cancelled else RunStatus.FAILED,
                failed_stage=current_stage(self._state),
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
    def guard(self) -> Iterator["ParentNotifier"]:
        """Torna este o notificador ativo; ao sair, finaliza (sucesso, ou falha/cancelamento antes de repropagar)."""
        global _active_parent  # noqa: PLW0603 - ponteiro de módulo restaurado ao sair
        previous, _active_parent = _active_parent, self
        try:
            yield self
        except BaseException as error:
            self.fail(error)
            raise
        else:
            self.succeed()
        finally:
            _active_parent = previous


_active_parent: ParentNotifier | None = None


def active_parent() -> ParentNotifier | None:
    """Notificador do pai em curso, ou ``None`` fora do pai (as tasks o encontram aqui, sem recebê-lo por parâmetro)."""
    return _active_parent

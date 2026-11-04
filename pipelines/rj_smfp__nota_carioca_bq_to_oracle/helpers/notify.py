"""Show the BigQuery to Oracle load in Discord: aggregate state, publishing and the end.

One run is one pod, with the tables in sequence. The flow itself changes the steps
(`enter`, `table_step`...), and the load and index tasks report the progress to the
active notifier (`current`), a module pointer that `LoadNotifier.guard` sets. The
tasks use `NO_CACHE`. Passing the notifier (with its lock and HTTP client) as a
parameter would make Prefect walk it when it records the inputs, and there is one flow
run for each process, so the pointer is safe. Nothing here raises for the load.

The heartbeat: with Discord on, `begin` starts a daemon thread that publishes the
current view every `heartbeat_seconds` (without force, so the minimum interval of the
client still holds). Without it the message looks frozen in the steps that report no
progress (`DROP` of indexes, `TRUNCATE`, the count, the statistics, the wait for the
In-Memory): the "Decorrido" and the "há X" of the stage only move when something
publishes. The thread stops when the run ends (`succeed` or `fail`) and when `guard`
ends. It publishes under the same lock as the other changes, so it never overwrites
the final message.
"""

import time
from asyncio import CancelledError
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from threading import Event, RLock, Thread, current_thread
from typing import Literal, final

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord import (
    DiscordStatusMessage,
    Environment,
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

CANCELLATIONS = (KeyboardInterrupt, SystemExit, CancelledError)
DEFAULT_HEARTBEAT_SECONDS = 60.0
HEARTBEAT_JOIN_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class NotifierConfig:
    """How to create the notifier.

    `url` is the link of the flow run, or `None` without a Prefect URL. `run_name` goes
    in the footer. `table_ids` are the tables of the run (empty until the listing, and
    also in `synonyms_only`). `sessions` are the SQL*Loader sessions that were asked
    for. `run_dbt` says whether dbt runs before, and `mode` is `full` or
    `synonyms_only`. `enabled=False` (`discord_notifications=false`) turns Discord off.
    `heartbeat_seconds` is the interval of the heartbeat that publishes the message
    again, and `0` or less turns it off. `environment` is the one of the deployment:
    staging marks the messages with `🧪 STAGING · `.
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
    environment: Environment = Environment.PROD


def is_cancellation(error: BaseException, extra: tuple[type[BaseException], ...] = ()) -> bool:
    """Tell whether an exception is an interruption and not a data failure.

    Ctrl+C, a cancellation or one of `extra` is an interruption.
    """
    return isinstance(error, (*CANCELLATIONS, *extra))


@final
class LoadNotifier:
    """The aggregate state of the load and the one Discord message.

    Any thread may call it.

    `config` describes the run. `message` is the client of the webhook, and `None` turns
    Discord off.
    """

    def __init__(self, config: NotifierConfig, message: DiscordStatusMessage | None) -> None:
        """Keep the config and the message, and start the state of the run."""
        self.message = message
        self.started = time.monotonic()
        self.step_started = self.started
        self.table_started: dict[str, float] = {}
        self.lock = RLock()
        self.finalized = False
        self.warn = Warner()
        self.cancellations = config.cancellations
        self.heartbeat_seconds = config.heartbeat_seconds
        self.heartbeat_stop = Event()
        self.heartbeat_thread: Thread | None = None
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
            staging=config.environment is Environment.STAGING,
        )

    @classmethod
    def create(cls, config: NotifierConfig) -> "LoadNotifier":
        """Create the notifier, with the webhook from Infisical or the Secret block.

        With neither, Discord is off.
        """
        message = DiscordStatusMessage(resolve_webhook()) if config.enabled else None
        return cls(config, message if message is not None and message.enabled else None)

    @property
    def state(self) -> RunState:
        """Return the current state, with the elapsed times and the time now.

        The times are those of the run and of the stage.
        """
        with self.lock:
            now = time.monotonic()
            return replace(
                self.run_state,
                elapsed_seconds=now - self.started,
                step_elapsed_seconds=now - self.step_started,
                updated_at=datetime.now(UTC),
            )

    def push(self, force: bool) -> None:
        """Publish the current view, and never raise."""
        if self.message is None:
            return
        try:
            payload = build_payload(build_view(self.state))
        except Exception as error:
            self.warn(f"Não foi possível montar a mensagem do Discord: {type(error).__name__}: {error}")
            return
        self.message.publish(payload, force=force)

    def change(self, force: bool = True, **changes: object) -> None:
        """Change the state of the run, and publish it."""
        with self.lock:
            if self.finalized:
                return
            before = stage_key(self.run_state)
            self.run_state = replace(self.run_state, **changes)
            if stage_key(self.run_state) != before:
                self.step_started = time.monotonic()
            self.push(force)

    def change_table(self, name: str, force: bool = True, **changes: object) -> None:
        """Change the state of one table, and publish it."""
        with self.lock:
            table = self.run_state.tables.get(name) or TableState(name)
            self.change(force, tables={**self.run_state.tables, name: replace(table, **changes)}, current_table=name)

    @never_raises
    def begin(self) -> None:
        """Create the Discord message and, if on, start the heartbeat.

        The heartbeat keeps the message updated.
        """
        with self.lock:
            self.step_started = time.monotonic()
        self.push(force=True)
        self.start_heartbeat()

    def start_heartbeat(self) -> None:
        """Start the heartbeat thread (only one).

        It does nothing without Discord, an interval or after the end.
        """
        with self.lock:
            if self.message is None or self.heartbeat_seconds <= 0 or self.finalized:
                return
            if self.heartbeat_thread is not None:
                return
            thread = Thread(target=self.heartbeat_loop, name="discord-heartbeat", daemon=True)
            thread.start()
            self.heartbeat_thread = thread

    def heartbeat_loop(self) -> None:
        """Publish the view every `heartbeat_seconds`, until the heartbeat stops."""
        while not self.heartbeat_stop.wait(self.heartbeat_seconds):
            self.beat()

    def beat(self) -> None:
        """Publish the current view with no force.

        A failure warns once, and the loop stays alive.
        """
        with self.lock:
            if self.finalized or self.heartbeat_stop.is_set():
                return
            try:
                self.push(force=False)
            except Exception as error:
                self.warn(f"Batimento do Discord falhou: {type(error).__name__}: {error}")

    def stop_heartbeat(self) -> None:
        """Stop the heartbeat thread and wait a moment for it.

        Call it outside the lock, which the thread may be asking for.
        """
        self.heartbeat_stop.set()
        thread = self.heartbeat_thread
        if thread is not None and thread is not current_thread():
            thread.join(timeout=HEARTBEAT_JOIN_TIMEOUT_SECONDS)

    @never_raises
    def enter(self, step: Step) -> None:
        """Enter a step and update the message at once.

        The steps are dbt, export, tables, the In-Memory wait and the swap.
        """
        self.change(step=step)

    @never_raises
    def dbt_finished(self, seconds: float) -> None:
        """Keep the duration of dbt, which shows in the fields."""
        self.change(force=False, dbt_seconds=seconds)

    @never_raises
    def set_tables(self, sizes: Mapping[str, int]) -> None:
        """Record the tables of the run and the weight of each one in the overall bar.

        `sizes` are the bytes exported for each table, in the load order. Use 1 while
        the volume is not known.
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
        """Change the stage of the table in process (it updates at once).

        `slot` comes only when it is known.
        """
        with self.lock:
            self.table_started.setdefault(table, time.monotonic())
            current = self.run_state.tables.get(table)
            self.change_table(table, step=step, slot=slot or (current.slot if current else None))

    @never_raises
    def exported(self, sizes: Mapping[str, int], rows: Mapping[str, object]) -> None:
        """Record the end of the export: table weights and BigQuery snapshot rows."""
        self.set_tables(sizes)
        self.set_bigquery_rows(rows)

    def set_bigquery_rows(self, rows: Mapping[str, object]) -> None:
        """Keep the rows of each table in the BigQuery snapshot, the source side.

        `rows` are the rows of each table as the schema of the exported snapshot gives
        them (`num_rows`). The conversion happens here, inside `never_raises`, so an
        unexpected value never reaches the flow.
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
        """Keep the physical Oracle table (slot) that receives the load of the table."""
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
        """Mark the table as done, with its validated rows and the time spent on it."""
        seconds = time.monotonic() - self.table_started.get(table, self.started)
        self.change_table(table, step=TableStep.DONE, rows=rows, seconds=seconds)

    @never_raises
    def loading(self, snapshot: ProgressSnapshot) -> None:
        """Report the SQL*Loader progress of the table in process, with no force.

        The load task calls it.
        """
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
        """Finish as a success with the total time and the summary of each table.

        Then send the final separate message.
        """
        with self.lock:
            if not self.finalized:
                self.run_state = replace(self.run_state, status=RunStatus.SUCCESS)
                self.finalize()
        self.stop_heartbeat()

    @never_raises
    def fail(self, error: BaseException) -> None:
        """Finish as a failure, or as cancelled for an interruption.

        It marks the step and table it was in.
        """
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
        """Mark the run as finished, stop the heartbeat, and publish the last view."""
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
        """Make this the active notifier, and finish when the block exits.

        It finishes as a success, or as a failure or cancel before it raises again.
        """
        global active  # noqa: PLW0603  # the module pointer goes back when the block exits
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
    """Return the notifier in use, or `None` outside the flow.

    The tasks find it here and do not get it as a parameter.
    """
    return active

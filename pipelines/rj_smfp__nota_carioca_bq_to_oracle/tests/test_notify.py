# ruff: noqa: PLR2004
import json
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Literal

import httpx
import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle import flow as flow_module
from pipelines.rj_smfp__nota_carioca_bq_to_oracle import tasks as tasks_module
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils import discord as discord_module
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils import slots, sqlldr
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.bigquery import ExportedFile
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import LoadPlan
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord import DiscordStatusMessage
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.discord_embed import (
    COLOR_CANCELLED,
    COLOR_FAILURE,
    COLOR_RUNNING,
    COLOR_SUCCESS,
    ItemStatus,
    RunStatus,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.notify import LoadNotifier, NotifierConfig, current
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.notify_view import (
    RunState,
    TableState,
    build_view,
    overall_fraction,
    table_fraction,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.stages import Step, TableStep

NOW = datetime(2026, 10, 9, 15, 0, 0, tzinfo=UTC)
GIB = 1024**3


class Discord:
    """Webhook falso que registra o que o notificador envia."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.transport = httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, json.loads(request.content)))
        return httpx.Response(200, json={"id": "42"})

    @property
    def embeds(self) -> list[dict[str, object]]:
        embeds = [body["embeds"] for _, body in self.calls if "embeds" in body]
        return [embed[0] for embed in embeds if isinstance(embed, list)]

    @property
    def standalone(self) -> list[str]:
        return [str(body["content"]) for _, body in self.calls if "content" in body]


def make(
    discord: Discord | None,
    tables: tuple[str, ...] = ("A", "B"),
    run_dbt: bool = False,
    mode: Literal["full", "synonyms_only"] = "full",
) -> LoadNotifier:
    config = NotifierConfig("nota_carioca_staging", "run-1", "nota_carioca_staging", tables, 2, run_dbt, mode, True)
    message = (
        DiscordStatusMessage("https://discord.example/api/webhooks/1/t", transport=discord.transport)
        if discord
        else None
    )
    return LoadNotifier(config, message)


def snapshot(done: int, total: int = 100 * GIB, elapsed: float = 100.0) -> sqlldr.ProgressSnapshot:
    return sqlldr.ProgressSnapshot(
        files_done=3, files_total=10, bytes_done=done, bytes_total=total, elapsed_seconds=elapsed
    )


@pytest.fixture(autouse=True)
def quiet_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(discord_module, "warn", lambda _: None)
    monkeypatch.setattr(discord_module, "info", lambda _: None)


def state(**overrides: object) -> RunState:
    base = RunState(
        dataset_id="ds",
        run_name="ds",
        url=None,
        status=RunStatus.RUNNING,
        mode="full",
        run_dbt=False,
        step=Step.TABLES,
        tables={},
        sessions=2,
        elapsed_seconds=10.0,
        updated_at=NOW,
    )
    return replace(base, **overrides)


# --- weights -----------------------------------------------------------------------------------------------------


def test_table_fraction_follows_stage_weights_and_load_bytes() -> None:
    # Given a table loading half of its bytes, and tables at later stages
    loading = TableState("A", step=TableStep.LOAD, load=snapshot(50 * GIB))
    indexing = TableState("A", step=TableStep.INDEXES, indexes_done=1, indexes_total=2)
    # Then the fraction is the finished stages plus the weighted inner progress, out of 100
    assert table_fraction(TableState("A")) == 0.0
    assert table_fraction(loading) == pytest.approx((5 + 60 * 0.5) / 100)
    assert table_fraction(indexing) == pytest.approx((5 + 60 + 3 + 20 * 0.5) / 100)
    assert table_fraction(TableState("A", step=TableStep.DONE)) == 1.0


def test_overall_fraction_normalizes_active_steps_and_weights_tables_by_volume() -> None:
    # Given two tables, A (3x the bytes of B) done and B untouched
    tables = {"A": TableState("A", weight=3, step=TableStep.DONE), "B": TableState("B", weight=1)}
    without_dbt = state(tables=tables)
    with_dbt = state(tables=tables, run_dbt=True, step=Step.TABLES)
    # Then the tables phase is 75% done, and steps are normalized (export 5 + tables 70 + wait 4 + swap 1 = 80)
    assert overall_fraction(without_dbt) == pytest.approx((5 + 70 * 0.75) / 80)
    assert overall_fraction(with_dbt) == pytest.approx((20 + 5 + 70 * 0.75) / 100)
    assert overall_fraction(state(status=RunStatus.SUCCESS)) == 1.0
    assert overall_fraction(state(mode="synonyms_only", step=Step.SWAP)) == 0.0


# --- notifier ----------------------------------------------------------------------------------------------------


def test_full_run_walks_the_steps_and_ends_green_with_per_table_summary() -> None:
    # Given a live notifier for two tables, dbt included
    discord = Discord()
    notifier = make(discord, run_dbt=True)
    # When the flow walks through its steps
    with notifier.guard():
        notifier.begin()
        notifier.dbt_finished(754)
        notifier.set_tables({"A": 10 * GIB, "B": 5 * GIB})
        notifier.enter(Step.EXPORT)
        notifier.enter(Step.TABLES)
        notifier.table_step("A", TableStep.PREPARATION, slot="B")
        notifier.table_step("A", TableStep.LOAD)
        notifier.loading(snapshot(5 * GIB, total=10 * GIB, elapsed=50))
        notifier.table_done("A", 1_000_000)
        notifier.table_done("B", 50)
        notifier.enter(Step.INMEMORY_WAIT)
        notifier.enter(Step.SWAP)
    # Then it is one POST and edits, the last embed is green with summaries, and a standalone line follows
    assert discord.calls[0][0] == "POST"
    assert {method for method, body in discord.calls if "embeds" in body} == {"POST", "PATCH"}
    last = discord.embeds[-1]
    assert last["color"] == COLOR_SUCCESS
    assert "✅ **A** — 1.000.000 linhas" in str(last["description"])
    last_fields = last["fields"]
    assert isinstance(last_fields, list)
    fields = {field["name"]: field["value"] for field in last_fields}
    assert fields["🧪 Duração do dbt"] == "12min 34s"
    assert discord.standalone[0].startswith("✅ **BigQuery → Oracle** (`nota_carioca_staging`) concluído em ")
    assert "2 tabelas" in discord.standalone[0]


def test_running_view_shows_slot_sessions_load_detail_and_forecast() -> None:
    # Given A loading 5 GB of 10 GB in 50 s and B waiting
    notifier = make(None)
    notifier.set_tables({"A": 10 * GIB, "B": 10 * GIB})
    notifier.enter(Step.TABLES)
    notifier.table_step("A", TableStep.LOAD, slot="B")
    notifier.loading(snapshot(5 * GIB, total=10 * GIB, elapsed=50))
    view = build_view(notifier.state)
    # Then the current table shows bytes, files, rate and ETA, the slot and sessions are facts, and B waits
    a, b = view.tables
    assert a.stage == "Carga SQL*Loader (slot B)"
    assert a.detail == "5,0 GB de 10,0 GB · 3/10 arquivos · 102,4 MB/s"
    assert a.eta_seconds == pytest.approx(50.0)
    assert b.status is ItemStatus.PENDING
    facts = {fact.name: fact.value for fact in view.facts}
    assert facts["💾 Slot em carga"] == "B (A)"
    assert facts["🔌 Sessões SQL*Loader"] == "2"
    # and the forecast counts the pending table at the measured rate
    assert view.eta_seconds == pytest.approx(50.0 + 10 * GIB / (5 * GIB / 50))
    assert view.stage == "A: Carga SQL*Loader"


def test_index_progress_is_shown_for_the_current_table() -> None:
    # Given a table creating its indexes
    notifier = make(None, tables=("A",))
    notifier.enter(Step.TABLES)
    notifier.table_step("A", TableStep.INDEXES)
    notifier.indexes(2, 5)
    # Then the block shows "índices 2/5" and a partial bar
    table = build_view(notifier.state).tables[0]
    assert table.detail == "índices 2/5"
    assert table.fraction == pytest.approx((5 + 60 + 3 + 20 * 0.4) / 100)


def test_failure_in_a_table_marks_it_and_the_step_and_reraises() -> None:
    # Given a run that dies while loading table B
    discord = Discord()
    notifier = make(discord)
    with pytest.raises(RuntimeError, match="sqlldr"), notifier.guard():
        notifier.begin()
        notifier.enter(Step.TABLES)
        notifier.table_step("B", TableStep.LOAD)
        raise RuntimeError("sqlldr exited 1")
    # Then the final message is red, names the table and stage, and the standalone line has the short error
    last = discord.embeds[-1]
    assert last["color"] == COLOR_FAILURE
    assert "Falhou em Carga SQL*Loader" in str(last["description"])
    assert "falhou na etapa **B: Carga SQL*Loader**" in discord.standalone[0]
    assert "RuntimeError: sqlldr exited 1" in discord.standalone[0]
    assert current() is None


def test_interruption_finalizes_as_cancelled() -> None:
    # Given a run interrupted during the export
    discord = Discord()
    notifier = make(discord)
    with pytest.raises(KeyboardInterrupt), notifier.guard():
        notifier.begin()
        notifier.enter(Step.EXPORT)
        raise KeyboardInterrupt
    # Then the message is gray and says cancelled in the export step
    assert discord.embeds[-1]["color"] == COLOR_CANCELLED
    assert "cancelado na etapa **Exportação do BigQuery**" in discord.standalone[0]


def test_synonyms_only_gets_a_minimal_message() -> None:
    # Given a synonyms-only run
    discord = Discord()
    notifier = make(discord, tables=(), mode="synonyms_only")
    with notifier.guard():
        notifier.begin()
        notifier.set_tables({"A": 1, "B": 1})
    # Then the checklist has just the swap, no table blocks, and it ends green
    first = discord.embeds[0]
    assert first["color"] == COLOR_RUNNING
    view = build_view(notifier.state)
    assert [item.label for item in view.checklist] == ["Troca dos sinônimos"]
    assert view.tables == ()
    assert discord.embeds[-1]["color"] == COLOR_SUCCESS


def test_disabled_notifier_sends_nothing_and_keeps_tracking() -> None:
    # Given no webhook
    notifier = make(None)
    notifier.begin()
    notifier.enter(Step.TABLES)
    notifier.table_step("A", TableStep.LOAD)
    notifier.fail(RuntimeError("x"))
    # Then nothing was sent but the state records the failure
    assert notifier.state.status is RunStatus.FAILED


def test_a_bug_in_the_notifier_never_reaches_the_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a view builder that crashes
    def broken(_: object) -> None:
        raise ZeroDivisionError

    notifier = make(Discord())
    monkeypatch.setattr("pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.notify.build_view", broken)
    # When the flow reports progress and finishes
    with notifier.guard():
        notifier.begin()
        notifier.enter(Step.TABLES)
    # Then nothing raised (the guard would have re-raised otherwise)
    assert notifier.state.status is RunStatus.SUCCESS


# --- reachability from tasks -------------------------------------------------------------------------------------


def test_load_task_feeds_the_active_notifier_without_receiving_it(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a load task whose SQL*Loader reports one progress tick, run inside the notifier guard
    notifier = make(None)
    snap = snapshot(25 * GIB)
    monkeypatch.setattr(tasks_module, "log", lambda *_: None)
    monkeypatch.setattr(tasks_module, "create_progress_artifact", lambda **_: "artifact")
    monkeypatch.setattr(tasks_module, "update_progress_artifact", lambda **_: None)
    monkeypatch.setattr(tasks_module.oracle, "read_oracle_config", lambda _: None)

    def fake_load(
        config: object, job: object, on_progress: object, progress_interval_seconds: float
    ) -> sqlldr.LoadResult:
        assert callable(on_progress)
        on_progress(snap)
        return sqlldr.LoadResult(rows=5, sessions=[], elapsed_seconds=1.0)

    monkeypatch.setattr(tasks_module.sqlldr, "load_from_gcs", fake_load)
    plan = LoadPlan(columns=[], fields=[], ignored=[], excluded=[])
    # When the task runs under the notifier
    with notifier.guard():
        notifier.enter(Step.TABLES)
        notifier.table_step("A", TableStep.LOAD)
        rows = tasks_module.load_into_oracle_task.fn(
            "/secret", "BQLOAD_A_B", plan, "bucket", [ExportedFile("f.csv.gz", 10)], 2, 30
        )
    # Then the tick reached the table state
    assert rows == 5
    assert notifier.state.tables["A"].load == snap


# --- flow wiring -------------------------------------------------------------------------------------------------


def run_flow_with_fakes(
    monkeypatch: pytest.MonkeyPatch, notifier: LoadNotifier, mode: Literal["full", "synonyms_only"] = "full"
) -> list[str]:
    order: list[str] = []
    files = [ExportedFile("a.csv.gz", 4 * GIB), ExportedFile("b.csv.gz", 4 * GIB)]
    exported = SimpleNamespace(schema={"num_rows": 7, "fields": []}, files=files, last_modified=NOW)
    slot_plan = slots.SlotPlan("BQLOAD_A", None, "BQLOAD_A_B")

    def fake(name: str, result: object = None):
        def call(**_: object) -> object:
            order.append(name)
            return result

        return call

    plain = {
        "rename_current_flow_run_task": None,
        "inject_bd_credentials_task": None,
        "refresh_synonyms_task": None,
        "plan_load_task": "plan",
        "plan_structure_task": "structure",
        "ensure_oracle_table_task": "BQLOAD_A_B",
        "drop_oracle_indexes_task": "BQLOAD_A_B",
        "truncate_oracle_table_task": "BQLOAD_A_B",
        "load_into_oracle_task": 7,
        "validate_row_count_task": 7,
        "delete_gcs_files_task": None,
        "create_oracle_indexes_task": "BQLOAD_A_B",
        "gather_oracle_stats_task": "BQLOAD_A_B",
        "grant_access_task": "BQLOAD_A_B",
        "record_load_task": slot_plan,
        "start_inmemory_population_task": None,
        "wait_inmemory_task": [slot_plan],
        "swap_synonyms_task": None,
        "list_tables_task": ["A"],
        "export_snapshot_task": {"A": exported},
        "resolve_slots_task": slot_plan,
    }
    for name, result in plain.items():
        monkeypatch.setattr(flow_module, name, fake(name, result))
    monkeypatch.setattr(flow_module.LoadNotifier, "create", lambda _: notifier)
    flow_module.rj_smfp__nota_carioca_bq_to_oracle.fn(mode=mode)
    return order


def test_flow_reports_every_table_step_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given the whole flow with fake tasks and a recording notifier
    seen: list[tuple[str, str]] = []
    discord = Discord()
    notifier = make(discord, tables=())
    original = notifier.table_step

    def spy(table: str, step: TableStep, slot: str | None = None) -> None:
        seen.append((table, step.name))
        original(table, step, slot)

    monkeypatch.setattr(notifier, "table_step", spy)
    # When it runs
    run_flow_with_fakes(monkeypatch, notifier)
    # Then the table walks through the seven stages in order, with the slot, and the run ends green
    names = [step for _, step in seen]
    assert [name for name in dict.fromkeys(names)] == [
        "PREPARATION",
        "LOAD",
        "VALIDATION",
        "INDEXES",
        "STATS",
        "ACCESS",
        "INMEMORY",
    ]
    assert notifier.state.tables["A"].slot == "B"
    assert notifier.state.tables["A"].rows == 7
    assert discord.embeds[-1]["color"] == COLOR_SUCCESS


def test_synonyms_only_flow_skips_the_load_and_finishes_green(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a synonyms-only run
    discord = Discord()
    notifier = make(discord, tables=(), mode="synonyms_only")
    order = run_flow_with_fakes(monkeypatch, notifier, mode="synonyms_only")
    # Then only the refresh ran and the message went green
    assert order == [
        "rename_current_flow_run_task",
        "inject_bd_credentials_task",
        "list_tables_task",
        "refresh_synonyms_task",
    ]
    assert discord.embeds[-1]["color"] == COLOR_SUCCESS

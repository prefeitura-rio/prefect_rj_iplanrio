# ruff: noqa: PLR2004
import json
from dataclasses import replace
from datetime import UTC, datetime

import httpx
import pytest
from google.api_core import exceptions as api_exceptions

from pipelines.rj_smfp__nota_carioca_oracle_to_bq import flow as flow_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq import table_run as table_run_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq import tasks as tasks_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import GCS_PREFIX, PROGRESS_PREFIX
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import discord as discord_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import table_progress as progress_module
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.discord import DiscordStatusMessage
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.discord_embed import (
    COLOR_CANCELLED,
    COLOR_FAILURE,
    COLOR_RUNNING,
    COLOR_SUCCESS,
    ItemStatus,
    RunStatus,
    build_payload,
    format_validation_line,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.checksum import ColumnChecksum
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractOptions, ExtractResult
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.gcs import blob_prefix
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.notify import (
    NotifierConfig,
    ParentNotifier,
    active_parent,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.notify_view import (
    ParentStage,
    ParentState,
    build_view,
    current_stage,
    overall_fraction,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import Snapshot
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import RunInfo, TableRunContext
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import TablePlan
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import Progress
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import TableLayout
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.supervise import Supervision, supervise
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.table_progress import (
    ProgressStore,
    TableProgress,
    TableReporter,
    TableStage,
    active_reporter,
    progress_prefix,
    reporting,
)

RUN_ID = "0a1b2c3d-1111-4222-8333-444455556666"
NOW = datetime(2026, 10, 9, 15, 0, 0, tzinfo=UTC)
TABLES = ("DPS", "NOTAS_NACIONAIS", "PESSOAS_NACIONAIS")


def tp(table: str, stage: TableStage, **fields: object) -> TableProgress:
    return replace(TableProgress(table=table, stage=stage), **fields)


def state(**overrides: object) -> ParentState:
    base = ParentState(
        dataset_id="brutos_nota_fiscal",
        run_name="brutos_nota_fiscal",
        url=None,
        status=RunStatus.RUNNING,
        stage=ParentStage.EXTRACTION,
        table_names=TABLES,
        tables={},
        workers=6,
        elapsed_seconds=125,
        updated_at=NOW,
    )
    return replace(base, **overrides)


# --- progress JSON -----------------------------------------------------------------------------------------------


def test_progress_json_round_trips_every_field() -> None:
    # Given a progress with optional fields filled
    original = tp(
        "DPS",
        TableStage.FAILED,
        chunks_read=12,
        chunks_uploaded=10,
        chunks_total=395,
        rows_read=1_234_567,
        oracle_rows=193_000_000,
        bq_rows=193_000_000,
        checksum_columns=("VALOR_SERVICO", "NUMERO_DPS"),
        bytes_uploaded=9_999,
        elapsed_seconds=61.5,
        extract_seconds=60.25,
        eta_seconds=1200.0,
        error="ValueError: boom",
        failed_stage=TableStage.EXTRACTION,
        updated_at="2026-10-09T15:00:00+00:00",
    )
    # When it goes through JSON
    restored = TableProgress.from_json(original.to_json())
    # Then nothing is lost, and the stage is serialized by its Portuguese value
    assert restored == original
    assert json.loads(original.to_json())["stage"] == "falhou"


def test_progress_json_without_the_validation_fields_still_parses_with_defaults() -> None:
    # Given a JSON written by an older child, before bq_rows and checksum_columns existed
    old = '{"table": "DPS", "stage": "validada", "oracle_rows": 7, "rows_read": 7}'
    # When the parent reads it
    restored = TableProgress.from_json(old)
    # Then the new fields take their defaults
    assert (restored.bq_rows, restored.checksum_columns) == (None, ())
    assert restored.oracle_rows == 7


@pytest.mark.parametrize("text", ["not json", "[]", '{"table": "DPS"}', '{"table": "DPS", "stage": "inexistente"}'])
def test_invalid_progress_json_is_a_value_error(text: str) -> None:
    # Given broken contents
    # When parsing, Then a ValueError (which the reader treats as "no progress yet")
    with pytest.raises(ValueError, match=r"."):
        TableProgress.from_json(text)


# --- reporter ----------------------------------------------------------------------------------------------------


def test_reporter_turns_extraction_ticks_and_stages_into_progress() -> None:
    # Given a reporter with a recording sink
    seen: list[TableProgress] = []
    reporter = TableReporter("DPS", [seen.append])
    # When the extraction ticks and then ends
    reporter.stage(TableStage.EXTRACTION)
    reporter.extraction(Progress(5, 4, 395, 500_000, 2_000, 1, 40.0))
    tick = seen[-1]
    # Then the tick carries the chunk counts, rows, bytes and the ETA from the uploaded-chunk rate
    assert (tick.stage, tick.chunks_uploaded, tick.chunks_total, tick.rows_read) == (
        TableStage.EXTRACTION,
        4,
        395,
        500_000,
    )
    assert tick.extract_seconds == 40.0
    assert tick.eta_seconds == pytest.approx(40.0 / 4 * (395 - 4))
    reporter.stage(TableStage.VALIDATION)
    assert seen[-1].eta_seconds is None
    assert seen[-1].chunks_total == 395


def test_reporter_records_checksum_columns_and_bigquery_rows_without_changing_the_stage() -> None:
    # Given a reporter after the extraction of a table with two checksum columns
    seen: list[TableProgress] = []
    reporter = TableReporter("DPS", [seen.append])
    result = ExtractResult(
        table="DPS",
        rows=10,
        bytes_written=1,
        chunks=2,
        files=2,
        prefix="p",
        seconds=1.0,
        checksums={"VALOR_SERVICO": ColumnChecksum(10, "5"), "NUMERO_DPS": ColumnChecksum(10, "9")},
        oracle_rows=10,
        max_pending_files=1,
        max_local_files=1,
    )
    reporter.extracted(result)
    # When the load and then the validation report the BigQuery rows
    reporter.bigquery_rows(10)
    # Then the columns and rows travel in the progress and the stage stays on the load
    assert seen[-1].checksum_columns == ("VALOR_SERVICO", "NUMERO_DPS")
    assert (seen[-1].stage, seen[-1].bq_rows) == (TableStage.LOAD, 10)


def test_reporter_guard_marks_the_failed_stage_even_for_base_exceptions_and_reraises() -> None:
    # Given a reporter that is loading
    seen: list[TableProgress] = []
    reporter = TableReporter("DPS", [seen.append])
    reporter.stage(TableStage.LOAD)
    # When the guarded block is interrupted
    with pytest.raises(KeyboardInterrupt), reporter.guard():
        raise KeyboardInterrupt
    # Then the table is FAILED at the stage it was in
    assert seen[-1].stage is TableStage.FAILED
    assert seen[-1].failed_stage is TableStage.LOAD
    assert seen[-1].error == "KeyboardInterrupt: "


def test_a_failing_sink_never_breaks_the_reporter(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a sink that always raises
    monkeypatch.setattr(discord_module, "warn", lambda _: None)

    def broken(_: TableProgress) -> None:
        raise RuntimeError("sink down")

    reporter = TableReporter("DPS", [broken])
    # When the reporter emits, Then nothing is raised and the state still advances
    reporter.stage(TableStage.EXTRACTION)
    assert reporter.current.stage is TableStage.EXTRACTION


def test_tasks_find_the_active_reporter_only_for_its_own_table() -> None:
    # Given a reporter set for DPS
    reporter = TableReporter("DPS", [])
    with reporting(reporter):
        # Then the DPS task gets it, other tables get a harmless no-op reporter
        assert active_reporter("DPS") is reporter
        assert active_reporter("OTHER") is not reporter
    assert active_reporter("DPS") is not reporter


# --- GCS store ---------------------------------------------------------------------------------------------------


class FakeBlob:
    def __init__(self, bucket: "FakeBucket", name: str) -> None:
        self.bucket = bucket
        self.name = name

    def upload_from_string(self, data: str, **options: object) -> None:
        self.bucket.options.append(dict(options))
        self.bucket.objects[self.name] = data

    def download_as_text(self, **_: object) -> str:
        if self.name not in self.bucket.objects:
            raise api_exceptions.NotFound("missing")
        return self.bucket.objects[self.name]

    def delete(self) -> None:
        del self.bucket.objects[self.name]


class FakeBucket:
    def __init__(self) -> None:
        self.objects: dict[str, str] = {}
        self.options: list[dict[str, object]] = []
        self.listed: list[str] = []

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self, name)

    def list_blobs(self, prefix: str) -> list[FakeBlob]:
        self.listed.append(prefix)
        return [FakeBlob(self, name) for name in list(self.objects) if name.startswith(prefix)]


@pytest.fixture
def bucket(monkeypatch: pytest.MonkeyPatch) -> FakeBucket:
    fake = FakeBucket()

    class FakeClient:
        def __init__(self, project: str) -> None:
            self.project = project

        def bucket(self, _: str) -> FakeBucket:
            return fake

    monkeypatch.setattr(progress_module.storage, "Client", FakeClient)
    monkeypatch.setattr(discord_module, "warn", lambda _: None)
    return fake


def test_store_writes_where_the_parent_reads_and_skips_missing_or_corrupt(bucket: FakeBucket) -> None:
    # Given a child that wrote DPS and a corrupt NOTAS object, and PESSOAS that never wrote
    store = ProgressStore("proj", "bkt", RUN_ID)
    store.write(tp("DPS", TableStage.EXTRACTION, chunks_total=395))
    bucket.objects[store.blob_name("NOTAS_NACIONAIS")] = "{corrupt"
    # When the parent reads all three
    found = store.read_all(TABLES)
    # Then only DPS comes back, from gs://bucket/oracle_to_bq_progress/<run>/DPS.json, written without extra options
    assert list(found) == ["DPS"]
    assert found["DPS"].chunks_total == 395
    assert f"{PROGRESS_PREFIX}/{RUN_ID}/DPS.json" in bucket.objects
    assert "retry" not in bucket.options[0]


def test_progress_prefix_never_overlaps_what_the_load_lists(bucket: FakeBucket) -> None:
    # Given the load glob of a table and the progress objects of the same run
    store = ProgressStore("proj", "bkt", RUN_ID)
    store.write(tp("DPS", TableStage.EXTRACTION))
    load_prefix = blob_prefix(GCS_PREFIX, "DPS", RUN_ID) + "/"
    # Then no progress object lives under any parquet prefix, and the parquet prefixes are not under the progress one
    assert not any(name.startswith(f"{GCS_PREFIX}/") for name in bucket.objects)
    assert not load_prefix.startswith(progress_prefix(RUN_ID) + "/")
    assert PROGRESS_PREFIX != GCS_PREFIX


def test_store_delete_removes_only_this_runs_progress(bucket: FakeBucket) -> None:
    # Given progress of this run and of another run, plus a parquet file
    store = ProgressStore("proj", "bkt", RUN_ID)
    store.write(tp("DPS", TableStage.EXTRACTION))
    store.write(tp("PESSOAS_NACIONAIS", TableStage.VALIDATED))
    bucket.objects[f"{PROGRESS_PREFIX}/other-run/DPS.json"] = "{}"
    bucket.objects[f"{GCS_PREFIX}/DPS/{RUN_ID}/chunk-000001.parquet"] = "x"
    # When it is deleted
    deleted = store.delete()
    # Then exactly this run's two JSONs went away, listed with a trailing slash
    assert deleted == 2
    assert bucket.listed == [f"{PROGRESS_PREFIX}/{RUN_ID}/"]
    assert sorted(bucket.objects) == [
        f"{GCS_PREFIX}/DPS/{RUN_ID}/chunk-000001.parquet",
        f"{PROGRESS_PREFIX}/other-run/DPS.json",
    ]


class RecordingStore:
    def __init__(self, project: str, bucket: str, run_id: str) -> None:
        self.args = (project, bucket, run_id)
        RecordingStore.created.append(self)

    created: list["RecordingStore"] = []  # noqa: RUF012
    deleted = 0

    def delete(self) -> int:
        RecordingStore.deleted += 1
        return 0


@pytest.fixture
def cleanup_env(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    RecordingStore.created, RecordingStore.deleted = [], 0
    monkeypatch.setattr(tasks_module, "log", lambda *_: None)
    monkeypatch.setattr(tasks_module, "ProgressStore", RecordingStore)
    monkeypatch.setattr(tasks_module.load, "cleanup", lambda *args: calls.append("cleanup"))
    return calls


def test_parent_cleanup_deletes_the_progress_prefix(cleanup_env: list[str]) -> None:
    # Given the parent's cleanup (the default)
    # When it runs
    tasks_module.cleanup_task.fn("proj", "ds", "bkt", [], RUN_ID)
    # Then the files are cleaned and the progress of this run is deleted
    assert cleanup_env == ["cleanup"]
    assert RecordingStore.created[0].args == ("proj", "bkt", RUN_ID)
    assert RecordingStore.deleted == 1


def test_parent_cleanup_deletes_progress_even_if_the_rest_fails(
    cleanup_env: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a GCS cleanup that fails
    def boom(*_: object) -> None:
        raise RuntimeError("gcs down")

    monkeypatch.setattr(tasks_module.load, "cleanup", boom)
    # When the parent cleans up
    with pytest.raises(RuntimeError, match="gcs down"):
        tasks_module.cleanup_task.fn("proj", "ds", "bkt", [], RUN_ID)
    # Then the progress prefix was still deleted
    assert RecordingStore.deleted == 1


def test_child_cleanup_keeps_the_progress_for_the_parent(cleanup_env: list[str]) -> None:
    # Given a child's cleanup
    tasks_module.cleanup_task.fn("proj", "ds", "bkt", [], RUN_ID, drop_temp=False, drop_progress=False)
    # Then it does not touch the progress prefix
    assert RecordingStore.created == []


# --- aggregation and view ----------------------------------------------------------------------------------------


def extracting_tables() -> dict[str, TableProgress]:
    return {
        "DPS": tp(
            "DPS", TableStage.EXTRACTION, chunks_read=120, chunks_uploaded=100, chunks_total=395, rows_read=48_000_000
        ),
        "NOTAS_NACIONAIS": tp("NOTAS_NACIONAIS", TableStage.EXTRACTION, chunks_uploaded=200, chunks_total=493),
        "PESSOAS_NACIONAIS": tp("PESSOAS_NACIONAIS", TableStage.LOAD, chunks_uploaded=31, chunks_total=31),
    }


def test_overall_fraction_weights_extraction_by_chunks_across_tables() -> None:
    # Given 331 of 919 chunks uploaded in total
    running = state(tables=extracting_tables())
    # When the overall progress is computed
    # Then extraction is 80% of the 87% table share on top of the 5% setup, weighted by chunks (not by table)
    assert overall_fraction(running) == pytest.approx(0.05 + 0.87 * 0.80 * 331 / 919)


def test_stage_follows_the_slowest_table_and_fixed_stages_have_fixed_shares() -> None:
    # Given tables at different stages
    tables = extracting_tables()
    assert current_stage(state(tables=tables)) is ParentStage.EXTRACTION
    # When every table passed extraction, then loading, then validation
    loaded = {name: replace(progress, stage=TableStage.LOAD) for name, progress in tables.items()}
    assert current_stage(state(tables=loaded)) is ParentStage.LOAD
    validated = {
        name: replace(progress, stage=TableStage.VALIDATED, chunks_uploaded=progress.chunks_total)
        for name, progress in tables.items()
    }
    # Then the shown stage advances with the slowest, and the shares add up to 92% before publishing
    assert current_stage(state(tables=validated)) is ParentStage.VALIDATION
    assert overall_fraction(state(tables=validated)) == pytest.approx(0.92)
    assert overall_fraction(state(tables=validated, stage=ParentStage.PUBLISH)) == pytest.approx(0.92)
    assert overall_fraction(state(stage=ParentStage.CLEANUP)) == pytest.approx(0.98)
    assert overall_fraction(state(stage=ParentStage.SNAPSHOT)) == 0.0
    assert overall_fraction(state(status=RunStatus.SUCCESS)) == 1.0


def test_table_blocks_show_rows_rate_chunks_and_eta() -> None:
    # Given a DPS mid-extraction and tables that have not reported
    dps = replace(extracting_tables()["DPS"], extract_seconds=100.0, bytes_uploaded=5 * 1024**3, eta_seconds=1800.0)
    view = build_view(state(tables={"DPS": dps}))
    first, second, _ = view.tables
    # Then DPS shows an estimated total, the read rate, chunk counts, volume and ETA; the others wait
    assert first.status is ItemStatus.RUNNING
    assert first.detail == "48.000.000 de ~158.000.000 linhas · 480.000 linhas/s · faixas 100/395 · 5,0 GB"
    assert first.eta_seconds == 1800.0
    assert second.stage == "Aguardando início"
    assert second.status is ItemStatus.PENDING
    assert [item.status for item in view.checklist][:3] == [ItemStatus.DONE, ItemStatus.DONE, ItemStatus.RUNNING]
    facts = {fact.name: fact.value for fact in view.facts}
    assert facts["👷 Workers por pod"] == "6"


def test_view_embeds_scn_and_snapshot_and_picks_the_slowest_eta() -> None:
    # Given a snapshot and two extracting tables with ETAs
    tables = extracting_tables()
    tables["DPS"] = replace(tables["DPS"], eta_seconds=300.0)
    tables["NOTAS_NACIONAIS"] = replace(tables["NOTAS_NACIONAIS"], eta_seconds=900.0)
    view = build_view(state(tables=tables, scn=987654321, snapshot_taken_at=NOW))
    # Then the SCN and the BRT snapshot time are facts, and the forecast is the slowest extraction
    facts = {fact.name: fact.value for fact in view.facts}
    assert facts["🔖 SCN"] == "`987654321`"
    assert facts["📸 Foto"] == "09/10 12:00:00"
    assert view.eta_seconds == 900.0


def test_success_summary_has_rows_and_duration_per_table() -> None:
    # Given validated tables
    tables = {name: tp(name, TableStage.VALIDATED, oracle_rows=1_000, elapsed_seconds=185.0) for name in TABLES}
    view = build_view(state(status=RunStatus.SUCCESS, stage=ParentStage.CLEANUP, tables=tables))
    # Then each table summary has rows and duration and every checklist item is done
    assert all(table.detail == "1.000 linhas · 3min 05s" for table in view.tables)
    assert all(item.status is ItemStatus.DONE for item in view.checklist)


def test_failed_child_shows_its_stage_and_clipped_error() -> None:
    # Given a child that failed while loading, with a huge error
    failed = tp("DPS", TableStage.FAILED, failed_stage=TableStage.LOAD, error="x" * 2000)
    view = build_view(
        state(
            status=RunStatus.FAILED,
            tables={"DPS": failed},
            failed_stage=ParentStage.LOAD,
            error="ParallelRunError: filhos falharam",
        )
    )
    # Then the table block names the stage, the error is clipped, and the checklist marks the failing stage
    assert view.tables[0].stage == "Falhou em Carga no BigQuery"
    assert len(view.tables[0].detail) <= 300
    assert view.failed_stage == "Carga no BigQuery"
    assert [item.status for item in view.checklist][3] is ItemStatus.FAILED
    assert [item.status for item in view.checklist][4] is ItemStatus.PENDING


def validated_tables(**fields: object) -> dict[str, TableProgress]:
    counts = {"oracle_rows": 1_000, "rows_read": 1_000, "bq_rows": 1_000, "checksum_columns": ("A", "B")}
    return {name: tp(name, TableStage.VALIDATED, **{**counts, **fields}) for name in TABLES}


def test_success_view_compares_oracle_files_and_bigquery_per_table() -> None:
    # Given three validated tables
    view = build_view(state(status=RunStatus.SUCCESS, stage=ParentStage.CLEANUP, tables=validated_tables()))
    # Then each table has one green comparison line, with the checksum columns in the note
    assert [format_validation_line(line) for line in view.validation] == [
        f"✅ {name} · Oracle (SCN) 1.000 = arquivos 1.000 = BigQuery 1.000 · Σ A, B iguais" for name in TABLES
    ]


def test_running_view_has_no_validation_section_yet() -> None:
    # Given a running parent with a validated table
    view = build_view(state(tables=validated_tables()))
    # Then the comparison waits for the final message
    assert view.validation == ()


def test_failure_view_keeps_full_comparisons_and_shows_what_is_known_of_the_failing_table() -> None:
    # Given DPS validated, NOTAS failed while loading (Oracle and files known) and PESSOAS never started
    tables = {
        "DPS": validated_tables()["DPS"],
        "NOTAS_NACIONAIS": tp(
            "NOTAS_NACIONAIS", TableStage.FAILED, failed_stage=TableStage.LOAD, oracle_rows=50, rows_read=50, error="x"
        ),
    }
    view = build_view(
        state(status=RunStatus.FAILED, tables=tables, failed_stage=ParentStage.LOAD, error="ParallelRunError: x")
    )
    # Then the validated one is complete, the failing one has a dash for BigQuery, the untouched one is pending
    assert [format_validation_line(line) for line in view.validation] == [
        "✅ DPS · Oracle (SCN) 1.000 = arquivos 1.000 = BigQuery 1.000 · Σ A, B iguais",
        "❌ NOTAS_NACIONAIS · Oracle (SCN) 50 = arquivos 50 · BigQuery —",
        "⬜ PESSOAS_NACIONAIS · Oracle (SCN) — · arquivos — · BigQuery —",
    ]


def test_a_count_mismatch_in_a_validated_child_renders_red() -> None:
    # Given a child whose BigQuery count differs from the Oracle one
    tables = validated_tables(bq_rows=999)
    view = build_view(state(status=RunStatus.FAILED, tables=tables, failed_stage=ParentStage.VALIDATION))
    # Then the line is ❌ with "≠" and no checksum note
    assert format_validation_line(view.validation[0]) == "❌ DPS · Oracle (SCN) 1.000 = arquivos 1.000 ≠ BigQuery 999"


def test_files_count_is_ignored_until_the_extraction_ends() -> None:
    # Given a table still extracting (rows_read is partial, oracle_rows unknown)
    view = build_view(
        state(status=RunStatus.FAILED, tables={"DPS": tp("DPS", TableStage.EXTRACTION, rows_read=42)}, error="x")
    )
    # Then the partial count is not shown as the files count
    assert format_validation_line(view.validation[0]) == "⬜ DPS · Oracle (SCN) — · arquivos — · BigQuery —"


# --- notifier ----------------------------------------------------------------------------------------------------


class Discord:
    """Webhook falso que registra o que o notificador envia."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.transport = httpx.MockTransport(self.handle)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"id": "42"})

    @property
    def embeds(self) -> list[dict[str, object]]:
        embeds = [body["embeds"] for _, _, body in self.calls if "embeds" in body]
        return [embed[0] for embed in embeds if isinstance(embed, list)]

    @property
    def standalone(self) -> list[str]:
        return [str(body["content"]) for _, _, body in self.calls if "content" in body]


def notifier(discord: Discord | None, store: ProgressStore | None = None) -> ParentNotifier:
    config = NotifierConfig("brutos_nota_fiscal", RUN_ID, "brutos_nota_fiscal", TABLES, 6, True, "proj", "bkt")
    message = (
        DiscordStatusMessage("https://discord.example/api/webhooks/1/t", transport=discord.transport)
        if discord
        else None
    )
    return ParentNotifier(config, message, store)


def run_info(state_type: str, name: str = "child") -> RunInfo:
    return RunInfo("r", name, state_type, state_type.title(), (), {})


@pytest.fixture(autouse=True)
def quiet_logs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(discord_module, "warn", lambda _: None)
    monkeypatch.setattr(discord_module, "info", lambda _: None)


def test_notifier_creates_one_message_edits_it_and_finishes_green_with_a_standalone_line() -> None:
    # Given a notifier on a live webhook
    discord = Discord()
    parent = notifier(discord)
    # When the parent walks through its stages and succeeds
    parent.begin()
    parent.set_snapshot(Snapshot(scn=11, taken_at=NOW))
    parent.set_stage(ParentStage.PLAN)
    parent.set_stage(ParentStage.PUBLISH)
    parent.succeed()
    # Then one POST then PATCHes of the same message, ending green, plus one standalone POST
    methods = [(method, path.rsplit("/", 1)[-1]) for method, path, _ in discord.calls]
    assert methods == [("POST", "t"), ("PATCH", "42"), ("PATCH", "42"), ("PATCH", "42"), ("POST", "t")]
    assert discord.embeds[0]["color"] == COLOR_RUNNING
    assert discord.embeds[-1]["color"] == COLOR_SUCCESS
    assert discord.standalone[0].startswith("✅ **Oracle → BigQuery** (`brutos_nota_fiscal`) concluído em ")


def test_failure_finalizes_red_in_the_failing_stage_and_is_idempotent() -> None:
    # Given a parent that fails while publishing
    discord = Discord()
    parent = notifier(discord)
    parent.begin()
    parent.set_stage(ParentStage.PUBLISH)
    # When it fails and then something else (the guard) tries to finalize again
    parent.fail(RuntimeError("copy failed"))
    count = len(discord.calls)
    parent.fail(ValueError("late"))
    parent.succeed()
    # Then the message is red with the stage and error, the standalone line says so, and nothing was sent twice
    assert discord.embeds[-1]["color"] == COLOR_FAILURE
    assert len(discord.calls) == count
    assert "falhou na etapa **Publicação**" in discord.standalone[0]
    assert "RuntimeError: copy failed" in discord.standalone[0]


def test_guard_finalizes_before_reraising_failure_and_cancellation() -> None:
    # Given two parents, one failing with an error and one interrupted
    for error, color in ((RuntimeError("boom"), COLOR_FAILURE), (KeyboardInterrupt(), COLOR_CANCELLED)):
        discord = Discord()
        parent = notifier(discord)
        # When the guarded body raises
        with pytest.raises(type(error)), parent.guard():
            assert active_parent() is parent
            raise error
        # Then the exception propagated, the message was finalized with the right color, and the holder was reset
        assert discord.embeds[-1]["color"] == color
        assert active_parent() is None


def test_guard_success_finalizes_green() -> None:
    # Given a parent whose body finishes normally
    discord = Discord()
    parent = notifier(discord)
    with parent.guard():
        parent.set_stage(ParentStage.CLEANUP)
    # Then the message turns green
    assert discord.embeds[-1]["color"] == COLOR_SUCCESS


def test_observe_reads_child_progress_and_marks_dead_children_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a store with progress for DPS only, and NOTAS whose flow run crashed without reporting
    discord = Discord()
    store = ProgressStore("proj", "bkt", RUN_ID)
    written = {"DPS": tp("DPS", TableStage.EXTRACTION, chunks_uploaded=10, chunks_total=100)}
    monkeypatch.setattr(store, "read_all", lambda _: written)
    parent = notifier(discord, store)
    parent.begin()
    parent.set_stage(ParentStage.EXTRACTION)
    # When the supervision hook runs
    parent.observe({"DPS": run_info("RUNNING"), "NOTAS_NACIONAIS": run_info("CRASHED")})
    # Then the state merged DPS and flagged NOTAS as failed
    tables = parent.state.tables
    assert tables["DPS"].chunks_uploaded == 10
    assert tables["NOTAS_NACIONAIS"].stage is TableStage.FAILED
    assert "CRASHED" in (tables["NOTAS_NACIONAIS"].error or "").upper()


def test_sequential_updates_arrive_in_memory_and_stage_changes_publish_immediately() -> None:
    # Given a sequential parent whose reporter feeds the notifier directly (no GCS)
    discord = Discord()
    parent = notifier(discord)
    parent.begin()
    reporter = TableReporter("DPS", [parent.update_table])
    # When the table changes stage and then ticks inside the throttle window
    reporter.stage(TableStage.EXTRACTION)
    sent = len(discord.calls)
    reporter.extraction(Progress(1, 1, 10, 100, 50, 0, 5.0))
    # Then the stage change was published at once, and the tick was throttled but is in the state
    assert sent == 2
    assert len(discord.calls) == sent
    assert parent.state.tables["DPS"].chunks_uploaded == 1


def fields_by_name(embed: dict[str, object]) -> dict[str, str]:
    fields = embed["fields"]
    assert isinstance(fields, list)
    return {field["name"]: field["value"] for field in fields}


def test_success_message_has_the_validation_section_and_the_announcement_summary() -> None:
    # Given a sequential parent whose three tables validated through the reporter path
    discord = Discord()
    parent = notifier(discord)
    parent.begin()
    for table in validated_tables().values():
        parent.update_table(table)
    # When it succeeds
    parent.succeed()
    # Then the final embed has one line per table and the standalone line counts them
    section = fields_by_name(discord.embeds[-1])["🔎 Validação"].splitlines()
    assert section[0] == "✅ DPS · Oracle (SCN) 1.000 = arquivos 1.000 = BigQuery 1.000 · Σ A, B iguais"
    assert len(section) == 3
    assert discord.standalone[0].endswith("· 3/3 tabelas validadas (linhas iguais na origem e no destino)")
    # and no earlier (running) embed carried the section
    assert all("🔎 Validação" not in fields_by_name(embed) for embed in discord.embeds[:-1])


def test_failure_message_counts_only_the_tables_that_validated() -> None:
    # Given one validated table and a failure in the next while loading
    discord = Discord()
    parent = notifier(discord)
    parent.begin()
    parent.update_table(validated_tables()["DPS"])
    parent.update_table(tp("NOTAS_NACIONAIS", TableStage.LOAD, oracle_rows=5, rows_read=5))
    parent.fail(RuntimeError("load exploded"))
    # Then the section keeps the full line of DPS and the partial one, and the summary says 1/3
    section = fields_by_name(discord.embeds[-1])["🔎 Validação"].splitlines()
    assert section[0].startswith("✅ DPS")
    assert section[1] == "❌ NOTAS_NACIONAIS · Oracle (SCN) 5 = arquivos 5 · BigQuery —"
    assert discord.standalone[0].endswith("`RuntimeError: load exploded` · 1/3 tabelas validadas")


def test_sequential_process_table_reports_bigquery_rows_and_checksum_columns(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given process_table with fake tasks, a Oracle COUNT, files and BigQuery counts, and a recording reporter
    extracted = ExtractResult(
        table="DPS",
        rows=7,
        bytes_written=1,
        chunks=1,
        files=1,
        prefix="p",
        seconds=1.0,
        checksums={"VALOR_SERVICO": ColumnChecksum(7, "1")},
        oracle_rows=7,
        max_pending_files=1,
        max_local_files=1,
    )
    monkeypatch.setattr(table_run_module, "extract_table_task", lambda **_: extracted)
    monkeypatch.setattr(table_run_module, "load_table_task", lambda **_: 7)
    monkeypatch.setattr(table_run_module, "validate_table_task", lambda **_: 7)
    monkeypatch.setattr(table_run_module, "stamp_validated_task", lambda **_: None)
    seen: list[TableProgress] = []
    reporter = TableReporter("DPS", [seen.append])
    ctx = TableRunContext("s", "p", "d", "b", "r", Snapshot(scn=1, taken_at=NOW), ExtractOptions())
    plan = TablePlan("DPS", "DFEN", (), (), TableLayout("DAY", "_airbyte_extracted_at", ("DPS",)), None)
    # When the table is processed
    table_run_module.process_table(plan, ctx, reporter)
    # Then the final progress carries every side of the comparison
    final = seen[-1]
    assert (final.stage, final.oracle_rows, final.rows_read, final.bq_rows) == (TableStage.VALIDATED, 7, 7, 7)
    assert final.checksum_columns == ("VALOR_SERVICO",)


def test_disabled_notifier_still_tracks_state_and_sends_nothing() -> None:
    # Given a notifier without webhook
    parent = notifier(None)
    # When used
    parent.begin()
    parent.update_table(tp("DPS", TableStage.EXTRACTION))
    parent.fail(RuntimeError("x"))
    # Then it is disabled and did not raise
    assert not parent.enabled
    payload = build_payload(build_view(parent.state))
    assert embed_color(payload) == COLOR_FAILURE


def embed_color(payload: dict[str, object]) -> object:
    embeds = payload["embeds"]
    assert isinstance(embeds, list)
    return embeds[0]["color"]


# --- supervision hook --------------------------------------------------------------------------------------------


def test_supervision_calls_the_hook_on_every_poll_with_runs_by_table() -> None:
    # Given two children that complete on the third poll
    reads = [
        [run_info("RUNNING"), run_info("RUNNING")],
        [run_info("RUNNING"), run_info("COMPLETED")],
        [run_info("COMPLETED"), run_info("COMPLETED")],
    ]
    children = {"DPS": "a", "PESSOAS": "b"}
    seen: list[dict[str, str]] = []

    def read(ids: object) -> list[RunInfo]:
        batch = reads.pop(0)
        return [replace(run, run_id=run_id) for run, run_id in zip(batch, children.values(), strict=True)]

    supervision = Supervision(
        read=read,
        cancel=lambda _: None,
        report=lambda _: None,
        on_poll=lambda runs: seen.append({table: run.state_type for table, run in runs.items()}),
        sleep=lambda _: None,
    )
    # When supervising
    supervise(children, supervision)
    # Then the hook saw each poll, keyed by table
    assert seen == [
        {"DPS": "RUNNING", "PESSOAS": "RUNNING"},
        {"DPS": "RUNNING", "PESSOAS": "COMPLETED"},
        {"DPS": "COMPLETED", "PESSOAS": "COMPLETED"},
    ]


def test_a_failing_hook_does_not_stop_the_supervision() -> None:
    # Given a hook that raises
    lines: list[str] = []

    def hook(_: object) -> None:
        raise RuntimeError("discord bug")

    supervision = Supervision(
        read=lambda ids: [replace(run_info("COMPLETED"), run_id=ids[0])],
        cancel=lambda _: None,
        report=lines.append,
        on_poll=hook,
        sleep=lambda _: None,
    )
    # When supervising, Then it completes normally and the failure was reported
    supervise({"DPS": "a"}, supervision)
    assert any("Gancho" in line for line in lines)


# --- flow wiring -------------------------------------------------------------------------------------------------


class Stop(Exception):
    pass


def test_parent_tells_children_whether_the_discord_is_on(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a parent run stopped at the child launch, with the Discord switched off by parameter
    captured: dict[str, object] = {}

    def fake_launch(table_ids: list[str], snapshot: Snapshot, passthrough: dict[str, object]) -> None:
        captured.update(passthrough)
        raise Stop

    class Plan:
        table_id = "DPS"

    def ignore(**_: object) -> None:
        return None

    fakes = {
        "rename_current_flow_run_task": ignore,
        "inject_bd_credentials_task": ignore,
        "ensure_exclusive_task": ignore,
        "drop_leftover_chunk_tasks_task": ignore,
        "take_snapshot_task": lambda infisical_secret_path: Snapshot(scn=1, taken_at=NOW),
        "plan_table_task": lambda **_: Plan(),
        "check_memory_budget_task": lambda plans, options: options,
        "launch_children_task": fake_launch,
        "cleanup_task": ignore,
    }
    for name, fake in fakes.items():
        monkeypatch.setattr(flow_module, name, fake)
    monkeypatch.setenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", "https://discord.example/api/webhooks/1/t")
    # When the parent runs
    with pytest.raises(Stop):
        flow_module.rj_smfp__nota_carioca_oracle_to_bq.fn(table_ids=["DPS"], discord_notifications=False)
    # Then children are told not to write progress (the parent will not read it)
    assert captured["discord_notifications"] is False

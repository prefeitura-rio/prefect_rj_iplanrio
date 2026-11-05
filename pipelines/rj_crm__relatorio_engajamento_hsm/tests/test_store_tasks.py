from datetime import UTC, date, datetime
from types import TracebackType

import polars as pl
import pytest

from pipelines.rj_crm__relatorio_engajamento_hsm import tasks
from pipelines.rj_crm__relatorio_engajamento_hsm.constants import EXAMPLES_SCHEMA
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers import bq
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers import drive as drive_helpers
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers import sheets as sheets_helpers
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import ValidCatalog, catalog_of
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Limits, Window
from utils.bifrost import Bifrost
from utils.constants import EMBEDDING_MODEL

NOW = datetime(2026, 11, 5, 8, 30, tzinfo=UTC)
FILE = {"id": "f1", "name": "x_revisada_2026-03-02", "modifiedTime": "2026-04-01T10:00:00Z"}


class FakeGateway:
    """A gateway that embeds every text as the same unit vector."""

    def __init__(self) -> None:
        """Start with no call."""
        self.embedded: list[list[str]] = []

    async def embed(self, texts: list[str], *, model: str) -> list[list[float]]:
        """Record the texts and return one vector for each."""
        assert model == EMBEDDING_MODEL
        self.embedded.append(list(texts))

        return [[1.0, 0.0] for _ in texts]


class Connection:
    """The async context of a gateway, as `Bifrost.connect` gives it."""

    def __init__(self, gateway: FakeGateway) -> None:
        """Keep the gateway that the context returns."""
        self.gateway = gateway

    async def __aenter__(self) -> FakeGateway:
        """Return the gateway."""
        return self.gateway

    async def __aexit__(
        self, error_type: type[BaseException] | None, error: BaseException | None, trace: TracebackType | None
    ) -> None:
        """Close nothing."""


class World:
    """The fake Drive, Sheets, BigQuery and gateway of a sync, and what it wrote."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, files: list[dict[str, str]], table: pl.DataFrame) -> None:
        """Replace every service that the sync reaches."""
        self.gateway = FakeGateway()
        self.written: list[pl.DataFrame] = []
        self.read_sheets: list[str] = []
        review = pl.DataFrame(
            {
                "sheet_row": [2],
                "id_sessao": ["s1"],
                "categoria": ["Health"],
                "conversa": ["AGENTE: oi\nCIDADÃO: preciso de ajuda"],
                "revisao": ["Wrong"],
                "categorias_corretas": ["Saúde"],
                "justificativa_revisao": ["because"],
            }
        )

        def connect(_model: str) -> Connection:
            return Connection(self.gateway)

        async def write(frame: pl.DataFrame) -> None:
            self.written.append(frame)

        def read_sheet(_sheets: object, file_id: str) -> pl.DataFrame:
            self.read_sheets.append(file_id)

            return review

        monkeypatch.setattr(tasks, "connect_drive", lambda: "drive")
        monkeypatch.setattr(sheets_helpers, "connect", lambda: "sheets")
        monkeypatch.setattr(sheets_helpers, "read_sheet", read_sheet)
        monkeypatch.setattr(drive_helpers, "find_reviewed_sheets", lambda _drive, _folder: files)
        monkeypatch.setattr(bq, "read_examples", lambda _sql: table)
        monkeypatch.setattr(Bifrost, "connect", connect)
        monkeypatch.setattr(tasks, "write_store", write)


def stored_row(**changes: object) -> pl.DataFrame:
    """Return one stored example of the campaign."""
    row = {
        "campaign": "camp",
        "session_id": "old",
        "conversation": "CIDADÃO: oi",
        "categories": ["Saúde"],
        "review": "Right",
        "review_note": "",
        "file_id": "f1",
        "file_name": "n",
        "modified_at": "2026-04-01T10:00:00Z",
        "revision_date": date(2026, 1, 1),
        "embedding": [1.0, 0.0],
        "model": EMBEDDING_MODEL,
        "ingested_at": NOW,
    } | changes

    return pl.DataFrame([row], schema=EXAMPLES_SCHEMA)


def catalog() -> ValidCatalog:
    """Return a validated catalog where Saúde is active."""
    active = catalog_of({"Saúde": "health"})

    return ValidCatalog(active=active, every=active)


async def test_a_new_reviewed_file_is_read_embedded_and_written_into_the_whole_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Embed what the citizen said, write the table once, and return the example."""
    world = World(monkeypatch, [FILE], pl.DataFrame(schema=EXAMPLES_SCHEMA))

    store = await tasks.sync_examples_task.fn("camp", "folder", catalog(), "model")

    assert world.read_sheets == ["f1"]
    assert world.gateway.embedded == [["preciso de ajuda"]]
    [table] = world.written
    assert table.row(0, named=True)["categories"] == ["Saúde"]
    assert table.row(0, named=True)["file_id"] == "f1"
    assert store.examples["session_id"].to_list() == ["s1"]
    assert store.vectors.shape == (1, 2)


async def test_a_file_that_did_not_change_is_not_read_or_written(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip the work, and still return the stored example."""
    world = World(monkeypatch, [FILE], stored_row())

    store = await tasks.sync_examples_task.fn("camp", "folder", catalog(), "model")

    assert world.read_sheets == []
    assert world.gateway.embedded == []
    assert world.written == []
    assert store.examples["session_id"].to_list() == ["old"]


async def test_a_file_embedded_with_another_model_is_read_again(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace its rows, so no vector of the old model stays in the table."""
    world = World(monkeypatch, [FILE], stored_row(model="old-model"))

    store = await tasks.sync_examples_task.fn("camp", "folder", catalog(), "model")

    assert world.read_sheets == ["f1"]
    [table] = world.written
    assert table["model"].to_list() == [EMBEDDING_MODEL]
    assert store.examples["session_id"].to_list() == ["s1"]


async def test_a_file_that_left_drive_loses_its_examples_and_other_campaigns_stay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write the table without the removed file, and keep other campaigns."""
    table = pl.concat([stored_row(), stored_row(campaign="other", session_id="theirs", file_id="g1")])
    world = World(monkeypatch, [], table)

    store = await tasks.sync_examples_task.fn("camp", "folder", catalog(), "model")

    [written] = world.written
    assert written["session_id"].to_list() == ["theirs"]
    assert store.examples.is_empty()


async def test_no_reviewed_file_and_an_empty_table_write_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Never create the table before a review exists."""
    world = World(monkeypatch, [], pl.DataFrame(schema=EXAMPLES_SCHEMA))

    store = await tasks.sync_examples_task.fn("camp", "folder", catalog(), "model")

    assert world.written == []
    assert store.examples.is_empty()


class FakeSheetsApi:
    """Record the rows that the Form read writes to the response sheet."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, frame: pl.DataFrame, *, fail: bool = False) -> None:
        """Replace the sheet reads and writes."""
        self.updates: list[tuple[tuple[str, str], dict[str, str]]] = []

        def update(
            _sheets: object, _location: object, _frame: pl.DataFrame, key: tuple[str, str], values: dict[str, str]
        ) -> None:
            if fail:
                raise ValueError("Expected one row of the request, found 0")

            self.updates.append((key, values))

        monkeypatch.setattr(sheets_helpers, "connect", lambda: "sheets")
        monkeypatch.setattr(sheets_helpers, "read_sheet", lambda *_: frame)
        monkeypatch.setattr(sheets_helpers, "update_request_row", update)


def form_frame(**row: str) -> pl.DataFrame:
    """Return a response sheet with one row."""
    values = {
        "Carimbo de data/hora": "05/11/2026 08:00:00",
        "Endereço de e-mail": "x@example.com",
        "Campanha": "camp",
        "Tipo de solicitação": "Relatório de engajamento",
        "Data de início das conversas": "01/11/2026",
        "Data de fim das conversas": "02/11/2026",
        "Data de geração": "05/11/2026",
        "status": "",
        "link_resultado": "",
        "processado_em": "",
    } | row

    return pl.DataFrame({"sheet_row": [2], **{name: [value] for name, value in values.items()}})


async def test_the_form_read_marks_what_runs_as_processing_and_closes_invalid_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write the processing mark before the work starts, so a second run skips it."""
    api = FakeSheetsApi(monkeypatch, form_frame())

    jobs = await tasks.read_form_jobs_task.fn(date(2026, 11, 5), [], "abcdef123456", NOW)

    assert [job.campaign for job in jobs] == ["camp"]
    assert api.updates == [
        (
            ("05/11/2026 08:00:00", "x@example.com"),
            {"status": "processando", "link_resultado": "", "processado_em": "05/11/2026 08:30:00"},
        )
    ]


async def test_the_form_read_closes_an_invalid_row_with_the_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tell the requester at once, and run nothing."""
    api = FakeSheetsApi(monkeypatch, form_frame(Campanha=""))

    jobs = await tasks.read_form_jobs_task.fn(date(2026, 11, 5), [], "abcdef123456", NOW)

    assert jobs == []
    assert api.updates[0][1]["status"] == "erro: The request has no campaign (run abcdef12)"


async def test_the_form_read_refuses_a_sheet_that_lost_a_column(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail loud, because a status in the wrong place is worse than none."""
    FakeSheetsApi(monkeypatch, form_frame().drop("status"))

    with pytest.raises(ValueError, match="status"):
        await tasks.read_form_jobs_task.fn(date(2026, 11, 5), [], None, NOW)


async def test_a_status_that_cannot_be_written_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Return False, so the other requests of the run go on."""
    FakeSheetsApi(monkeypatch, form_frame(), fail=True)

    assert await tasks.mark_status_task.fn(("t", "e"), "concluido", "", NOW) is False


async def test_a_status_is_written_with_the_time_of_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """Write the status, the link and the processing time on the row."""
    api = FakeSheetsApi(monkeypatch, form_frame())

    assert await tasks.mark_status_task.fn(("t", "e"), "concluido", "https://x", NOW) is True
    assert api.updates == [
        (("t", "e"), {"status": "concluido", "link_resultado": "https://x", "processado_em": "05/11/2026 08:30:00"})
    ]


def test_the_extraction_query_runs_with_parameters_and_the_report_shows_literals() -> None:
    """Show the real values in the report and run the safe query with parameters."""
    window = Window("O'Brien", "nome_campanha", date(2026, 11, 1), date(2026, 11, 4))

    running = tasks.conversations_sql(window, literal=False)
    shown = tasks.conversations_sql(window, literal=True)

    assert "c.hsm.nome_campanha = @value" in running
    assert "between @start and @end" in running
    assert "unnest(@sources)" in running
    assert "c.hsm.nome_campanha = 'O''Brien'" in shown
    assert "between '2026-11-01' and '2026-11-04'" in shown
    assert "('HSM', 'AI_AGENT_CIDADAO', 'AI_AGENT_AGENTE')" in shown


def test_limits_are_a_typed_dict() -> None:
    """Keep the names of the limits that the flow documents."""
    assert {"ceiling", "judge_ceiling", "discovery_cap", "concurrency", "rag_examples"} <= set(Limits.__annotations__)

from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import cast

import numpy as np
import polars as pl
import pytest

from pipelines.rj_crm__relatorio_engajamento_hsm import flow as flow_module
from pipelines.rj_crm__relatorio_engajamento_hsm import tasks
from pipelines.rj_crm__relatorio_engajamento_hsm.constants import (
    TYPE_QUICK,
    TYPE_REPORT,
    TYPE_VALIDATION,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.catalog import catalog_of, interpret_catalog
from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.requests import RequestError
from pipelines.rj_crm__relatorio_engajamento_hsm.tests.conftest import ClassificationFactory, ConversationFactory
from pipelines.rj_crm__relatorio_engajamento_hsm.types import (
    Discovery,
    ExampleStore,
    Extraction,
    Job,
    Limits,
    Outcome,
    Prepared,
    Report,
    Run,
    Window,
)

NOW = datetime(2026, 11, 5, 8, 30, tzinfo=UTC)
WINDOW = Window("camp", "nome_campanha", date(2026, 11, 1), date(2026, 11, 4))


def job(request_type: str = TYPE_REPORT, **changes: object) -> Job:
    """Return a job of a campaign, with changes for any field."""
    return Job("camp", request_type, WINDOW, **changes)  # pyright: ignore[reportArgumentType]  # the factory takes changes of any field


def run(**limits: int) -> Run:
    """Return the shared state of a run, with limits."""
    return Run("root", NOW, "model", cast("Limits", cast("object", limits)), "abcdef123456")


class Steps:
    """Fake step tasks that record their calls and return scripted results."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, defaults: dict[str, object]) -> None:
        """Replace every step task of the module with a recording fake."""
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.results = dict(defaults)

        for name in defaults:
            fake = self.make(name)

            if name != "process_request_task":
                monkeypatch.setattr(tasks, name, fake)

            if hasattr(flow_module, name):
                monkeypatch.setattr(flow_module, name, fake)

    def make(self, name: str) -> Callable[..., object]:
        """Return the fake of one step."""

        async def fake(*args: object) -> object:
            self.calls.append((name, args))
            value = self.results[name]

            if isinstance(value, Exception):
                raise value

            return value(*args) if callable(value) else value

        return fake

    def names(self) -> list[str]:
        """Return the names of the steps that ran, in order."""
        return [name for name, _ in self.calls]


@pytest.fixture
def steps(
    monkeypatch: pytest.MonkeyPatch, make_conversation: ConversationFactory, make_classification: ClassificationFactory
) -> Steps:
    """Return the fake steps with the results of a good run."""
    valid = interpret_catalog(
        pl.DataFrame({"Categoria": ["A", "B"], "Descrição": ["about a", "about b"], "Decisão": ["Keep", ""]})
    )
    store = ExampleStore(pl.DataFrame({"session_id": ["x"]}), np.empty((0, 0)))

    return Steps(
        monkeypatch,
        {
            "prepare_task": Prepared("folder"),
            "catalog_task": valid,
            "extract_task": Extraction([make_conversation("s1")], 3, 1),
            "discover_task": Discovery({"A": "about a"}, {"A": ["s1"]}),
            "publish_material_task": "https://material",
            "classify_task": [make_classification("s1", "A")],
            "sync_examples_task": store,
            "classify_reviewed_task": [make_classification("s1", "A")],
            "suggest_task": {"NEW": "new topic"},
            "judge_task": lambda items, *_: items,
            "publish_task": "https://round",
            "read_form_jobs_task": [],
            "read_dispatch_jobs_task": [],
            "mark_status_task": True,
            "process_request_task": Outcome("concluido", "https://x"),
        },
    )


async def test_a_validation_whose_catalog_exists_stops_and_points_to_it(steps: Steps) -> None:
    """Skip with the link of the existing `Catalog`, and read no conversation."""
    steps.results["prepare_task"] = Prepared("folder", catalog_id="sheet", catalog_link="https://catalog")

    outcome = await tasks.process_request_task.fn(job(TYPE_VALIDATION), run())

    assert outcome == Outcome("pulado: Catálogo já existe (renomeie o atual para gerar de novo)", "https://catalog")
    assert steps.names() == ["prepare_task"]


async def test_a_validation_discovers_and_publishes_the_material(steps: Steps) -> None:
    """Extract, discover the categories, and publish the `Catalog` in the folder."""
    outcome = await tasks.process_request_task.fn(job(TYPE_VALIDATION), run())

    assert outcome == Outcome("concluido", "https://material")
    assert steps.names() == ["prepare_task", "extract_task", "discover_task", "publish_material_task"]
    assert steps.calls[-1][1][0] == "folder"


async def test_a_validation_without_conversations_is_skipped(steps: Steps) -> None:
    """Publish nothing when the period has no conversation."""
    steps.results["extract_task"] = Extraction([], 3, 0)

    outcome = await tasks.process_request_task.fn(job(TYPE_VALIDATION), run())

    assert outcome.status.startswith("pulado: nenhuma conversa")
    assert "discover_task" not in steps.names()


async def test_a_round_that_already_exists_ends_the_request_as_skipped(steps: Steps) -> None:
    """Never read a conversation for a round that is done."""
    steps.results["prepare_task"] = Prepared("folder", Outcome("pulado: relatório 'x' já existe", "https://old"))

    outcome = await tasks.process_request_task.fn(job(), run())

    assert outcome.link == "https://old"
    assert steps.names() == ["prepare_task"]


async def test_an_engagement_report_uses_the_catalog_the_examples_the_judge_and_the_suggestions(steps: Steps) -> None:
    """Run every step in order, and say how many categories had no decision."""
    outcome = await tasks.process_request_task.fn(job(TYPE_REPORT), run())

    assert outcome == Outcome("concluido — 1 categoria(s) sem decisão no Catálogo foram ignoradas", "https://round")
    assert steps.names() == [
        "prepare_task",
        "catalog_task",
        "extract_task",
        "sync_examples_task",
        "classify_reviewed_task",
        "suggest_task",
        "judge_task",
        "publish_task",
    ]
    report = cast("Report", steps.calls[-1][1][0])
    assert report.quick is False
    assert report.suggestions == {"NEW": "new topic"}
    assert report.catalog == {"A": "about a"}
    assert "1 categoria(s) sem decisão" in report.notes[-1]


async def test_a_quick_report_has_no_catalog_and_no_examples(steps: Steps) -> None:
    """Discover the categories, classify with no example, and never touch the store."""
    outcome = await tasks.process_request_task.fn(job(TYPE_QUICK), run())

    assert outcome == Outcome("concluido", "https://round")
    assert steps.names() == [
        "prepare_task",
        "extract_task",
        "discover_task",
        "classify_task",
        "judge_task",
        "publish_task",
    ]
    report = cast("Report", steps.calls[-1][1][0])
    assert report.quick is True
    assert report.auto_examples == {"A": ["s1"]}


async def test_a_missing_catalog_is_an_error_without_a_run_id(steps: Steps) -> None:
    """Give the requester the reason, and no id, because no log helps."""
    steps.results["catalog_task"] = RequestError("Catálogo não encontrado na pasta da campanha")

    outcome = await tasks.process_request_task.fn(job(TYPE_REPORT), run())

    assert outcome == Outcome("erro: Catálogo não encontrado na pasta da campanha")
    assert "extract_task" not in steps.names()


@pytest.mark.parametrize(
    ("released", "ceiling", "stops"),
    [
        pytest.param(False, 0, True, id="above_the_ceiling_waits"),
        pytest.param(True, 0, False, id="above_the_ceiling_goes_on_when_released"),
        pytest.param(False, 5000, False, id="below_the_ceiling_goes_on"),
    ],
)
async def test_the_ceiling_stops_a_report_before_the_model(
    steps: Steps, released: bool, ceiling: int, stops: bool
) -> None:
    """Send nothing to the model when the request waits for a release."""
    outcome = await tasks.process_request_task.fn(job(TYPE_QUICK, released=released), run(ceiling=ceiling))

    assert outcome.status.startswith("aguardando liberação") is stops
    assert ("discover_task" in steps.names()) is not stops


async def test_a_report_with_no_classified_conversation_is_skipped(steps: Steps) -> None:
    """Publish no empty report."""
    steps.results["classify_task"] = []

    outcome = await tasks.process_request_task.fn(job(TYPE_QUICK), run())

    assert outcome == Outcome("pulado: nenhuma conversa foi classificada")
    assert "publish_task" not in steps.names()


async def test_an_unexpected_error_becomes_a_status_with_the_run_id_and_is_returned(steps: Steps) -> None:
    """Isolate the request: keep the error for the flow, which fails at the end."""
    steps.results["extract_task"] = TimeoutError("BigQuery is down")

    outcome = await tasks.process_request_task.fn(job(TYPE_QUICK), run())

    assert outcome.status == "erro: TimeoutError: BigQuery is down (run abcdef12)"
    assert outcome.unexpected == "camp: TimeoutError: BigQuery is down"


async def test_the_form_flow_writes_each_status_and_fails_at_the_end_if_a_request_failed(steps: Steps) -> None:
    """Process the second request even after the first fails, then raise once."""
    first, second = job(key=("t1", "a@x")), job(key=("t2", "b@x"))
    steps.results["read_form_jobs_task"] = [first, second]
    steps.results["process_request_task"] = lambda current, _: (
        Outcome("erro: boom", unexpected="camp: boom") if current is first else Outcome("concluido", "https://ok")
    )

    with pytest.raises(RuntimeError, match=r"1 request\(s\) failed with an unexpected error"):
        await flow_module.rj_crm__relatorio_engajamento_hsm.fn(reference_date="2026-11-05")

    marks = [args for name, args in steps.calls if name == "mark_status_task"]
    assert [(key, status, link) for key, status, link, _ in marks] == [
        (("t1", "a@x"), "erro: boom", ""),
        (("t2", "b@x"), "concluido", "https://ok"),
    ]


async def test_the_dispatches_flow_writes_no_status(steps: Steps) -> None:
    """Read the dispatches, make the reports, and write no sheet."""
    steps.results["read_dispatch_jobs_task"] = [job(TYPE_QUICK)]

    await flow_module.rj_crm__relatorio_engajamento_hsm.fn(source="dispatches", reference_date="2026-11-05")

    assert steps.names() == ["read_dispatch_jobs_task", "process_request_task"]


async def test_the_flow_ends_quietly_when_nothing_is_due(steps: Steps) -> None:
    """Make nothing and fail nothing."""
    await flow_module.rj_crm__relatorio_engajamento_hsm.fn(reference_date="2026-11-05")

    assert steps.names() == ["read_form_jobs_task"]
    assert steps.calls[0][1][:2] == (date(2026, 11, 5), [])


def test_the_catalog_helper_builds_a_catalog_from_descriptions() -> None:
    """Keep the helper that the tasks use to rebuild a catalog."""
    assert catalog_of({"A": "a"}).as_dict() == {"A": "a"}

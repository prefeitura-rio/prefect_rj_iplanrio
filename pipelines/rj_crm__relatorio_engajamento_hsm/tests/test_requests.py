from datetime import date, datetime

import polars as pl
import pytest

from pipelines.rj_crm__relatorio_engajamento_hsm.helpers.requests import (
    error_status,
    gate_conversations,
    is_due,
    is_stale,
    is_waiting,
    job_from_dispatch,
    job_from_request,
    parse_request,
    plan_pending,
    skipped_status,
    summarize_status,
    waiting_status,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.types import Dispatch, Request, Window


def row(**changes: str) -> dict[str, str]:
    """Return one valid Form row with the given changes."""
    values = {
        "Carimbo de data/hora": "05/11/2026 08:00:00",
        "Endereço de e-mail": "x@example.com",
        "Campanha": "campaign",
        "Tipo de solicitação": "Relatório de engajamento",
        "Data de início das conversas": "01/11/2026",
        "Data de fim das conversas": "2026-11-04",
        "Data de geração": "05/11/2026",
        "status": "",
        "link_resultado": "",
        "processado_em": "",
    }
    values.update(changes)

    return values


def request(**changes: object) -> Request:
    """Return a valid request with the given changes."""
    base = parse_request(7, row())

    return Request(**{**base.__dict__, **changes})  # pyright: ignore[reportArgumentType]  # the factory builds a dataclass from a dict


def test_a_form_row_is_parsed_with_both_date_formats() -> None:
    """Read the Brazilian and ISO dates that the Form produces."""
    parsed = parse_request(7, row())

    assert (parsed.row, parsed.campaign, parsed.start, parsed.end) == (
        7,
        "campaign",
        date(2026, 11, 1),
        date(2026, 11, 4),
    )
    assert parsed.generation_date == date(2026, 11, 5)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        pytest.param({"Campanha": " "}, "no campaign", id="an_empty_campaign"),
        pytest.param({"Tipo de solicitação": "Outro"}, "Unknown request type", id="an_unknown_type"),
        pytest.param(
            {"Data de início das conversas": "31/02/2026"}, "start is not a valid date", id="an_impossible_date"
        ),
        pytest.param({"Data de fim das conversas": ""}, "end is not a valid date", id="an_empty_date"),
        pytest.param({"Data de início das conversas": "10/11/2026"}, "after the end", id="a_start_after_the_end"),
        pytest.param({"Data de geração": "amanhã"}, "generation date is not a valid date", id="a_text_generation_date"),
    ],
)
def test_an_invalid_row_names_its_problem(changes: dict[str, str], message: str) -> None:
    """Raise `ValueError` with the field that is wrong."""
    with pytest.raises(ValueError, match=message):
        parse_request(7, row(**changes))


@pytest.mark.parametrize(
    ("changes", "today", "released", "expected"),
    [
        pytest.param({}, date(2026, 11, 5), set(), True, id="due_on_the_generation_date"),
        pytest.param({}, date(2026, 11, 6), set(), True, id="due_after_the_generation_date"),
        pytest.param({}, date(2026, 11, 4), set(), False, id="a_future_date_waits"),
        pytest.param({"status": "processando"}, date(2026, 11, 5), set(), False, id="processing_is_not_due"),
        pytest.param({"status": "concluido"}, date(2026, 11, 5), set(), False, id="done_is_not_due"),
        pytest.param({"status": "erro: x"}, date(2026, 11, 5), set(), False, id="an_error_is_not_due"),
        pytest.param(
            {"status": "aguardando liberação: 6000"}, date(2026, 11, 5), set(), False, id="waiting_without_release"
        ),
        pytest.param(
            {"status": "aguardando liberação: 6000"}, date(2026, 11, 5), {"campaign"}, True, id="waiting_with_release"
        ),
        pytest.param(
            {"status": "aguardando liberação: 6000"},
            date(2026, 11, 5),
            {"other"},
            False,
            id="another_campaign_released",
        ),
        pytest.param({"status": ""}, date(2026, 11, 5), {"campaign"}, True, id="a_new_request_ignores_the_release"),
    ],
)
def test_the_due_rule(changes: dict[str, str], today: date, released: set[str], expected: bool) -> None:
    """Run only the requests that the rule accepts."""
    assert is_due(request(**changes), today, released) is expected


@pytest.mark.parametrize(
    ("status", "processed_at", "expected"),
    [
        pytest.param("processando", "05/11/2026 08:00:00", False, id="processing_for_one_hour"),
        pytest.param("processando", "05/11/2026 02:59:59", True, id="processing_for_more_than_six_hours"),
        pytest.param("processando", "05/11/2026 03:00:00", False, id="processing_for_exactly_six_hours"),
        pytest.param("processando", "", True, id="processing_with_no_time_is_stale"),
        pytest.param("processando", "not a time", True, id="processing_with_a_bad_time_is_stale"),
        pytest.param("concluido", "01/01/2026 00:00:00", False, id="done_never_goes_stale"),
        pytest.param("", "", False, id="an_empty_status_is_not_processing"),
    ],
)
def test_the_stale_processing_rule(status: str, processed_at: str, expected: bool) -> None:
    """Mark a request stale only when it has processed for over six hours."""
    assert is_stale(status, processed_at, datetime(2026, 11, 5, 9, 0, 0)) is expected


def test_the_status_texts() -> None:
    """Build each status text of the cycle."""
    assert skipped_status("no rows") == "pulado: no rows"
    assert (
        waiting_status(6000, 5000)
        == "aguardando liberação: 6000 conversas com resposta em texto no período (teto: 5000)"
    )
    assert is_waiting(waiting_status(6000, 5000))
    assert error_status("drive  is\ndown", "abcdef123456") == "erro: drive is down (run abcdef12)"
    assert error_status("x" * 400).endswith("…")
    assert len(error_status("x" * 400)) <= len("erro: ") + 300


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        pytest.param("pulado: no rows", "pulado", id="skipped"),
        pytest.param("concluido — 2 categoria(s) sem decisão", "concluido", id="done_with_a_note"),
        pytest.param("erro: drive", "erro", id="error"),
        pytest.param("aguardando liberação: 1", "aguardando liberação", id="waiting"),
    ],
)
def test_a_status_summary_drops_its_detail(status: str, expected: str) -> None:
    """Return the status kind without its reason or note."""
    assert summarize_status(status) == expected


NOW = datetime(2026, 11, 5, 9, 0, 0)  # São Paulo wall time, as the sheet writes it
TODAY = date(2026, 11, 5)


def sheet(*rows: dict[str, str]) -> pl.DataFrame:
    """Return the response sheet as `read_sheet` gives it."""
    return pl.DataFrame(
        [{"sheet_row": str(number), **values} for number, values in enumerate(rows, start=2)]
    ).with_columns(pl.col("sheet_row").cast(pl.Int64))


@pytest.mark.parametrize(
    ("changes", "released", "due", "rejected"),
    [
        pytest.param({}, set(), True, None, id="a_new_due_row_runs"),
        pytest.param({"Data de geração": "06/11/2026"}, set(), False, None, id="a_future_row_waits"),
        pytest.param({"status": "concluido"}, set(), False, None, id="a_done_row_is_left_alone"),
        pytest.param(
            {"status": "processando", "processado_em": "05/11/2026 08:00:00"},
            set(),
            False,
            None,
            id="a_fresh_processing_row_is_left_alone",
        ),
        pytest.param(
            {"status": "processando", "processado_em": "05/11/2026 01:00:00"},
            set(),
            False,
            "erro: execução interrompida, envie a solicitação novamente (run abc12345)",
            id="a_stale_processing_row_is_closed_with_an_error",
        ),
        pytest.param(
            {"Campanha": ""},
            set(),
            False,
            "erro: The request has no campaign (run abc12345)",
            id="an_invalid_new_row_is_closed_at_once",
        ),
        pytest.param(
            {"Data de geração": "amanhã"},
            set(),
            False,
            "erro: generation date is not a valid date: 'amanhã' (run abc12345)",
            id="an_invalid_date_is_closed_even_before_its_day",
        ),
        pytest.param({"status": "aguardando liberação: 6000"}, set(), False, None, id="a_waiting_row_stays_waiting"),
        pytest.param(
            {"status": "aguardando liberação: 6000"},
            {"campaign"},
            True,
            None,
            id="a_waiting_row_of_a_released_campaign_runs",
        ),
        pytest.param(
            {"status": "erro: x", "Campanha": ""},
            set(),
            False,
            None,
            id="an_invalid_row_with_a_status_is_not_closed_twice",
        ),
    ],
)
def test_a_run_decides_what_to_do_with_each_row(
    changes: dict[str, str], released: set[str], due: bool, rejected: str | None
) -> None:
    """Run the due rows, close the stalled and invalid ones, and leave the others."""
    pending = plan_pending(sheet(row(**changes)), TODAY, released, NOW, "abc12345-0000")

    assert [request.row for request in pending.requests] == ([2] if due else [])
    assert [rejection.status for rejection in pending.rejections] == ([rejected] if rejected else [])


def test_only_a_request_that_was_waiting_carries_the_release() -> None:
    """Let a new request of a released campaign follow the normal ceiling."""
    frame = sheet(row(status="aguardando liberação: 6000"), row())

    pending = plan_pending(frame, TODAY, {"campaign"}, NOW, None)

    assert [(request.row, request.released) for request in pending.requests] == [(2, True), (3, False)]


def test_a_rejection_has_the_key_that_finds_its_row() -> None:
    """Keep the timestamp and the email, which are all that an invalid row can offer."""
    pending = plan_pending(sheet(row(Campanha="")), TODAY, set(), NOW, None)

    assert pending.rejections[0].key == ("05/11/2026 08:00:00", "x@example.com")


def test_a_form_request_becomes_a_job_by_campaign_with_its_row_key() -> None:
    """Filter by campaign, over the days of the dispatches, and keep the row key."""
    job = job_from_request(request(released=True))

    assert job.window == Window("campaign", "nome_campanha", date(2026, 11, 1), date(2026, 11, 4))
    assert job.request_type == "Relatório de engajamento"
    assert job.released
    assert job.key == ("05/11/2026 08:00:00", "x@example.com")


def test_a_dispatch_becomes_a_quick_job_by_hsm_name_that_needs_no_release() -> None:
    """Keep the baseline: by HSM name, since the dispatch day, to the reference day."""
    job = job_from_dispatch(Dispatch("hsm", date(2026, 11, 1)), date(2026, 11, 5))

    assert job.window == Window("hsm", "nome_hsm", date(2026, 11, 1), date(2026, 11, 5))
    assert job.request_type == "Relatório rápido (catálogo automático)"
    assert job.released
    assert job.key is None


@pytest.mark.parametrize(
    ("count", "released", "expected"),
    [
        pytest.param(
            0, False, "pulado: nenhuma conversa com resposta em texto do cidadão no período", id="no_conversation"
        ),
        pytest.param(5000, False, None, id="at_the_ceiling_goes_on"),
        pytest.param(
            5001,
            False,
            "aguardando liberação: 5001 conversas com resposta em texto no período (teto: 5000)",
            id="above_the_ceiling_waits",
        ),
        pytest.param(5001, True, None, id="above_the_ceiling_goes_on_when_released"),
    ],
)
def test_the_gate_stops_an_empty_or_a_large_request_before_the_model(
    count: int, released: bool, expected: str | None
) -> None:
    """Skip when empty, wait above the ceiling, and let a release pass."""
    assert gate_conversations(count, 5000, released=released) == expected

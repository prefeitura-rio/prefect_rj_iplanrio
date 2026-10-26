from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from httpx2 import Response

from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers import discord as discord_module
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers import mailman as mailman_module
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.configuracao import ler_destinatarios
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.discord import Contexto, enviar, montar_mensagem
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.janela import (
    CONFIG_PADRAO,
    Janela,
    resolver,
    ultimo_corte,
)
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.mailman import ConexaoDataRelay
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.ocorrencias import (
    Ocorrencia,
    descrever_ultima_ocorrencia,
    extrair_urls,
    frequencias,
    ler_ultima_ocorrencia,
    normalizar,
    para_template,
    urls_distintas,
)

SP = ZoneInfo("America/Sao_Paulo")


def moment(day: int, hour: int = 9) -> datetime:
    """Return a time of September 2026 in São Paulo."""
    return datetime(2026, 9, day, hour, tzinfo=SP)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param(["http://a", " http://b "], ("http://a", "http://b"), id="a_list"),
        pytest.param('["http://a", "", "http://b"]', ("http://a", "http://b"), id="json_text_of_a_list"),
        pytest.param("http://a", ("http://a",), id="plain_text_that_is_not_json"),
        pytest.param('"http://a"', ("http://a",), id="json_text_of_a_string"),
        pytest.param("null", (), id="json_null"),
        pytest.param("", (), id="empty"),
        pytest.param(None, (), id="nothing"),
        pytest.param(("http://a",), ("http://a",), id="a_tuple"),
    ],
)
def test_the_redacted_urls_come_in_any_format_of_the_column(raw: object, expected: tuple[str, ...]) -> None:
    """Accept a list, JSON text, a scalar and nothing."""
    assert extrair_urls(raw) == expected


def test_the_rows_of_the_query_become_occurrences_and_the_ones_with_no_url_are_dropped() -> None:
    """Keep the order, and drop a row without a usable URL or without a time."""
    rows: list[dict[str, object]] = [
        {"id_passo": "1", "inicio_datahora": moment(1), "redacted_urls": '["http://a"]'},
        {"id_passo": "2", "inicio_datahora": moment(2), "redacted_urls": "[]"},
        {"id_passo": "3", "inicio_datahora": None, "redacted_urls": "http://b"},
        {"id_passo": "4", "inicio_datahora": moment(3), "redacted_urls": ["http://c", "http://a"]},
    ]

    found = normalizar(rows)

    assert [(item.id_passo, item.urls) for item in found] == [("1", ("http://a",)), ("4", ("http://c", "http://a"))]
    assert normalizar([]) == []


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        pytest.param([{"ultima_ocorrencia": moment(5)}], moment(5), id="a_time"),
        pytest.param([{"ultima_ocorrencia": None}], None, id="never_seen"),
        pytest.param([], None, id="no_row"),
    ],
)
def test_the_last_occurrence_is_a_time_or_nothing(rows: list[dict[str, object]], expected: datetime | None) -> None:
    """Read the first row."""
    assert ler_ultima_ocorrencia(rows) == expected


@pytest.mark.parametrize(
    ("last", "text"),
    [
        pytest.param(None, "Nenhuma ocorrência registrada na fonte até o momento.", id="never"),
        pytest.param(moment(10), "Última ocorrência conhecida: 10/09/2026.", id="the_same_day"),
        pytest.param(moment(9), "Última ocorrência conhecida: 09/09/2026 (há 1 dia).", id="one_day_ago"),
        pytest.param(moment(1), "Última ocorrência conhecida: 01/09/2026 (há 9 dias).", id="many_days_ago"),
    ],
)
def test_the_last_occurrence_is_told_by_its_distance(last: datetime | None, text: str) -> None:
    """Measure the days against the end of the window."""
    assert descrever_ultima_ocorrencia(last, moment(10)) == text


def occurrences() -> list[Ocorrencia]:
    """Build two occurrences that share a URL."""
    return [
        Ocorrencia("1", datetime(2026, 9, 1, 8, 5, tzinfo=SP), ("http://a", "http://b")),
        Ocorrencia("2", datetime(2026, 9, 1, 9, 5, tzinfo=SP), ("http://b",)),
    ]


def test_the_urls_are_unique_counted_and_flattened_for_the_email() -> None:
    """Keep first-seen order, count by frequency, and write a line for each URL."""
    assert urls_distintas(occurrences()) == ["http://a", "http://b"]
    assert frequencias(occurrences()) == [{"url": "http://b", "total": 2}, {"url": "http://a", "total": 1}]
    assert para_template(occurrences())[0] == {"datahora": "01/09/2026 08:05", "url": "http://a", "id_passo": "1"}
    assert len(para_template(occurrences())) == 3


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("a@x.com, b@x.com", ["a@x.com", "b@x.com"], id="two_addresses"),
        pytest.param(" a@x.com ,, ", ["a@x.com"], id="blanks_are_dropped"),
        pytest.param("", [], id="empty"),
        pytest.param(None, [], id="nothing"),
    ],
)
def test_the_recipients_come_from_a_comma_list(raw: str | None, expected: list[str]) -> None:
    """Split by commas and trim."""
    assert ler_destinatarios(raw) == expected


@pytest.mark.parametrize(
    ("start", "end", "anchor", "now", "origin", "begin"),
    [
        pytest.param("2026-09-14 09:00:00", "2026-09-15 09:00:00", None, moment(20), "parametros", moment(14)),
        pytest.param(None, None, moment(15), moment(20), "agendamento", moment(14)),
        pytest.param(None, None, None, moment(20, 12), "corte_local", moment(19)),
        pytest.param(None, None, None, moment(20, 8), "corte_local", moment(18)),
    ],
)
def test_the_window_comes_from_the_parameters_then_the_schedule_then_the_last_cut(
    start: str | None, end: str | None, anchor: datetime | None, now: datetime, origin: str, begin: datetime
) -> None:
    """Follow the order of precedence, with 24 hours of window."""
    window = resolver(start, end, anchor, now, CONFIG_PADRAO)

    assert window.origem == origin
    assert window.inicio == begin


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        pytest.param("2026-09-14 09:00:00", None, "juntos", id="only_a_start"),
        pytest.param("2026-09-15 09:00:00", "2026-09-14 09:00:00", "anterior", id="a_start_after_the_end"),
    ],
)
def test_a_window_that_does_not_make_sense_is_refused(start: str, end: str | None, message: str) -> None:
    """Raise with the reason."""
    with pytest.raises(ValueError, match=message):
        resolver(start, end, None, moment(20), CONFIG_PADRAO)


def test_the_last_cut_is_the_most_recent_one_that_passed() -> None:
    """Go back one day if the cut of today has not come."""
    assert ultimo_corte(moment(20, 12), 9) == moment(20)
    assert ultimo_corte(moment(20, 8), 9) == moment(19)


def window() -> Janela:
    """Build the window of the 15th."""
    return Janela(moment(14), moment(15), "agendamento")


def test_the_discord_message_has_the_count_and_who_got_the_email() -> None:
    """Show the occurrences, the distinct URLs and the recipients."""
    text = montar_mensagem(window(), occurrences(), ["a@x.com"], Contexto("prod", "http://run"), None)

    assert "> Janela: 14/09/2026 09:00 → 15/09/2026 09:00" in text
    assert "**2** ocorrências no período, com 2 endereços distintos." in text
    assert "Detalhamento enviado para: a@x.com" in text
    assert "[rj_crm__whitelist_whatsapp_relatorio](http://run)" in text


def test_the_discord_message_says_that_the_email_failed_when_there_is_no_recipient() -> None:
    """Warn that nobody got the detail."""
    text = montar_mensagem(window(), occurrences(), [], Contexto("prod", ""), None)

    assert "O envio do e-mail falhou" in text
    assert "Execução" not in text


def test_a_day_without_occurrences_says_when_the_last_one_was() -> None:
    """Say that there was none, and how long ago the last one came."""
    text = montar_mensagem(window(), [], [], Contexto("staging", ""), moment(1))

    assert "Nenhuma ocorrência no período." in text
    assert "há 14 dias" in text


def test_a_message_over_the_limit_of_discord_is_refused() -> None:
    """Raise before any request."""
    with pytest.raises(ValueError, match="excede"):
        enviar("http://hook", "x" * 2001)


def stub_post(monkeypatch: pytest.MonkeyPatch, module: object, response: Response) -> list[dict[str, object]]:
    """Replace `post` of a helper module with a stub that records each call.

    The stub gives the response.
    """
    calls: list[dict[str, object]] = []

    def post(url: str, **options: object) -> Response:
        """Record the call."""
        calls.append({"url": url, **options})

        return response

    monkeypatch.setattr(module, "post", post)

    return calls


def test_discord_gets_the_message_as_content_and_waits_for_the_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Post the content with `wait=true` to the webhook."""
    calls = stub_post(monkeypatch, discord_module, Response(204))

    enviar("http://hook", "oi", timeout=5)

    assert calls == [{"url": "http://hook", "json": {"content": "oi"}, "params": {"wait": "true"}, "timeout": 5}]


def test_discord_refuses_a_failed_post(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise with the status of the answer."""
    stub_post(monkeypatch, discord_module, Response(500, text="boom"))

    with pytest.raises(ValueError, match="500"):
        enviar("http://hook", "oi")


@pytest.mark.parametrize(
    ("response", "message"),
    [
        pytest.param(Response(500, text="boom"), "500", id="a_failed_status_is_refused"),
        pytest.param(Response(200, json={"success": False, "message": "no"}), "recusou", id="a_refusal_is_refused"),
    ],
)
def test_the_mailman_refuses_a_failed_send(monkeypatch: pytest.MonkeyPatch, response: Response, message: str) -> None:
    """Raise when the status or the body says that the send failed."""
    stub_post(monkeypatch, mailman_module, response)

    with pytest.raises(ValueError, match=message):
        mailman_module.enviar(ConexaoDataRelay("http://relay", "key"), ["a@b.c"], "assunto", "<p>x</p>")


def test_the_mailman_sends_the_html_with_the_key_and_the_recipients(monkeypatch: pytest.MonkeyPatch) -> None:
    """Post the body as HTML, with the key in the header."""
    calls = stub_post(monkeypatch, mailman_module, Response(200, json={"success": True}))

    mailman_module.enviar(ConexaoDataRelay("http://relay", "key", 7), ["a@b.c"], "assunto", "<p>x</p>")

    assert calls == [
        {
            "url": "http://relay",
            "headers": {"x-api-key": "key"},
            "json": {"to_addresses": ["a@b.c"], "subject": "assunto", "body": "<p>x</p>", "is_html_body": True},
            "timeout": 7,
        }
    ]

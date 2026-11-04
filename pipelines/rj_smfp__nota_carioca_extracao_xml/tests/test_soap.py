from ssl import CERT_REQUIRED

import pytest
from httpx2 import AsyncClient, ConnectError, ConnectTimeout, MockTransport, Request, Response

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import SOAP_ACTION
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers import soap
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.soap import (
    build_envelope,
    build_tls_context,
    call_extraction,
    decide,
    parse_response,
)
from pipelines.rj_smfp__nota_carioca_extracao_xml.types import (
    Decision,
    ExtractionRefusedError,
    Outcome,
    RoutineResponse,
    WebserviceError,
)

NAMESPACE = "http://notacarioca.rio.gov.br/intranet"


def envelope(status: str, task: str = "0", message: str = "") -> str:
    """Return the SOAP answer of the service."""
    return (
        '<?xml version="1.0"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>'
        f'<Response xmlns="{NAMESPACE}"><StatusCode>{status}</StatusCode>'
        f"<ErrorMessage>{message}</ErrorMessage><TarefaId>{task}</TarefaId></Response></soap:Body></soap:Envelope>"
    )


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        pytest.param(envelope("Accepted", "7", "ok"), RoutineResponse("Accepted", "ok", 7), id="accepted_with_task"),
        pytest.param(envelope("OK"), RoutineResponse("OK", "", 0), id="ok_without_task"),
        pytest.param(envelope("Conflict", message="busy"), RoutineResponse("Conflict", "busy", 0), id="conflict"),
        pytest.param(
            f'<R xmlns="{NAMESPACE}"><StatusCode>OK</StatusCode></R>', RoutineResponse("OK", "", 0), id="bare_element"
        ),
    ],
)
def test_the_answer_is_read_from_the_xml(body: str, expected: RoutineResponse) -> None:
    """Read the status, the message and the task, with the task 0 when it is absent."""
    assert parse_response(body, 200, "text/xml") == expected


@pytest.mark.parametrize(
    ("body", "content_type", "match"),
    [
        pytest.param("<html><title>Requisição Negada</title></html>", "text/html", "Requisição Negada", id="html_page"),
        pytest.param(
            "<soap:Envelope xmlns:soap='x'><soap:Body><soap:Fault><faultstring>boom</faultstring></soap:Fault>"
            + "</soap:Body></soap:Envelope>",
            "text/xml",
            "SOAP Fault: boom",
            id="soap_fault",
        ),
        pytest.param("<!DOCTYPE x><a/>", "text/xml", "DOCTYPE", id="doctype"),
        pytest.param("not xml", "text/xml", "não é XML", id="not_xml"),
        pytest.param("<a><b/></a>", "text/xml", "sem StatusCode", id="no_status"),
        pytest.param(envelope("Accepted", "abc"), "text/xml", "TarefaId inválido", id="bad_task_id"),
    ],
)
def test_an_unreadable_answer_is_refused(body: str, content_type: str, match: str) -> None:
    """Raise `WebserviceError` and say what was wrong."""
    with pytest.raises(WebserviceError, match=match):
        parse_response(body, 200, content_type)


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        pytest.param(RoutineResponse("Accepted", "", 5), Decision(Outcome.STARTED, 5), id="accepted"),
        pytest.param(RoutineResponse("OK", "", 5), Decision(Outcome.STARTED, 5), id="ok_with_task"),
        pytest.param(RoutineResponse("OK", "", 0), Decision(Outcome.NOTHING_PENDING, 0), id="nothing_pending"),
        pytest.param(RoutineResponse("Conflict", "", 0), Decision(Outcome.ALREADY_RUNNING, 0), id="conflict"),
    ],
)
def test_the_answer_decides_what_to_follow(response: RoutineResponse, expected: Decision) -> None:
    """Map each answer to its outcome and task."""
    assert decide(response) == expected


@pytest.mark.parametrize(
    "response",
    [
        pytest.param(RoutineResponse("Accepted", "no task", 0), id="accepted_without_task"),
        pytest.param(RoutineResponse("InternalServerError", "boom", 0), id="other_code"),
    ],
)
def test_an_answer_that_does_not_start_is_refused(response: RoutineResponse) -> None:
    """Raise `ExtractionRefusedError` for a code that is no start and no conflict."""
    with pytest.raises(ExtractionRefusedError):
        decide(response)


def test_the_token_is_escaped_in_the_envelope() -> None:
    """Write the token as XML text, so that a `<` cannot break the envelope."""
    assert "<token>a&lt;b&amp;c</token>" in build_envelope("a<b&c")


URL = "https://notacariocahom.rio.gov.br/intranet/RotinasAutomaticas.asmx"
TOKEN = "token-de-teste-<&>"
XML = {"content-type": "text/xml; charset=utf-8"}


@pytest.fixture
def waits(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the waits between tries, and do not wait."""
    seen: list[float] = []

    async def record(seconds: float) -> None:
        """Keep the wait."""
        seen.append(seconds)

    monkeypatch.setattr(soap, "sleep", record)

    return seen


async def test_the_call_posts_the_envelope_with_the_action_header(waits: list[float]) -> None:
    """Send one POST with `SOAPAction` and read the task of the answer."""
    seen: list[Request] = []

    def handler(request: Request) -> Response:
        """Answer that the extraction started."""
        seen.append(request)
        return Response(200, text=envelope("Accepted", "134191"), headers=XML)

    async with AsyncClient(transport=MockTransport(handler)) as client:
        response = await call_extraction(client, URL, TOKEN)

    assert response.task_id == 134191
    assert [(r.method, str(r.url), r.headers["SOAPAction"]) for r in seen] == [("POST", URL, f'"{SOAP_ACTION}"')]
    assert waits == []


async def test_a_connection_error_is_tried_again_with_a_longer_wait(waits: list[float]) -> None:
    """Wait 30 s and then 60 s, and read the answer of the third try."""
    calls: list[int] = []

    def handler(request: Request) -> Response:
        """Refuse twice, and then answer."""
        calls.append(1)

        if len(calls) < 3:
            raise ConnectError("refused", request=request)

        return Response(200, text=envelope("Accepted", "1"), headers=XML)

    async with AsyncClient(transport=MockTransport(handler)) as client:
        response = await call_extraction(client, URL, TOKEN)

    assert response.status_code == "Accepted"
    assert waits == [30, 60]


async def test_the_call_gives_up_without_the_token(waits: list[float]) -> None:
    """Raise after three tries, with the address and not the token."""

    def handler(request: Request) -> Response:
        """Time out."""
        raise ConnectTimeout("timed out", request=request)

    async with AsyncClient(transport=MockTransport(handler)) as client:
        with pytest.raises(WebserviceError) as error:
            await call_extraction(client, URL, TOKEN)

    assert "3 tentativas" in str(error.value)
    assert URL in str(error.value)
    assert TOKEN not in str(error.value)
    assert waits == [30, 60]


async def test_an_answer_of_the_service_is_not_tried_again(waits: list[float]) -> None:
    """Raise at once for a firewall page: only the transport failures are repeated."""
    calls: list[int] = []

    def handler(request: Request) -> Response:
        """Answer with the page of the firewall."""
        del request
        calls.append(1)
        return Response(
            200, text="<html><title>Requisição Negada</title></html>", headers={"content-type": "text/html"}
        )

    async with AsyncClient(transport=MockTransport(handler)) as client:
        with pytest.raises(WebserviceError, match="Requisição Negada"):
            await call_extraction(client, URL, TOKEN)

    assert len(calls) == 1
    assert waits == []


def test_the_tls_context_accepts_the_rsa_gcm_ciphers_and_still_verifies() -> None:
    """Add the two ciphers that the server needs, with the certificate check on."""
    context = build_tls_context()
    names = {cipher["name"] for cipher in context.get_ciphers()}

    assert {"AES256-GCM-SHA384", "AES128-GCM-SHA256", "ECDHE-RSA-AES256-GCM-SHA384"} <= names
    assert context.verify_mode == CERT_REQUIRED
    assert context.check_hostname

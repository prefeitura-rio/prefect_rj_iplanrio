import ssl

import httpx
import pytest

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import SOAP_ACTION, SOAP_NAMESPACE
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.soap import (
    Decision,
    ExtractionRefusedError,
    Outcome,
    RoutineResponse,
    WebserviceError,
    build_envelope,
    build_tls_context,
    call_extraction,
    decide,
    parse_response,
)

URL = "https://notacariocahom.rio.gov.br/intranet/RotinasAutomaticas.asmx"
TOKEN = "token-de-teste-<&>"
XML = "text/xml; charset=utf-8"


def soap_response(status: str, message: str, task_id: int) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
        "<soap:Body>"
        f'<ExtrairDocumentosFiscaisNacionaisAutomaticoResponse xmlns="{SOAP_NAMESPACE}">'
        "<ExtrairDocumentosFiscaisNacionaisAutomaticoResult>"
        f"<StatusCode>{status}</StatusCode><ErrorMessage>{message}</ErrorMessage><TarefaId>{task_id}</TarefaId>"
        "</ExtrairDocumentosFiscaisNacionaisAutomaticoResult>"
        "</ExtrairDocumentosFiscaisNacionaisAutomaticoResponse>"
        "</soap:Body></soap:Envelope>"
    )


ACCEPTED = soap_response(
    "Accepted", "Processamento iniciado em background. Consulte o status pelo TarefaId retornado.", 134191
)


def test_build_envelope_sends_only_the_escaped_token_as_the_wsdl_defines() -> None:
    # O WSDL de produção e o de homologação declaram só ``token`` nesta operação (o modo é fixo no servidor).
    envelope = build_envelope("a<b&c>")

    assert f'<ExtrairDocumentosFiscaisNacionaisAutomaticoRequest xmlns="{SOAP_NAMESPACE}">' in envelope
    assert "<token>a&lt;b&amp;c&gt;</token>" in envelope
    assert "<mode>" not in envelope


def test_parse_response_reads_status_message_and_task_of_the_soap_body() -> None:
    response = parse_response(ACCEPTED, 200, XML)

    assert response == RoutineResponse(
        status_code="Accepted",
        message="Processamento iniciado em background. Consulte o status pelo TarefaId retornado.",
        task_id=134191,
    )


def test_parse_response_accepts_the_bare_return_element() -> None:
    bare = (
        '<RotinaAutomaticaRetorno xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
        f'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="{SOAP_NAMESPACE}">'
        "<StatusCode>Conflict</StatusCode><ErrorMessage>já em execução</ErrorMessage><TarefaId>0</TarefaId>"
        "</RotinaAutomaticaRetorno>"
    )

    assert parse_response(bare, 200, XML) == RoutineResponse("Conflict", "já em execução", 0)


def test_parse_response_treats_missing_task_and_message_as_zero_and_empty() -> None:
    body = soap_response("OK", "", 0).replace("<ErrorMessage></ErrorMessage>", "")

    assert parse_response(body, 200, XML) == RoutineResponse("OK", "", 0)


def test_parse_response_explains_the_firewall_page_instead_of_parsing_html() -> None:
    page = (
        "<html><head><title>Requisição Negada</title></head><body>Sua requisição foi negada.<br><br>"
        "Em caso de necessidade de contato, informe o support ID: 123</body></html>"
    )

    with pytest.raises(WebserviceError, match=r"HTTP 200.*Requisição Negada"):
        parse_response(page, 200, "text/html; charset=utf-8")


def test_parse_response_raises_the_soap_fault_text() -> None:
    fault = (
        '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><soap:Fault>'
        "<faultcode>soap:Client</faultcode><faultstring>Servidor não reconheceu o SOAPAction</faultstring>"
        "</soap:Fault></soap:Body></soap:Envelope>"
    )

    with pytest.raises(WebserviceError, match=r"HTTP 500.*Servidor não reconheceu o SOAPAction"):
        parse_response(fault, 500, XML)


def test_parse_response_refuses_a_document_type_declaration() -> None:
    with pytest.raises(WebserviceError, match="DOCTYPE"):
        parse_response('<!DOCTYPE x [<!ENTITY a "b">]><x>&a;</x>', 200, XML)


def test_parse_response_refuses_a_body_without_status() -> None:
    with pytest.raises(WebserviceError, match="StatusCode"):
        parse_response(f'<RotinaAutomaticaRetorno xmlns="{SOAP_NAMESPACE}"/>', 200, XML)


@pytest.mark.parametrize(
    ("response", "decision"),
    [
        (RoutineResponse("Accepted", "iniciado", 134191), Decision(Outcome.STARTED, 134191)),
        (RoutineResponse("OK", "", 0), Decision(Outcome.NOTHING_PENDING, 0)),
        (RoutineResponse("OK", "", 77), Decision(Outcome.STARTED, 77)),
        (RoutineResponse("Conflict", "já em execução", 0), Decision(Outcome.ALREADY_RUNNING, 0)),
    ],
)
def test_decide_maps_the_answers_that_let_the_chain_go_on(response: RoutineResponse, decision: Decision) -> None:
    assert decide(response) == decision


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (RoutineResponse("Unauthorized", "Token inválido", 0), r"Unauthorized: Token inválido"),
        (RoutineResponse("InternalServerError", "exceção não tratada", 0), r"InternalServerError"),
        (RoutineResponse("ServiceUnavailable", "indisponível", 0), r"ServiceUnavailable"),
        (RoutineResponse("Accepted", "sem tarefa", 0), r"Accepted sem TarefaId"),
    ],
)
def test_decide_refuses_errors_and_inconsistent_answers(response: RoutineResponse, expected: str) -> None:
    with pytest.raises(ExtractionRefusedError, match=expected):
        decide(response)


def client_with(handler: httpx.MockTransport) -> httpx.Client:
    return httpx.Client(transport=handler)


def test_call_extraction_posts_soap_with_the_action_header_and_reads_the_answer() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, text=ACCEPTED, headers={"content-type": XML})

    response = call_extraction(client_with(httpx.MockTransport(handler)), URL, TOKEN, sleep=lambda _s: None)

    assert response.task_id == 134191
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST"
    assert str(request.url) == URL
    assert request.headers["SOAPAction"] == f'"{SOAP_ACTION}"'
    assert request.headers["content-type"].startswith("text/xml")
    assert b"<mode>" not in request.content


def test_call_extraction_retries_connection_errors_and_then_succeeds() -> None:
    calls = {"n": 0}
    slept: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            raise httpx.ConnectError("conexão recusada", request=request)
        return httpx.Response(200, text=ACCEPTED, headers={"content-type": XML})

    response = call_extraction(
        client_with(httpx.MockTransport(handler)), URL, TOKEN, attempts=3, backoff_seconds=30, sleep=slept.append
    )

    assert response.status_code == "Accepted"
    assert calls["n"] == 3
    assert slept == [30, 60]


def test_call_extraction_gives_up_without_leaking_the_token() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("tempo esgotado", request=request)

    with pytest.raises(WebserviceError) as error:
        call_extraction(client_with(httpx.MockTransport(handler)), URL, TOKEN, attempts=2, sleep=lambda _s: None)

    assert "2 tentativas" in str(error.value)
    assert URL in str(error.value)
    assert TOKEN not in str(error.value)


def test_call_extraction_does_not_retry_an_answer_of_the_webservice() -> None:
    calls = {"n": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, text="<html><title>Requisição Negada</title></html>", headers={"content-type": "text/html"})

    with pytest.raises(WebserviceError, match="Requisição Negada"):
        call_extraction(client_with(httpx.MockTransport(handler)), URL, TOKEN, attempts=3, sleep=lambda _s: None)

    assert calls["n"] == 1


def test_tls_context_accepts_the_rsa_gcm_ciphers_of_the_nota_carioca_server_and_still_verifies() -> None:
    # O servidor só negocia TLS 1.2 com troca de chaves RSA (AES256-GCM-SHA384), que o padrão do Python exclui.
    context = build_tls_context()

    names = {cipher["name"] for cipher in context.get_ciphers()}
    assert {"AES256-GCM-SHA384", "AES128-GCM-SHA256"} <= names
    assert "ECDHE-RSA-AES256-GCM-SHA384" in names
    assert not any("CBC" in name or name.endswith("-SHA") for name in names if name.startswith("AES"))
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname

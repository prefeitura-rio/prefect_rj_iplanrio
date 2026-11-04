"""The SOAP 1.1 call to `ExtrairDocumentosFiscaisNacionaisAutomatico`, and its answer.

The operation starts the extraction in the background and answers at once with a
`RotinaAutomaticaRetorno` (`StatusCode`, `ErrorMessage`, `TarefaId`). `Accepted` with a
`TarefaId` is an extraction that started. `OK` with `TarefaId=0` is nothing pending to
extract. `Conflict` is another extraction that already runs, and the service does not
queue. Any other code is an error. The token never goes to a log or an error.
"""

from asyncio import sleep
from html import unescape
from re import DOTALL, IGNORECASE, search, sub
from ssl import SSLContext, create_default_context
from xml.etree.ElementTree import Element, ParseError, fromstring
from xml.sax.saxutils import escape

from httpx2 import AsyncClient, TransportError
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import (
    CALL_ATTEMPTS,
    CALL_BACKOFF_SECONDS,
    REQUEST_ELEMENT,
    REQUEST_TIMEOUT_SECONDS,
    SNIPPET_LIMIT,
    SOAP_ACTION,
    SOAP_ENVELOPE_NAMESPACE,
    SOAP_NAMESPACE,
    TLS_CIPHERS,
)
from pipelines.rj_smfp__nota_carioca_extracao_xml.types import (
    Decision,
    ExtractionRefusedError,
    Outcome,
    RoutineResponse,
    WebserviceError,
)


def build_tls_context() -> SSLContext:
    """Return the TLS context of the service, with the RSA-GCM ciphers added."""
    context = create_default_context()
    context.set_ciphers(TLS_CIPHERS)

    return context


def build_envelope(token: str) -> str:
    """Return the SOAP envelope of the operation, with the token escaped.

    The WSDL declares only `token`: the operation `...Automatico` fixes the mode on the
    server.
    """
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        f'<soap:Envelope xmlns:soap="{SOAP_ENVELOPE_NAMESPACE}">'
        "<soap:Body>"
        f'<{REQUEST_ELEMENT} xmlns="{SOAP_NAMESPACE}">'
        f"<token>{escape(token)}</token>"
        f"</{REQUEST_ELEMENT}>"
        "</soap:Body>"
        "</soap:Envelope>"
    )


def summarize(text: str) -> str:
    """Return the title of an HTML page, or its text with no markup, cut short."""
    title = search(r"<title[^>]*>(.*?)</title>", text, DOTALL | IGNORECASE)
    raw = title.group(1) if title else sub(r"<[^>]+>", " ", text)

    return " ".join(unescape(raw).split())[:SNIPPET_LIMIT]


def local_name(tag: str) -> str:
    """Return the name of an element with no namespace."""
    return tag.rsplit("}", 1)[-1]


def find_text(root: Element, name: str) -> str | None:
    """Return the text of the first element with the local name, at any depth."""
    for element in root.iter():
        if local_name(element.tag) == name:
            return (element.text or "").strip()

    return None


def parse_response(text: str, http_status: int, content_type: str) -> RoutineResponse:
    """Read the `RotinaAutomaticaRetorno` of a SOAP envelope, or of the bare element.

    Raise `WebserviceError` for an HTML page (such as a firewall block), a SOAP Fault, a
    DOCTYPE, text that is not XML, or a body with no `StatusCode`.
    """
    stripped = text.lstrip()

    if "html" in content_type.lower() or stripped[:5].lower() == "<html":
        raise WebserviceError(
            f"O webservice respondeu HTTP {http_status} com uma página HTML, não SOAP: {summarize(text)}. "
            + "Se for 'Requisição Negada', o firewall da prefeitura bloqueou a origem; peça a liberação do IP de saída."
        )

    if "<!DOCTYPE" in text.upper():
        raise WebserviceError(f"Resposta HTTP {http_status} com DOCTYPE recusada (o webservice não envia DTD).")

    try:
        root = fromstring(stripped)
    except ParseError as error:
        raise WebserviceError(f"Resposta HTTP {http_status} não é XML: {error}; início: {summarize(text)}") from error

    fault = find_text(root, "faultstring")

    if fault is not None:
        raise WebserviceError(f"O webservice respondeu HTTP {http_status} com SOAP Fault: {fault}")

    status = find_text(root, "StatusCode")

    if not status:
        raise WebserviceError(f"Resposta HTTP {http_status} sem StatusCode: {summarize(text)}")

    task = find_text(root, "TarefaId") or "0"

    try:
        task_id = int(task)
    except ValueError as error:
        raise WebserviceError(f"TarefaId inválido na resposta: {task!r}") from error

    return RoutineResponse(status, find_text(root, "ErrorMessage") or "", task_id)


def decide(response: RoutineResponse) -> Decision:
    """Turn the answer into the outcome for the load.

    Raise `ExtractionRefusedError` for any other code, and for `Accepted` with no task.
    """
    match response.status_code:
        case "Accepted" | "OK" if response.task_id > 0:
            return Decision(Outcome.STARTED, response.task_id)
        case "OK":
            return Decision(Outcome.NOTHING_PENDING, 0)
        case "Conflict":
            return Decision(Outcome.ALREADY_RUNNING, 0)
        case "Accepted":
            raise ExtractionRefusedError(f"O webservice respondeu Accepted sem TarefaId: {response.message}")
        case _:
            raise ExtractionRefusedError(f"O webservice recusou a extração: {response.status_code}: {response.message}")


async def call_extraction(
    client: AsyncClient, url: str, token: str, *, backoff_seconds: float = CALL_BACKOFF_SECONDS
) -> RoutineResponse:
    """Call the operation and read the answer. Only a transport failure is tried again.

    The wait before the try `n+1` is `backoff_seconds * n`. Raise `WebserviceError` if
    no try connects, or if the answer is not readable.
    """
    headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": f'"{SOAP_ACTION}"'}
    body = build_envelope(token).encode("utf-8")
    last: TransportError | None = None

    for attempt in range(1, CALL_ATTEMPTS + 1):
        try:
            response = await client.post(url, content=body, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
        except TransportError as error:
            last = error
            logger.warning(
                "Webservice inacessível: attempt=%d of=%d error=%s", attempt, CALL_ATTEMPTS, type(error).__name__
            )

            if attempt < CALL_ATTEMPTS:
                await sleep(backoff_seconds * attempt)

            continue

        return parse_response(response.text, response.status_code, response.headers.get("content-type", ""))

    detail = f"{type(last).__name__}: {last}" if last else "sem detalhe"

    raise WebserviceError(f"Webservice {url} inacessível após {CALL_ATTEMPTS} tentativas ({detail}).")

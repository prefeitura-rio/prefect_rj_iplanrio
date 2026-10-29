"""Chamada SOAP 1.1 à operação ``ExtrairDocumentosFiscaisNacionaisAutomatico`` e leitura da resposta.

A operação só dispara a extração em segundo plano e devolve na hora ``RotinaAutomaticaRetorno`` (``StatusCode``,
``ErrorMessage``, ``TarefaId``). Comportamento do webservice (``NotaNacional.vb``,
``ExtrairDocumentosFiscaisNacionaisAsync``): ``Accepted`` com ``TarefaId`` = extração iniciada; ``OK`` com
``TarefaId=0`` = nada pendente para extrair; ``Conflict`` = outra extração já em execução (não enfileira); qualquer
outro código = erro. O token nunca aparece em log nem em erro.
"""

import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from html import unescape
from re import DOTALL, IGNORECASE, search, sub
from xml.sax.saxutils import escape

import httpx

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import REQUEST_ELEMENT, SOAP_ACTION, SOAP_NAMESPACE

SOAP_ENVELOPE_NAMESPACE = "http://schemas.xmlsoap.org/soap/envelope/"
REQUEST_TIMEOUT_SECONDS = 60.0
SNIPPET_LIMIT = 300


class WebserviceError(RuntimeError):
    """O webservice não pôde ser chamado ou não respondeu com um ``RotinaAutomaticaRetorno`` legível."""


class ExtractionRefusedError(RuntimeError):
    """O webservice respondeu, mas recusou ou não iniciou a extração."""


@dataclass(frozen=True)
class RoutineResponse:
    """``RotinaAutomaticaRetorno`` devolvido pelo webservice.

    :param status_code: Nome do ``HttpStatusCode`` do .NET (``Accepted``, ``OK``, ``Conflict``...).
    :param message: ``ErrorMessage`` (texto informativo também nos casos de sucesso).
    :param task_id: ``TarefaId`` em ``NFSE_OWNER.TAREFAS``; 0 quando nenhuma tarefa foi criada.
    """

    status_code: str
    message: str
    task_id: int


class Outcome(StrEnum):
    """O que a resposta significa para a carga."""

    STARTED = "iniciada"
    NOTHING_PENDING = "nada pendente"
    ALREADY_RUNNING = "já em execução"


@dataclass(frozen=True)
class Decision:
    """Desfecho da chamada e a tarefa a acompanhar (0 quando não há tarefa).

    :param outcome: O que aconteceu.
    :param task_id: Tarefa criada pela chamada.
    """

    outcome: Outcome
    task_id: int


def build_envelope(token: str) -> str:
    """Monta o envelope SOAP 1.1 da operação, com o token escapado.

    O WSDL (produção e homologação) declara só ``token``: a operação ``...Automatico`` fixa o modo no servidor, e o modo
    da extração só é validado no .NET, sem efeito.

    :param token: Token do webservice (lista ``wsNotaNacionalToken`` da aplicação).
    :returns: O XML do envelope.
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


def _snippet(text: str) -> str:
    """Resume uma página HTML: o título, se houver, senão o texto sem marcação, truncado."""
    title = search(r"<title[^>]*>(.*?)</title>", text, DOTALL | IGNORECASE)
    raw = title.group(1) if title else sub(r"<[^>]+>", " ", text)
    return " ".join(unescape(raw).split())[:SNIPPET_LIMIT]


def _local(tag: str) -> str:
    """Nome do elemento sem o namespace."""
    return tag.rsplit("}", 1)[-1]


def _first_text(root: ET.Element, name: str) -> str | None:
    """Texto do primeiro elemento com o nome local dado, em qualquer nível."""
    for element in root.iter():
        if _local(element.tag) == name:
            return (element.text or "").strip()
    return None


def parse_response(text: str, http_status: int, content_type: str) -> RoutineResponse:
    """Lê o ``RotinaAutomaticaRetorno`` de um envelope SOAP (ou do elemento puro).

    :param text: Corpo da resposta.
    :param http_status: Código HTTP da resposta.
    :param content_type: Cabeçalho ``Content-Type``.
    :returns: Status, mensagem e tarefa.
    :raises WebserviceError: Página HTML (ex.: bloqueio do firewall), SOAP Fault, DOCTYPE ou corpo sem ``StatusCode``.
    """
    stripped = text.lstrip()
    if "html" in content_type.lower() or stripped[:5].lower() == "<html":
        raise WebserviceError(
            f"O webservice respondeu HTTP {http_status} com uma página HTML, não SOAP: {_snippet(text)}. "
            "Se for 'Requisição Negada', o firewall da prefeitura bloqueou a origem; peça a liberação do IP de saída."
        )
    if "<!DOCTYPE" in text.upper():
        raise WebserviceError(f"Resposta HTTP {http_status} com DOCTYPE recusada (o webservice não envia DTD).")
    try:
        root = ET.fromstring(stripped)
    except ET.ParseError as error:
        raise WebserviceError(f"Resposta HTTP {http_status} não é XML: {error}; início: {_snippet(text)}") from error
    fault = _first_text(root, "faultstring")
    if fault is not None:
        raise WebserviceError(f"O webservice respondeu HTTP {http_status} com SOAP Fault: {fault}")
    status = _first_text(root, "StatusCode")
    if not status:
        raise WebserviceError(f"Resposta HTTP {http_status} sem StatusCode: {_snippet(text)}")
    task = _first_text(root, "TarefaId") or "0"
    try:
        task_id = int(task)
    except ValueError as error:
        raise WebserviceError(f"TarefaId inválido na resposta: {task!r}") from error
    return RoutineResponse(status_code=status, message=_first_text(root, "ErrorMessage") or "", task_id=task_id)


def decide(response: RoutineResponse) -> Decision:
    """Traduz a resposta no desfecho para a carga.

    :param response: Resposta lida.
    :returns: Extração iniciada (com a tarefa), nada pendente ou já em execução.
    :raises ExtractionRefusedError: Qualquer outro código, ou ``Accepted`` sem tarefa.
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


def call_extraction(  # noqa: PLR0913
    client: httpx.Client,
    url: str,
    token: str,
    *,
    attempts: int = 3,
    backoff_seconds: float = 30.0,
    sleep: Callable[[float], None] = time.sleep,
) -> RoutineResponse:
    """Chama a operação e lê a resposta; só falhas de transporte (rede, tempo) são repetidas.

    :param client: Cliente HTTP.
    :param url: Endereço do ``RotinasAutomaticas.asmx`` do ambiente.
    :param token: Token do webservice.
    :param attempts: Tentativas para falhas de transporte.
    :param backoff_seconds: Espera antes da tentativa ``n+1`` = ``backoff_seconds * n``.
    :param sleep: Função de espera (os testes passam uma falsa).
    :returns: A resposta do webservice.
    :raises WebserviceError: Se nenhuma tentativa conectar, ou se a resposta não for legível.
    """
    headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": f'"{SOAP_ACTION}"'}
    body = build_envelope(token).encode("utf-8")
    last: httpx.TransportError | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = client.post(url, content=body, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
        except httpx.TransportError as error:
            last = error
            if attempt < attempts:
                sleep(backoff_seconds * attempt)
            continue
        return parse_response(response.text, response.status_code, response.headers.get("content-type", ""))
    detail = f"{type(last).__name__}: {last}" if last else "sem detalhe"
    raise WebserviceError(f"Webservice {url} inacessível após {attempts} tentativas ({detail}).")

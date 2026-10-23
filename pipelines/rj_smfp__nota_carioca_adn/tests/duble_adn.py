"""Dublê de contrato do ADN (``GET /municipios/DFe/{nsu}``), só para testes.

As respostas seguem o schema ``LoteDistribuicaoNSUResponse`` do swagger oficial (espelhado em
``schema_lote_distribuicao.json``) e carregam XML sintético (``xml_sintetico``). O servidor fala HTTPS e **exige
certificado de cliente** (mTLS de verdade), assinado pela CA de teste de ``certificados``.

As duas divergências entre manual e swagger são configuráveis: cursor exclusivo (manual, app) ou inclusivo (swagger) e
lote de 50 ou de 100. Também simula fim de dados em três formas (404, 200 vazio, 200 com status), NSUs que o lote
omite mas o ``lote=false`` devolve (lacunas), NSUs inexistentes, respostas forçadas (429, 503, 400 com ``Erros``...)
e corrupção do ``ArquivoXml``.
"""

import base64
import gzip
import json
import ssl
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Literal
from urllib.parse import parse_qs, urlsplit

from pipelines.rj_smfp__nota_carioca_adn.tests import xml_sintetico
from pipelines.rj_smfp__nota_carioca_adn.tests.certificados import Pki

FimComo = Literal["404", "200_vazio", "200_status"]
Corrupcao = Literal["gzip", "base64", "utf8"]
FUSO_BRASILIA = timezone(timedelta(hours=-3))


@dataclass(frozen=True)
class RespostaForcada:
    """Resposta que o dublê devolve no lugar da normal, uma por requisição, na ordem do roteiro."""

    status: int
    corpo: dict[str, object] | None = None


def erro_adn(status: int, codigo: str, descricao: str = "erro sintetico") -> RespostaForcada:
    """Rejeição do ADN: corpo no schema, com ``StatusProcessamento=REJEICAO`` e um item em ``Erros``."""
    return RespostaForcada(
        status, {"StatusProcessamento": "REJEICAO", "Erros": [{"Codigo": codigo, "Descricao": descricao}]}
    )


@dataclass
class Comportamento:
    """O que o dublê oferece. Alterável durante o teste."""

    total_nsu: int = 500
    lote_max: int = 50
    cursor_inclusivo: bool = False
    lacunas: frozenset[int] = frozenset()
    inexistentes: frozenset[int] = frozenset()
    fim_como: FimComo = "200_status"
    corrupcao: dict[int, Corrupcao] = field(default_factory=dict)
    roteiro: deque[RespostaForcada] = field(default_factory=deque)
    arquivo_xml_ausente: frozenset[int] = frozenset()
    sem_data_geracao: frozenset[int] = frozenset()

    def existe(self, nsu: int) -> bool:
        """Diz se o NSU existe no ADN simulado (1..total, menos os inexistentes)."""
        return 1 <= nsu <= self.total_nsu and nsu not in self.inexistentes


@dataclass(frozen=True)
class Requisicao:
    """O que o servidor viu numa chamada."""

    nsu: int
    lote: bool
    tipo_nsu: str
    cn_cliente: str | None
    tls: str | None
    instante: float


def _arquivo_xml(nsu: int, corrupcao: Corrupcao | None) -> str:
    xml = xml_sintetico.documento_xml(nsu).encode()
    match corrupcao:
        case "gzip":
            return base64.b64encode(b"isto nao e gzip").decode()
        case "base64":
            return "@@@ isto nao e base64 @@@"
        case "utf8":
            return base64.b64encode(gzip.compress(b"<NFSe>\xff\xfe\xfd</NFSe>", mtime=0)).decode()
        case None:
            return base64.b64encode(gzip.compress(xml, mtime=0)).decode()


def item_do_nsu(nsu: int, comportamento: Comportamento) -> dict[str, object]:
    """Monta o ``DistribuicaoNSU`` do swagger para o NSU."""
    tipo = xml_sintetico.tipo_documento(nsu)
    item: dict[str, object] = {
        "NSU": nsu,
        "ChaveAcesso": xml_sintetico.chave_acesso(nsu),
        "TipoDocumento": tipo,
        "ArquivoXml": None
        if nsu in comportamento.arquivo_xml_ausente
        else _arquivo_xml(nsu, comportamento.corrupcao.get(nsu)),
        "DataHoraGeracao": None
        if nsu in comportamento.sem_data_geracao
        else xml_sintetico.data_hora_geracao(nsu).astimezone(FUSO_BRASILIA).isoformat(),
    }
    if tipo == "EVENTO":
        item["TipoEvento"] = "CANCELAMENTO"
    return item


def _envelope(status: str, itens: list[dict[str, object]]) -> dict[str, object]:
    return {
        "StatusProcessamento": status,
        "LoteDFe": itens,
        "Alertas": [],
        "Erros": [],
        "TipoAmbiente": "HOMOLOGACAO",
        "VersaoAplicativo": "duble-1.0",
        "DataHoraProcessamento": xml_sintetico.BASE_GERACAO.isoformat(),
    }


class DubleAdn:
    """Servidor HTTPS com mTLS numa porta efêmera de ``127.0.0.1``; use como gerenciador de contexto."""

    def __init__(self, pki: Pki, comportamento: Comportamento | None = None) -> None:
        self.comportamento = comportamento or Comportamento()
        self.requisicoes: list[Requisicao] = []
        self.max_concorrencia = 0
        self._em_voo = 0
        self._trava = threading.Lock()
        contexto = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        contexto.load_cert_chain(pki.servidor_cert, pki.servidor_chave)
        contexto.load_verify_locations(cafile=pki.ca_pem)
        contexto.verify_mode = ssl.CERT_REQUIRED
        duble = self

        class Servidor(ThreadingHTTPServer):
            daemon_threads = True

            def get_request(self) -> tuple[ssl.SSLSocket, tuple[str, int]]:
                conexao, endereco = self.socket.accept()
                return contexto.wrap_socket(conexao, server_side=True, do_handshake_on_connect=False), endereco

            def handle_error(self, request: object, client_address: object) -> None:
                """Falhas de handshake (cliente sem certificado) são o comportamento esperado; não polui a saída."""

        class Manipulador(BaseHTTPRequestHandler):
            def setup(self) -> None:
                self.request.do_handshake()  # type: ignore[attr-defined]
                super().setup()

            def log_message(self, *_args: object) -> None:
                return

            def do_GET(self) -> None:  # noqa: N802 - nome exigido pelo http.server
                duble._atender(self)

        self._servidor = Servidor(("127.0.0.1", 0), Manipulador)
        self._thread = threading.Thread(target=self._servidor.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        """URL base (``ADN_BASE_URL`` dos testes)."""
        return f"https://127.0.0.1:{self._servidor.server_address[1]}"

    def __enter__(self) -> "DubleAdn":
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._servidor.shutdown()
        self._servidor.server_close()

    def _atender(self, manipulador: BaseHTTPRequestHandler) -> None:
        partes = urlsplit(manipulador.path)
        consulta = parse_qs(partes.query)
        segmentos = partes.path.strip("/").split("/")
        with self._trava:
            self._em_voo += 1
            self.max_concorrencia = max(self.max_concorrencia, self._em_voo)
        try:
            if len(segmentos) != 3 or segmentos[0].lower() != "municipios" or segmentos[1].lower() != "dfe":
                self._responder(manipulador, 404, {"Erros": [{"Codigo": "E0000", "Descricao": "rota"}]})
                return
            nsu = int(segmentos[2])
            lote = consulta.get("lote", ["true"])[0].lower() == "true"
            soquete: ssl.SSLSocket = manipulador.request  # type: ignore[assignment]
            certificado = soquete.getpeercert() or {}
            cn = next((v for rdn in certificado.get("subject", ()) for k, v in rdn if k == "commonName"), None)
            self.requisicoes.append(
                Requisicao(nsu, lote, consulta.get("tipoNSU", [""])[0], cn, soquete.version(), time.monotonic())
            )
            time.sleep(0.002)
            self._responder_nsu(manipulador, nsu, lote)
        finally:
            with self._trava:
                self._em_voo -= 1

    def _responder_nsu(self, manipulador: BaseHTTPRequestHandler, nsu: int, lote: bool) -> None:
        comportamento = self.comportamento
        if comportamento.roteiro:
            forcada = comportamento.roteiro.popleft()
            self._responder(manipulador, forcada.status, forcada.corpo or {})
            return
        proximos = self._candidatos(nsu, lote)
        if not proximos:
            match comportamento.fim_como:
                case "404":
                    self._responder(manipulador, 404, {"Erros": [{"Codigo": "E2020", "Descricao": "nenhum documento"}]})
                case "200_vazio":
                    self._responder(manipulador, 200, _envelope("DOCUMENTOS_LOCALIZADOS", []))
                case "200_status":
                    self._responder(manipulador, 200, _envelope("NENHUM_DOCUMENTO_LOCALIZADO", []))
            return
        self._responder(manipulador, 200, _envelope("DOCUMENTOS_LOCALIZADOS", [item_do_nsu(n, comportamento) for n in proximos]))

    def _candidatos(self, nsu: int, lote: bool) -> list[int]:
        """NSUs devolvidos: a partir do cursor (exclusivo ou inclusivo); no lote, sem as lacunas e até ``lote_max``."""
        comportamento = self.comportamento
        inicio = nsu if comportamento.cursor_inclusivo else nsu + 1
        if not lote:
            primeiro = next((n for n in range(max(inicio, 1), comportamento.total_nsu + 1) if comportamento.existe(n)), None)
            return [] if primeiro is None else [primeiro]
        achados: list[int] = []
        for n in range(max(inicio, 1), comportamento.total_nsu + 1):
            if comportamento.existe(n) and n not in comportamento.lacunas:
                achados.append(n)
                if len(achados) >= comportamento.lote_max:
                    break
        return achados

    @staticmethod
    def _responder(manipulador: BaseHTTPRequestHandler, status: int, corpo: dict[str, object]) -> None:
        dados = json.dumps(corpo).encode()
        manipulador.send_response(status)
        manipulador.send_header("Content-Type", "application/json; charset=utf-8")
        manipulador.send_header("Content-Length", str(len(dados)))
        manipulador.end_headers()
        manipulador.wfile.write(dados)

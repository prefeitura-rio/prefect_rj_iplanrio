"""Cliente HTTP do ADN (distribuição de DF-e para municípios) com mTLS, retry e circuit breaker.

Contrato (manual §1.4 e swagger ``/municipios/DFe/{NSU}``): ``lote=true`` devolve até 50 (manual) ou 100 (swagger)
documentos a partir do cursor; ``lote=false`` devolve um. O manual e o app tratam o NSU informado como **exclusivo**
(documentos com NSU maior); o swagger o descreve como inclusivo. ``AdnConfig.cursor_inclusivo`` escolhe a variante
(padrão: exclusivo, como o app) e ``tamanho_lote_esperado`` o tamanho do lote; o cliente recusa respostas que
contradigam as duas configurações em vez de pular ou repetir NSUs em silêncio.
"""

import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from types import TracebackType
from typing import Self

import httpx

from pipelines.rj_smfp__nota_carioca_adn.utils.adn_certificado import contexto_ssl_cliente
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_decode import DocumentoAdn, ItemAdn
from pipelines.rj_smfp__nota_carioca_adn.utils.adn_errors import (
    AdnErroHttp,
    AdnErroTransporte,
    AdnIndisponivel,
    AdnRejeicao,
    ConfiguracaoAdnInvalida,
    ContratoAdnViolado,
    NenhumDocumentoLocalizado,
    rejeicao_do_codigo,
)
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

STATUS_THROTTLE = frozenset({429, 503})
STATUS_NAO_ENCONTRADO = 404
STATUS_SUCESSO = range(200, 300)
TRECHO_MAXIMO = 200
TIPO_NSU = "DISTRIBUICAO"
CAMINHO_DFE = "/municipios/DFe/{nsu}"
AMBIENTE_PFX = "ADN_PFX_BASE64"
AMBIENTE_SENHA = "ADN_PFX_SENHA"
AMBIENTE_URL = "ADN_BASE_URL"
AMBIENTE_CA = "ADN_CA_BUNDLE"


@dataclass(frozen=True, slots=True)
class AdnConfig:
    """Configuração do cliente do ADN.

    :param base_url: ``https://adn.producaorestrita.nfse.gov.br`` (staging) ou ``https://adn.nfse.gov.br`` (prod).
    :param pfx_base64: PFX do e-CNPJ em base64.
    :param pfx_senha: Senha do PFX.
    :param cursor_inclusivo: ``False`` (padrão, como o app) se o NSU do lote é exclusivo; ``True`` se inclusivo.
    :param tamanho_lote_esperado: Máximo de documentos por lote (50 no manual, 100 no swagger).
    :param ca_bundle: PEM com CAs extras a confiar no servidor (``None`` usa as do sistema).
    :param timeout_s: Tempo limite de cada requisição.
    :param max_tentativas: Tentativas por consulta diante de 429/503.
    :param espera_base_s: Espera antes da segunda tentativa; dobra a cada uma (5, 10, 20, 40 s).
    :param limite_consecutivos: Quantos 429/503 seguidos abrem o circuito.
    :param pausa_circuito_s: Pausa com o circuito aberto (15 min).
    """

    base_url: str
    pfx_base64: str = field(repr=False)
    pfx_senha: str = field(repr=False)
    cursor_inclusivo: bool = False
    tamanho_lote_esperado: int = 50
    ca_bundle: str | None = None
    timeout_s: float = 60.0
    max_tentativas: int = 5
    espera_base_s: float = 5.0
    limite_consecutivos: int = 3
    pausa_circuito_s: float = 900.0

    @classmethod
    def do_ambiente(cls, ambiente: Mapping[str, str] | None = None, **alteracoes: object) -> Self:
        """Lê ``ADN_BASE_URL``, ``ADN_PFX_BASE64`` e ``ADN_PFX_SENHA`` (e, opcional, ``ADN_CA_BUNDLE``).

        :param ambiente: Variáveis de ambiente; o padrão é ``os.environ``.
        :param alteracoes: Campos a trocar depois da leitura (variante do cursor, tamanho do lote...).
        :raises ConfiguracaoAdnInvalida: Se faltar uma das três variáveis obrigatórias.
        """
        variaveis = os.environ if ambiente is None else ambiente

        def obrigatoria(nome: str) -> str:
            valor = variaveis.get(nome, "").strip()
            if not valor:
                raise ConfiguracaoAdnInvalida(f"Variável de ambiente {nome} ausente ou vazia")
            return valor

        configuracao = cls(
            base_url=obrigatoria(AMBIENTE_URL),
            pfx_base64=obrigatoria(AMBIENTE_PFX),
            pfx_senha=obrigatoria(AMBIENTE_SENHA),
            ca_bundle=variaveis.get(AMBIENTE_CA) or None,
        )
        return replace(configuracao, **alteracoes)  # type: ignore[arg-type]


class AdnClient:
    """Consumidor sequencial do ADN: uma requisição por vez, com o certificado do município (mTLS).

    :param config: Configuração.
    :param dormir: Função de espera (``time.sleep``); os testes passam um registrador.
    """

    def __init__(self, config: AdnConfig, *, dormir: Callable[[float], None] = time.sleep) -> None:
        self._config = config
        self._dormir = dormir
        self._throttles_seguidos = 0
        contexto = contexto_ssl_cliente(config.pfx_base64, config.pfx_senha, config.ca_bundle)
        self._http = httpx.Client(
            base_url=config.base_url,
            verify=contexto,
            timeout=config.timeout_s,
            headers={"Accept": "application/json"},
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, tipo: type[BaseException] | None, erro: BaseException | None, rastro: TracebackType | None
    ) -> None:
        self.close()

    def close(self) -> None:
        """Fecha as conexões."""
        self._http.close()

    def consultar(self, nsu: int, lote: bool) -> tuple[ItemAdn, ...]:
        """Consulta ``/municipios/DFe/{nsu}`` exatamente com o NSU dado, sem traduzir cursor.

        :param nsu: Valor do caminho.
        :param lote: ``True`` para ``lote=true``.
        :returns: Itens da resposta, ou ``()`` no fim de dados (404, lista vazia, ``NENHUM_DOCUMENTO_LOCALIZADO`` ou
            E2020/E2040/E2070).
        :raises AdnIndisponivel: Com 429/503 além do tolerado.
        :raises AdnRejeicao: Com rejeição do ADN (E2000, E2001...).
        """
        resposta = self._requisitar(nsu, lote)
        return self._interpretar(resposta)

    def buscar_lote_bruto(self, cursor: int) -> tuple[ItemAdn, ...]:
        """Lote de documentos com NSU maior que ``cursor``, sem decodificar o XML.

        :param cursor: Último NSU já obtido.
        :raises ContratoAdnViolado: Se o lote exceder ``tamanho_lote_esperado`` ou trouxer NSU que não passe do cursor.
        """
        inicio = cursor + 1 if self._config.cursor_inclusivo else cursor
        itens = self.consultar(inicio, lote=True)
        self._validar_lote(itens, cursor)
        return itens

    def buscar_lote(self, cursor: int) -> tuple[DocumentoAdn, ...]:
        """Como ``buscar_lote_bruto``, decodificando todos os XMLs.

        :raises DocumentoIlegivel: No primeiro item que não decodifica (traz o NSU).
        """
        return tuple(item.decodificar() for item in self.buscar_lote_bruto(cursor))

    def buscar_um_bruto(self, nsu: int) -> ItemAdn | None:
        """Item do NSU exato (``lote=false``), ou ``None`` se o ADN não o tiver (devolver outro NSU é ausência)."""
        caminho = nsu if self._config.cursor_inclusivo else max(nsu - 1, 0)
        return next((item for item in self.consultar(caminho, lote=False) if item.nsu == nsu), None)

    def buscar_um(self, nsu: int) -> DocumentoAdn | None:
        """Como ``buscar_um_bruto``, decodificando o XML.

        :raises DocumentoIlegivel: Se o XML do NSU não decodifica.
        """
        item = self.buscar_um_bruto(nsu)
        return None if item is None else item.decodificar()

    def _validar_lote(self, itens: tuple[ItemAdn, ...], cursor: int) -> None:
        if len(itens) > self._config.tamanho_lote_esperado:
            raise ContratoAdnViolado(
                f"O ADN devolveu um lote de {len(itens)} documentos; o esperado é até "
                f"{self._config.tamanho_lote_esperado}. Ajuste tamanho_lote_esperado (50 no manual, 100 no swagger)."
            )
        anterior = cursor
        for item in itens:
            if item.nsu <= anterior:
                raise ContratoAdnViolado(
                    f"O NSU {item.nsu} não passa de {anterior}: a variante do cursor configurada "
                    f"(cursor_inclusivo={self._config.cursor_inclusivo}) não confere com a do ADN."
                )
            anterior = item.nsu

    def _requisitar(self, nsu: int, lote: bool) -> httpx.Response:
        """GET com retry em 429/503 (5/10/20/40 s) e circuit breaker (3 seguidos, 15 min)."""
        config = self._config
        for tentativa in range(1, config.max_tentativas + 1):
            resposta = self._get(nsu, lote)
            if resposta.status_code not in STATUS_THROTTLE:
                self._throttles_seguidos = 0
                return resposta
            self._throttles_seguidos += 1
            logger.warning(
                "ADN respondeu %s (tentativa %d, %d seguidos) ao consultar NSU %d",
                resposta.status_code,
                tentativa,
                self._throttles_seguidos,
                nsu,
            )
            if self._throttles_seguidos >= config.limite_consecutivos:
                self._throttles_seguidos = 0
                logger.error("Circuit breaker do ADN aberto: pausa de %.0f s", config.pausa_circuito_s)
                self._dormir(config.pausa_circuito_s)
                raise AdnIndisponivel(resposta.status_code, config.pausa_circuito_s)
            if tentativa == config.max_tentativas:
                raise AdnIndisponivel(resposta.status_code, 0.0)
            self._dormir(config.espera_base_s * 2 ** (tentativa - 1))
        raise AssertionError("max_tentativas deve ser pelo menos 1")  # pragma: no cover

    def _get(self, nsu: int, lote: bool) -> httpx.Response:
        try:
            return self._http.get(
                CAMINHO_DFE.format(nsu=nsu), params={"tipoNSU": TIPO_NSU, "lote": "true" if lote else "false"}
            )
        except httpx.HTTPError as erro:
            raise AdnErroTransporte(f"Falha ao falar com o ADN: {type(erro).__name__}: {erro}") from erro

    @staticmethod
    def _interpretar(resposta: httpx.Response) -> tuple[ItemAdn, ...]:
        status = resposta.status_code
        corpo = _corpo_json(resposta)
        erros = corpo.get("Erros") if corpo else None
        if isinstance(erros, list) and erros and isinstance(erros[0], dict):
            primeiro = erros[0]
            rejeicao = rejeicao_do_codigo(
                str(primeiro.get("Codigo") or ""), str(primeiro.get("Descricao") or ""), status
            )
            if isinstance(rejeicao, NenhumDocumentoLocalizado):
                return ()
            raise rejeicao
        if corpo and corpo.get("StatusProcessamento") == "REJEICAO":
            raise AdnRejeicao("", "StatusProcessamento REJEICAO sem Erros", status)
        if status == STATUS_NAO_ENCONTRADO:
            return ()
        if status not in STATUS_SUCESSO:
            raise AdnErroHttp(status, resposta.text[:TRECHO_MAXIMO])
        if corpo is None:
            raise ContratoAdnViolado("Resposta de sucesso do ADN sem corpo JSON")
        if corpo.get("StatusProcessamento") == "NENHUM_DOCUMENTO_LOCALIZADO":
            return ()
        lista = corpo.get("LoteDFe") or []
        if not isinstance(lista, list):
            raise ContratoAdnViolado("LoteDFe não é uma lista")
        return tuple(ItemAdn.do_json(item) for item in lista)


def _corpo_json(resposta: httpx.Response) -> dict[str, object] | None:
    try:
        corpo = resposta.json()
    except ValueError:
        return None
    return corpo if isinstance(corpo, dict) else None

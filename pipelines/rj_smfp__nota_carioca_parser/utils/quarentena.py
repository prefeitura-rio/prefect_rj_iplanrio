"""Prioridade da quarentena (P1 a P5), porta de ``ClassificadorPrioridadeQuarentena``."""

import re
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from enum import IntEnum

from lxml import etree

from pipelines.rj_smfp__nota_carioca_parser.constants import IBGE_RIO_DE_JANEIRO
from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import ler_xml, nome_local
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.erros import ErroDocumento


class Prioridade(IntEnum):
    """``ePrioridadeQuarentena``: ordem crescente de urgência de reprocessamento.

    P1 nota com incidência no Rio e ISS maior que zero (impacto em receita); P2 documento ilegível (XML corrompido ou
    sem município/ISS); P3 nota no Rio com ISS zero; P4 nota fora do Rio; P5 evento sem nota correspondente.
    """

    RECEITA = 1
    ILEGIVEL = 2
    RIO_SEM_RECEITA = 3
    FORA_MUNICIPIO = 4
    EVENTO_ORFAO = 5


BuscarXmlNota = Callable[[str], str | None]

_DECIMAL = re.compile(
    r"^\s*(?P<abre>\()?\s*(?P<s1>[+-])?\s*¤?\s*(?P<int>\d+(?:,\d+)*)?(?:\.(?P<frac>\d*))?"
    r"(?:[eE](?P<exp>[+-]?\d+))?\s*(?P<s2>[+-])?\s*(?P<fecha>\))?\s*$"
)


def decimal_invariante(texto: str) -> Decimal | None:
    """``Decimal.TryParse(texto, NumberStyles.Any, CultureInfo.InvariantCulture)``: vírgula é milhar, ponto é decimal.

    Aceita espaços, sinal à frente ou atrás, parênteses (negativo), símbolo de moeda, milhar e expoente.

    :returns: O valor, ou ``None`` se o texto não for um número.
    """
    casou = _DECIMAL.match(texto)
    if casou is None or (casou["int"] is None and not casou["frac"]):
        return None
    if bool(casou["abre"]) != bool(casou["fecha"]) or (casou["s1"] and casou["s2"]):
        return None
    inteiro = (casou["int"] or "0").replace(",", "")
    expoente = f"E{casou['exp']}" if casou["exp"] else ""
    try:
        valor = Decimal(f"{inteiro}.{casou['frac'] or '0'}{expoente}")
    except InvalidOperation:
        return None
    negativo = "-" in {casou["s1"], casou["s2"]} or bool(casou["abre"])
    return -valor if negativo else valor


def _texto_do_primeiro(raiz: etree._Element, nome: str) -> str | None:
    """Texto do primeiro elemento com o nome local, em qualquer profundidade (``//*[local-name()='nome']``)."""
    for no in raiz.iter():
        if isinstance(no.tag, str) and nome_local(no) == nome:
            return "".join(no.itertext())
    return None


def _classificar_nota(xml: str | None) -> Prioridade:
    if xml is None or not xml.strip():
        return Prioridade.ILEGIVEL
    try:
        raiz = ler_xml(xml)
    except ErroDocumento:
        return Prioridade.ILEGIVEL
    municipio = _texto_do_primeiro(raiz, "cLocIncid")
    iss_texto = _texto_do_primeiro(raiz, "vISSQN")
    if municipio is None or not municipio.strip() or iss_texto is None or not iss_texto.strip():
        return Prioridade.ILEGIVEL
    iss = decimal_invariante(iss_texto)
    if iss is None or iss < 0:
        return Prioridade.ILEGIVEL
    if municipio == IBGE_RIO_DE_JANEIRO:
        return Prioridade.RECEITA if iss > 0 else Prioridade.RIO_SEM_RECEITA
    return Prioridade.FORA_MUNICIPIO


def classificar_quarentena(
    tipo: TipoDocumento | None,
    xml: str | None,
    chave_acesso: str | None,
    buscar_xml_nota: BuscarXmlNota,
) -> Prioridade:
    """Prioridade P1 a P5 de um documento que vai para a quarentena.

    Notas são classificadas pelo município de incidência (``cLocIncid``) e pelo ISS (``vISSQN``) do próprio XML; eventos
    herdam a prioridade da nota de mesma chave de acesso (principal ou, na falta, quarentena), e viram P5 sem nota.
    Qualquer outro tipo (inclusive desconhecido) é ilegível.

    :param tipo: Tipo do documento, ``None`` se desconhecido.
    :param xml: XML sanitizado.
    :param chave_acesso: Chave de acesso do evento.
    :param buscar_xml_nota: Devolve o XML da NFS-e com a chave dada (ou ``None``).
    """
    if tipo is TipoDocumento.NFSE:
        return _classificar_nota(xml)
    if tipo is TipoDocumento.EVENTO:
        if not chave_acesso or not chave_acesso.strip():
            return Prioridade.EVENTO_ORFAO
        xml_nota = buscar_xml_nota(chave_acesso)
        return Prioridade.EVENTO_ORFAO if xml_nota is None else _classificar_nota(xml_nota)
    return Prioridade.ILEGIVEL

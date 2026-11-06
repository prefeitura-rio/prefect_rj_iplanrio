"""Erros de esquema do libxml2 em forma estruturada, para as regras de tolerância decidirem sem casar texto solto.

O .NET decide a tolerância casando trechos da mensagem do ``XmlSchemaSet`` (em português e em inglês). O libxml2 tem
outras mensagens e não cita o tipo simples do XSD, então cada erro é decomposto em elemento, pai, atributo, faceta,
valor e lista de elementos esperados, que é o que as regras portadas usam.
"""

import re
from dataclasses import dataclass, field
from enum import StrEnum

from lxml import etree

_MENSAGEM = re.compile(r"^Element '(?:\{[^}]*\})?([^']+)'(?:, attribute '([^']+)')?: (.*)$", re.DOTALL)
_FACETA = re.compile(r"^\[facet '([^']+)'\] (.*)$", re.DOTALL)
_VALOR = re.compile(r"The value '(.*?)' (?:is not accepted|is not an element|has a length|does not match)", re.DOTALL)
_TAMANHO = re.compile(r"has a length of '(\d+)'")
_PADRAO = re.compile(r"is not accepted by the pattern '(.*)'\.$", re.DOTALL)
_ESPERADOS = re.compile(r"Expected is (?:one of )?\( (.*) \)\.$", re.DOTALL)
_NOME_NS = re.compile(r"\{[^}]*\}([\w.\-]+)")
CODIGO_RUIDO = "SCHEMAV_CVC_IDC"


class TipoErro(StrEnum):
    """Família do erro, o que as regras de tolerância precisam distinguir."""

    FACETA = "faceta"
    ATRIBUTO_FIXO = "atributo_fixo"
    ATRIBUTO_AUSENTE = "atributo_ausente"
    INESPERADO = "inesperado"
    INCOMPLETO = "incompleto"
    NAO_NULAVEL = "nao_nulavel"
    OUTRO = "outro"


@dataclass(frozen=True, slots=True)
class ErroXsd:
    """Um erro de validação do libxml2 decomposto.

    :param codigo: ``type_name`` do libxml2 (``SCHEMAV_CVC_PATTERN_VALID``...).
    :param mensagem: Mensagem original, para o registro na quarentena.
    :param tipo: Família do erro.
    :param elemento: Nome local do elemento citado (para ``INESPERADO``, o filho que não era esperado).
    :param pai: Nome local do pai do elemento citado ('' na raiz).
    :param atributo: Atributo, quando o erro é de valor de atributo.
    :param faceta: ``pattern``, ``minLength``, ``maxLength``, ``length``, ``enumeration``...
    :param valor: Valor recusado.
    :param padrao: Texto da expressão regular, quando a faceta é ``pattern``.
    :param esperados: Nomes locais que o esquema esperava no lugar.
    :param no: Nó do documento onde o erro ocorreu (fora da comparação).
    """

    codigo: str
    mensagem: str
    tipo: TipoErro
    elemento: str
    pai: str = ""
    atributo: str | None = None
    faceta: str | None = None
    valor: str | None = None
    padrao: str | None = None
    esperados: tuple[str, ...] = ()
    no: etree._Element | None = field(default=None, compare=False, repr=False)

    @property
    def container(self) -> str:
        """Elemento cujo conteúdo está errado: o pai do filho inesperado, ou o próprio elemento incompleto."""
        return self.pai if self.tipo is TipoErro.INESPERADO else self.elemento

    def __str__(self) -> str:
        return self.mensagem


def eh_ruido(codigo: str, mensagem: str) -> bool:
    """O libxml2 acrescenta um aviso ``Warning: No precomputed value`` depois de um valor de atributo recusado.

    O erro de verdade já está no log; o aviso não existe no .NET e é descartado.
    """
    return codigo == CODIGO_RUIDO and mensagem.split(": ", 1)[-1].startswith("Warning:")


def normalizar_erro(codigo: str, mensagem: str, caminho_nomes: tuple[str, ...] = ()) -> ErroXsd:
    """Decompõe a mensagem do libxml2.

    :param codigo: ``type_name`` da entrada do log.
    :param mensagem: ``message`` da entrada do log.
    :param caminho_nomes: Nomes locais da raiz até o nó do erro (o último é o próprio nó).
    :returns: O erro estruturado.
    """
    pai = caminho_nomes[-2] if len(caminho_nomes) > 1 else ""
    casou = _MENSAGEM.match(mensagem)
    if casou is None:
        return ErroXsd(codigo, mensagem, TipoErro.OUTRO, caminho_nomes[-1] if caminho_nomes else "", pai)
    elemento, atributo, resto = casou.group(1), casou.group(2), casou.group(3)
    base = {"codigo": codigo, "mensagem": mensagem, "elemento": elemento, "pai": pai, "atributo": atributo}
    faceta = _FACETA.match(resto)
    if faceta is not None:
        valor = _VALOR.search(faceta.group(2))
        padrao = _PADRAO.search(faceta.group(2))
        tamanho = _TAMANHO.search(faceta.group(2))
        # A mensagem de tamanho do libxml2 não repete o valor; com tamanho 0 sabemos que o elemento está vazio.
        vazio = tamanho is not None and tamanho.group(1) == "0"
        return ErroXsd(
            **base,
            tipo=TipoErro.FACETA,
            faceta=faceta.group(1),
            valor=valor.group(1) if valor else ("" if vazio else None),
            padrao=padrao.group(1) if padrao else None,
        )
    esperados = tuple(_NOME_NS.findall(_ESPERADOS.search(resto).group(1))) if _ESPERADOS.search(resto) else ()
    if "does not match the fixed value constraint" in resto:
        valor = _VALOR.search(resto)
        return ErroXsd(**base, tipo=TipoErro.ATRIBUTO_FIXO, faceta="fixed", valor=valor.group(1) if valor else None)
    for marca, tipo in (
        ("This element is not expected.", TipoErro.INESPERADO),
        ("Missing child element(s).", TipoErro.INCOMPLETO),
        ("is required but missing", TipoErro.ATRIBUTO_AUSENTE),
        ("is not 'nillable'", TipoErro.NAO_NULAVEL),
    ):
        if marca in resto:
            return ErroXsd(**base, tipo=tipo, esperados=esperados)
    return ErroXsd(**base, tipo=TipoErro.OUTRO)

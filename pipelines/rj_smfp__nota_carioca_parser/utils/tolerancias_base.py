"""Predicados comuns às regras de tolerância portadas de ``TolerantValidationHandler``/``ValidationRule`` (.NET).

Cada regra é uma função ``ErroXsd -> bool`` que devolve ``True`` quando o erro deve ser ignorado. O .NET casa trechos
da mensagem (PT e EN) do ``XmlSchemaSet``; aqui o mesmo critério é expresso sobre o erro decomposto do libxml2.
"""

from collections.abc import Callable

from pipelines.rj_smfp__nota_carioca_parser.utils.erros_xsd import ErroXsd, TipoErro

Regra = Callable[[ErroXsd], bool]

FACETAS_DE_TAMANHO = frozenset({"minLength", "maxLength", "length"})
# Padrão do TSDec15V2 (valores monetários) no XSD oficial: não aceita sinal negativo.
PADRAO_TSDEC15V2 = r"0|0\.[0-9]{2}|[1-9]{1}[0-9]{0,14}(\.[0-9]{2})?"


def padrao_invalido(e: ErroXsd, *elementos: str) -> bool:
    """Valor de um dos elementos recusado pela faceta ``pattern``."""
    return e.tipo is TipoErro.FACETA and e.faceta == "pattern" and e.atributo is None and e.elemento in elementos


def valor_vazio(e: ErroXsd, elemento: str) -> bool:
    """Elemento vazio recusado por ``pattern`` ou ``minLength``."""
    return (
        e.tipo is TipoErro.FACETA
        and e.atributo is None
        and e.elemento == elemento
        and e.valor == ""
        and e.faceta in {"pattern", "minLength"}
    )


def tamanho_ou_vazio(e: ErroXsd, elemento: str) -> bool:
    """Elemento recusado por tamanho (``MinLength``/``MaxLength``) ou vazio."""
    return (
        e.tipo is TipoErro.FACETA
        and e.atributo is None
        and e.elemento == elemento
        and (e.faceta in FACETAS_DE_TAMANHO or e.valor == "")
    )


def atributo_invalido(e: ErroXsd, elemento: str | None, atributo: str, *facetas: str) -> bool:
    """Valor do atributo recusado por uma das facetas; ``elemento`` ``None`` aceita qualquer elemento."""
    return (
        e.tipo is TipoErro.FACETA
        and e.atributo == atributo
        and e.faceta in facetas
        and (elemento is None or e.elemento == elemento)
    )


def filho_inesperado(e: ErroXsd, pai: str, filho: str, *esperando: str) -> bool:
    """O ``filho`` apareceu em ``pai`` fora do lugar; se ``esperando`` vier, o esquema esperava exatamente isso."""
    if e.tipo is not TipoErro.INESPERADO or e.pai != pai or e.elemento != filho:
        return False
    return not esperando or e.esperados == esperando


def cita(e: ErroXsd, container: str, *nomes: str) -> bool:
    """Erro de conteúdo (filho inesperado ou conteúdo incompleto) de ``container`` que cita um dos ``nomes``.

    Espelha as regras do .NET que só exigem o nome do elemento e dos filhos na mensagem.
    """
    if e.tipo not in {TipoErro.INESPERADO, TipoErro.INCOMPLETO} or e.container != container:
        return False
    return e.elemento in nomes or bool(set(nomes) & set(e.esperados))


def inesperado_cita(e: ErroXsd, pai: str, *nomes: str) -> bool:
    """Como :func:`cita`, só para filho inesperado."""
    return e.tipo is TipoErro.INESPERADO and cita(e, pai, *nomes)


def incompleto_esperando(e: ErroXsd, container: str, *esperando: str) -> bool:
    """``container`` está incompleto e o esquema esperava algum dos ``esperando``."""
    return e.tipo is TipoErro.INCOMPLETO and e.elemento == container and bool(set(esperando) & set(e.esperados))

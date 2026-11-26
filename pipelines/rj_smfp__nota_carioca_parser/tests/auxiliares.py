"""Auxiliares dos testes das regras de tolerância: erros brutos do libxml2 e erros sintéticos no formato dele."""

from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import ler_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.erros_xsd import ErroXsd, normalizar_erro
from pipelines.rj_smfp__nota_carioca_parser.utils.sanitizar import sanitizar_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias_base import Regra
from pipelines.rj_smfp__nota_carioca_parser.utils.versao import Versao
from pipelines.rj_smfp__nota_carioca_parser.utils.xsd import coletar_erros_xsd

NS = "{http://www.sped.fazenda.gov.br/nfse}"


def erros_brutos(xml: str, versao_xsd: str, tipo: TipoDocumento = TipoDocumento.NFSE) -> list[ErroXsd]:
    """Todos os erros do libxml2 para o documento, sem tolerância, menos o da ``serie`` (compensado à parte)."""
    raiz = ler_xml(sanitizar_xml(xml) or "")
    erros = coletar_erros_xsd(raiz, Versao.ler(versao_xsd), tipo)
    return [e for e in erros if e.elemento != "serie"]


def assert_regra_ignora(regra: Regra, xml: str, versao_xsd: str, tipo: TipoDocumento = TipoDocumento.NFSE) -> None:
    """O documento tem erro e a regra, sozinha, ignora todos os que existem."""
    erros = erros_brutos(xml, versao_xsd, tipo)
    assert erros, "o defeito do teste não gerou nenhum erro de XSD"
    recusados = [str(e) for e in erros if not regra(e)]
    assert not recusados, f"{regra.__name__} não ignorou: {recusados}"


def inesperado(elemento: str, pai: str, *esperados: str) -> ErroXsd:
    """Erro de filho inesperado no formato do libxml2."""
    lista = ""
    if esperados:
        nomes = ", ".join(NS + n for n in esperados)
        lista = f" Expected is ( {nomes} )." if len(esperados) == 1 else f" Expected is one of ( {nomes} )."
    mensagem = f"Element '{NS}{elemento}': This element is not expected.{lista}"
    return normalizar_erro("SCHEMAV_ELEMENT_CONTENT", mensagem, ("NFSe", pai, elemento))


def incompleto(elemento: str, *esperados: str) -> ErroXsd:
    """Erro de conteúdo incompleto no formato do libxml2."""
    nomes = ", ".join(NS + n for n in esperados)
    lista = f"( {nomes} )" if len(esperados) == 1 else f"one of ( {nomes} )"
    mensagem = f"Element '{NS}{elemento}': Missing child element(s). Expected is {lista}."
    return normalizar_erro("SCHEMAV_ELEMENT_CONTENT", mensagem, ("NFSe", elemento))


def faceta(elemento: str, faceta_: str, valor: str, pai: str = "emit", atributo: str | None = None) -> ErroXsd:
    """Erro de faceta (``enumeration``, ``pattern``...) no formato do libxml2."""
    alvo = f"Element '{NS}{elemento}'" + (f", attribute '{atributo}'" if atributo else "")
    mensagem = f"{alvo}: [facet '{faceta_}'] The value '{valor}' is not an element of the set {{'1'}}."
    if faceta_ == "pattern":
        mensagem = f"{alvo}: [facet 'pattern'] The value '{valor}' is not accepted by the pattern '[0-9]+'."
    return normalizar_erro("SCHEMAV_CVC_PATTERN_VALID", mensagem, ("NFSe", pai, elemento))

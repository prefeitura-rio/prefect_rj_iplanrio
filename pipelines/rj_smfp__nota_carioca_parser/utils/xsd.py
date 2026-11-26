"""Esquemas XSD oficiais vendorizados e a coleta dos erros de validação."""

from dataclasses import replace
from functools import lru_cache
from pathlib import Path

from lxml import etree

from pipelines.rj_smfp__nota_carioca_parser.constants import SCHEMAS_DIR
from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import nome_local
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.erros import XsdInexistente
from pipelines.rj_smfp__nota_carioca_parser.utils.erros_xsd import ErroXsd, TipoErro, eh_ruido, normalizar_erro
from pipelines.rj_smfp__nota_carioca_parser.utils.versao import Versao

VERSAO_CNC = Versao(1, 0)


def arquivo_xsd(versao: Versao, tipo: TipoDocumento, base: Path = SCHEMAS_DIR) -> Path:
    """Caminho do XSD raiz do tipo; o CNC usa sempre a v1.00, como no .NET (``GetSchemaPaths``).

    :raises XsdInexistente: Se a pasta da versão não existir.
    """
    if tipo is TipoDocumento.CNC:
        versao = VERSAO_CNC
    pasta = base / versao.pasta
    if not pasta.is_dir():
        raise XsdInexistente(versao.pasta)
    nome = {TipoDocumento.NFSE: "NFSe", TipoDocumento.EVENTO: "evento", TipoDocumento.CNC: "CNC"}[tipo]
    sufixo = "v1.00" if tipo is TipoDocumento.CNC else f"v{versao.pasta}"
    return pasta / f"{nome}_{sufixo}.xsd"


@lru_cache(maxsize=16)
def carregar_schema(versao: Versao, tipo: TipoDocumento) -> etree.XMLSchema:
    """Compila (uma vez por processo) o XSD da versão e do tipo.

    :raises XsdInexistente: Se a versão não tiver XSD.
    """
    return etree.XMLSchema(etree.parse(str(arquivo_xsd(versao, tipo))))


def _nomes_do_caminho(raiz: etree._Element, caminho: str) -> tuple[etree._Element | None, tuple[str, ...]]:
    """Resolve o XPath posicional do libxml2 e devolve o nó e os nomes locais da raiz até ele."""
    encontrados = raiz.getroottree().xpath(caminho) if caminho else []
    if not encontrados or not isinstance(encontrados[0], etree._Element):
        return None, ()
    no = encontrados[0]
    nomes = [nome_local(a) for a in reversed(list(no.iterancestors()))] + [nome_local(no)]
    return no, tuple(nomes)


def colapsar_facetas(erros: list[ErroXsd]) -> list[ErroXsd]:
    """Mantém só o erro de ``pattern`` quando o mesmo valor também falha em tamanho ou enumeração.

    O .NET confere as facetas léxicas (``pattern``) antes das de valor (tamanho, enumeração) e para na primeira que
    falha: um valor gera no máximo um erro. O libxml2 reporta todas; sem este colapso as regras de ``pattern`` portadas
    não cobririam o erro de tamanho do mesmo valor.
    """
    com_padrao = {
        (id(e.no), e.atributo)
        for e in erros
        if e.tipo is TipoErro.FACETA and e.faceta == "pattern" and e.no is not None
    }
    return [
        e
        for e in erros
        if not (e.tipo is TipoErro.FACETA and e.faceta != "pattern" and (id(e.no), e.atributo) in com_padrao)
    ]


def coletar_erros_xsd(raiz: etree._Element, versao: Versao, tipo: TipoDocumento) -> list[ErroXsd]:
    """Valida a árvore contra o XSD e devolve todos os erros, sem aplicar tolerância.

    :param raiz: Elemento raiz do documento.
    :param versao: Versão do XSD a usar.
    :param tipo: Tipo do documento.
    :raises XsdInexistente: Se a versão não tiver XSD.
    """
    schema = carregar_schema(versao, tipo)
    if schema.validate(raiz):
        return []
    erros: list[ErroXsd] = []
    for entrada in schema.error_log:
        if eh_ruido(entrada.type_name, entrada.message):
            continue
        no, nomes = _nomes_do_caminho(raiz, entrada.path)
        erro = normalizar_erro(entrada.type_name, entrada.message, nomes)
        erros.append(replace(erro, no=no))
    return colapsar_facetas(erros)

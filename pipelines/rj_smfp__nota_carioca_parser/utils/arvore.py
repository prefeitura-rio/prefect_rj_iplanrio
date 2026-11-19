"""Leitura segura do XML e navegação por nome local, independente de namespace."""

from lxml import etree

from pipelines.rj_smfp__nota_carioca_parser.utils.erros import XmlMalformado

# Documentos vêm de fora: sem resolver entidades, sem rede e sem carregar DTD. O encoding é sempre UTF-8 (a declaração
# do XML é ignorada): o texto já foi decodificado pelo extrator.
PARSER_SEGURO = etree.XMLParser(
    resolve_entities=False,
    no_network=True,
    load_dtd=False,
    dtd_validation=False,
    remove_comments=True,
    remove_pis=True,
    huge_tree=False,
    encoding="utf-8",
)


def ler_xml(xml: str) -> etree._Element:
    """Lê o XML (já sanitizado) e devolve o elemento raiz.

    :raises XmlMalformado: Se o XML não for bem formado.
    """
    try:
        return etree.fromstring(xml.encode("utf-8"), parser=PARSER_SEGURO)
    except etree.XMLSyntaxError as erro:
        raise XmlMalformado(str(erro)) from erro


def nome_local(no: etree._Element) -> str:
    """Nome do elemento sem namespace."""
    return etree.QName(no).localname


def filho(no: etree._Element | None, nome: str) -> etree._Element | None:
    """Primeiro filho direto com o nome local dado, ou ``None``."""
    if no is None:
        return None
    for candidato in no:
        if isinstance(candidato.tag, str) and nome_local(candidato) == nome:
            return candidato
    return None


def caminho(no: etree._Element | None, *nomes: str) -> etree._Element | None:
    """Desce pelos nomes locais a partir de ``no``; ``None`` se algum degrau faltar."""
    for nome in nomes:
        no = filho(no, nome)
        if no is None:
            return None
    return no


def filhos(no: etree._Element | None, nome: str) -> list[etree._Element]:
    """Todos os filhos diretos com o nome local dado."""
    if no is None:
        return []
    return [c for c in no if isinstance(c.tag, str) and nome_local(c) == nome]

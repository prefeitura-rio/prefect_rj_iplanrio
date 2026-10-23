"""Validação de um documento: sanitização, versão, XSD oficial e tolerâncias.

Porta de ``DFeFactory.Criar`` e ``DFe.Validar`` (``DFe.vb``/``DFeFactory.vb``): o que passa só por tolerância é
registrado em ``toleradas``; o que sobra de erro vira ``XsdInvalido`` e o documento segue para a quarentena.
"""

import copy
from collections.abc import Callable
from dataclasses import dataclass

from lxml import etree

from pipelines.rj_smfp__nota_carioca_parser.constants import MAX_RODADAS_VALIDACAO
from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import ler_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.erros import XmlVazio, XsdInvalido
from pipelines.rj_smfp__nota_carioca_parser.utils.erros_xsd import ErroXsd, TipoErro
from pipelines.rj_smfp__nota_carioca_parser.utils.sanitizar import sanitizar_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias import regras_para, tolera
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias_base import Regra
from pipelines.rj_smfp__nota_carioca_parser.utils.versao import Versao, VersaoDocumento, detectar_versao_arvore
from pipelines.rj_smfp__nota_carioca_parser.utils.xsd import coletar_erros_xsd

RegrasPorVersao = Callable[[Versao], tuple[Regra, ...]]


@dataclass(frozen=True, slots=True)
class DocumentoValidado:
    """Resultado de uma validação bem-sucedida.

    :param versao: Versão declarada e versão da estrutura (promoção de 1.00 para 1.01).
    :param raiz: Elemento raiz do XML original (com os nós tolerados), pronto para a extração.
    :param xml: XML sanitizado.
    :param toleradas: Erros de XSD ignorados pelas regras de tolerância.
    """

    versao: VersaoDocumento
    raiz: etree._Element
    xml: str
    toleradas: tuple[ErroXsd, ...]


def regras_do_documento(versao: VersaoDocumento, por_versao: RegrasPorVersao = regras_para) -> tuple[Regra, ...]:
    """Regras aplicáveis: as da versão declarada (como no .NET) mais as da versão do XSD usado, se for outra."""
    regras = por_versao(versao.declarada)
    if versao.modelo != versao.declarada:
        regras += tuple(r for r in por_versao(versao.modelo) if r not in regras)
    return regras


def _sem_nos_inesperados(raiz: etree._Element, erros: list[ErroXsd]) -> etree._Element | None:
    """Copia a árvore sem os filhos inesperados (já tolerados); ``None`` se não houver o que remover."""
    caminhos = [e.no.getroottree().getpath(e.no) for e in erros if e.tipo is TipoErro.INESPERADO and e.no is not None]
    if not caminhos:
        return None
    copia = copy.deepcopy(raiz)
    for caminho in caminhos:
        for no in copia.getroottree().xpath(caminho):
            no.getparent().remove(no)
    return copia


def validar_documento(
    xml: str | None,
    tipo: TipoDocumento,
    por_versao: RegrasPorVersao = regras_para,
) -> DocumentoValidado:
    """Sanitiza, detecta a versão, valida contra o XSD e aplica as tolerâncias.

    O XSD usado é o da versão promovida (um ``1.00`` com a estrutura da ``1.01`` é validado pela 1.01). Como o libxml2
    para no primeiro erro estrutural de um elemento, os filhos inesperados tolerados são removidos de uma cópia e o
    documento é validado de novo, até não sobrarem erros ocultos (no máximo ``MAX_RODADAS_VALIDACAO`` rodadas).

    :param xml: XML original, como veio do ADN.
    :param tipo: Tipo do documento.
    :param por_versao: Regras de tolerância por versão (injetável para reavaliar com um conjunto novo).
    :returns: O documento validado.
    :raises ErroDocumento: Qualquer subclasse (vazio, malformado, versão, XSD inexistente ou inválido).
    """
    limpo = sanitizar_xml(xml)
    if limpo is None or not limpo.strip():
        raise XmlVazio
    raiz = ler_xml(limpo)
    versao = detectar_versao_arvore(raiz, tipo)
    regras = regras_do_documento(versao, por_versao)
    toleradas: list[ErroXsd] = []
    trabalho = raiz
    for _ in range(MAX_RODADAS_VALIDACAO):
        erros = coletar_erros_xsd(trabalho, versao.modelo, tipo)
        if not erros:
            break
        fatais = [e for e in erros if not tolera(e, regras)]
        if fatais:
            raise XsdInvalido(fatais)
        toleradas.extend(erros)
        trabalho = _sem_nos_inesperados(trabalho, erros)
        if trabalho is None:
            break
    return DocumentoValidado(versao, raiz, limpo, tuple(toleradas))

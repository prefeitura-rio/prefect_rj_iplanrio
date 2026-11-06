"""Versão do documento: leitura do atributo ``versao`` e promoção estrutural de 1.00 para 1.01.

Porta de ``DFeFactory.Criar`` e ``DFeFactory.DetectModelVersion`` (``DFeFactory.vb``): o governo envia XMLs com
``versao="1.00"`` e a estrutura da 1.01 (``IBSCBS`` em ``infNFSe``, ``nDFSe`` em ``infEvento``...).
"""

import re
from dataclasses import dataclass

from lxml import etree

from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import nome_local
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.erros import VersaoIlegivel, VersaoNaoSuportada

# ``Version.Parse`` aceita de 2 (major.minor) a 4 componentes (major.minor.build.revision).
COMPONENTES_MIN = 2
COMPONENTES_MAX = 4


@dataclass(frozen=True, order=True, slots=True)
class Versao:
    """``System.Version`` reduzido a major.minor (``"1.01"`` é 1.1, como no .NET)."""

    major: int
    minor: int

    @classmethod
    def ler(cls, texto: str | None) -> "Versao":
        """Interpreta ``major.minor[.build[.revision]]`` como ``Version.Parse``.

        :raises VersaoIlegivel: Se o texto estiver vazio, tiver menos de dois componentes ou não for numérico.
        """
        if texto is None or not texto.strip():
            raise VersaoIlegivel(texto)
        partes = texto.strip().split(".")
        if not COMPONENTES_MIN <= len(partes) <= COMPONENTES_MAX or not all(re.fullmatch(r"\+?\d+", p) for p in partes):
            raise VersaoIlegivel(texto)
        return cls(int(partes[0]), int(partes[1]))

    @property
    def pasta(self) -> str:
        """Nome da pasta do XSD, ``{major}.{minor:02d}`` (``1.00``, ``1.01``)."""
        return f"{self.major}.{self.minor:02d}"


VERSAO_MAXIMA = Versao(1, 1)
VERSAO_1_01 = Versao(1, 1)


@dataclass(frozen=True, slots=True)
class VersaoDocumento:
    """``declarada`` é a do atributo ``versao``; ``modelo`` é a da estrutura detectada (após a promoção)."""

    declarada: Versao
    modelo: Versao

    @property
    def promovida(self) -> bool:
        """Verdadeiro quando o XML declara 1.00 mas tem a estrutura da 1.01."""
        return self.modelo > self.declarada


def detectar_versao_arvore(raiz: etree._Element, tipo: TipoDocumento) -> VersaoDocumento:
    """Lê a versão declarada na raiz e promove 1.00 para 1.01 quando a estrutura for a da 1.01.

    :raises VersaoIlegivel: Se ``versao`` faltar ou não for uma versão.
    :raises VersaoNaoSuportada: Se a versão declarada for maior que 1.01.
    """
    texto = raiz.get("versao")
    declarada = Versao.ler(texto)
    if declarada > VERSAO_MAXIMA:
        raise VersaoNaoSuportada((texto or "").strip(), f"{VERSAO_MAXIMA.major}.{VERSAO_MAXIMA.minor}")
    return VersaoDocumento(declarada, _versao_do_modelo(raiz, tipo, declarada))


def _versao_do_modelo(raiz: etree._Element, tipo: TipoDocumento, declarada: Versao) -> Versao:
    """Porta literal de ``DetectModelVersion``, inclusive a profundidade em que cada marca é procurada."""
    if declarada >= VERSAO_1_01 or tipo not in {TipoDocumento.NFSE, TipoDocumento.EVENTO}:
        return declarada
    marcas = _MarcasEstrutura(tipo)
    profundidade = -1
    for evento, no in etree.iterwalk(raiz, events=("start", "end")):
        if evento == "end":
            profundidade -= 1
            marcas.fechou(profundidade + 1)
            continue
        profundidade += 1
        marcas.abriu(nome_local(no), profundidade)
        if marcas.indica_1_01:
            return VERSAO_1_01
    return declarada


class _MarcasEstrutura:
    """Estado da varredura: onde está ``infEvento``/``infNFSe`` e o que já foi visto dentro deles."""

    def __init__(self, tipo: TipoDocumento) -> None:
        self.evento = tipo is TipoDocumento.EVENTO
        self.prof_inf_evento = -1
        self.prof_inf_nfse = -1
        self.ndfse = False
        self.inf_ped_reg = False
        self.n_ped_reg_evento = False
        self.ibscbs = False

    def fechou(self, profundidade: int) -> None:
        if profundidade == self.prof_inf_evento:
            self.prof_inf_evento = -1
        if profundidade == self.prof_inf_nfse:
            self.prof_inf_nfse = -1

    def abriu(self, nome: str, profundidade: int) -> None:
        if self.evento and self.prof_inf_evento >= 0 and profundidade > self.prof_inf_evento:
            if profundidade == self.prof_inf_evento + 1:
                self.ndfse = self.ndfse or nome == "nDFSe"
                self.inf_ped_reg = self.inf_ped_reg or nome == "infPedReg"
            self.n_ped_reg_evento = self.n_ped_reg_evento or nome == "nPedRegEvento"
        if not self.evento and self.prof_inf_nfse >= 0 and profundidade == self.prof_inf_nfse + 1 and nome == "IBSCBS":
            self.ibscbs = True
        if nome == "infEvento" and self.evento:
            self.prof_inf_evento = profundidade
        elif nome == "infNFSe" and not self.evento:
            self.prof_inf_nfse = profundidade

    @property
    def indica_1_01(self) -> bool:
        if self.evento:
            return self.ndfse or (self.inf_ped_reg and not self.n_ped_reg_evento)
        return self.ibscbs

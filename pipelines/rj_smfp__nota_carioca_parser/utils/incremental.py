"""Processamento incremental por NSU: lê o índice acima da marca, processa, carrega e só então avança a marca.

Garantias:

* a marca (``parser_marca``) só avança depois de todos os loads do lote (nfse, dps, pessoas, eventos e quarentena);
  ela vale para qualquer versão do parser: uma versão nova continua da marca anterior e só reavalia a quarentena;
* o reprocessamento depois de uma falha não duplica: cada load leva um ``lote_id`` determinístico (versão, faixa de
  NSU e resumo dos NSUs), e a carga de um ``lote_id`` já feito não faz nada (o adaptador usa o id do load job);
* nenhum documento some: o que não passa vira linha de ``quarentena`` com a prioridade P1 a P5;
* não há deduplicação aqui (``NSU``, ``NOTA_NACIONAL``, ``DPS`` e ``EVENTO`` repetidos são a R-04, no dbt).

Limitação conhecida: a marca é um NSU máximo. Um NSU abaixo da marca que o extrator só preencha depois (lacuna) não é
visto por esta versão do parser; reprocesse a faixa com uma ``versao_parser`` nova ou rebaixando a marca.
"""

import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from pipelines.rj_smfp__nota_carioca_parser.utils.avaliacao import Reprovado, linha_quarentena
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import Documento
from pipelines.rj_smfp__nota_carioca_parser.utils.entradas import ArmazemRaw, EntradaIndice, LeitorObjetos
from pipelines.rj_smfp__nota_carioca_parser.utils.erros import XmlVazio
from pipelines.rj_smfp__nota_carioca_parser.utils.processamento import (
    Contexto,
    ResultadoLote,
    montar_documento,
    processar_documento,
)
from pipelines.rj_smfp__nota_carioca_parser.utils.quarentena import Prioridade
from pipelines.rj_smfp__nota_carioca_parser.utils.tabelas import ORDEM_DE_CARGA
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias import regras_para
from pipelines.rj_smfp__nota_carioca_parser.utils.validar import RegrasPorVersao
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)
NSU_AUSENTE_DO_INDICE = "NSU em quarentena não encontrado no índice documentos."


@dataclass(frozen=True, slots=True)
class ConfigIncremental:
    """Parâmetros de uma execução.

    :param versao_parser: Versão do parser: a marca é por versão e a quarentena é reavaliada quando ela muda.
    :param nsus_por_lote: Quantidade de entradas do índice lidas por lote.
    :param max_lotes: Máximo de lotes na execução (``None`` processa até esgotar o índice).
    :param reavaliar_quarentena: Reavalia antes a quarentena de outra versão (devolução).
    :param quarentena_por_execucao: Máximo de documentos reavaliados por execução.
    """

    versao_parser: str
    nsus_por_lote: int = 10_000
    max_lotes: int | None = None
    reavaliar_quarentena: bool = True
    quarentena_por_execucao: int = 5_000


@dataclass(slots=True)
class Resumo:
    """O que a execução fez."""

    lotes: int = 0
    documentos: int = 0
    aprovados: int = 0
    recusados: int = 0
    devolvidos: int = 0
    cnc: int = 0
    ultimo_nsu: int = 0
    linhas_por_tabela: dict[str, int] | None = None

    def somar(self, lote: ResultadoLote) -> None:
        self.documentos += lote.documentos
        self.aprovados += lote.aprovados
        self.recusados += lote.recusados
        self.devolvidos += lote.devolvidos
        self.cnc += lote.cnc
        totais = self.linhas_por_tabela or {}
        for tabela, linhas in lote.linhas.items():
            totais[tabela] = totais.get(tabela, 0) + len(linhas)
        self.linhas_por_tabela = totais


class ObjetosLidos:
    """Lê cada objeto do GCS uma vez por execução e resolve a NFS-e de um evento pela chave de acesso."""

    def __init__(self, armazem: ArmazemRaw, leitor: LeitorObjetos) -> None:
        self.armazem = armazem
        self.leitor = leitor
        self.objetos: dict[str, Mapping[int, Mapping[str, object]]] = {}
        self.notas: dict[str, str | None] = {}

    def linhas(self, gcs_uri: str) -> Mapping[int, Mapping[str, object]]:
        if gcs_uri not in self.objetos:
            self.objetos[gcs_uri] = self.leitor.ler_linhas(gcs_uri)
        return self.objetos[gcs_uri]

    def esquecer(self) -> None:
        """Libera os objetos lidos (um lote termina, o próximo lê os seus)."""
        self.objetos.clear()

    def xml_da_nota(self, chave_acesso: str) -> str | None:
        """XML da NFS-e com a chave (do índice e do GCS); ``None`` se o índice não a conhece."""
        if chave_acesso not in self.notas:
            entrada = self.armazem.buscar_nfse_por_chave(chave_acesso)
            linha = None if entrada is None else self.linhas(entrada.gcs_uri).get(entrada.nsu)
            xml = None if linha is None else linha.get("xml")
            self.notas[chave_acesso] = xml if isinstance(xml, str) else None
        return self.notas[chave_acesso]


def identificador_do_lote(versao_parser: str, nsus: Sequence[int], prefixo: str = "") -> str:
    """``lote_id`` determinístico: o mesmo conjunto de NSUs na mesma versão dá sempre o mesmo id."""
    ordenados = sorted(nsus)
    resumo = hashlib.sha256(",".join(map(str, ordenados)).encode()).hexdigest()[:12]
    return f"{prefixo}{versao_parser}-{ordenados[0]}-{ordenados[-1]}-{resumo}"


def processar_entradas(
    entradas: Sequence[EntradaIndice], objetos: ObjetosLidos, contexto: Contexto, *, devolucao: bool
) -> ResultadoLote:
    """Processa as entradas do índice: lê os objetos, avalia cada documento e junta as linhas por tabela."""
    resultado = ResultadoLote()
    for entrada in entradas:
        linha = objetos.linhas(entrada.gcs_uri).get(entrada.nsu)
        processar_documento(montar_documento(entrada, linha), contexto, devolucao=devolucao, resultado=resultado)
    return resultado


def carregar_lote(armazem: ArmazemRaw, resultado: ResultadoLote, lote_id: str) -> None:
    """Carrega as tabelas do lote na ordem fixa; a marca fica para quem chama, depois que isto termina."""
    for tabela in ORDEM_DE_CARGA:
        linhas = resultado.linhas.get(tabela)
        if linhas:
            armazem.carregar(tabela, lote_id, linhas)


def _reavaliar(
    armazem: ArmazemRaw, objetos: ObjetosLidos, config: ConfigIncremental, contexto: Contexto, resumo: Resumo
) -> None:
    pendentes = list(armazem.listar_quarentena_pendente(config.versao_parser, config.quarentena_por_execucao))
    if not pendentes:
        return
    entradas = {e.nsu: e for e in armazem.entradas_por_nsu(pendentes)}
    resultado = processar_entradas([entradas[n] for n in pendentes if n in entradas], objetos, contexto, devolucao=True)
    for nsu in (n for n in pendentes if n not in entradas):
        ausente = Documento(nsu=nsu, xml=None, tipo_documento=None)
        reprovado = Reprovado(ausente, f"{XmlVazio()} {NSU_AUSENTE_DO_INDICE}", Prioridade.ILEGIVEL)
        resultado.documentos += 1
        resultado.recusados += 1
        resultado.somar("quarentena", [linha_quarentena(reprovado, config.versao_parser, contexto.instante)])
    carregar_lote(armazem, resultado, identificador_do_lote(config.versao_parser, pendentes, "devolucao-"))
    resumo.somar(resultado)
    logger.info("Quarentena reavaliada: %s documentos, %s devolvidos.", resultado.documentos, resultado.devolvidos)


def executar_incremental(
    armazem: ArmazemRaw,
    leitor: LeitorObjetos,
    config: ConfigIncremental,
    agora: Callable[[], datetime] = lambda: datetime.now(UTC),
    por_versao: RegrasPorVersao = regras_para,
) -> Resumo:
    """Reavalia a quarentena (se pedido) e processa o índice acima da marca, lote a lote.

    :param armazem: BigQuery (índice, marca, quarentena e cargas).
    :param leitor: GCS (objetos NDJSON.gz).
    :param config: Parâmetros da execução.
    :param agora: Relógio (injetável nos testes).
    :param por_versao: Regras de tolerância por versão.
    :returns: O resumo do que foi feito; ``ultimo_nsu`` é a marca ao final.
    :raises Exception: Qualquer falha de carga interrompe a execução sem avançar a marca do lote em andamento.
    """
    objetos = ObjetosLidos(armazem, leitor)
    resumo = Resumo(ultimo_nsu=armazem.ultima_marca())
    contexto = Contexto(objetos.xml_da_nota, config.versao_parser, agora(), por_versao)
    if config.reavaliar_quarentena:
        _reavaliar(armazem, objetos, config, contexto, resumo)
        objetos.esquecer()
    while config.max_lotes is None or resumo.lotes < config.max_lotes:
        entradas = armazem.listar_indice(resumo.ultimo_nsu, config.nsus_por_lote)
        if not entradas:
            break
        contexto = Contexto(objetos.xml_da_nota, config.versao_parser, agora(), por_versao)
        resultado = processar_entradas(entradas, objetos, contexto, devolucao=False)
        nsus = [e.nsu for e in entradas]
        carregar_lote(armazem, resultado, identificador_do_lote(config.versao_parser, nsus))
        armazem.gravar_marca(config.versao_parser, max(nsus))
        resumo.ultimo_nsu = max(nsus)
        resumo.lotes += 1
        resumo.somar(resultado)
        objetos.esquecer()
        logger.info("Lote %s: %s documentos, marca em %s.", resumo.lotes, resultado.documentos, resumo.ultimo_nsu)
    return resumo

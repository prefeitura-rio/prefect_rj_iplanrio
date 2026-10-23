"""Um documento do ADN até as linhas raw: avaliar (sanitizar, versão, XSD), extrair ou mandar para a quarentena."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime

from pipelines.rj_smfp__nota_carioca_parser.utils.avaliacao import (
    Aprovado,
    avaliar_documento,
    linha_devolvido,
    linha_quarentena,
    recusar,
)
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import Documento, TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.dps import extrair_dps
from pipelines.rj_smfp__nota_carioca_parser.utils.entradas import EntradaIndice
from pipelines.rj_smfp__nota_carioca_parser.utils.erros import ErroDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.eventos import extrair_evento
from pipelines.rj_smfp__nota_carioca_parser.utils.leitor import No
from pipelines.rj_smfp__nota_carioca_parser.utils.linhas import com_metadados
from pipelines.rj_smfp__nota_carioca_parser.utils.nfse import extrair_nfse
from pipelines.rj_smfp__nota_carioca_parser.utils.pessoas import extrair_pessoas, ler_pessoas
from pipelines.rj_smfp__nota_carioca_parser.utils.quarentena import BuscarXmlNota
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias import regras_para
from pipelines.rj_smfp__nota_carioca_parser.utils.validar import RegrasPorVersao


@dataclass(frozen=True, slots=True)
class Contexto:
    """O que não muda de um documento para outro no mesmo lote."""

    buscar_xml_nota: BuscarXmlNota
    versao_parser: str
    instante: datetime
    por_versao: RegrasPorVersao = regras_para


@dataclass(slots=True)
class ResultadoLote:
    """Linhas por tabela raw (inclusive ``quarentena``) e a contagem do que aconteceu com os documentos."""

    linhas: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    documentos: int = 0
    aprovados: int = 0
    recusados: int = 0
    devolvidos: int = 0
    cnc: int = 0

    def somar(self, tabela: str, linhas: Sequence[Mapping[str, object]]) -> None:
        if linhas:
            self.linhas.setdefault(tabela, []).extend(dict(linha) for linha in linhas)


def montar_documento(entrada: EntradaIndice, linha: Mapping[str, object] | None) -> Documento:
    """Junta a linha do índice com a do objeto (o XML); sem a linha, o documento vai para a quarentena sem XML."""
    xml = None if linha is None else linha.get("xml")
    tipo_bruto = entrada.tipo_documento if entrada.tipo_documento is not None else (linha or {}).get("tipo_documento")
    try:
        tipo = TipoDocumento.de_valor(tipo_bruto)
    except ErroDocumento:
        tipo = None
    chave = entrada.chave_acesso or (linha or {}).get("chave_acesso")
    return Documento(
        nsu=entrada.nsu,
        xml=xml if isinstance(xml, str) else None,
        tipo_documento=tipo,
        chave_acesso=chave if isinstance(chave, str) else None,
        tipo_evento=None if entrada.tipo_evento is None else str(entrada.tipo_evento),
        data_hora_geracao=entrada.data_hora_geracao,
    )


def _linhas_do_aprovado(aprovado: Aprovado) -> dict[str, list[dict[str, object]]]:
    """Linhas das tabelas raw do documento válido (sem metadados); CNC não tem tabela."""
    documento = aprovado.documento
    raiz = No(aprovado.validado.raiz)
    if documento.tipo_documento is TipoDocumento.NFSE:
        pessoas = ler_pessoas(raiz)
        return {
            "nfse": [extrair_nfse(raiz, pessoas, documento.nsu)],
            "dps": [extrair_dps(raiz, pessoas)],
            "pessoas": extrair_pessoas(raiz),
        }
    if documento.tipo_documento is TipoDocumento.EVENTO:
        return {"eventos": [extrair_evento(raiz, documento.nsu)]}
    return {}


def processar_documento(documento: Documento, contexto: Contexto, *, devolucao: bool, resultado: ResultadoLote) -> None:
    """Avalia e extrai o documento, somando as linhas ao ``resultado``; nunca descarta sem registrar.

    :param documento: Documento do ADN.
    :param contexto: Versão do parser, instante e regras de tolerância.
    :param devolucao: Reavaliação da quarentena: o que passa a ser válido ganha a linha de devolução.
    :param resultado: Onde as linhas e as contagens são acumuladas.
    """
    resultado.documentos += 1
    avaliado = avaliar_documento(documento, contexto.buscar_xml_nota, contexto.por_versao)
    if isinstance(avaliado, Aprovado):
        try:
            linhas = _linhas_do_aprovado(avaliado)
        except ErroDocumento as erro:
            avaliado = recusar(documento, erro, contexto.buscar_xml_nota)
        else:
            for tabela, itens in linhas.items():
                resultado.somar(
                    tabela,
                    [
                        com_metadados(
                            item, documento.nsu, documento.data_hora_geracao, contexto.versao_parser, contexto.instante
                        )
                        for item in itens
                    ],
                )
            resultado.aprovados += 1
            resultado.cnc += 0 if linhas else 1
            if devolucao:
                resultado.devolvidos += 1
                resultado.somar("quarentena", [linha_devolvido(documento, contexto.versao_parser, contexto.instante)])
            return
    resultado.recusados += 1
    resultado.somar("quarentena", [linha_quarentena(avaliado, contexto.versao_parser, contexto.instante)])

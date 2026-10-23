"""Avaliação de um documento (aprovado ou quarentena) e devolução da quarentena a cada nova ``versao_parser``."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from pipelines.rj_smfp__nota_carioca_parser.utils.documento import Documento, TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.erros import ErroDocumento, TipoDocumentoDesconhecido
from pipelines.rj_smfp__nota_carioca_parser.utils.quarentena import BuscarXmlNota, Prioridade, classificar_quarentena
from pipelines.rj_smfp__nota_carioca_parser.utils.sanitizar import sanitizar_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias import regras_para
from pipelines.rj_smfp__nota_carioca_parser.utils.validar import DocumentoValidado, RegrasPorVersao, validar_documento


@dataclass(frozen=True, slots=True)
class Aprovado:
    """Documento válido: segue para a extração."""

    documento: Documento
    validado: DocumentoValidado


@dataclass(frozen=True, slots=True)
class Reprovado:
    """Documento recusado: vai para a quarentena (nunca é descartado em silêncio)."""

    documento: Documento
    erro: str
    prioridade: Prioridade


def avaliar_documento(
    documento: Documento,
    buscar_xml_nota: BuscarXmlNota,
    por_versao: RegrasPorVersao = regras_para,
) -> Aprovado | Reprovado:
    """Valida o documento; se falhar, classifica a prioridade da quarentena.

    :param documento: Documento do ADN.
    :param buscar_xml_nota: Busca o XML da NFS-e por chave (usada na prioridade dos eventos).
    :param por_versao: Regras de tolerância por versão.
    """
    try:
        if documento.tipo_documento is None:
            raise TipoDocumentoDesconhecido(None)
        return Aprovado(documento, validar_documento(documento.xml, documento.tipo_documento, por_versao))
    except ErroDocumento as erro:
        xml = sanitizar_xml(documento.xml)
        prioridade = classificar_quarentena(documento.tipo_documento, xml, documento.chave_acesso, buscar_xml_nota)
        return Reprovado(documento, str(erro), prioridade)


def linha_quarentena(reprovado: Reprovado, versao_parser: str, avaliado_em: datetime) -> dict[str, object]:
    """Linha de ``raw_nfse_nacional.quarentena`` para o documento recusado."""
    return {
        "nsu": reprovado.documento.nsu,
        "prioridade": int(reprovado.prioridade),
        "erro": reprovado.erro,
        "versao_parser": versao_parser,
        "avaliado_em": avaliado_em.isoformat(),
    }


def linha_devolvido(documento: Documento, versao_parser: str, avaliado_em: datetime) -> dict[str, object]:
    """Linha de ``quarentena`` que encerra a quarentena do NSU: ``prioridade`` e ``erro`` nulos (válido na versão)."""
    return {
        "nsu": documento.nsu,
        "prioridade": None,
        "erro": None,
        "versao_parser": versao_parser,
        "avaliado_em": avaliado_em.isoformat(),
    }


@dataclass(frozen=True, slots=True)
class ResultadoReavaliacao:
    """Quem saiu da quarentena (``devolvidos``, para seguir ao parse) e quem continua nela."""

    devolvidos: tuple[Aprovado, ...]
    mantidos: tuple[Reprovado, ...]


def reavaliar_quarentena(
    documentos: Iterable[Documento],
    buscar_xml_nota: BuscarXmlNota,
    por_versao: RegrasPorVersao = regras_para,
) -> ResultadoReavaliacao:
    """Reavalia os documentos em quarentena com as regras da versão atual (devolução, B-09).

    Os de prioridade mais urgente (P1) vêm primeiro na lista de mantidos.
    """
    devolvidos: list[Aprovado] = []
    mantidos: list[Reprovado] = []
    for documento in documentos:
        resultado = avaliar_documento(documento, buscar_xml_nota, por_versao)
        if isinstance(resultado, Aprovado):
            devolvidos.append(resultado)
        else:
            mantidos.append(resultado)
    mantidos.sort(key=lambda r: (r.prioridade, r.documento.nsu))
    return ResultadoReavaliacao(tuple(devolvidos), tuple(mantidos))


__all__ = [
    "Aprovado",
    "Documento",
    "Reprovado",
    "ResultadoReavaliacao",
    "TipoDocumento",
    "avaliar_documento",
    "linha_devolvido",
    "linha_quarentena",
    "reavaliar_quarentena",
]

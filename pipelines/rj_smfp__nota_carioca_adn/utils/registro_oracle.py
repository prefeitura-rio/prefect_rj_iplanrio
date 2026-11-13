"""Linha do Oracle no formato do raw: tipos por descrição, data com fuso e a marca da quarentena."""

from datetime import datetime

from pipelines.rj_smfp__nota_carioca_adn.utils.fonte import LinhaOracle
from pipelines.rj_smfp__nota_carioca_adn.utils.janela import FUSO_SAO_PAULO
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import RegistroDocumento

TIPOS_DOCUMENTO = {1: "NFSE", 2: "EVENTO", 3: "CNC"}
TIPOS_EVENTO = {
    2: "CANCELAMENTO",
    3: "CANCELAMENTO_POR_SUBSTITUICAO",
    4: "SOLICITACAO_CANCELAMENTO_ANALISE_FISCAL",
    5: "CANCELAMENTO_DEFERIDO_ANALISE_FISCAL",
    6: "CANCELAMENTO_INDEFERIDO_ANALISE_FISCAL",
    7: "CONFIRMACAO_PRESTADOR",
    8: "CONFIRMACAO_TOMADOR",
    9: "CONFIRMACAO_INTERMEDIARIO",
    10: "CONFIRMACAO_TACITA",
    11: "REJEICAO_PRESTADOR",
    12: "REJEICAO_TOMADOR",
    13: "REJEICAO_INTERMEDIARIO",
    14: "ANULACAO_REJEICAO",
    15: "CANCELAMENTO_POR_OFICIO",
    16: "BLOQUEIO_POR_OFICIO",
    17: "DESBLOQUEIO_POR_OFICIO",
}
"""``eTipoEvento`` da app (``DFe.vb``) com as descrições que o ADN envia; 0 (todos) e 1 (emissão) não vêm do ADN."""


def registro_do_oracle(linha: LinhaOracle) -> RegistroDocumento:
    """Linha do Oracle no formato do raw: tipos por descrição (1 e 0 de ``TIPO_EVENTO`` viram ``None``, como o ADN
    nunca os envia) e ``DATE`` local de São Paulo como instante com fuso."""
    tipo_evento = (
        None if linha.tipo_evento in (None, 0, 1) else TIPOS_EVENTO.get(linha.tipo_evento, str(linha.tipo_evento))
    )
    tipo_documento = (
        None if linha.tipo_documento is None else TIPOS_DOCUMENTO.get(linha.tipo_documento, str(linha.tipo_documento))
    )
    gerado: datetime | None = linha.data_hora_geracao
    return RegistroDocumento(
        nsu=linha.nsu,
        chave_acesso=linha.chave_acesso,
        tipo_documento=tipo_documento,
        tipo_evento=tipo_evento,
        data_hora_geracao=None if gerado is None else gerado.replace(tzinfo=FUSO_SAO_PAULO),
        xml=linha.arquivo_xml,
        ilegivel="ARQUIVO_XML nulo no Oracle" if linha.arquivo_xml is None else None,
        quarentena_oracle=linha.quarentena,
    )

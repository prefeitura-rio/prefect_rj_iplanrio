"""Extração do evento, porta de ``EventoItem(XmlEvento)`` e do INSERT de ``EVENTOS_DFEN`` (``Eventos.vb``).

As 26 colunas são as que ``Eventos.vb`` grava (não há DDL de ``EVENTOS_DFEN`` no repositório do .NET). Tipos de evento
(``DFe.vb``, ``eTipoEvento``): ``e101101`` cancelamento, ``e105102`` cancelamento por substituição, ``e105104``
deferido em análise fiscal, ``e305101`` cancelamento por ofício, ``e305102`` bloqueio, ``e305103`` desbloqueio...
O que os eventos fazem com a nota (status, cancelamento, substituta) é do dbt (R-08, R-09).
"""

import re

from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import nome_local
from pipelines.rj_smfp__nota_carioca_parser.utils.leitor import (
    No,
    data_hora,
    inteiro_ou_zero,
    iso,
    texto,
)

COLUNAS_EVENTO = (
    "PEDIDO_EVENTO",
    "CHAVE_ACESSO",
    "TIPO_EVENTO",
    "SEQUENCIAL_EVENTO",
    "NUMERO_PEDIDO",
    "DATA_EVENTO",
    "DATA_REGISTRO",
    "NUMERO_DFE",
    "NSU",
    "CODIGO_MOTIVO",
    "MOTIVO_EVENTO",
    "CPF_CNPJ_AUTOR_PEDIDO",
    "CPF_AGENTE_TRIBUTARIO",
    "CHAVE_ACESSO_SUBSTITUTA",
    "NUMERO_PROCESSO_ADM",
    "DESCRICAO_PROCESSO_ADM",
    "TIPO_EVENTO_NFSE",
    "EVENTO_BLOQUEIO_OFICIO",
    "EVENTO_MANIFESTA_REJEICAO",
    "AMBIENTE_GERADOR",
    "TIPO_AMBIENTE",
    "VERSAO_EVENTO",
    "VERSAO_PEDIDO",
    "VERSAO_APLICACAO_EVENTO",
    "VERSAO_APLICACAO_PEDIDO",
    "EVENTO",
)
# Filhos de infPedReg que não são o detalhe do evento; o primeiro outro elemento é o detalhe (``e101101``...).
ELEMENTOS_DO_PEDIDO = frozenset({"tpAmb", "verAplic", "dhEvento", "CNPJAutor", "CPFAutor", "chNFSe", "nPedRegEvento"})
PREFIXO_ID_EVENTO = 3  # "EVT"
CODIGO_DO_EVENTO = re.compile(r"e[0-9]{6}")


def _detalhe(pedido: No) -> No:
    """O elemento de detalhe do evento (``XmlInfPedReg.TipoEvento``), ou um nó vazio."""
    if pedido:
        for candidato in pedido.elemento:
            if isinstance(candidato.tag, str) and nome_local(candidato) not in ELEMENTOS_DO_PEDIDO:
                return No(candidato)
    return No(None)


def _tipo_do_evento(detalhe: No) -> str | None:
    """``eNNNNNN`` do detalhe; o .NET lê o código da tag de fechamento, que um detalhe vazio não tem."""
    if not detalhe or (len(detalhe.elemento) == 0 and not detalhe.texto):
        return None
    nome = nome_local(detalhe.elemento)
    return nome if CODIGO_DO_EVENTO.fullmatch(nome) else None


def _motivo(detalhe: No) -> tuple[str, str | None]:
    """Código e descrição do motivo, com a retrocompatibilidade de ``infRej`` (rejeição antiga)."""
    codigo = str(inteiro_ou_zero(detalhe("cMotivo"), "cMotivo"))
    descricao = detalhe("xMotivo").texto
    rejeicao = detalhe("infRej")
    if codigo in {"", "0"} and rejeicao:
        codigo = str(inteiro_ou_zero(rejeicao("cMotivo"), "cMotivo"))
        descricao = rejeicao("xMotivo").texto if rejeicao("xMotivo") else descricao
    return codigo, descricao


def _anulacao(detalhe: No, motivo: str | None, cpf: str | None) -> tuple[str | None, str | None, str | None]:
    """Motivo, CPF do agente e evento rejeitado, com ``infAnRej`` ou com os campos soltos (TE205208 inline)."""
    anulacao = detalhe("infAnRej")
    if anulacao:
        motivo = anulacao("xMotivo").texto if anulacao("xMotivo") else motivo
        cpf = anulacao("CPFAgTrib").texto if anulacao("CPFAgTrib") else cpf
        return motivo, cpf, anulacao("idEvManifRej").texto
    motivo = detalhe("xMotivo").texto if not motivo else motivo
    cpf = detalhe("CPFAgTrib").texto if not cpf else cpf
    return motivo, cpf, detalhe("idEvManifRej").texto


def extrair_evento(raiz: No, nsu: int) -> dict[str, object]:
    """Colunas de ``DFEN.EVENTOS_DFEN`` do evento, com os valores gravados pelo .NET (vazio é ``NULL``)."""
    inf = raiz("infEvento")
    pedido = inf("pedRegEvento", "infPedReg")
    detalhe = _detalhe(pedido)
    codigo, motivo = _motivo(detalhe)
    cpf_agente = detalhe("CPFAgTrib").texto
    motivo, cpf_agente, manifesta = _anulacao(detalhe, motivo, cpf_agente)
    identificador = inf.atributo("Id")
    numero_dfe = inteiro_ou_zero(inf("nDFSe"), "nDFSe") or inteiro_ou_zero(inf("nDFe"), "nDFe")
    return {
        "PEDIDO_EVENTO": pedido.atributo("Id") or None,
        "CHAVE_ACESSO": texto(pedido("chNFSe")),
        "TIPO_EVENTO": _tipo_do_evento(detalhe),
        "SEQUENCIAL_EVENTO": str(inteiro_ou_zero(inf("nSeqEvento"), "nSeqEvento")),
        "NUMERO_PEDIDO": texto(pedido("nPedRegEvento")),
        "DATA_EVENTO": iso(data_hora(pedido("dhEvento"), "dhEvento")),
        "DATA_REGISTRO": iso(data_hora(inf("dhProc"), "dhProc")),
        "NUMERO_DFE": numero_dfe or None,
        "NSU": nsu or None,
        "CODIGO_MOTIVO": codigo if detalhe else None,
        "MOTIVO_EVENTO": motivo or None,
        "CPF_CNPJ_AUTOR_PEDIDO": texto(pedido("CNPJAutor")) or texto(pedido("CPFAutor")),
        "CPF_AGENTE_TRIBUTARIO": cpf_agente or None,
        "CHAVE_ACESSO_SUBSTITUTA": texto(detalhe("chSubstituta")),
        "NUMERO_PROCESSO_ADM": texto(detalhe("nProcAdm")),
        "DESCRICAO_PROCESSO_ADM": texto(detalhe("xProcAdm")),
        "TIPO_EVENTO_NFSE": texto(detalhe("codEvento")),
        "EVENTO_BLOQUEIO_OFICIO": texto(detalhe("idBloqOfic")),
        "EVENTO_MANIFESTA_REJEICAO": manifesta or None,
        "AMBIENTE_GERADOR": texto(inf("ambGer")),
        "TIPO_AMBIENTE": inteiro_ou_zero(pedido("tpAmb"), "tpAmb") if pedido else None,
        "VERSAO_EVENTO": raiz.atributo("versao") or None,
        "VERSAO_PEDIDO": inf("pedRegEvento").atributo("versao") or None,
        "VERSAO_APLICACAO_EVENTO": texto(inf("verAplic")),
        "VERSAO_APLICACAO_PEDIDO": texto(pedido("verAplic")),
        "EVENTO": (identificador[PREFIXO_ID_EVENTO:] or None) if identificador else None,
    }

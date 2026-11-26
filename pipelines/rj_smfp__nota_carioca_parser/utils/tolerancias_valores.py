"""Regras de tolerância de valores recusados por faceta (``pattern``, ``minLength``, ``enumeration``...).

Portadas de ``DFeFactory.AddValidationRules`` (``DFeFactory.vb``). O .NET tem uma versão PT e outra EN de cada regra
(a mensagem depende da cultura do servidor); aqui há uma função por regra. Não portada: ``TSIdPedRefEvt`` em ``Id``
das regras comuns só casa a v1.00 do XSD; ela é a mesma de :func:`tolerar_id_pedido_evento`.
"""

import re

from pipelines.rj_smfp__nota_carioca_parser.utils.erros_xsd import ErroXsd, TipoErro
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias_base import (
    PADRAO_TSDEC15V2,
    atributo_invalido,
    padrao_invalido,
    tamanho_ou_vazio,
    valor_vazio,
)

SERIE_ANCORADA = r"^0{0,4}\d{1,5}$"


def tolerar_xnbs_texto(e: ErroXsd) -> bool:
    """Ignora Pattern TSString (espaço à direita) em ``xNBS``."""
    return padrao_invalido(e, "xNBS")


def tolerar_xtribmun_texto(e: ErroXsd) -> bool:
    """Ignora Pattern TSString (espaço à direita) em ``xTribMun``."""
    return padrao_invalido(e, "xTribMun")


def tolerar_cep_curto(e: ErroXsd) -> bool:
    """Ignora Pattern TSCEP (dígitos insuficientes) em ``CEP``."""
    return padrao_invalido(e, "CEP")


def tolerar_fone_fora_da_faixa(e: ErroXsd) -> bool:
    """Ignora Pattern TSTelefone (tamanho inválido) em ``fone``."""
    return padrao_invalido(e, "fone")


def tolerar_dtfim_invalida(e: ErroXsd) -> bool:
    """Ignora Pattern TSData (ano inválido) em ``dtFim``."""
    return padrao_invalido(e, "dtFim")


def tolerar_dtini_invalida(e: ErroXsd) -> bool:
    """Ignora Pattern TSData (ano inválido) em ``dtIni``."""
    return padrao_invalido(e, "dtIni")


def tolerar_id_pedido_evento(e: ErroXsd) -> bool:
    """Ignora Pattern do ``Id`` do pedido de evento (``TSIdPedRefEvt`` na v1.00 do XSD)."""
    return atributo_invalido(e, "infPedReg", "Id", "pattern")


def tolerar_clocincid_invalido(e: ErroXsd) -> bool:
    """Ignora Pattern TSCodMunIBGE (vazio ou inválido) em ``cLocIncid``."""
    return padrao_invalido(e, "cLocIncid")


def tolerar_xbairro_vazio(e: ErroXsd) -> bool:
    """Ignora Pattern TSBairro (string vazia) em ``xBairro``."""
    return valor_vazio(e, "xBairro")


def tolerar_xdescserv_vazio(e: ErroXsd) -> bool:
    """Ignora Pattern TSDesc2000 (string vazia) em ``xDescServ``."""
    return valor_vazio(e, "xDescServ")


def tolerar_versao_fora_do_padrao(e: ErroXsd) -> bool:
    """Ignora Pattern TVerNFSe no atributo ``versao`` (por exemplo ``1.0`` no lugar de ``1.00``)."""
    return atributo_invalido(e, None, "versao", "pattern")


def tolerar_im_vazia(e: ErroXsd) -> bool:
    """Ignora MinLength TSInscMun (elemento ``IM`` vazio)."""
    return tamanho_ou_vazio(e, "IM")


def tolerar_cendpost_vazio(e: ErroXsd) -> bool:
    """Ignora MinLength TSCodigoEndPostal (elemento ``cEndPost`` vazio)."""
    return tamanho_ou_vazio(e, "cEndPost")


def tolerar_xnome_vazio(e: ErroXsd) -> bool:
    """Ignora MinLength TSNomeRazaoSocial (elemento ``xNome`` de pessoa vazio)."""
    return e.pai in {"emit", "prest", "toma", "interm"} and tamanho_ou_vazio(e, "xNome")


def tolerar_padrao_xlgr(e: ErroXsd) -> bool:
    """Ignora Pattern (espaço à direita) em ``xLgr``."""
    return padrao_invalido(e, "xLgr")


def tolerar_padrao_nro(e: ErroXsd) -> bool:
    """Ignora Pattern (espaço à direita) em ``nro``."""
    return padrao_invalido(e, "nro")


def tolerar_padrao_xcpl(e: ErroXsd) -> bool:
    """Ignora Pattern (espaço à direita) em ``xCpl``."""
    return padrao_invalido(e, "xCpl")


def tolerar_padrao_xbairro(e: ErroXsd) -> bool:
    """Ignora Pattern (espaço à direita) em ``xBairro``."""
    return padrao_invalido(e, "xBairro")


def tolerar_padrao_xnome(e: ErroXsd) -> bool:
    """Ignora Pattern (espaço à direita) em ``xNome``."""
    return padrao_invalido(e, "xNome")


def tolerar_padrao_xfant(e: ErroXsd) -> bool:
    """Ignora Pattern (espaço à direita) em ``xFant``."""
    return padrao_invalido(e, "xFant")


def tolerar_padrao_email(e: ErroXsd) -> bool:
    """Ignora Pattern (espaço à direita) em ``email``."""
    return padrao_invalido(e, "email")


def tolerar_valor_monetario_fora_do_padrao(e: ErroXsd) -> bool:
    """Ignora Pattern TSDec15V2 (valor negativo) em qualquer elemento monetário."""
    return e.tipo is TipoErro.FACETA and e.faceta == "pattern" and e.padrao == PADRAO_TSDEC15V2


def tolerar_xsi_nil_indevido(e: ErroXsd) -> bool:
    """Ignora o atributo ``xsi:nil`` em elemento que não é ``nillable``."""
    return e.tipo is TipoErro.NAO_NULAVEL


def tolerar_serie_dps_ancorada(e: ErroXsd) -> bool:
    """Compensa o libxml2: o XSD de ``serie`` usa ``^...$`` e o libxml2 lê os âncoras como caracteres literais.

    Não existe no .NET (a expressão ancorada funciona lá). Só ignora quando o valor casa com a expressão de verdade,
    ``0{0,4}\\d{1,5}``; qualquer outro valor de ``serie`` continua sendo erro.
    """
    if not padrao_invalido(e, "serie") or e.padrao != SERIE_ANCORADA or e.valor is None:
        return False
    return re.fullmatch(r"0{0,4}[0-9]{1,5}", e.valor) is not None

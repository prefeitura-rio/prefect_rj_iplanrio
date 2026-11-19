"""Regras de tolerância de retrocompatibilidade por versão do XSD.

Portadas de ``AddValidationRulesV100`` e ``AddValidationRulesV101`` (``DFeFactory.vb``): o XSD de 2026-02-09 mudou
estruturas (``nDFe`` por ``nDFSe``, ``infRej``, ``Id`` do evento...) e o ADN ainda entrega os formatos antigos.
"""

from pipelines.rj_smfp__nota_carioca_parser.utils.erros_xsd import ErroXsd, TipoErro
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias_base import (
    atributo_invalido,
    cita,
    filho_inesperado,
)

ELEMENTOS_ANULACAO = ("CPFAgTrib", "idEvManifRej", "xMotivo")
FACETAS_DO_ID = ("pattern", "length", "maxLength", "minLength")


def _valor_enumerado(e: ErroXsd, elemento: str, *valores: str) -> bool:
    return e.tipo is TipoErro.FACETA and e.faceta == "enumeration" and e.elemento == elemento and e.valor in valores


def tolerar_ndfe_legado_em_infevento(e: ErroXsd) -> bool:
    """Ignora ``nDFe`` em ``infEvento`` quando o XSD atualizado espera ``nDFSe`` (V1.00 e V1.01)."""
    return e.tipo is TipoErro.INESPERADO and filho_inesperado(e, "infEvento", "nDFe") and "nDFSe" in e.esperados


def tolerar_npedregevento_em_infpedreg(e: ErroXsd) -> bool:
    """Ignora ``nPedRegEvento`` em ``infPedReg``, removido do XSD (V1.00 e V1.01)."""
    return cita(e, "infPedReg", "nPedRegEvento")


def tolerar_inforej_legado(e: ErroXsd) -> bool:
    """Ignora ``infRej`` em evento de rejeição, onde o XSD espera ``cMotivo``/``xMotivo`` (V1.00 e V1.01)."""
    if e.tipo not in {TipoErro.INESPERADO, TipoErro.INCOMPLETO}:
        return False
    return "infRej" in {e.elemento, e.container, *e.esperados}


def tolerar_xdesc_valor_antigo(e: ErroXsd) -> bool:
    """Ignora ``xDesc`` com texto antigo (fora da enumeração do XSD atual) (V1.00 e V1.01)."""
    return e.tipo is TipoErro.FACETA and e.faceta == "enumeration" and e.elemento == "xDesc"


def tolerar_cstat_107(e: ErroXsd) -> bool:
    """V1.00: ignora ``cStat`` ``107``, valor que só entrou na enumeração TStat da v1.01."""
    return _valor_enumerado(e, "cStat", "107")


def tolerar_xnome_em_atvevento(e: ErroXsd) -> bool:
    """V1.00: ignora ``xNome`` em ``atvEvento`` (o XSD atualizado espera ``desc``)."""
    return cita(e, "atvEvento", "xNome")


def tolerar_idatvevt_em_atvevento(e: ErroXsd) -> bool:
    """V1.00: ignora ``idAtvEvt`` em ``atvEvento`` (o XSD atualizado espera ``id``)."""
    return cita(e, "atvEvento", "idAtvEvt")


def tolerar_cmotivoemisti_em_infdps(e: ErroXsd) -> bool:
    """V1.00: ignora ``cMotivoEmisTI`` em ``infDPS`` (removido do XSD atualizado)."""
    return cita(e, "infDPS", "cMotivoEmisTI")


def tolerar_chnfserej_em_infdps(e: ErroXsd) -> bool:
    """V1.00: ignora ``chNFSeRej`` em ``infDPS`` (removido do XSD atualizado)."""
    return cita(e, "infDPS", "chNFSeRej")


def tolerar_cep_em_endereco_simples(e: ErroXsd) -> bool:
    """V1.00: ignora ``CEP`` como filho de endereço simples (o XSD atualizado usa ``cEndPost``)."""
    esperando = {"cEndPost", "xCidade", "xEstProvReg"}
    return e.tipo is TipoErro.INESPERADO and e.elemento == "CEP" and bool(esperando & set(e.esperados))


def tolerar_endext_em_endereco_simples(e: ErroXsd) -> bool:
    """V1.00: ignora ``endExt`` como filho de endereço simples (removido do XSD atualizado)."""
    return e.tipo is TipoErro.INESPERADO and e.elemento == "endExt"


def tolerar_inscimobfisc_em_obra(e: ErroXsd) -> bool:
    """V1.00: ignora ``inscImobFisc`` em ``obra`` (o XSD atualizado usa ``choice``)."""
    return cita(e, "obra", "inscImobFisc")


def tolerar_tpbm_valor_antigo(e: ErroXsd) -> bool:
    """V1.00: ignora ``tpBM`` com valor numérico TBMISSQN (o XSD atualizado usa TSDesc40)."""
    return e.tipo is TipoErro.FACETA and e.elemento == "tpBM" and e.faceta in {"pattern", "enumeration"}


def tolerar_anulacao_inline_te205208(e: ErroXsd) -> bool:
    """V1.00: ignora ``CPFAgTrib``/``idEvManifRej``/``xMotivo`` inline em ``e205208`` (sem ``infAnRej``)."""
    if e.tipo not in {TipoErro.INESPERADO, TipoErro.INCOMPLETO}:
        return False
    tema = e.container == "e205208" or "infAnRej" in {e.elemento, *e.esperados}
    return tema and (e.elemento in ELEMENTOS_ANULACAO or bool(set(ELEMENTOS_ANULACAO) & set(e.esperados)))


def tolerar_ordem_em_e305102(e: ErroXsd) -> bool:
    """V1.00: ignora a ordem de ``codEvento``/``xMotivo`` em ``e305102``."""
    return cita(e, "e305102", "codEvento", "xMotivo")


def tolerar_ibscbs_em_infnfse(e: ErroXsd) -> bool:
    """V1.00: ignora ``IBSCBS`` em ``infNFSe`` (estrutura da v1.01 contra o esquema da v1.00)."""
    return cita(e, "infNFSe", "IBSCBS")


def tolerar_id_pedido_formato_curto(e: ErroXsd) -> bool:
    """V1.00: ignora ``Id`` do pedido de evento fora do padrão ou do tamanho (formato curto da v1.01)."""
    return atributo_invalido(e, "infPedReg", "Id", *FACETAS_DO_ID)


def tolerar_cstat_101(e: ErroXsd) -> bool:
    """V1.01: ignora ``cStat`` ``101``, valor que só existia na enumeração TStat da v1.00."""
    return _valor_enumerado(e, "cStat", "101")


def tolerar_eventos_907202_967203(e: ErroXsd) -> bool:
    """V1.01: ignora os eventos ``e907202``/``e967203``, removidos do XSD (TSCodigoEventoNFSe)."""
    codigos = ("e907202", "e967203")
    if e.tipo is TipoErro.INESPERADO:
        return e.elemento in codigos
    return e.tipo is TipoErro.FACETA and e.faceta == "enumeration" and e.valor in codigos


def tolerar_id_pedido_formato_antigo(e: ErroXsd) -> bool:
    """V1.01: ignora ``Id`` do ``infPedReg`` no formato antigo (TSIdPedRegEvt com ``nPedRegEvento``)."""
    return atributo_invalido(e, "infPedReg", "Id", "pattern")


def tolerar_id_evento_formato_antigo(e: ErroXsd) -> bool:
    """V1.01: ignora ``Id`` do ``infEvento`` no formato antigo (TSIdEvento)."""
    return atributo_invalido(e, "infEvento", "Id", "pattern")

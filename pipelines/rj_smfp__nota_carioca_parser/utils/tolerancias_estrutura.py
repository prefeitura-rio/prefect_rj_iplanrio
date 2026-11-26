"""Regras de tolerância de estrutura: filhos fora de ordem, conteúdo incompleto, assinatura e NT007.

Portadas de ``DFeFactory.AddValidationRules`` (``DFeFactory.vb``), uma função por regra (o .NET tem uma versão PT e
outra EN de cada uma). O libxml2 interrompe a validação do conteúdo de um elemento no primeiro erro estrutural; o
validador compensa removendo os nós tolerados e validando de novo (``validar.py``).
"""

from pipelines.rj_smfp__nota_carioca_parser.utils.erros_xsd import ErroXsd, TipoErro
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias_base import (
    atributo_invalido,
    cita,
    filho_inesperado,
    incompleto_esperando,
    inesperado_cita,
)


def tolerar_paliq_fora_de_ordem(e: ErroXsd) -> bool:
    """Ignora erro de ordem: ``tribMun`` com o filho ``pAliq`` onde o esquema esperava outro."""
    return filho_inesperado(e, "tribMun", "pAliq")


def tolerar_xbairro_antes_de_nro(e: ErroXsd) -> bool:
    """Ignora ``xBairro`` antes de ``nro`` em ``enderNac``."""
    return filho_inesperado(e, "enderNac", "xBairro", "nro")


def tolerar_cmun_antes_de_complemento(e: ErroXsd) -> bool:
    """Ignora ``cMun`` antes de ``xCpl``/``xBairro`` em ``enderNac``."""
    return filho_inesperado(e, "enderNac", "cMun", "xCpl", "xBairro")


def tolerar_cpaisprestacao_em_locprest(e: ErroXsd) -> bool:
    """Ignora ``cPaisPrestacao`` inválido em ``locPrest``."""
    return filho_inesperado(e, "locPrest", "cPaisPrestacao")


def tolerar_locprest_incompleto(e: ErroXsd) -> bool:
    """Ignora ``locPrest`` incompleto (faltam ``cLocPrestacao``/``cPaisPrestacao``)."""
    return (
        e.tipo is TipoErro.INCOMPLETO
        and e.elemento == "locPrest"
        and e.esperados == ("cLocPrestacao", "cPaisPrestacao")
    )


def tolerar_obra_incompleta(e: ErroXsd) -> bool:
    """Ignora ``obra`` incompleta (faltam ``cObra``/``inscImobFisc``/``end``)."""
    return incompleto_esperando(e, "obra", "cObra")


def tolerar_cobra_em_obra(e: ErroXsd) -> bool:
    """Ignora ``cObra`` inválido em ``obra`` (estrutura da v1.01 contra o esquema da v1.00)."""
    return inesperado_cita(e, "obra", "cObra")


def tolerar_end_em_obra(e: ErroXsd) -> bool:
    """Ignora ``end`` inválido em ``obra`` (estrutura da v1.01 contra o esquema da v1.00)."""
    return inesperado_cita(e, "obra", "end")


def tolerar_tribnac_antes_de_tribfed(e: ErroXsd) -> bool:
    """Ignora ``tribNac`` antes de ``tribFed``/``totTrib`` em ``trib``."""
    return filho_inesperado(e, "trib", "tribNac", "tribFed", "totTrib")


def tolerar_xoutinf_fora_de_ordem(e: ErroXsd) -> bool:
    """Ignora ``xOutInf`` em ``infNFSe`` em ordem inesperada (antes de ``valores``)."""
    return inesperado_cita(e, "infNFSe", "xOutInf")


def tolerar_algoritmo_fora_da_enumeracao(e: ErroXsd) -> bool:
    """Ignora ``Algorithm`` fora da enumeração TTransformURI (c14n exclusivo na assinatura)."""
    return atributo_invalido(e, None, "Algorithm", "enumeration")


def tolerar_algoritmo_diferente_do_fixo(e: ErroXsd) -> bool:
    """Ignora ``Algorithm`` diferente do valor fixo do XSD (xmldsig com algoritmos mais modernos)."""
    return e.tipo is TipoErro.ATRIBUTO_FIXO and e.atributo == "Algorithm"


def tolerar_transforms_sem_transform(e: ErroXsd) -> bool:
    """Ignora ``Transforms`` vazio (falta ``Transform``)."""
    return incompleto_esperando(e, "Transforms", "Transform")


def tolerar_uri_vazia_em_reference(e: ErroXsd) -> bool:
    """Ignora ``URI=""`` em ``Reference`` (MinLength do AnyUri)."""
    return atributo_invalido(e, None, "URI", "minLength", "length")


def tolerar_nbm_antes_de_tpbm(e: ErroXsd) -> bool:
    """Ignora ``nBM`` antes de ``tpBM`` em ``BM``."""
    return e.tipo is TipoErro.INESPERADO and e.pai == "BM" and e.elemento == "nBM" and "tpBM" in e.esperados


def tolerar_transform_invalido_em_transforms(e: ErroXsd) -> bool:
    """Ignora ``Transform`` inválido em ``Transforms`` (perfil completo do xmldsig)."""
    return inesperado_cita(e, "Transforms", "Transform")


def tolerar_reference_invalida_em_signedinfo(e: ErroXsd) -> bool:
    """Ignora ``Reference`` inválido em ``SignedInfo`` (perfil completo do xmldsig)."""
    return inesperado_cita(e, "SignedInfo", "Reference")


def tolerar_x509subjectname_em_x509data(e: ErroXsd) -> bool:
    """Ignora ``X509SubjectName`` inválido em ``X509Data`` (o governo envia o assunto além do certificado)."""
    return inesperado_cita(e, "X509Data", "X509SubjectName")


def tolerar_object_em_signature(e: ErroXsd) -> bool:
    """Ignora ``Object`` inválido em ``Signature`` (elemento de comprometimento)."""
    return inesperado_cita(e, "Signature", "Object")


def tolerar_nt007_novos_elementos_em_ibscbs(e: ErroXsd) -> bool:
    """NT007: ignora ``indZFMALC``, ``indDoacao`` e ``gLocBensMoveis`` como filhos de ``IBSCBS``."""
    return cita(e, "IBSCBS", "indZFMALC", "indDoacao", "gLocBensMoveis")


def tolerar_nt007_indfinal_ausente(e: ErroXsd) -> bool:
    """NT007: ignora ``indFinal`` ausente em ``IBSCBS``."""
    return cita(e, "IBSCBS", "indFinal")


def tolerar_nt007_gdedredibscbs(e: ErroXsd) -> bool:
    """NT007: ignora ``gDedRedIBSCBS`` como filho de ``valores``."""
    return cita(e, "valores", "gDedRedIBSCBS")


def tolerar_nt007_gestornocred_gpagantecipado(e: ErroXsd) -> bool:
    """NT007: ignora ``gEstornoCred`` e ``gPagAntecipado`` como filhos de ``gIBSCBS``."""
    return cita(e, "gIBSCBS", "gEstornoCred", "gPagAntecipado")


def tolerar_nt007_vcalcdedredibscbs(e: ErroXsd) -> bool:
    """NT007: ignora ``vCalcDedRedIBSCBS`` como filho de ``valores``."""
    return cita(e, "valores", "vCalcDedRedIBSCBS")


def tolerar_nt007_xped(e: ErroXsd) -> bool:
    """NT007: ignora ``xPed`` como filho de ``infoCompl``."""
    return cita(e, "infoCompl", "xPed")


def tolerar_nt007_gitemped(e: ErroXsd) -> bool:
    """NT007: ignora ``gItemPed`` como filho de ``infoCompl``."""
    return cita(e, "infoCompl", "gItemPed")


def tolerar_nt007_ccib_em_obra(e: ErroXsd) -> bool:
    """NT007: ignora ``cCIB`` como filho de ``obra``."""
    return cita(e, "obra", "cCIB")


def tolerar_uf_vazio(e: ErroXsd) -> bool:
    """ADR-028: ignora ``uf`` de IBS/CBS sem ``pIBSUF`` (container vazio)."""
    return incompleto_esperando(e, "uf", "pIBSUF")


def tolerar_mun_vazio(e: ErroXsd) -> bool:
    """ADR-028: ignora ``mun`` de IBS/CBS sem ``pIBSMun`` (container vazio)."""
    return incompleto_esperando(e, "mun", "pIBSMun")


def tolerar_fed_vazio(e: ErroXsd) -> bool:
    """ADR-028: ignora ``fed`` de IBS/CBS sem ``pCBS`` (container vazio)."""
    return incompleto_esperando(e, "fed", "pCBS")


def tolerar_uf_sem_aliquota_efetiva(e: ErroXsd) -> bool:
    """Ignora ``uf`` com a alíquota base mas sem ``pAliqEfetUF``."""
    return incompleto_esperando(e, "uf", "pAliqEfetUF")


def tolerar_mun_sem_aliquota_efetiva(e: ErroXsd) -> bool:
    """Ignora ``mun`` com a alíquota base mas sem ``pAliqEfetMun``."""
    return incompleto_esperando(e, "mun", "pAliqEfetMun")


def tolerar_fed_sem_aliquota_efetiva(e: ErroXsd) -> bool:
    """Ignora ``fed`` com a alíquota base mas sem ``pAliqEfetCBS``."""
    return incompleto_esperando(e, "fed", "pAliqEfetCBS")

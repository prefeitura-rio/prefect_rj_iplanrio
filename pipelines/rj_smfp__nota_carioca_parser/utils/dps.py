"""Extração da DPS, porta de ``DpsItem(XmlDps)`` e do INSERT de ``DPS`` (``Dps.vb``).

Fica de fora, por serem das regras do dbt (R-01, R-02): ``DATA_COMPETENCIA_MUNICIPIO``, ``PESSOA_RESPONSAVEL`` e
``PESSOA_CONTRAPARTE``. As subtabelas ``DPS_*`` (IBSCBS, obra, comércio exterior...) não alimentam nenhuma regra R-xx.
"""

from decimal import Decimal

from pipelines.rj_smfp__nota_carioca_parser.utils.leitor import (
    No,
    data_hora,
    decimal_ou_zero,
    inteiro,
    inteiro_ou_zero,
    iso,
    preenchido_a_esquerda,
    texto,
)
from pipelines.rj_smfp__nota_carioca_parser.utils.pessoa import Pessoa
from pipelines.rj_smfp__nota_carioca_parser.utils.pessoas import PessoasDoDocumento

ESCALA_VALOR = 2


def _valor(no: No, nome: str) -> Decimal:
    return decimal_ou_zero(no, ESCALA_VALOR, nome)


def _zero_vira_nulo(valor: int) -> int | None:
    return None if valor == 0 else valor


def _hash(pessoa: Pessoa | None) -> str | None:
    return None if pessoa is None else pessoa.hash_hex


def _servico(serv: No) -> dict[str, object]:
    codigo = serv("cServ")
    nacional = inteiro_ou_zero(codigo("cTribNac"), "cTribNac")
    municipal = inteiro_ou_zero(codigo("cTribMun"), "cTribMun")
    descricao = codigo("xDescServ").texto
    complementar = serv("infoCompl")
    return {
        "LOCALIDADE_PRESTACAO": inteiro_ou_zero(serv("locPrest", "cLocPrestacao"), "cLocPrestacao"),
        "PAIS_PRESTACAO": texto(serv("locPrest", "cPaisPrestacao")),
        "OPCAO_CONSUMO_SERVICO": None,
        "TRIBUTACAO_NACIONAL": nacional,
        "TRIBUTACAO_MUNICIPAL": municipal,
        "DESCRICAO_SERVICO": descricao if descricao and descricao.strip() else None,
        "NBS": inteiro_ou_zero(codigo("cNBS"), "cNBS"),
        "CODIGO_CONTRIBUINTE": texto(codigo("cIntContrib")),
        "RESPONSABILIDADE_TECNICA": texto(complementar("idDocTec")),
        "DOC_REFERENCIA": texto(complementar("docRef")),
        "INFORMACAO_COMPLEMENTAR": texto(complementar("xInfComp")),
        "NUMERO_PEDIDO": texto(complementar("xPed")),
        "GRUPO_SERVICO": preenchido_a_esquerda(nacional, 6)[:2],
        "SUBGRUPO_SERVICO": preenchido_a_esquerda(nacional, 6)[:4],
        "SERVICO_NACIONAL": preenchido_a_esquerda(nacional, 6) + preenchido_a_esquerda(municipal, 3),
    }


def _valores_basicos(valores: No) -> dict[str, object]:
    servico_prestado, desconto, deducao = valores("vServPrest"), valores("vDescCondIncond"), valores("vDedRed")
    return {
        "VALOR_RECEBIDO": _valor(servico_prestado("vReceb"), "vReceb"),
        "VALOR_SERVICO": _valor(servico_prestado("vServ"), "vServ"),
        "VALOR_DESCONTO_INCONDICIONADO": _valor(desconto("vDescIncond"), "vDescIncond"),
        "VALOR_DESCONTO_CONDICIONADO": _valor(desconto("vDescCond"), "vDescCond"),
        "PERCENTUAL_DEDUCAO_REDUCAO": decimal_ou_zero(deducao("pDR"), 2, "pDR"),
        "VALOR_DEDUCAO_REDUCAO": _valor(deducao("vDR"), "vDR"),
    }


def _tributo_municipal(trib_mun: No) -> dict[str, object]:
    beneficio, suspensao = trib_mun("BM"), trib_mun("exigSusp")
    nbm = inteiro_ou_zero(beneficio("nBM"), "nBM") if beneficio else None
    return {
        "ISSQN": inteiro(trib_mun("tribISSQN"), "tribISSQN"),
        "PAIS_RESULTADO": texto(trib_mun("cPaisResult")),
        "BENEFICIO_NACIONAL": inteiro(beneficio("tpBM"), "tpBM"),
        "BENEFICIO": None if nbm is None else str(nbm),
        "VALOR_REDUCAO_BC_BENEFICIO": _valor(beneficio("vRedBCBM"), "vRedBCBM"),
        "PERC_REDUCAO_BC_BENEFICIO": decimal_ou_zero(beneficio("pRedBCBM"), 2, "pRedBCBM"),
        "TIPO_SUSPENSAO": inteiro_ou_zero(suspensao("tpSusp"), "tpSusp") if suspensao else None,
        "NUMERO_PROCESSO": texto(suspensao("nProcesso")),
        "TIPO_IMUNIDADE_ISSQN": inteiro(trib_mun("tpImunidade"), "tpImunidade"),
        "ALIQUOTA": decimal_ou_zero(trib_mun("pAliq"), 2, "pAliq"),
        "TIPO_RETENCAO_ISSQN": inteiro(trib_mun("tpRetISSQN"), "tpRetISSQN"),
    }


def _tributo_federal(trib: No) -> dict[str, object]:
    """``tribFed`` e, na falta dele, ``tribNac`` (nome da estrutura antiga)."""
    federal = trib("tribFed") or trib("tribNac")
    pis = federal("piscofins")
    return {
        "CST": inteiro(pis("CST"), "CST"),
        "VALOR_BASE_CALCULO_PIS_COFINS": _valor(pis("vBCPisCofins"), "vBCPisCofins"),
        "ALIQUOTA_PIS": decimal_ou_zero(pis("pAliqPis"), 2, "pAliqPis"),
        "ALIQUOTA_COFINS": decimal_ou_zero(pis("pAliqCofins"), 2, "pAliqCofins"),
        "VALOR_PIS": _valor(pis("vPis"), "vPis"),
        "VALOR_COFINS": _valor(pis("vCofins"), "vCofins"),
        "TIPO_RETENCAO_PIS_COFINS": inteiro_ou_zero(pis("tpRetPisCofins"), "tpRetPisCofins") if pis else None,
        "VALOR_CP": _valor(federal("vRetCP"), "vRetCP"),
        "VALOR_IRRF": _valor(federal("vRetIRRF"), "vRetIRRF"),
        "VALOR_CSLL": _valor(federal("vRetCSLL"), "vRetCSLL"),
    }


def _tributo_total(total: No) -> dict[str, object]:
    monetario, percentual = total("vTotTrib"), total("pTotTrib")
    return {
        "VALOR_TOTAL_TRIBUTO_FEDERAL": _valor(monetario("vTotTribFed"), "vTotTribFed"),
        "VALOR_TOTAL_TRIBUTO_ESTADUAL": _valor(monetario("vTotTribEst"), "vTotTribEst"),
        "VALOR_TOTAL_TRIBUTO_MUNICIPAL": _valor(monetario("vTotTribMun"), "vTotTribMun"),
        "PERC_TOTAL_TRIBUTO_FEDERAL": decimal_ou_zero(percentual("pTotTribFed"), 2, "pTotTribFed"),
        "PERC_TOTAL_TRIBUTO_ESTADUAL": decimal_ou_zero(percentual("pTotTribEst"), 2, "pTotTribEst"),
        "PERC_TOTAL_TRIBUTO_MUNICIPAL": decimal_ou_zero(percentual("pTotTribMun"), 2, "pTotTribMun"),
        "INDICA_VALOR_TOTAL_TRIBUTOS": inteiro(total("indTotTrib"), "indTotTrib"),
        "PERC_TRIBUTO_SIMPLES_NACIONAL": decimal_ou_zero(total("pTotTribSN"), 2, "pTotTribSN"),
    }


def _identificacao(dps: No, inf: No) -> dict[str, object]:
    substituicao = inf("subst")
    motivo_subst = inteiro_ou_zero(substituicao("cMotivo"), "cMotivo")
    identificador = inf.atributo("Id")
    return {
        "DPS": identificador[3:] if identificador else None,
        "VERSAO": dps.atributo("versao") or None,
        "TIPO_AMBIENTE": inteiro_ou_zero(inf("tpAmb"), "tpAmb"),
        "DATA_EMISSAO": iso(data_hora(inf("dhEmi"), "dhEmi")),
        "VERSAO_APLICACAO": texto(inf("verAplic")),
        "SERIE_DPS": inteiro_ou_zero(inf("serie"), "serie"),
        "NUMERO_DPS": inteiro_ou_zero(inf("nDPS"), "nDPS"),
        "DATA_COMPETENCIA": iso(data_hora(inf("dCompet"), "dCompet")),
        "TIPO_EMITENTE": inteiro_ou_zero(inf("tpEmit"), "tpEmit"),
        "MUNICIPIO_EMISSOR": inteiro_ou_zero(inf("cLocEmi"), "cLocEmi"),
        "CHAVE_ACESSO_SUBSTITUIDA": texto(substituicao("chSubstda")),
        "MOTIVO_SUBSTITUICAO": _zero_vira_nulo(motivo_subst),
        "JUSTIFICATIVA_SUBSTITUICAO": texto(substituicao("xMotivo")),
        "CHAVE_ACESSO_REJEITADA": texto(inf("chNFSeRej")),
        "MOTIVO_EMISSAO_TOMADOR_INTERM": _zero_vira_nulo(inteiro_ou_zero(inf("cMotivoEmisTI"), "cMotivoEmisTI")),
    }


def extrair_dps(raiz: No, pessoas: PessoasDoDocumento) -> dict[str, object]:
    """Colunas de ``DFEN.DPS`` da DPS embutida na NFS-e (sem as derivadas do dbt)."""
    dps = raiz("infNFSe", "DPS")
    inf = dps("infDPS")
    valores = inf("valores")
    trib = valores("trib")
    linha = _identificacao(dps, inf)
    linha |= {
        "PESSOA_PRESTADOR": _hash(pessoas.prestador),
        "PESSOA_TOMADOR": _hash(pessoas.tomador),
        "PESSOA_INTERMEDIARIO": _hash(pessoas.intermediario),
    }
    linha |= _servico(inf("serv")) | _valores_basicos(valores)
    linha |= _tributo_municipal(trib("tribMun")) | _tributo_federal(trib) | _tributo_total(trib("totTrib"))
    return linha

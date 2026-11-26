"""Extração da NFS-e, porta de ``NotaNacionalItem(XmlNfse)`` e do INSERT de ``NOTAS_NACIONAIS`` (``NotasNacionais.vb``).

Fica de fora, por ser derivado a partir dos eventos e do cadastro (dbt: R-01, R-02, R-06, R-08, R-10): ``STATUS_NOTA``,
``DATA_CANCELAMENTO``, ``CHAVE_ACESSO_SUBSTITUTA``, ``DATA_COMPMUNICIPIO``, ``CENARIO_EXIGIBILIDADE``, ``FISCALIZACAO``
e ``CPF_CNPJ_RESPONSAVEL``. ``NOTA_NACIONAL``, ``CHAVE_ACESSO``, ``DPS`` e ``VALOR_DED_RED`` são derivados aqui.
"""

from decimal import Decimal

from pipelines.rj_smfp__nota_carioca_parser.utils.leitor import (
    No,
    data_hora,
    decimal_ou_zero,
    inteiro,
    inteiro_ou_zero,
    iso,
    texto,
)
from pipelines.rj_smfp__nota_carioca_parser.utils.pessoas import PessoasDoDocumento

PREFIXO_ID = 3  # "NFS"/"DPS" antes da chave


def _valor(no: No, nome: str, escala: int = 2) -> Decimal:
    return decimal_ou_zero(no, escala, nome)


def valor_ded_red(inf: No) -> Decimal:
    """``vCalcDR`` se maior que zero, senão ``vDR`` da DPS (``NotasNacionais.vb``), com 0 como último recurso.

    Só usa ``vDR`` da DPS se o grupo ``vDedRed`` existir; sem ele o campo ``Double`` do .NET fica em 0.
    """
    calculado = _valor(inf("valores", "vCalcDR"), "vCalcDR")
    if calculado > 0:
        return calculado
    deducao = inf("DPS", "infDPS", "valores", "vDedRed")
    return _valor(deducao("vDR"), "vDR") if deducao else Decimal("0.00")


def _descricao(inf: No) -> str | None:
    """``xOutInf`` de ``infNFSe`` (estrutura 1.01); na falta dele, o de ``valores`` (estrutura 1.00)."""
    return texto(inf("xOutInf")) or texto(inf("valores", "xOutInf"))


def extrair_nfse(raiz: No, pessoas: PessoasDoDocumento, nsu: int) -> dict[str, object]:
    """Colunas de ``DFEN.NOTAS_NACIONAIS`` da NFS-e (sem as derivadas do dbt), com os valores gravados pelo .NET."""
    inf = raiz("infNFSe")
    valores = inf("valores")
    identificador = inf.atributo("Id") or ""
    id_dps = inf("DPS", "infDPS").atributo("Id")
    return {
        "NOTA_NACIONAL": identificador or None,
        "CHAVE_ACESSO": identificador[3:] or None,
        "NOTA_FISCAL": inteiro_ou_zero(inf("nNFSe"), "nNFSe"),
        "INSCRICAO_EMITENTE": (inf("emit", "IM").texto or "") or None,
        "PESSOA_EMITENTE": None if pessoas.emitente is None else pessoas.emitente.hash_hex,
        "DESC_MUNICIPIO_EMISSOR": texto(inf("xLocEmi")),
        "DESC_LOCAL_PRESTACAO": texto(inf("xLocPrestacao")),
        "COD_LOCAL_INCIDENCIA": texto(inf("cLocIncid")),
        "DESC_LOCAL_INCIDENCIA": texto(inf("xLocIncid")),
        "DESC_TRIB_NACIONAL_ISSQN": texto(inf("xTribNac")),
        "DESC_TRIB_MUNICIPAL_ISSQN": texto(inf("xTribMun")),
        "DESC_NBS": texto(inf("xNBS")),
        "TIPO_EMISSAO_DPS": inteiro_ou_zero(inf("tpEmis"), "tpEmis"),
        "MODO_EMISSAO_DPS": inteiro_ou_zero(inf("procEmi"), "procEmi"),
        "COD_STATUS_MSG": inteiro(inf("cStat"), "cStat"),
        "DATA_VALIDACAO": iso(data_hora(inf("dhProc"), "dhProc")),
        "DPS": id_dps[PREFIXO_ID:] if id_dps else None,
        "VALOR_DED_RED": valor_ded_red(inf),
        "BENEFICIO_NACIONAL": texto(valores("tpBM")),
        "CALC_BM": _valor(valores("vCalcBM"), "vCalcBM"),
        "VALOR_BASE_CALCULO": _valor(valores("vBC"), "vBC"),
        "ALIQUOTA": _valor(valores("pAliqAplic"), "pAliqAplic"),
        "VALOR_ISSQN": _valor(valores("vISSQN"), "vISSQN"),
        "VALOR_TOTAL_RETENCOES": _valor(valores("vTotalRet"), "vTotalRet"),
        "VALOR_LIQUIDO": _valor(valores("vLiq"), "vLiq"),
        "DESCRICAO": _descricao(inf),
        "CHAVE_ACESSO_SUBSTITUIDA": texto(inf("DPS", "infDPS", "subst", "chSubstda")),
        "DFSE_SEQ": texto(inf("nDFSe")),
        "NSU": nsu,
        "VERSAO": raiz.atributo("versao") or None,
        "VERSAO_APLICACAO": texto(inf("verAplic")),
        "AMBIENTE_GERADOR": inteiro_ou_zero(inf("ambGer"), "ambGer"),
    }

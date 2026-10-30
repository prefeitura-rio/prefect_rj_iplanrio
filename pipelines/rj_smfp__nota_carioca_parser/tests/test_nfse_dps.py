"""Mapeamento de NFS-e e DPS: colunas de ``DFEN.NOTAS_NACIONAIS`` e ``DFEN.DPS`` com os valores que o .NET grava."""

from decimal import Decimal

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import ler_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.dps import extrair_dps
from pipelines.rj_smfp__nota_carioca_parser.utils.leitor import ErroExtracao, No
from pipelines.rj_smfp__nota_carioca_parser.utils.nfse import extrair_nfse
from pipelines.rj_smfp__nota_carioca_parser.utils.pessoas import ler_pessoas

BASE = xmls.nfse_xml()
D = Decimal


def mapear(xml: str = BASE, nsu: int = 1001) -> tuple[dict, dict]:
    raiz = No(ler_xml(xml))
    pessoas = ler_pessoas(raiz)
    return extrair_nfse(raiz, pessoas, nsu), extrair_dps(raiz, pessoas)


def test_nfse_identificadores_derivados_do_id_como_no_dotnet():
    nfse, _ = mapear()
    assert nfse["NOTA_NACIONAL"] == "NFS" + xmls.CHAVE_NFSE
    assert nfse["CHAVE_ACESSO"] == xmls.CHAVE_NFSE
    assert nfse["DPS"] == xmls.ID_DPS[3:]
    assert (nfse["NOTA_FISCAL"], nfse["DFSE_SEQ"], nfse["NSU"]) == (123, "123", 1001)


def test_nfse_colunas_de_texto_e_de_emissao():
    nfse, _ = mapear()
    assert nfse["DESC_MUNICIPIO_EMISSOR"] == "Rio de Janeiro"
    assert nfse["COD_LOCAL_INCIDENCIA"] == "3304557"
    assert nfse["DESC_TRIB_NACIONAL_ISSQN"] == "Servico de teste"
    assert nfse["DESC_NBS"] == "Servico NBS de teste"
    assert nfse["INSCRICAO_EMITENTE"] == "12345"
    assert (nfse["TIPO_EMISSAO_DPS"], nfse["MODO_EMISSAO_DPS"], nfse["AMBIENTE_GERADOR"]) == (1, 1, 2)
    assert (nfse["COD_STATUS_MSG"], nfse["VERSAO"], nfse["VERSAO_APLICACAO"]) == (100, "1.01", "sintetico-1.0")


def test_nfse_valores_em_numeric_com_escala_do_oracle():
    nfse, _ = mapear()
    assert (nfse["VALOR_BASE_CALCULO"], nfse["VALOR_ISSQN"], nfse["VALOR_LIQUIDO"], nfse["ALIQUOTA"]) == (
        D("1000.00"),
        D("50.00"),
        D("1000.00"),
        D("5.00"),
    )
    assert (nfse["CALC_BM"], nfse["VALOR_TOTAL_RETENCOES"]) == (D("0.00"), D("0.00"))


def test_nfse_data_de_validacao_na_hora_de_brasilia():
    assert mapear()[0]["DATA_VALIDACAO"] == "2026-02-10T10:15:30"
    outro_fuso = BASE.replace("2026-02-10T10:15:30-03:00", "2026-02-10T10:15:30-02:00")
    assert mapear(outro_fuso)[0]["DATA_VALIDACAO"] == "2026-02-10T09:15:30"


def test_nfse_nao_calcula_o_que_e_do_dbt():
    nfse, dps = mapear()
    derivadas = {
        "STATUS_NOTA",
        "DATA_CANCELAMENTO",
        "CHAVE_ACESSO_SUBSTITUTA",
        "DATA_COMPMUNICIPIO",
        "CENARIO_EXIGIBILIDADE",
        "FISCALIZACAO",
        "CPF_CNPJ_RESPONSAVEL",
    }
    assert derivadas.isdisjoint(nfse)
    assert {"DATA_COMPETENCIA_MUNICIPIO", "PESSOA_RESPONSAVEL", "PESSOA_CONTRAPARTE"}.isdisjoint(dps)


def test_nfse_referencia_as_pessoas_pelo_hash():
    raiz = No(ler_xml(BASE))
    pessoas = ler_pessoas(raiz)
    nfse, dps = mapear()
    assert nfse["PESSOA_EMITENTE"] == pessoas.emitente.hash_hex
    assert dps["PESSOA_PRESTADOR"] == pessoas.prestador.hash_hex
    assert dps["PESSOA_TOMADOR"] == pessoas.tomador.hash_hex
    assert dps["PESSOA_INTERMEDIARIO"] is None


@pytest.mark.parametrize(
    ("calc_dr", "ded_red", "esperado"),
    [
        (None, None, "0.00"),
        ("10.00", None, "10.00"),
        ("10.00", "<vDedRed><vDR>7.50</vDR></vDedRed>", "10.00"),
        (None, "<vDedRed><vDR>7.50</vDR></vDedRed>", "7.50"),
        ("0", "<vDedRed><vDR>7.50</vDR></vDedRed>", "7.50"),
        (None, "<vDedRed><pDR>10.00</pDR></vDedRed>", "0.00"),
    ],
    ids=["sem-deducao", "calculada", "calculada-vence", "da-dps", "calculada-zero", "so-percentual"],
)
def test_nfse_valor_ded_red_com_fallback(calc_dr, ded_red, esperado):
    xml = xmls.nfse_xml(xmls.Nfse(ded_red=ded_red))
    if calc_dr is not None:
        xml = xmls.inserir(xml, "//n:infNFSe/n:valores/n:vBC", f"<vCalcDR>{calc_dr}</vCalcDR>", depois=False)
    assert mapear(xml)[0]["VALOR_DED_RED"] == D(esperado)


def test_nfse_descricao_vem_do_xoutinf_de_infnfse_ou_de_valores():
    assert mapear()[0]["DESCRICAO"] is None
    em_inf = xmls.nfse_xml(xmls.Nfse(extra_inf_nfse="<xOutInf>Info da nota</xOutInf>"))
    assert mapear(em_inf)[0]["DESCRICAO"] == "Info da nota"
    em_valores = xmls.inserir(BASE, "//n:infNFSe/n:valores/n:vLiq", "<xOutInf>Info antiga</xOutInf>")
    assert mapear(em_valores)[0]["DESCRICAO"] == "Info antiga"


def test_nfse_ausencia_vira_zero_nos_numericos_e_nulo_nos_textos():
    sem_valores = xmls.mutar(BASE, "//n:infNFSe/n:valores/n:vISSQN", None)
    sem_texto = xmls.mutar(sem_valores, "//n:infNFSe/n:xTribMun", None)
    nfse, _ = mapear(sem_texto)
    assert nfse["VALOR_ISSQN"] == D("0.00")
    assert nfse["DESC_TRIB_MUNICIPAL_ISSQN"] is None


def test_nfse_texto_vazio_e_gravado_como_nulo_como_faz_o_oracle():
    assert mapear(xmls.mutar(BASE, "//n:emit/n:IM", ""))[0]["INSCRICAO_EMITENTE"] is None


def test_nfse_substituicao_traz_a_chave_substituida_e_o_motivo_da_dps():
    subst = f"<subst><chSubstda>{xmls.CHAVE_SUBSTITUTA}</chSubstda><cMotivo>01</cMotivo><xMotivo>Motivo da troca</xMotivo></subst>"
    xml = xmls.inserir(BASE, "//n:infDPS/n:cLocEmi", subst)
    nfse, dps = mapear(xml)
    assert nfse["CHAVE_ACESSO_SUBSTITUIDA"] == xmls.CHAVE_SUBSTITUTA
    assert (dps["CHAVE_ACESSO_SUBSTITUIDA"], dps["MOTIVO_SUBSTITUICAO"]) == (xmls.CHAVE_SUBSTITUTA, 1)
    assert dps["JUSTIFICATIVA_SUBSTITUICAO"] == "Motivo da troca"


def test_nfse_valor_numerico_invalido_aceito_pela_tolerancia_falha_na_extracao_como_no_dotnet():
    with pytest.raises(ErroExtracao):
        mapear(xmls.mutar(BASE, "//n:infNFSe/n:valores/n:vBC", "abc"))


def test_dps_identificacao_e_datas():
    _, dps = mapear()
    assert dps["DPS"] == xmls.ID_DPS[3:]
    assert (dps["SERIE_DPS"], dps["NUMERO_DPS"], dps["MUNICIPIO_EMISSOR"]) == (1, 123, 3304557)
    assert (dps["TIPO_AMBIENTE"], dps["TIPO_EMITENTE"], dps["VERSAO"]) == (2, 1, "1.01")
    assert dps["DATA_COMPETENCIA"] == "2026-02-10T00:00:00"
    assert dps["DATA_EMISSAO"] == "2026-02-10T09:00:00"


def test_dps_servico_e_derivadas_de_grupo_subgrupo_e_servico_nacional():
    _, dps = mapear()
    assert (dps["TRIBUTACAO_NACIONAL"], dps["TRIBUTACAO_MUNICIPAL"], dps["NBS"]) == (10101, 1, 115090000)
    assert (dps["GRUPO_SERVICO"], dps["SUBGRUPO_SERVICO"], dps["SERVICO_NACIONAL"]) == ("01", "0101", "010101001")
    assert (dps["LOCALIDADE_PRESTACAO"], dps["CODIGO_CONTRIBUINTE"]) == (3304557, "ABC123")


def test_dps_servico_sem_codigo_municipal_nem_nbs_grava_zero_como_o_dotnet():
    xml = xmls.mutar(xmls.mutar(BASE, "//n:cServ/n:cTribMun", None), "//n:cServ/n:cNBS", None)
    _, dps = mapear(xml)
    assert (dps["TRIBUTACAO_MUNICIPAL"], dps["NBS"], dps["SERVICO_NACIONAL"]) == (0, 0, "010101000")


@pytest.mark.parametrize("tp_ret", [1, 2, 3])
def test_dps_tributacao_municipal_e_tipo_de_retencao(tp_ret):
    _, dps = mapear(xmls.nfse_xml(xmls.Nfse(tp_ret=tp_ret)))
    assert (dps["ISSQN"], dps["TIPO_RETENCAO_ISSQN"], dps["ALIQUOTA"]) == (1, tp_ret, D("5.00"))


def test_dps_valores_ausentes_gravam_zero_e_grupos_ausentes_gravam_nulo():
    _, dps = mapear()
    assert dps["VALOR_SERVICO"] == D("1000.00")
    for coluna in ("VALOR_RECEBIDO", "VALOR_PIS", "VALOR_IRRF", "VALOR_TOTAL_TRIBUTO_FEDERAL", "PERC_TRIBUTO_SIMPLES_NACIONAL"):
        assert dps[coluna] == D("0.00"), coluna
    for coluna in ("CST", "TIPO_RETENCAO_PIS_COFINS", "BENEFICIO", "TIPO_SUSPENSAO", "MOTIVO_SUBSTITUICAO", "OPCAO_CONSUMO_SERVICO"):
        assert dps[coluna] is None, coluna
    assert dps["INDICA_VALOR_TOTAL_TRIBUTOS"] == 0


def test_dps_tributos_federais_totais_e_beneficio_quando_presentes():
    federal = (
        "<tribFed><piscofins><CST>01</CST><vBCPisCofins>100.00</vBCPisCofins><pAliqPis>0.65</pAliqPis>"
        "<pAliqCofins>3.00</pAliqCofins><vPis>0.65</vPis><vCofins>3.00</vCofins><tpRetPisCofins>1</tpRetPisCofins>"
        "</piscofins><vRetCP>1.00</vRetCP><vRetIRRF>2.00</vRetIRRF><vRetCSLL>3.00</vRetCSLL></tribFed>"
    )
    xml = xmls.inserir(BASE, "//n:trib/n:tribMun", federal)
    xml = xmls.inserir(
        xml, "//n:tribMun/n:tribISSQN", "<BM><tpBM>2</tpBM><nBM>00012345678901</nBM><pRedBCBM>10.00</pRedBCBM></BM>"
    )
    total = (
        "<totTrib><vTotTrib><vTotTribFed>1.10</vTotTribFed><vTotTribEst>2.20</vTotTribEst>"
        "<vTotTribMun>3.30</vTotTribMun></vTotTrib><indTotTrib>1</indTotTrib><pTotTribSN>4.00</pTotTribSN></totTrib>"
    )
    xml = xml.replace("<totTrib><indTotTrib>0</indTotTrib></totTrib>", total)
    _, dps = mapear(xml)
    assert (dps["CST"], dps["VALOR_BASE_CALCULO_PIS_COFINS"], dps["ALIQUOTA_PIS"], dps["ALIQUOTA_COFINS"]) == (
        1,
        D("100.00"),
        D("0.65"),
        D("3.00"),
    )
    assert (dps["TIPO_RETENCAO_PIS_COFINS"], dps["VALOR_CP"], dps["VALOR_IRRF"], dps["VALOR_CSLL"]) == (
        1,
        D("1.00"),
        D("2.00"),
        D("3.00"),
    )
    assert (dps["BENEFICIO_NACIONAL"], dps["BENEFICIO"], dps["PERC_REDUCAO_BC_BENEFICIO"]) == (2, "12345678901", D("10.00"))
    assert (dps["VALOR_TOTAL_TRIBUTO_FEDERAL"], dps["VALOR_TOTAL_TRIBUTO_MUNICIPAL"]) == (D("1.10"), D("3.30"))
    assert (dps["INDICA_VALOR_TOTAL_TRIBUTOS"], dps["PERC_TRIBUTO_SIMPLES_NACIONAL"]) == (1, D("4.00"))


def test_dps_tribnac_substitui_tribfed_quando_so_ele_existe():
    nacional = "<tribNac><vRetCP>9.00</vRetCP></tribNac>"
    assert mapear(xmls.inserir(BASE, "//n:trib/n:tribMun", nacional))[1]["VALOR_CP"] == D("9.00")


def test_dps_deducao_percentual_e_valor():
    xml = xmls.nfse_xml(xmls.Nfse(ded_red="<vDedRed><vDR>7.50</vDR></vDedRed>"))
    _, dps = mapear(xml)
    assert (dps["VALOR_DEDUCAO_REDUCAO"], dps["PERCENTUAL_DEDUCAO_REDUCAO"]) == (D("7.50"), D("0.00"))


def test_dps_intermediario_e_tomador_ausente():
    _, dps = mapear(xmls.nfse_xml(xmls.Nfse(tomador=None, intermediario=True, tp_ret=3)))
    assert dps["PESSOA_TOMADOR"] is None
    assert dps["PESSOA_INTERMEDIARIO"] is not None


def test_dps_arredonda_na_escala_da_coluna_como_o_oracle():
    assert mapear(xmls.mutar(BASE, "//n:tribMun/n:pAliq", "5.005"))[1]["ALIQUOTA"] == D("5.01")

"""Regras de tolerância de estrutura (ordem, conteúdo incompleto, assinatura, NT007, IBS/CBS), contra o libxml2 real."""

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.tests.auxiliares import (
    assert_regra_ignora,
    incompleto,
    inesperado,
)
from pipelines.rj_smfp__nota_carioca_parser.utils import tolerancias_estrutura as e
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento

BASE = xmls.nfse_xml()
BASE_1_00 = xmls.nfse_xml(xmls.Nfse(versao="1.00"))
ORDEM_1_01 = "<tpRetISSQN>1</tpRetISSQN><pAliq>5.00</pAliq>"
IBSCBS_DPS = (
    "<IBSCBS><finNFSe>0</finNFSe><cIndOp>100301</cIndOp><indDest>0</indDest>"
    "<valores><trib><gIBSCBS><CST>000</CST><cClassTrib>000001</cClassTrib></gIBSCBS></trib></valores></IBSCBS>"
)


def sem(xml: str, trecho: str, novo: str = "") -> str:
    assert trecho in xml
    return xml.replace(trecho, novo, 1)


def com_obra(xml: str, obra: str) -> str:
    return xmls.inserir(xml, "//n:serv/n:cServ", obra)


def com_ibscbs_dps() -> str:
    return xmls.inserir(BASE, "//n:infDPS/n:valores", IBSCBS_DPS)


def sem_transforms(xml: str) -> str:
    inicio = xml.index("<Transforms>")
    return xml[:inicio] + "<Transforms/>" + xml[xml.index("</Transforms>") + len("</Transforms>") :]


def mais_um_transform(xml: str) -> str:
    extra = '<Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>'
    return sem(xml, "</Transforms>", extra + "</Transforms>")


NO_ENDERECO = "<xLgr>Rua Ficticia</xLgr><nro>100</nro><xCpl>Sala 1</xCpl><xBairro>Centro</xBairro><cMun>3304557</cMun>"
LINHAS = [
    (
        e.tolerar_paliq_fora_de_ordem,
        lambda: sem(BASE, ORDEM_1_01, "<pAliq>5.00</pAliq><tpRetISSQN>1</tpRetISSQN>"),
        "1.01",
    ),
    (
        e.tolerar_xbairro_antes_de_nro,
        lambda: sem(BASE, NO_ENDERECO, "<xLgr>Rua Ficticia</xLgr><xBairro>Centro</xBairro><nro>100</nro><xCpl>Sala 1</xCpl><cMun>3304557</cMun>"),
        "1.01",
    ),
    (
        e.tolerar_cmun_antes_de_complemento,
        lambda: sem(BASE, NO_ENDERECO, "<xLgr>Rua Ficticia</xLgr><nro>100</nro><cMun>3304557</cMun><xCpl>Sala 1</xCpl><xBairro>Centro</xBairro>"),
        "1.01",
    ),
    (
        e.tolerar_cpaisprestacao_em_locprest,
        lambda: sem(BASE, "</cLocPrestacao>", "</cLocPrestacao><cPaisPrestacao>BR</cPaisPrestacao>"),
        "1.01",
    ),
    (e.tolerar_locprest_incompleto, lambda: sem(BASE, "<locPrest><cLocPrestacao>3304557</cLocPrestacao></locPrest>", "<locPrest/>"), "1.01"),
    (e.tolerar_obra_incompleta, lambda: com_obra(BASE, "<obra/>"), "1.01"),
    (
        e.tolerar_cobra_em_obra,
        lambda: com_obra(BASE_1_00, "<obra><inscImobFisc>123</inscImobFisc><cObra>1</cObra></obra>"),
        "1.00",
    ),
    (
        e.tolerar_end_em_obra,
        lambda: com_obra(
            BASE_1_00,
            "<obra><inscImobFisc>123</inscImobFisc><end><CEP>20000000</CEP><xLgr>Rua</xLgr><nro>1</nro><xBairro>Centro</xBairro></end></obra>",
        ),
        "1.00",
    ),
    (e.tolerar_tribnac_antes_de_tribfed, lambda: xmls.inserir(BASE, "//n:trib/n:tribMun", "<tribNac/>"), "1.01"),
    (
        e.tolerar_xoutinf_fora_de_ordem,
        lambda: xmls.inserir(BASE, "//n:infNFSe/n:valores", "<xOutInf>Info</xOutInf>", depois=False),
        "1.01",
    ),
    (
        e.tolerar_algoritmo_fora_da_enumeracao,
        lambda: sem(BASE_1_00, 'Transform Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"', 'Transform Algorithm="http://www.w3.org/TR/2001/REC-xml-exc-c14n#"'),
        "1.00",
    ),
    (
        e.tolerar_algoritmo_diferente_do_fixo,
        lambda: sem(BASE_1_00, "rsa-sha1", "rsa-sha256"),
        "1.00",
    ),
    (e.tolerar_transforms_sem_transform, lambda: sem_transforms(BASE_1_00), "1.00"),
    (e.tolerar_uri_vazia_em_reference, lambda: sem(BASE_1_00, f'URI="#NFS{xmls.CHAVE_NFSE}"', 'URI=""'), "1.00"),
    (
        e.tolerar_nbm_antes_de_tpbm,
        lambda: xmls.inserir(BASE_1_00, "//n:tribMun/n:tribISSQN", "<BM><nBM>123</nBM><tpBM>1</tpBM></BM>"),
        "1.00",
    ),
    (e.tolerar_transform_invalido_em_transforms, lambda: mais_um_transform(BASE_1_00), "1.00"),
    (
        e.tolerar_reference_invalida_em_signedinfo,
        lambda: sem(BASE_1_00, "</Reference>", '</Reference><Reference URI="#x"><DigestMethod Algorithm="http://www.w3.org/2000/09/xmldsig#sha1"/><DigestValue>AAAA</DigestValue></Reference>'),
        "1.00",
    ),
    (
        e.tolerar_x509subjectname_em_x509data,
        lambda: sem(BASE_1_00, "</X509Certificate>", "</X509Certificate><X509SubjectName>CN=x</X509SubjectName>"),
        "1.00",
    ),
    (e.tolerar_object_em_signature, lambda: sem(BASE_1_00, "</Signature>", "<Object/></Signature>"), "1.00"),
    (
        e.tolerar_nt007_novos_elementos_em_ibscbs,
        lambda: xmls.inserir(com_ibscbs_dps(), "//n:infDPS/n:IBSCBS/n:cIndOp", "<indZFMALC>1</indZFMALC>"),
        "1.01",
    ),
    (
        e.tolerar_nt007_gdedredibscbs,
        lambda: xmls.inserir(com_ibscbs_dps(), "//n:infDPS/n:IBSCBS/n:valores/n:trib", "<gDedRedIBSCBS/>", depois=False),
        "1.01",
    ),
    (
        e.tolerar_nt007_gestornocred_gpagantecipado,
        lambda: xmls.inserir(com_ibscbs_dps(), "//n:gIBSCBS/n:cClassTrib", "<gEstornoCred/>"),
        "1.01",
    ),
    (
        e.tolerar_nt007_vcalcdedredibscbs,
        lambda: xmls.inserir(xmls.nfse_xml(xmls.Nfse(ibscbs=True)), "//n:infNFSe/n:IBSCBS/n:valores/n:vBC", "<vCalcDedRedIBSCBS>1.00</vCalcDedRedIBSCBS>"),
        "1.01",
    ),
]


def ibscbs_sem(trecho: str, novo: str = "") -> str:
    return sem(xmls.nfse_xml(xmls.Nfse(ibscbs=True)), trecho, novo)


LINHAS_IBS = [
    (e.tolerar_uf_vazio, lambda: ibscbs_sem("<uf><pIBSUF>0.10</pIBSUF><pAliqEfetUF>0.10</pAliqEfetUF></uf>", "<uf/>")),
    (e.tolerar_mun_vazio, lambda: ibscbs_sem("<mun><pIBSMun>0.00</pIBSMun><pAliqEfetMun>0.00</pAliqEfetMun></mun>", "<mun/>")),
    (e.tolerar_fed_vazio, lambda: ibscbs_sem("<fed><pCBS>0.90</pCBS><pAliqEfetCBS>0.90</pAliqEfetCBS></fed>", "<fed/>")),
    (e.tolerar_uf_sem_aliquota_efetiva, lambda: ibscbs_sem("<pAliqEfetUF>0.10</pAliqEfetUF>")),
    (e.tolerar_mun_sem_aliquota_efetiva, lambda: ibscbs_sem("<pAliqEfetMun>0.00</pAliqEfetMun>")),
    (e.tolerar_fed_sem_aliquota_efetiva, lambda: ibscbs_sem("<pAliqEfetCBS>0.90</pAliqEfetCBS>")),
]


@pytest.mark.parametrize("linha", LINHAS, ids=[linha[0].__name__ for linha in LINHAS])
def test_validar_regra_de_estrutura_ignora_o_erro_do_libxml2(linha):
    regra, defeito, versao = linha
    assert_regra_ignora(regra, defeito(), versao)


@pytest.mark.parametrize("linha", LINHAS_IBS, ids=[linha[0].__name__ for linha in LINHAS_IBS])
def test_validar_regra_de_container_ibs_cbs_ignora_o_erro_do_libxml2(linha):
    regra, defeito = linha
    assert_regra_ignora(regra, defeito(), "1.01")


SINTETICAS = [
    (e.tolerar_nt007_indfinal_ausente, incompleto("IBSCBS", "indFinal", "cIndOp")),
    (e.tolerar_nt007_xped, inesperado("xPed", "infoCompl", "idDocTec", "docRef")),
    (e.tolerar_nt007_gitemped, inesperado("gItemPed", "infoCompl", "xInfComp")),
    (e.tolerar_nt007_ccib_em_obra, inesperado("cCIB", "obra", "cObra")),
    (e.tolerar_nt007_novos_elementos_em_ibscbs, inesperado("gLocBensMoveis", "IBSCBS", "valores")),
    (e.tolerar_nt007_novos_elementos_em_ibscbs, inesperado("indDoacao", "IBSCBS", "valores")),
]
COBERTAS = {linha[0] for linha in [*LINHAS, *LINHAS_IBS]} | {regra for regra, _ in SINTETICAS}


@pytest.mark.parametrize(("regra", "erro"), SINTETICAS)
def test_validar_regra_nt007_ignora_o_erro_no_formato_do_libxml2(regra, erro):
    assert regra(erro)


@pytest.mark.parametrize(
    ("regra", "erro"),
    [
        (e.tolerar_paliq_fora_de_ordem, inesperado("pAliq", "tribISSQN")),
        (e.tolerar_paliq_fora_de_ordem, inesperado("tpRetISSQN", "tribMun")),
        (e.tolerar_xbairro_antes_de_nro, inesperado("xBairro", "enderNac", "nro", "xCpl")),
        (e.tolerar_cmun_antes_de_complemento, inesperado("cMun", "enderNac", "xCpl")),
        (e.tolerar_locprest_incompleto, incompleto("locPrest", "cLocPrestacao")),
        (e.tolerar_obra_incompleta, incompleto("obra", "end")),
        (e.tolerar_xoutinf_fora_de_ordem, inesperado("xOutInf", "valores", "vLiq")),
        (e.tolerar_uf_vazio, incompleto("mun", "pIBSMun")),
        (e.tolerar_nt007_xped, inesperado("xPed", "obra", "cObra")),
        (e.tolerar_object_em_signature, inesperado("Object", "SignedInfo")),
    ],
)
def test_validar_regra_de_estrutura_nao_ignora_o_mesmo_defeito_em_outro_lugar(regra, erro):
    assert not regra(erro)

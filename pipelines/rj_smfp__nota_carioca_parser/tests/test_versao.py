"""Versão do documento: atributo ``versao``, limite 1.01 e promoção estrutural de 1.00 para 1.01."""

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import ler_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.erros import VersaoIlegivel, VersaoNaoSuportada
from pipelines.rj_smfp__nota_carioca_parser.utils.versao import Versao, detectar_versao_arvore

V1_00 = Versao(1, 0)
V1_01 = Versao(1, 1)


def detectar(xml: str, tipo: TipoDocumento = TipoDocumento.NFSE):
    return detectar_versao_arvore(ler_xml(xml), tipo)


@pytest.mark.parametrize(("texto", "esperada"), [("1.00", V1_00), ("1.01", V1_01), ("1.0", V1_00), (" 1.1 ", V1_01)])
def test_versao_lida_como_system_version(texto, esperada):
    assert Versao.ler(texto) == esperada


@pytest.mark.parametrize("texto", [None, "", "  ", "1", "abc", "1.x", "1.2.3.4.5"])
def test_versao_ilegivel_lanca_erro(texto):
    with pytest.raises(VersaoIlegivel):
        Versao.ler(texto)


@pytest.mark.parametrize("pasta", [(Versao(1, 0), "1.00"), (Versao(1, 1), "1.01"), (Versao(1, 10), "1.10")])
def test_versao_pasta_do_xsd(pasta):
    versao, esperada = pasta
    assert versao.pasta == esperada


@pytest.mark.parametrize("declarada", ["1.2", "1.02", "1.10", "2.0"])
def test_versao_acima_de_1_1_nao_suportada(declarada):
    xml = xmls.nfse_xml().replace('versao="1.01"', f'versao="{declarada}"', 1)
    with pytest.raises(VersaoNaoSuportada):
        detectar(xml)


def test_versao_ausente_ou_invalida_no_elemento_raiz_lanca_ilegivel():
    sem_versao = xmls.nfse_xml().replace(' versao="1.01"', "", 1)
    with pytest.raises(VersaoIlegivel):
        detectar(sem_versao)


def test_versao_1_00_com_ibscbs_em_infnfse_e_promovida_para_1_01():
    xml = xmls.nfse_xml(xmls.Nfse(versao="1.00", ibscbs=True))
    resultado = detectar(xml)
    assert (resultado.declarada, resultado.modelo, resultado.promovida) == (V1_00, V1_01, True)


def test_versao_1_00_sem_ibscbs_continua_1_00():
    resultado = detectar(xmls.nfse_xml(xmls.Nfse(versao="1.00")))
    assert (resultado.declarada, resultado.modelo, resultado.promovida) == (V1_00, V1_00, False)


def test_versao_ibscbs_dentro_da_dps_nao_promove():
    # Só conta IBSCBS filho direto de infNFSe; o da DPS (infNFSe/DPS/infDPS/IBSCBS) não promove.
    xml = xmls.inserir(
        xmls.nfse_xml(xmls.Nfse(versao="1.00")), "//n:infDPS/n:valores", "<IBSCBS><finNFSe>0</finNFSe></IBSCBS>"
    )
    assert not detectar(xml).promovida


def test_versao_declarada_1_01_nao_e_alterada():
    resultado = detectar(xmls.nfse_xml(xmls.Nfse(versao="1.01", ibscbs=True)))
    assert (resultado.declarada, resultado.modelo) == (V1_01, V1_01)


def test_versao_evento_1_00_com_ndfse_filho_de_infevento_e_promovido():
    resultado = detectar(xmls.evento_xml("e101101", versao="1.00"), TipoDocumento.EVENTO)
    assert (resultado.declarada, resultado.modelo) == (V1_00, V1_01)


def test_versao_evento_1_00_com_ndfe_legado_continua_1_00():
    xml = xmls.evento_xml("e101101", versao="1.00").replace("<nDFSe>123</nDFSe>", "<nDFe>123</nDFe>")
    assert detectar(xml, TipoDocumento.EVENTO).modelo == V1_00


def test_versao_evento_com_npedregevento_nao_e_promovido_por_infpedreg():
    # foundInfPedReg só vale se infPedReg for filho DIRETO de infEvento (profundidade + 1); na estrutura real não é.
    xml = xmls.evento_xml("e101101", versao="1.00").replace("<nDFSe>123</nDFSe>", "")
    assert detectar(xml, TipoDocumento.EVENTO).modelo == V1_00


def test_versao_evento_infpedreg_filho_direto_sem_npedregevento_e_promovido():
    xml = (
        f'<evento xmlns="{xmls.NS}" versao="1.00"><infEvento><infPedReg><tpAmb>2</tpAmb></infPedReg></infEvento></evento>'
    )
    assert detectar(xml, TipoDocumento.EVENTO).modelo == V1_01


def test_versao_evento_npedregevento_visto_antes_de_infpedreg_impede_a_promocao():
    # A varredura é em ordem de documento: nPedRegEvento só impede se vier ANTES de infPedReg ser aberto.
    xml = (
        f'<evento xmlns="{xmls.NS}" versao="1.00"><infEvento><nPedRegEvento>001</nPedRegEvento>'
        "<infPedReg><tpAmb>2</tpAmb></infPedReg></infEvento></evento>"
    )
    assert detectar(xml, TipoDocumento.EVENTO).modelo == V1_00


def test_versao_cnc_nunca_e_promovido():
    xml = f'<CNC xmlns="{xmls.NS}" versao="1.00"><infCNC><IBSCBS/></infCNC></CNC>'
    assert detectar(xml, TipoDocumento.CNC).modelo == V1_00

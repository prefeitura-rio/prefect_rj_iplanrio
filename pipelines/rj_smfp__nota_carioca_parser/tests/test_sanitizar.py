"""Sanitização do XML: porta de ``DFe.SanitizarXml`` (remove o que o XML 1.0 não admite, mantém o resto)."""

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.sanitizar import sanitizar_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.validar import validar_documento


def test_sanitizar_remove_byte_nulo_de_gzip():
    assert sanitizar_xml("<a>x\x00y</a>") == "<a>xy</a>"


@pytest.mark.parametrize("controle", ["\x01", "\x08", "\x0b", "\x0c", "\x0e", "\x1f"])
def test_sanitizar_remove_controles_abaixo_de_0x20(controle):
    assert sanitizar_xml(f"<a>x{controle}y</a>") == "<a>xy</a>"


def test_sanitizar_mantem_tab_lf_e_cr():
    assert sanitizar_xml("<a>\tx\ny\rz</a>") == "<a>\tx\ny\rz</a>"


def test_sanitizar_mantem_o_bom_porque_o_regex_do_dotnet_o_inclui_no_intervalo_valido():
    # [\uE000-\uFFFD] contém U+FEFF: o .NET NÃO remove o BOM (a conferência byte a byte com o Oracle depende disso).
    assert sanitizar_xml("\ufeff<a/>") == "\ufeff<a/>"


@pytest.mark.parametrize("fora", ["\ufffe", "\uffff", "\U0001f600", "\ud800"])
def test_sanitizar_remove_o_que_fica_fora_do_intervalo_valido_do_xml(fora):
    assert sanitizar_xml(f"<a>x{fora}y</a>") == "<a>xy</a>"


def test_sanitizar_mantem_acentos_e_texto_normal():
    texto = "<a>Serviço de manutenção — ação</a>"
    assert sanitizar_xml(texto) is texto


@pytest.mark.parametrize("vazio", [None, ""])
def test_sanitizar_devolve_nulo_e_vazio_como_vieram(vazio):
    assert sanitizar_xml(vazio) == vazio


def test_sanitizar_antes_de_validar_deixa_passar_xml_com_byte_nulo():
    sujo = xmls.nfse_xml().replace("<xLocEmi>Rio de Janeiro", "<xLocEmi>Rio\x00 de Janeiro")
    assert validar_documento(sujo, TipoDocumento.NFSE).versao.declarada.minor == 1


def test_sanitizar_antes_de_validar_aceita_xml_com_bom():
    com_bom = "\ufeff" + xmls.nfse_xml()
    assert validar_documento(com_bom, TipoDocumento.NFSE).xml.startswith("\ufeff")

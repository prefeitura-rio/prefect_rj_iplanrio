"""Validação de ponta a ponta: XSD oficial, promoção de versão, tolerâncias, erros fatais e XSD vendorizados."""

import hashlib

import pytest

from pipelines.rj_smfp__nota_carioca_parser.constants import SCHEMAS_DIR
from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import ler_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.erros import (
    VersaoNaoSuportada,
    XmlMalformado,
    XmlVazio,
    XsdInvalido,
)
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias import REGRAS_V101, regras_para
from pipelines.rj_smfp__nota_carioca_parser.utils.validar import validar_documento
from pipelines.rj_smfp__nota_carioca_parser.utils.versao import Versao
from pipelines.rj_smfp__nota_carioca_parser.utils.xsd import arquivo_xsd, coletar_erros_xsd


def erros_fatais(xml: str, tipo: TipoDocumento = TipoDocumento.NFSE) -> list[str]:
    with pytest.raises(XsdInvalido) as capturado:
        validar_documento(xml, tipo)
    return [str(e) for e in capturado.value.erros]


def test_validar_nfse_1_01_valida_e_so_toleram_a_serie_ancorada():
    validado = validar_documento(xmls.nfse_xml(), TipoDocumento.NFSE)
    assert validado.versao.declarada == Versao(1, 1)
    assert [e.elemento for e in validado.toleradas] == ["serie"]


def test_validar_nfse_1_00_valida_pelo_xsd_1_00():
    validado = validar_documento(xmls.nfse_xml(xmls.Nfse(versao="1.00")), TipoDocumento.NFSE)
    assert (validado.versao.declarada, validado.versao.modelo) == (Versao(1, 0), Versao(1, 0))
    assert validado.toleradas == ()


@pytest.mark.parametrize("versao", ["1.00", "1.01"])
def test_validar_evento_cancelamento_valido(versao):
    validado = validar_documento(xmls.evento_xml("e101101", versao), TipoDocumento.EVENTO)
    assert validado.versao.declarada == Versao.ler(versao)


def test_validar_1_00_com_ibscbs_e_promovido_e_validado_pelo_xsd_1_01():
    xml = xmls.nfse_xml(xmls.Nfse(versao="1.00", estrutura="1.01", ibscbs=True))
    validado = validar_documento(xml, TipoDocumento.NFSE)
    assert validado.versao.promovida
    assert validado.versao.modelo == Versao(1, 1)
    assert all(e.elemento == "serie" for e in validado.toleradas)  # o IBSCBS foi validado de verdade, sem tolerância


def test_validar_1_00_com_ibscbs_invalido_falha_porque_o_ibscbs_e_validado_na_1_01():
    base = xmls.nfse_xml(xmls.Nfse(versao="1.00", estrutura="1.01", ibscbs=True))
    xml = xmls.mutar(base, "//n:infNFSe/n:IBSCBS/n:cLocalidadeIncid", "x")
    assert any("cLocalidadeIncid" in erro for erro in erros_fatais(xml))


def test_validar_cep_curto_e_tolerado_e_fica_registrado():
    validado = validar_documento(xmls.mutar(xmls.nfse_xml(), "//n:emit//n:CEP", "1234"), TipoDocumento.NFSE)
    assert "CEP" in {e.elemento for e in validado.toleradas}


def test_validar_erro_fora_das_tolerancias_e_fatal():
    erros = erros_fatais(xmls.mutar(xmls.nfse_xml(), "//n:infDPS/n:tpAmb", "9"))
    assert len(erros) == 1
    assert "tpAmb" in erros[0]


def test_validar_cnpj_alfanumerico_nao_e_tolerado_e_vai_para_a_quarentena():
    # No .NET o CNPJ alfanumérico não está no TolerantValidationHandler: falha o XSD (NFSE-979, CA16).
    xml = xmls.mutar(xmls.nfse_xml(), "//n:emit/n:CNPJ", "AB123456000199")
    assert any("CNPJ" in erro for erro in erros_fatais(xml))


def test_validar_serie_nao_numerica_nao_e_ignorada_pela_compensacao_do_libxml2():
    erros = erros_fatais(xmls.mutar(xmls.nfse_xml(), "//n:infDPS/n:serie", "AB"))
    assert any("serie" in erro for erro in erros)


def test_validar_erro_oculto_depois_de_estrutura_tolerada_e_encontrado_na_revalidacao():
    # xOutInf fora de ordem é tolerado, mas o libxml2 pararia ali; o tpAmb inválido da DPS não pode passar.
    xml = xmls.inserir(xmls.nfse_xml(), "//n:infNFSe/n:valores", "<xOutInf>Info</xOutInf>", depois=False)
    xml = xmls.mutar(xml, "//n:infDPS/n:tpAmb", "9")
    assert any("tpAmb" in erro for erro in erros_fatais(xml))


def test_validar_estrutura_tolerada_sem_outros_erros_passa():
    xml = xmls.inserir(xmls.nfse_xml(), "//n:infNFSe/n:valores", "<xOutInf>Info</xOutInf>", depois=False)
    validado = validar_documento(xml, TipoDocumento.NFSE)
    assert "xOutInf" in {e.elemento for e in validado.toleradas}


@pytest.mark.parametrize("vazio", [None, "", "   \n"])
def test_validar_xml_vazio_lanca_xmlvazio(vazio):
    with pytest.raises(XmlVazio):
        validar_documento(vazio, TipoDocumento.NFSE)


def test_validar_xml_malformado_lanca_xmlmalformado():
    with pytest.raises(XmlMalformado):
        validar_documento("<NFSe><infNFSe></NFSe>", TipoDocumento.NFSE)


def test_validar_versao_1_2_lanca_versaonaosuportada():
    with pytest.raises(VersaoNaoSuportada):
        validar_documento(xmls.nfse_xml().replace('versao="1.01"', 'versao="1.2"', 1), TipoDocumento.NFSE)


def test_validar_regra_removida_deixa_o_defeito_fatal():
    sem_cep = lambda v: tuple(r for r in regras_para(v) if r.__name__ != "tolerar_cep_curto")  # noqa: E731
    xml = xmls.mutar(xmls.nfse_xml(), "//n:emit//n:CEP", "1234")
    with pytest.raises(XsdInvalido):
        validar_documento(xml, TipoDocumento.NFSE, sem_cep)


def test_validar_regras_da_v101_nao_valem_para_documento_1_00_sem_promocao():
    assert not set(REGRAS_V101) <= set(regras_para(Versao(1, 0)))


def test_validar_cnc_usa_sempre_o_xsd_1_00():
    assert arquivo_xsd(Versao(1, 1), TipoDocumento.CNC).parts[-2:] == ("1.00", "CNC_v1.00.xsd")


def test_validar_xsd_vendorizados_conferem_com_o_origem_txt():
    linhas = (SCHEMAS_DIR / "ORIGEM.txt").read_text(encoding="utf-8").splitlines()
    registrados = {}
    for linha in linhas:
        partes = linha.split()
        if len(partes) == 2 and len(partes[0]) == 64 and partes[1].endswith(".xsd"):
            registrados[partes[1]] = partes[0]
    arquivos = {str(p.relative_to(SCHEMAS_DIR)) for p in SCHEMAS_DIR.glob("*/*.xsd")}
    assert set(registrados) == arquivos
    for nome, esperado in registrados.items():
        assert hashlib.sha256((SCHEMAS_DIR / nome).read_bytes()).hexdigest() == esperado


def test_validar_faceta_de_tamanho_some_quando_o_pattern_do_mesmo_valor_tambem_falha():
    # O .NET para na primeira faceta (pattern antes de tamanho): um valor gera um erro só. O libxml2 reporta os dois.
    xml = xmls.mutar_atributo(
        xmls.evento_xml("e101101", "1.01"), "//n:infPedReg", "Id", "PRE" + xmls.CHAVE_NFSE + "101101001"
    )
    raiz = ler_xml(xml)
    erros = [e for e in coletar_erros_xsd(raiz, Versao(1, 1), TipoDocumento.EVENTO) if e.atributo == "Id"]
    assert [e.faceta for e in erros] == ["pattern"]

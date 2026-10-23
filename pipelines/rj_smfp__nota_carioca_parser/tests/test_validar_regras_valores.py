"""Uma regra de tolerância de valor por função, exercitada contra o erro real do libxml2 (XSD oficial)."""

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.tests.auxiliares import assert_regra_ignora, faceta
from pipelines.rj_smfp__nota_carioca_parser.utils import tolerancias_valores as v
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.erros_xsd import normalizar_erro

BASE = xmls.nfse_xml()
ATV_EVENTO = (
    "<atvEvento><xNome>Evento de teste</xNome><dtIni>2026-02-01</dtIni><dtFim>2026-02-02</dtFim>"
    "<idAtvEvt>ID123</idAtvEvt></atvEvento>"
)


def com_atv_evento() -> str:
    return xmls.inserir(BASE, "//n:serv/n:cServ", ATV_EVENTO)


def com_endereco_exterior() -> str:
    exterior = "<endExt><cPais>US</cPais><cEndPost>12345</cEndPost><xCidade>Miami</xCidade><xEstProvReg>FL</xEstProvReg></endExt>"
    xml = xmls.inserir(BASE, "//n:prest//n:endNac", exterior)
    return xmls.mutar(xml, "//n:prest//n:endNac", None)


LINHAS = [
    (v.tolerar_xnbs_texto, lambda: xmls.mutar(BASE, "//n:infNFSe/n:xNBS", "Servico NBS "), "1.01"),
    (v.tolerar_xtribmun_texto, lambda: xmls.mutar(BASE, "//n:infNFSe/n:xTribMun", "Servico "), "1.01"),
    (v.tolerar_cep_curto, lambda: xmls.mutar(BASE, "//n:emit//n:CEP", "1234"), "1.01"),
    (v.tolerar_fone_fora_da_faixa, lambda: xmls.mutar(BASE, "//n:emit/n:fone", "123"), "1.01"),
    (v.tolerar_dtfim_invalida, lambda: xmls.mutar(com_atv_evento(), "//n:atvEvento/n:dtFim", "1999-02-02"), "1.01"),
    (v.tolerar_dtini_invalida, lambda: xmls.mutar(com_atv_evento(), "//n:atvEvento/n:dtIni", "1999-02-01"), "1.01"),
    (
        v.tolerar_id_pedido_evento,
        lambda: xmls.mutar_atributo(xmls.evento_xml("e101101"), "//n:infPedReg", "Id", "PRE123"),
        "1.01",
        TipoDocumento.EVENTO,
    ),
    (v.tolerar_clocincid_invalido, lambda: xmls.mutar(BASE, "//n:infNFSe/n:cLocIncid", ""), "1.01"),
    (v.tolerar_xbairro_vazio, lambda: xmls.mutar(BASE, "//n:emit//n:xBairro", ""), "1.01"),
    (v.tolerar_xdescserv_vazio, lambda: xmls.mutar(BASE, "//n:xDescServ", ""), "1.01"),
    (
        v.tolerar_versao_fora_do_padrao,
        lambda: xmls.nfse_xml(xmls.Nfse(versao="1.00")).replace('versao="1.00"', 'versao="1.0"', 1),
        "1.00",
    ),
    (v.tolerar_im_vazia, lambda: xmls.mutar(BASE, "//n:emit/n:IM", ""), "1.01"),
    (v.tolerar_cendpost_vazio, lambda: xmls.mutar(com_endereco_exterior(), "//n:prest//n:cEndPost", ""), "1.01"),
    (v.tolerar_xnome_vazio, lambda: xmls.mutar(BASE, "//n:emit/n:xNome", ""), "1.01"),
    (v.tolerar_padrao_xlgr, lambda: xmls.mutar(BASE, "//n:emit//n:xLgr", "Rua Ficticia "), "1.01"),
    (v.tolerar_padrao_nro, lambda: xmls.mutar(BASE, "//n:emit//n:nro", "100 "), "1.01"),
    (v.tolerar_padrao_xcpl, lambda: xmls.mutar(BASE, "//n:emit//n:xCpl", "Sala 1 "), "1.01"),
    (v.tolerar_padrao_xbairro, lambda: xmls.mutar(BASE, "//n:emit//n:xBairro", "Centro "), "1.01"),
    (v.tolerar_padrao_xnome, lambda: xmls.mutar(com_atv_evento(), "//n:atvEvento/n:xNome", "Evento "), "1.01"),
    (v.tolerar_padrao_email, lambda: xmls.mutar(BASE, "//n:emit/n:email", "ficticio@example.invalid "), "1.01"),
    (v.tolerar_valor_monetario_fora_do_padrao, lambda: xmls.mutar(BASE, "//n:infNFSe/n:valores/n:vBC", "-1.00"), "1.01"),
    (
        v.tolerar_xsi_nil_indevido,
        lambda: BASE.replace(
            "<xNBS>", '<xNBS xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:nil="true">', 1
        ),
        "1.01",
    ),
]


COBERTAS = {linha[0] for linha in LINHAS} | {v.tolerar_padrao_xfant, v.tolerar_serie_dps_ancorada}


@pytest.mark.parametrize("linha", LINHAS, ids=[linha[0].__name__ for linha in LINHAS])
def test_validar_regra_de_valor_ignora_o_erro_do_libxml2(linha):
    regra, defeito, versao, *tipo = linha
    assert_regra_ignora(regra, defeito(), versao, *tipo)


def test_validar_regra_xfant_ignora_pattern_do_elemento():
    # O XSD oficial não dá pattern ao xFant do emitente; a regra do .NET cobre o elemento onde ele existir.
    assert v.tolerar_padrao_xfant(faceta("xFant", "pattern", "FANTASIA "))
    assert not v.tolerar_padrao_xfant(faceta("xNome", "pattern", "FANTASIA "))


def test_validar_serie_ancorada_e_erro_do_libxml2_que_a_regra_compensa():
    ancorado = normalizar_erro(
        "SCHEMAV_CVC_PATTERN_VALID",
        f"Element '{{{xmls.NS}}}serie': [facet 'pattern'] The value '00001' is not accepted by the pattern '{v.SERIE_ANCORADA}'.",
        ("NFSe", "infDPS", "serie"),
    )
    assert v.tolerar_serie_dps_ancorada(ancorado)


@pytest.mark.parametrize("valor", ["AB", "", "123456789012", "1 "])
def test_validar_serie_que_nao_casa_com_a_expressao_real_continua_erro(valor):
    erro = normalizar_erro(
        "SCHEMAV_CVC_PATTERN_VALID",
        f"Element '{{{xmls.NS}}}serie': [facet 'pattern'] The value '{valor}' is not accepted by the pattern '{v.SERIE_ANCORADA}'.",
        ("NFSe", "infDPS", "serie"),
    )
    assert not v.tolerar_serie_dps_ancorada(erro)


@pytest.mark.parametrize(
    ("regra", "elemento"),
    [
        (v.tolerar_cep_curto, "fone"),
        (v.tolerar_fone_fora_da_faixa, "CEP"),
        (v.tolerar_xnbs_texto, "xTribMun"),
        (v.tolerar_padrao_xlgr, "xTribNac"),
        (v.tolerar_clocincid_invalido, "cLocalidadeIncid"),
    ],
)
def test_validar_regra_de_valor_nao_ignora_o_mesmo_defeito_em_outro_elemento(regra, elemento):
    assert not regra(faceta(elemento, "pattern", "abc"))


def test_validar_valor_monetario_so_ignora_o_padrao_do_tsdec15v2():
    assert not v.tolerar_valor_monetario_fora_do_padrao(faceta("vBC", "pattern", "-1"))
    assert not v.tolerar_valor_monetario_fora_do_padrao(faceta("vBC", "enumeration", "-1"))

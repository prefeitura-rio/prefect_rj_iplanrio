"""Regras de tolerância de retrocompatibilidade por versão do XSD (V1.00 e V1.01), contra o libxml2 real."""

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.tests.auxiliares import (
    assert_regra_ignora,
    faceta,
    incompleto,
    inesperado,
)
from pipelines.rj_smfp__nota_carioca_parser.utils import tolerancias_versoes as t
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import TipoDocumento

EVENTO = TipoDocumento.EVENTO
NFSE_1_00 = xmls.nfse_xml(xmls.Nfse(versao="1.00"))
ATV_1_00 = "<atvEvento><{a}>Evento de teste</{a}><dtIni>2026-02-01</dtIni><dtFim>2026-02-02</dtFim><{b}>ID123</{b}></atvEvento>"


def sem(xml: str, trecho: str, novo: str = "") -> str:
    assert trecho in xml
    return xml.replace(trecho, novo, 1)


def evento_rejeicao(versao: str, detalhe: str) -> str:
    desc = "Manifestação de NFS-e - Rejeição do Prestador"
    return xmls.evento_xml("e202205", versao, detalhe=f"<e202205><xDesc>{desc}</xDesc>{detalhe}</e202205>")


def evento_ndfe(versao: str) -> str:
    return sem(xmls.evento_xml("e101101", versao), "<nDFSe>123</nDFSe>", "<nDFe>123</nDFe>")


LINHAS = [
    (t.tolerar_cstat_107, lambda: sem(NFSE_1_00, "<cStat>100</cStat>", "<cStat>107</cStat>"), "1.00", TipoDocumento.NFSE),
    (t.tolerar_ndfe_legado_em_infevento, lambda: evento_ndfe("1.00"), "1.00", EVENTO),
    (t.tolerar_ndfe_legado_em_infevento, lambda: evento_ndfe("1.01"), "1.01", EVENTO),
    (
        t.tolerar_xnome_em_atvevento,
        lambda: xmls.inserir(NFSE_1_00, "//n:serv/n:cServ", ATV_1_00.format(a="xNome", b="id")),
        "1.00",
        TipoDocumento.NFSE,
    ),
    (
        t.tolerar_idatvevt_em_atvevento,
        lambda: xmls.inserir(NFSE_1_00, "//n:serv/n:cServ", ATV_1_00.format(a="desc", b="idAtvEvt")),
        "1.00",
        TipoDocumento.NFSE,
    ),
    (
        t.tolerar_cmotivoemisti_em_infdps,
        lambda: xmls.inserir(NFSE_1_00, "//n:infDPS/n:tpEmit", "<cMotivoEmisTI>1</cMotivoEmisTI>"),
        "1.00",
        TipoDocumento.NFSE,
    ),
    (
        t.tolerar_chnfserej_em_infdps,
        lambda: xmls.inserir(NFSE_1_00, "//n:infDPS/n:tpEmit", f"<chNFSeRej>{xmls.CHAVE_NFSE}</chNFSeRej>"),
        "1.00",
        TipoDocumento.NFSE,
    ),
    (
        t.tolerar_npedregevento_em_infpedreg,
        lambda: xmls.inserir(xmls.evento_xml("e101101", "1.00"), "//n:infPedReg/n:chNFSe", "<nPedRegEvento>001</nPedRegEvento>"),
        "1.00",
        EVENTO,
    ),
    (
        t.tolerar_npedregevento_em_infpedreg,
        lambda: xmls.inserir(xmls.evento_xml("e101101", "1.01"), "//n:infPedReg/n:chNFSe", "<nPedRegEvento>001</nPedRegEvento>"),
        "1.01",
        EVENTO,
    ),
    (
        t.tolerar_inscimobfisc_em_obra,
        lambda: xmls.inserir(NFSE_1_00, "//n:serv/n:cServ", "<obra><cObra>1</cObra><inscImobFisc>123</inscImobFisc></obra>"),
        "1.00",
        TipoDocumento.NFSE,
    ),
    (
        t.tolerar_tpbm_valor_antigo,
        lambda: xmls.inserir(NFSE_1_00, "//n:tribMun/n:tribISSQN", "<BM><tpBM>9</tpBM><nBM>12345678901234</nBM></BM>"),
        "1.00",
        TipoDocumento.NFSE,
    ),
    (
        t.tolerar_xdesc_valor_antigo,
        lambda: sem(xmls.evento_xml("e101101", "1.01"), "Cancelamento de NFS-e", "Cancelamento antigo"),
        "1.01",
        EVENTO,
    ),
    (
        t.tolerar_xdesc_valor_antigo,
        lambda: sem(xmls.evento_xml("e101101", "1.00"), "Cancelamento de NFS-e", "Cancelamento antigo"),
        "1.00",
        EVENTO,
    ),
    (
        t.tolerar_inforej_legado,
        lambda: evento_rejeicao("1.01", "<infRej><cMotivo>1</cMotivo><xMotivo>Motivo da rejeicao</xMotivo></infRej>"),
        "1.01",
        EVENTO,
    ),
    (
        t.tolerar_ordem_em_e305102,
        lambda: xmls.evento_xml("e305102", "1.00"),
        "1.00",
        EVENTO,
    ),
    (
        t.tolerar_ibscbs_em_infnfse,
        lambda: xmls.nfse_xml(xmls.Nfse(versao="1.00", ibscbs=True)),
        "1.00",
        TipoDocumento.NFSE,
    ),
    (
        t.tolerar_id_pedido_formato_curto,
        lambda: xmls.mutar_atributo(xmls.evento_xml("e101101", "1.00"), "//n:infPedReg", "Id", "PRE123"),
        "1.00",
        EVENTO,
    ),
    (t.tolerar_cstat_101, lambda: sem(xmls.nfse_xml(), "<cStat>100</cStat>", "<cStat>101</cStat>"), "1.01", TipoDocumento.NFSE),
    (
        t.tolerar_eventos_907202_967203,
        lambda: xmls.evento_xml(
            "e101101", "1.01", detalhe="<e907202><xDesc>Evento removido do XSD</xDesc></e907202>"
        ),
        "1.01",
        EVENTO,
    ),
    (
        t.tolerar_id_pedido_formato_antigo,
        lambda: xmls.mutar_atributo(
            xmls.evento_xml("e101101", "1.01"), "//n:infPedReg", "Id", "PRE" + xmls.CHAVE_NFSE + "101101001"
        ),
        "1.01",
        EVENTO,
    ),
    (
        t.tolerar_id_evento_formato_antigo,
        lambda: xmls.mutar_atributo(xmls.evento_xml("e101101", "1.01"), "//n:infEvento", "Id", "EVT123"),
        "1.01",
        EVENTO,
    ),
]


@pytest.mark.parametrize("linha", LINHAS, ids=[f"{i}-{linha[0].__name__}" for i, linha in enumerate(LINHAS)])
def test_validar_regra_de_versao_ignora_o_erro_do_libxml2(linha):
    regra, defeito, versao, tipo = linha
    assert_regra_ignora(regra, defeito(), versao, tipo)


SINTETICAS = [
    (t.tolerar_cep_em_endereco_simples, inesperado("CEP", "end", "cEndPost", "xCidade")),
    (t.tolerar_endext_em_endereco_simples, inesperado("endExt", "end", "xLgr")),
    (t.tolerar_anulacao_inline_te205208, inesperado("CPFAgTrib", "e205208", "infAnRej")),
    (t.tolerar_anulacao_inline_te205208, inesperado("xMotivo", "e205208", "idEvManifRej")),
    (t.tolerar_inforej_legado, incompleto("infRej", "cMotivo")),
]
COBERTAS = {linha[0] for linha in LINHAS} | {regra for regra, _ in SINTETICAS}


@pytest.mark.parametrize(("regra", "erro"), SINTETICAS)
def test_validar_regra_de_versao_ignora_o_erro_no_formato_do_libxml2(regra, erro):
    assert regra(erro)


@pytest.mark.parametrize(
    ("regra", "erro"),
    [
        (t.tolerar_cstat_107, faceta("cStat", "enumeration", "999", "infNFSe")),
        (t.tolerar_cstat_101, faceta("cStat", "enumeration", "107", "infNFSe")),
        (t.tolerar_tpbm_valor_antigo, faceta("tpAmb", "pattern", "9", "infDPS")),
        (t.tolerar_xdesc_valor_antigo, faceta("cMotivo", "enumeration", "9", "e101101")),
        (t.tolerar_ndfe_legado_em_infevento, inesperado("nDFe", "infEvento", "pedRegEvento")),
        (t.tolerar_eventos_907202_967203, inesperado("e101101", "infPedReg", "chNFSe")),
        (t.tolerar_id_evento_formato_antigo, faceta("infPedReg", "pattern", "x", "pedRegEvento", atributo="Id")),
        (t.tolerar_anulacao_inline_te205208, inesperado("CPFAgTrib", "e305102", "xDesc")),
    ],
)
def test_validar_regra_de_versao_nao_ignora_o_mesmo_defeito_em_outro_lugar(regra, erro):
    assert not regra(erro)

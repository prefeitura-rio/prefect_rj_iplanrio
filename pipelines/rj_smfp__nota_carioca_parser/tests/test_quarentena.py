"""Quarentena: prioridade P1 a P5 (``ClassificadorPrioridadeQuarentena``) e devolução a cada nova versao_parser."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.utils.avaliacao import (
    Aprovado,
    Reprovado,
    avaliar_documento,
    linha_devolvido,
    linha_quarentena,
    reavaliar_quarentena,
)
from pipelines.rj_smfp__nota_carioca_parser.utils.documento import Documento, TipoDocumento
from pipelines.rj_smfp__nota_carioca_parser.utils.quarentena import (
    Prioridade,
    classificar_quarentena,
    decimal_invariante,
)
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias import regras_para

AGORA = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)


def nota(municipio: str = "3304557", iss: str = "50.00") -> str:
    xml = xmls.mutar(xmls.nfse_xml(), "//n:infNFSe/n:cLocIncid", municipio)
    return xmls.mutar(xml, "//n:infNFSe/n:valores/n:vISSQN", iss)


def sem_nota(_chave: str) -> str | None:
    return None


def classificar(xml, tipo=TipoDocumento.NFSE, chave=None, busca=sem_nota):
    return classificar_quarentena(tipo, xml, chave, busca)


def test_quarentena_p1_nota_no_rio_com_iss_maior_que_zero():
    assert classificar(nota("3304557", "50.00")) is Prioridade.RECEITA


def test_quarentena_p3_nota_no_rio_com_iss_zero():
    assert classificar(nota("3304557", "0")) is Prioridade.RIO_SEM_RECEITA


def test_quarentena_p4_nota_fora_do_rio():
    assert classificar(nota("3550308", "50.00")) is Prioridade.FORA_MUNICIPIO


@pytest.mark.parametrize(
    "xml",
    [None, "", "   ", "<NFSe><infNFSe>", "<NFSe/>", nota("3304557", "-1.00"), nota("3304557", "abc"), nota("", "5.00")],
    ids=["nulo", "vazio", "branco", "malformado", "sem-tags", "iss-negativo", "iss-texto", "municipio-vazio"],
)
def test_quarentena_p2_documento_ilegivel(xml):
    assert classificar(xml) is Prioridade.ILEGIVEL


def test_quarentena_p2_sem_a_tag_de_iss():
    sem_iss = xmls.mutar(xmls.nfse_xml(), "//n:infNFSe/n:valores/n:vISSQN", None)
    assert classificar(sem_iss) is Prioridade.ILEGIVEL


def test_quarentena_evento_herda_a_prioridade_da_nota_de_mesma_chave():
    chaves = {}

    def busca(chave):
        chaves["buscada"] = chave
        return nota("3304557", "50.00")

    prioridade = classificar(xmls.evento_xml(), TipoDocumento.EVENTO, xmls.CHAVE_NFSE, busca)
    assert prioridade is Prioridade.RECEITA
    assert chaves["buscada"] == xmls.CHAVE_NFSE


def test_quarentena_p5_evento_sem_nota_correspondente():
    assert classificar(xmls.evento_xml(), TipoDocumento.EVENTO, xmls.CHAVE_NFSE) is Prioridade.EVENTO_ORFAO


@pytest.mark.parametrize("chave", [None, "", "  "])
def test_quarentena_p5_evento_sem_chave_de_acesso(chave):
    assert classificar(xmls.evento_xml(), TipoDocumento.EVENTO, chave, lambda _: nota()) is Prioridade.EVENTO_ORFAO


@pytest.mark.parametrize("tipo", [None, TipoDocumento.CNC])
def test_quarentena_outros_tipos_sao_ilegiveis(tipo):
    assert classificar("<x/>", tipo) is Prioridade.ILEGIVEL


@pytest.mark.parametrize(
    ("texto", "valor"),
    [("50.00", "50.00"), (" 1,000.50 ", "1000.50"), ("(5)", "-5"), ("5-", "-5"), ("1e2", "100"), (".5", "0.5"), ("0", "0")],
)
def test_quarentena_decimal_invariante_aceita_o_que_o_dotnet_aceita(texto, valor):
    assert decimal_invariante(texto) == Decimal(valor)


@pytest.mark.parametrize("texto", ["", " ", "abc", "1,5,x", "--1", "1.2.3", "(1", "+1-"])
def test_quarentena_decimal_invariante_recusa_o_que_nao_e_numero(texto):
    assert decimal_invariante(texto) is None


def test_quarentena_erro_de_xsd_fora_das_tolerancias_recebe_a_prioridade_da_nota():
    defeito = xmls.mutar(nota("3304557", "50.00"), "//n:infDPS/n:tpAmb", "9")
    resultado = avaliar_documento(Documento(7, defeito, TipoDocumento.NFSE), sem_nota)
    assert isinstance(resultado, Reprovado)
    assert resultado.prioridade is Prioridade.RECEITA
    assert "tpAmb" in resultado.erro


def test_quarentena_nota_de_fora_do_rio_com_defeito_e_p4():
    defeito = xmls.mutar(nota("3550308", "50.00"), "//n:infDPS/n:tpAmb", "9")
    resultado = avaliar_documento(Documento(8, defeito, TipoDocumento.NFSE), sem_nota)
    assert isinstance(resultado, Reprovado)
    assert resultado.prioridade is Prioridade.FORA_MUNICIPIO


def test_quarentena_versao_nao_suportada_vai_para_a_quarentena_com_a_prioridade_da_nota():
    xml = nota().replace('versao="1.01"', 'versao="1.2"', 1)
    resultado = avaliar_documento(Documento(9, xml, TipoDocumento.NFSE), sem_nota)
    assert isinstance(resultado, Reprovado)
    assert resultado.prioridade is Prioridade.RECEITA
    assert "não suportada" in resultado.erro


def test_quarentena_tipo_de_documento_desconhecido_e_ilegivel_e_registrado():
    resultado = avaliar_documento(Documento(10, "<x/>", None), sem_nota)
    assert isinstance(resultado, Reprovado)
    assert resultado.prioridade is Prioridade.ILEGIVEL


def test_quarentena_xml_vazio_nunca_e_descartado_sem_registro():
    resultado = avaliar_documento(Documento(11, None, TipoDocumento.NFSE), sem_nota)
    assert isinstance(resultado, Reprovado)
    linha = linha_quarentena(resultado, "v1", AGORA)
    assert linha == {
        "nsu": 11,
        "prioridade": 2,
        "erro": "Arquivo XML não encontrado ou vazio.",
        "versao_parser": "v1",
        "avaliado_em": AGORA.isoformat(),
    }


def test_quarentena_devolucao_com_tolerancia_nova_tira_o_documento_da_quarentena():
    cep_curto = xmls.mutar(nota(), "//n:emit//n:CEP", "1234")
    documento = Documento(21, cep_curto, TipoDocumento.NFSE)

    def regras_antigas(versao):
        return tuple(r for r in regras_para(versao) if r.__name__ != "tolerar_cep_curto")

    antes = avaliar_documento(documento, sem_nota, regras_antigas)
    assert isinstance(antes, Reprovado)
    assert antes.prioridade is Prioridade.RECEITA

    resultado = reavaliar_quarentena([documento], sem_nota, regras_para)
    assert [a.documento.nsu for a in resultado.devolvidos] == [21]
    assert isinstance(resultado.devolvidos[0], Aprovado)
    assert resultado.mantidos == ()
    assert linha_devolvido(documento, "v2", AGORA) == {
        "nsu": 21,
        "prioridade": None,
        "erro": None,
        "versao_parser": "v2",
        "avaliado_em": AGORA.isoformat(),
    }


def test_quarentena_reavaliacao_mantem_o_que_continua_invalido_com_a_prioridade_atual_em_ordem():
    p4 = Documento(31, xmls.mutar(nota("3550308"), "//n:infDPS/n:tpAmb", "9"), TipoDocumento.NFSE)
    p1 = Documento(32, xmls.mutar(nota("3304557", "50.00"), "//n:infDPS/n:tpAmb", "9"), TipoDocumento.NFSE)
    bom = Documento(33, nota(), TipoDocumento.NFSE)
    resultado = reavaliar_quarentena([p4, p1, bom], sem_nota, regras_para)
    assert [a.documento.nsu for a in resultado.devolvidos] == [33]
    assert [(m.documento.nsu, m.prioridade) for m in resultado.mantidos] == [
        (32, Prioridade.RECEITA),
        (31, Prioridade.FORA_MUNICIPIO),
    ]

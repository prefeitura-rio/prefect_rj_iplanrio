"""Eventos: colunas de ``DFEN.EVENTOS_DFEN`` com os valores que ``Eventos.vb`` grava."""

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.utils.arvore import ler_xml
from pipelines.rj_smfp__nota_carioca_parser.utils.eventos import COLUNAS_EVENTO, extrair_evento
from pipelines.rj_smfp__nota_carioca_parser.utils.leitor import No


def evento(xml: str, nsu: int = 2002) -> dict[str, object]:
    return extrair_evento(No(ler_xml(xml)), nsu)


def test_evento_cancelamento_e101101_mapeia_as_26_colunas():
    linha = evento(xmls.evento_xml("e101101"))
    assert tuple(linha) == COLUNAS_EVENTO
    assert len(linha) == 26
    assert linha["TIPO_EVENTO"] == "e101101"
    assert linha["CHAVE_ACESSO"] == xmls.CHAVE_NFSE
    assert linha["EVENTO"] == xmls.CHAVE_NFSE + "101101001"
    assert linha["PEDIDO_EVENTO"] == "PRE" + xmls.CHAVE_NFSE + "101101"
    assert (linha["CODIGO_MOTIVO"], linha["MOTIVO_EVENTO"]) == ("1", "Erro na emissao do documento fiscal")
    assert linha["CPF_CNPJ_AUTOR_PEDIDO"] == xmls.CNPJ_PRESTADOR
    assert (linha["SEQUENCIAL_EVENTO"], linha["NUMERO_DFE"], linha["NSU"]) == ("1", 123, 2002)
    assert (linha["TIPO_AMBIENTE"], linha["AMBIENTE_GERADOR"]) == (2, "2")
    assert (linha["VERSAO_EVENTO"], linha["VERSAO_PEDIDO"]) == ("1.01", "1.01")
    assert (linha["VERSAO_APLICACAO_EVENTO"], linha["VERSAO_APLICACAO_PEDIDO"]) == ("sintetico-1.0",) * 2
    assert (linha["DATA_EVENTO"], linha["DATA_REGISTRO"]) == ("2026-02-11T07:30:00", "2026-02-11T08:00:00")


def test_evento_bloqueio_e305102_traz_agente_codigo_do_evento_bloqueado_e_motivo():
    linha = evento(xmls.evento_xml("e305102"))
    assert linha["TIPO_EVENTO"] == "e305102"
    assert linha["CPF_AGENTE_TRIBUTARIO"] == xmls.CPF_AUTOR
    assert linha["TIPO_EVENTO_NFSE"] == "e101101"
    assert linha["MOTIVO_EVENTO"] == "Bloqueio sintetico para teste"
    assert linha["CODIGO_MOTIVO"] == "0"


def test_evento_cancelamento_por_substituicao_e105102_traz_a_chave_substituta():
    linha = evento(xmls.evento_xml("e105102"))
    assert linha["TIPO_EVENTO"] == "e105102"
    assert (linha["CHAVE_ACESSO_SUBSTITUTA"], linha["CODIGO_MOTIVO"]) == (xmls.CHAVE_SUBSTITUTA, "1")


def test_evento_cancelamento_por_oficio_e305101_traz_o_processo_administrativo():
    linha = evento(xmls.evento_xml("e305101"))
    assert (linha["NUMERO_PROCESSO_ADM"], linha["DESCRICAO_PROCESSO_ADM"]) == ("123456", "Processo administrativo sintetico")
    assert linha["CPF_AGENTE_TRIBUTARIO"] == xmls.CPF_AUTOR


def test_evento_desbloqueio_e305103_traz_o_id_do_bloqueio():
    assert evento(xmls.evento_xml("e305103"))["EVENTO_BLOQUEIO_OFICIO"] == "0" * 59


def test_evento_anulacao_inline_e205208_le_motivo_agente_e_evento_rejeitado():
    detalhe = (
        f"<e205208><xDesc>Anulação</xDesc><CPFAgTrib>{xmls.CPF_AUTOR}</CPFAgTrib>"
        f"<idEvManifRej>{'1' * 59}</idEvManifRej><xMotivo>Anulacao sintetica da rejeicao</xMotivo></e205208>"
    )
    linha = evento(xmls.evento_xml("e205208", detalhe=detalhe))
    assert linha["EVENTO_MANIFESTA_REJEICAO"] == "1" * 59
    assert (linha["CPF_AGENTE_TRIBUTARIO"], linha["MOTIVO_EVENTO"]) == (xmls.CPF_AUTOR, "Anulacao sintetica da rejeicao")


def test_evento_anulacao_com_infanrej_prefere_os_campos_do_wrapper():
    detalhe = (
        f"<e205208><xDesc>Anulação</xDesc><infAnRej><CPFAgTrib>{xmls.CPF_AUTOR}</CPFAgTrib>"
        f"<idEvManifRej>{'2' * 59}</idEvManifRej><xMotivo>Motivo do wrapper</xMotivo></infAnRej></e205208>"
    )
    linha = evento(xmls.evento_xml("e205208", detalhe=detalhe))
    assert (linha["EVENTO_MANIFESTA_REJEICAO"], linha["MOTIVO_EVENTO"]) == ("2" * 59, "Motivo do wrapper")


def test_evento_rejeicao_antiga_com_infrej_usa_o_codigo_e_o_motivo_de_dentro():
    detalhe = "<e202205><xDesc>Rejeição</xDesc><infRej><cMotivo>3</cMotivo><xMotivo>Motivo antigo</xMotivo></infRej></e202205>"
    linha = evento(xmls.evento_xml("e202205", detalhe=detalhe))
    assert (linha["CODIGO_MOTIVO"], linha["MOTIVO_EVENTO"]) == ("3", "Motivo antigo")


def test_evento_numero_dfe_cai_para_ndfe_legado_e_zero_vira_nulo():
    legado = xmls.evento_xml("e101101").replace("<nDFSe>123</nDFSe>", "<nDFe>77</nDFe>")
    assert evento(legado)["NUMERO_DFE"] == 77
    sem_numero = xmls.evento_xml("e101101").replace("<nDFSe>123</nDFSe>", "")
    assert evento(sem_numero)["NUMERO_DFE"] is None


def test_evento_pedido_antigo_com_npedregevento_e_cpf_autor():
    xml = xmls.evento_xml("e101101").replace(
        f"<CNPJAutor>{xmls.CNPJ_PRESTADOR}</CNPJAutor>", f"<CPFAutor>{xmls.CPF_AUTOR}</CPFAutor>"
    )
    xml = xml.replace(f"<chNFSe>{xmls.CHAVE_NFSE}</chNFSe>", f"<chNFSe>{xmls.CHAVE_NFSE}</chNFSe><nPedRegEvento>001</nPedRegEvento>")
    linha = evento(xml)
    assert (linha["CPF_CNPJ_AUTOR_PEDIDO"], linha["NUMERO_PEDIDO"]) == (xmls.CPF_AUTOR, "001")


def test_evento_sem_detalhe_do_evento_grava_codigo_e_tipo_nulos():
    xml = xmls.evento_xml("e101101", detalhe="")
    linha = evento(xml)
    assert (linha["TIPO_EVENTO"], linha["CODIGO_MOTIVO"], linha["MOTIVO_EVENTO"]) == (None, None, None)


@pytest.mark.parametrize("nsu", [0])
def test_evento_nsu_zero_vira_nulo_como_no_oracle(nsu):
    assert evento(xmls.evento_xml("e101101"), nsu)["NSU"] is None

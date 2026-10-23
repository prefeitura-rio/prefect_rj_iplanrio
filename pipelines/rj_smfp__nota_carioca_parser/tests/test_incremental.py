"""Processamento incremental por NSU: lote, marca, falha de carga, reprocessamento sem duplicar e devolução."""

from collections import Counter

import pytest

from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.tests.fakes import INSTANTE, ArmazemLocal, FalhaDeCarga, LeitorLocal, montar
from pipelines.rj_smfp__nota_carioca_parser.utils.incremental import (
    ConfigIncremental,
    executar_incremental,
    identificador_do_lote,
)
from pipelines.rj_smfp__nota_carioca_parser.utils.tolerancias import regras_para

V1 = "teste-1"
V2 = "teste-2"
NFSE = xmls.nfse_xml()
CANCELAMENTO = xmls.evento_xml("e101101")
BLOQUEIO = xmls.evento_xml("e305102")
CHAVE = xmls.CHAVE_NFSE


def lote_basico() -> tuple[ArmazemLocal, LeitorLocal]:
    return montar(
        [
            (101, "NFSE", NFSE, CHAVE),
            (102, "EVENTO", CANCELAMENTO, CHAVE),
            (103, "EVENTO", BLOQUEIO, CHAVE),
        ]
    )


def rodar(armazem, leitor, versao=V1, por_versao=regras_para, **opcoes):
    config = ConfigIncremental(versao_parser=versao, **opcoes)
    return executar_incremental(armazem, leitor, config, lambda: INSTANTE, por_versao)


def sem_duplicados(armazem: ArmazemLocal) -> None:
    for tabela in ("nfse", "dps", "eventos"):
        assert len(set(armazem.nsus(tabela))) == len(armazem.nsus(tabela)), tabela
    chaves = [(p["NSU"], p["HASH_ID"]) for p in armazem.linhas("pessoas")]
    assert len(set(chaves)) == len(chaves)


def test_incremental_lote_com_nfse_cancelamento_e_bloqueio_gera_linhas_nas_quatro_tabelas_e_avanca_a_marca():
    armazem, leitor = lote_basico()
    resumo = rodar(armazem, leitor)
    assert armazem.nsus("nfse") == [101]
    assert armazem.nsus("dps") == [101]
    assert sorted(set(armazem.nsus("pessoas"))) == [101]
    assert armazem.nsus("eventos") == [102, 103]
    assert [e["TIPO_EVENTO"] for e in armazem.linhas("eventos")] == ["e101101", "e305102"]
    assert armazem.linhas("quarentena") == []
    assert armazem.ultima_marca() == 103
    assert (resumo.lotes, resumo.documentos, resumo.aprovados, resumo.recusados, resumo.ultimo_nsu) == (1, 3, 3, 0, 103)
    assert armazem.marcas == [(V1, 103)]


def test_incremental_linhas_levam_nsu_geracao_versao_e_instante_de_ingestao():
    armazem, leitor = lote_basico()
    rodar(armazem, leitor)
    for linha in armazem.linhas("eventos") + armazem.linhas("dps"):
        assert linha["VERSAO_PARSER"] == V1
        assert linha["INGERIDO_EM"] == INSTANTE.isoformat()
        assert linha["DATA_HORA_GERACAO"] == "2026-02-10T13:15:30+00:00"


def test_incremental_falha_no_load_de_eventos_nao_avanca_a_marca_e_o_reprocessamento_nao_duplica():
    armazem, leitor = lote_basico()
    armazem.falhas["eventos"] = 1
    with pytest.raises(FalhaDeCarga):
        rodar(armazem, leitor)
    assert armazem.ultima_marca() == 0
    assert armazem.nsus("nfse") == [101]  # nfse, dps e pessoas já tinham sido carregados
    assert armazem.nsus("eventos") == []

    resumo = rodar(armazem, leitor)

    assert armazem.ultima_marca() == 103
    assert resumo.ultimo_nsu == 103
    assert armazem.nsus("nfse") == [101]
    assert armazem.nsus("eventos") == [102, 103]
    sem_duplicados(armazem)
    assert Counter(armazem.cargas).most_common(1)[0][1] == 1


def test_incremental_reprocessamento_usa_o_mesmo_lote_id_por_tabela():
    armazem, leitor = lote_basico()
    armazem.falhas["eventos"] = 1
    with pytest.raises(FalhaDeCarga):
        rodar(armazem, leitor)
    antes = list(armazem.cargas)
    rodar(armazem, leitor)
    assert armazem.cargas[: len(antes)] == antes
    assert {lote for _, lote in armazem.cargas} == {identificador_do_lote(V1, [101, 102, 103])}


def test_incremental_sem_documentos_novos_nao_carrega_nem_move_a_marca():
    armazem, leitor = lote_basico()
    rodar(armazem, leitor)
    cargas = list(armazem.cargas)
    resumo = rodar(armazem, leitor)
    assert armazem.cargas == cargas
    assert (resumo.lotes, resumo.documentos, resumo.ultimo_nsu) == (0, 0, 103)


def test_incremental_so_le_o_que_esta_acima_da_marca():
    armazem, leitor = lote_basico()
    rodar(armazem, leitor)
    armazem.entradas.append(montar([(104, "EVENTO", CANCELAMENTO, CHAVE)])[0].entradas[0])
    leitor.objetos["gs://bucket/adn/producao/dt=2026-02-10/lote_0000.ndjson.gz"][104] = {"nsu": 104, "xml": CANCELAMENTO}
    rodar(armazem, leitor)
    assert armazem.nsus("eventos") == [102, 103, 104]
    assert armazem.nsus("nfse") == [101]


def test_incremental_lotes_menores_avancam_a_marca_a_cada_lote_e_param_na_falha():
    docs = [(n, "EVENTO", CANCELAMENTO, CHAVE) for n in range(201, 207)]
    armazem, leitor = montar(docs)
    armazem.falhas["eventos"] = 0
    rodar(armazem, leitor, nsus_por_lote=2, max_lotes=2)
    assert armazem.ultima_marca() == 204
    assert armazem.nsus("eventos") == [201, 202, 203, 204]
    armazem.falhas["eventos"] = 1
    with pytest.raises(FalhaDeCarga):
        rodar(armazem, leitor, nsus_por_lote=2)
    assert armazem.ultima_marca() == 204  # a marca fica no fim do último lote carregado
    rodar(armazem, leitor, nsus_por_lote=2)
    assert armazem.nsus("eventos") == [201, 202, 203, 204, 205, 206]


def test_incremental_documento_invalido_vai_para_a_quarentena_com_prioridade_e_a_marca_avanca():
    defeituoso = xmls.mutar(NFSE, "//n:infDPS/n:tpAmb", "9")
    armazem, leitor = montar([(301, "NFSE", defeituoso, CHAVE), (302, "EVENTO", CANCELAMENTO, CHAVE)])
    resumo = rodar(armazem, leitor)
    [recusa] = armazem.linhas("quarentena")
    assert (recusa["nsu"], recusa["prioridade"], recusa["versao_parser"]) == (301, 1, V1)
    assert "tpAmb" in recusa["erro"]
    assert armazem.nsus("nfse") == []
    assert armazem.nsus("eventos") == [302]
    assert (resumo.aprovados, resumo.recusados, armazem.ultima_marca()) == (1, 1, 302)


def test_incremental_evento_recusado_herda_a_prioridade_da_nfse_do_indice():
    armazem, leitor = montar([(401, "NFSE", NFSE, CHAVE), (402, "EVENTO", "<evento><infEvento>", CHAVE)])
    rodar(armazem, leitor)
    [recusa] = armazem.linhas("quarentena")
    assert (recusa["nsu"], recusa["prioridade"]) == (402, 1)


def test_incremental_evento_recusado_sem_nfse_no_indice_e_p5():
    armazem, leitor = montar([(501, "EVENTO", "<evento><infEvento>", CHAVE)])
    rodar(armazem, leitor)
    assert armazem.linhas("quarentena")[0]["prioridade"] == 5


def test_incremental_nsu_do_indice_sem_linha_no_objeto_nao_e_descartado():
    armazem, leitor = montar([(601, "NFSE", NFSE, CHAVE), (602, "NFSE", NFSE, CHAVE)], sem_linha=[602])
    rodar(armazem, leitor)
    [recusa] = armazem.linhas("quarentena")
    assert (recusa["nsu"], recusa["prioridade"]) == (602, 2)
    assert "vazio" in recusa["erro"]
    assert armazem.ultima_marca() == 602


def test_incremental_tipo_de_documento_desconhecido_e_registrado():
    armazem, leitor = montar([(701, "OUTRO", "<x/>", None)])
    rodar(armazem, leitor)
    assert armazem.linhas("quarentena")[0]["prioridade"] == 2


def test_incremental_nao_deduplica_a_mesma_nfse_em_dois_nsus():
    armazem, leitor = montar([(801, "NFSE", NFSE, CHAVE), (802, "NFSE", NFSE, CHAVE)])
    rodar(armazem, leitor)
    assert armazem.nsus("nfse") == [801, 802]
    assert [linha["NOTA_NACIONAL"] for linha in armazem.linhas("nfse")] == ["NFS" + CHAVE] * 2


def test_incremental_cnc_valido_nao_gera_linhas_mas_e_contado_e_a_marca_avanca():
    armazem, leitor = montar([(901, "CNC", xmls.cnc_xml(), None)])
    resumo = rodar(armazem, leitor)
    assert all(not linhas for linhas in armazem.tabelas.values())
    assert (resumo.documentos, resumo.aprovados, resumo.cnc, resumo.recusados) == (1, 1, 1, 0)
    assert armazem.ultima_marca() == 901


def test_incremental_cnc_invalido_vai_para_a_quarentena_como_ilegivel():
    armazem, leitor = montar([(902, "CNC", xmls.mutar(xmls.cnc_xml(), "//n:cMun", "x"), None)])
    rodar(armazem, leitor)
    assert armazem.linhas("quarentena")[0]["prioridade"] == 2


def test_incremental_objeto_e_lido_uma_vez_por_lote():
    armazem, leitor = lote_basico()
    rodar(armazem, leitor)
    assert set(leitor.leituras.values()) == {1}


def test_incremental_devolucao_com_tolerancia_nova_tira_o_documento_da_quarentena():
    cep_curto = xmls.mutar(NFSE, "//n:emit//n:CEP", "1234")
    armazem, leitor = montar([(1001, "NFSE", cep_curto, CHAVE)])

    def regras_antigas(versao):
        return tuple(r for r in regras_para(versao) if r.__name__ != "tolerar_cep_curto")

    rodar(armazem, leitor, V1, regras_antigas)
    assert armazem.linhas("quarentena")[0]["prioridade"] == 1
    assert armazem.nsus("nfse") == []

    resumo = rodar(armazem, leitor, V2)

    assert armazem.nsus("nfse") == [1001]
    assert resumo.devolvidos == 1
    ultimas = {}
    for linha in armazem.linhas("quarentena"):
        ultimas[linha["nsu"]] = linha
    assert ultimas[1001]["prioridade"] is None and ultimas[1001]["versao_parser"] == V2
    assert armazem.listar_quarentena_pendente("versao-futura", 10) == []
    assert armazem.ultima_marca() == 1001
    sem_duplicados(armazem)


def test_incremental_versao_nova_continua_da_marca_anterior_sem_reprocessar_o_historico():
    armazem, leitor = lote_basico()
    rodar(armazem, leitor, V1)
    cargas = list(armazem.cargas)
    resumo = rodar(armazem, leitor, V2)
    assert armazem.cargas == cargas
    assert (resumo.lotes, resumo.documentos, resumo.ultimo_nsu) == (0, 0, 103)
    assert armazem.marcas == [(V1, 103)]


def test_incremental_quarentena_so_e_reavaliada_quando_a_versao_muda():
    defeituoso = xmls.mutar(NFSE, "//n:infDPS/n:tpAmb", "9")
    armazem, leitor = montar([(1101, "NFSE", defeituoso, CHAVE)])
    rodar(armazem, leitor, V1)
    rodar(armazem, leitor, V1)
    assert len(armazem.linhas("quarentena")) == 1
    resumo = rodar(armazem, leitor, V2)
    assert [(q["versao_parser"], q["prioridade"]) for q in armazem.linhas("quarentena")] == [(V1, 1), (V2, 1)]
    assert resumo.devolvidos == 0 and resumo.recusados >= 1


def test_incremental_devolucao_pode_ser_desligada():
    defeituoso = xmls.mutar(NFSE, "//n:emit//n:CEP", "1234")
    armazem, leitor = montar([(1201, "NFSE", defeituoso, CHAVE)])
    rodar(armazem, leitor, V1, lambda v: tuple(r for r in regras_para(v) if r.__name__ != "tolerar_cep_curto"))
    resumo = rodar(armazem, leitor, V2, reavaliar_quarentena=False)
    assert resumo.devolvidos == 0 and armazem.nsus("nfse") == []


def test_incremental_quarentena_reavaliada_vem_na_ordem_de_prioridade():
    p4 = xmls.mutar(xmls.mutar(NFSE, "//n:infNFSe/n:cLocIncid", "3550308"), "//n:infDPS/n:tpAmb", "9")
    p1 = xmls.mutar(NFSE, "//n:infDPS/n:tpAmb", "9")
    armazem, leitor = montar([(1301, "NFSE", p4, CHAVE), (1302, "NFSE", p1, CHAVE)])
    rodar(armazem, leitor, V1)
    assert armazem.listar_quarentena_pendente(V2, 10) == [1302, 1301]
    assert armazem.listar_quarentena_pendente(V2, 1) == [1302]


def test_incremental_identificador_do_lote_nao_depende_da_ordem_e_muda_com_o_conjunto():
    assert identificador_do_lote(V1, [3, 1, 2]) == identificador_do_lote(V1, [1, 2, 3])
    assert identificador_do_lote(V1, [1, 2, 3]) != identificador_do_lote(V1, [1, 2, 4])
    assert identificador_do_lote(V1, [1, 2, 3]) != identificador_do_lote(V2, [1, 2, 3])

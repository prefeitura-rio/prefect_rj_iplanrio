"""Schemas BigQuery versionados em JSON: iguais ao que os extratores produzem e ao contrato com o dbt."""

import json

import pytest

from pipelines.rj_smfp__nota_carioca_parser.constants import SCHEMAS_BQ_DIR
from pipelines.rj_smfp__nota_carioca_parser.tests import xmls
from pipelines.rj_smfp__nota_carioca_parser.tests.fakes import INSTANTE, montar
from pipelines.rj_smfp__nota_carioca_parser.utils.incremental import ConfigIncremental, executar_incremental
from pipelines.rj_smfp__nota_carioca_parser.utils.linhas import COLUNAS_METADADOS, serializar_ndjson
from pipelines.rj_smfp__nota_carioca_parser.utils.tabelas import ESPECIFICACOES, ORDEM_DE_CARGA, colunas, schema_bigquery

TIPOS_BQ = {"STRING", "INT64", "NUMERIC", "DATETIME", "TIMESTAMP"}
DE_DADOS = ("nfse", "dps", "pessoas", "eventos")


def test_schemas_bq_existe_um_json_por_tabela():
    assert {p.stem for p in SCHEMAS_BQ_DIR.glob("*.json")} == set(ESPECIFICACOES)


@pytest.mark.parametrize("tabela", sorted(ESPECIFICACOES))
def test_schemas_bq_campos_bem_formados(tabela):
    for campo in schema_bigquery(tabela):
        assert campo["type"] in TIPOS_BQ
        assert campo["mode"] in {"NULLABLE", "REQUIRED"}
    nomes = colunas(tabela)
    assert len(set(nomes)) == len(nomes)


@pytest.mark.parametrize("tabela", DE_DADOS)
def test_schemas_bq_tabelas_de_dados_tem_os_metadados_e_nomes_do_oracle_em_maiusculas(tabela):
    nomes = colunas(tabela)
    assert all(n == n.upper() for n in nomes)
    assert set(COLUNAS_METADADOS) <= set(nomes)
    modos = {c["name"]: c["mode"] for c in schema_bigquery(tabela)}
    assert (modos["NSU"], modos["VERSAO_PARSER"], modos["INGERIDO_EM"]) == ("REQUIRED",) * 3


def test_schemas_bq_quarentena_e_marca_seguem_o_contrato():
    assert colunas("quarentena") == ("nsu", "prioridade", "erro", "versao_parser", "avaliado_em")
    assert colunas("parser_marca") == ("versao_parser", "ultimo_nsu")


def test_schemas_bq_tabelas_de_dados_particionadas_por_data_de_geracao_e_com_cluster():
    for tabela in DE_DADOS:
        especificacao = ESPECIFICACOES[tabela]
        assert especificacao.particao == "DATA_HORA_GERACAO"
        assert especificacao.cluster and set(especificacao.cluster) <= set(colunas(tabela))
    assert set(ORDEM_DE_CARGA) == {*DE_DADOS, "quarentena"}


def test_schemas_bq_nao_tem_colunas_derivadas_do_dbt_nem_o_guid_do_oracle():
    proibidas = {
        "STATUS_NOTA",
        "DATA_CANCELAMENTO",
        "CHAVE_ACESSO_SUBSTITUTA_NOTA",
        "DATA_COMPMUNICIPIO",
        "CENARIO_EXIGIBILIDADE",
        "FISCALIZACAO",
        "CPF_CNPJ_RESPONSAVEL",
        "DATA_COMPETENCIA_MUNICIPIO",
        "PESSOA_RESPONSAVEL",
        "PESSOA_CONTRAPARTE",
        "PESSOA_NACIONAL",
    }
    todas = {n for t in DE_DADOS for n in colunas(t)}
    assert not proibidas & todas


def test_schemas_bq_linhas_dos_extratores_tem_exatamente_as_colunas_do_json():
    docs = [
        (1, "NFSE", xmls.nfse_xml(xmls.Nfse(intermediario=True)), xmls.CHAVE_NFSE),
        (2, "EVENTO", xmls.evento_xml("e305102"), xmls.CHAVE_NFSE),
    ]
    armazem, leitor = montar(docs)
    executar_incremental(armazem, leitor, ConfigIncremental("v"), lambda: INSTANTE)
    for tabela in DE_DADOS:
        assert armazem.linhas(tabela), tabela
        for linha in armazem.linhas(tabela):
            assert set(linha) == set(colunas(tabela))  # o fake também confere isto a cada carga


def test_schemas_bq_serializacao_ndjson_mantem_numeric_exato_e_nulos():
    armazem, leitor = montar([(1, "NFSE", xmls.nfse_xml(), xmls.CHAVE_NFSE)])
    executar_incremental(armazem, leitor, ConfigIncremental("v"), lambda: INSTANTE)
    registro = json.loads(serializar_ndjson(armazem.linhas("nfse")).decode())
    assert registro["VALOR_ISSQN"] == "50.00"
    assert registro["NOTA_FISCAL"] == 123
    assert registro["DESCRICAO"] is None
    assert registro["DATA_VALIDACAO"] == "2026-02-10T10:15:30"

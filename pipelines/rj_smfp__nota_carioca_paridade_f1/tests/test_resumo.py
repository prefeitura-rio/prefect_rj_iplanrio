"""Leitura do resumo no BigQuery: SQL do arquivo em queries/ e conversão das linhas para o contrato."""

import pytest

from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.resumo import BigQueryResumoReader, render_query


def test_query_reads_the_contract_columns_of_the_given_table() -> None:
    # Given / When: a consulta renderizada para a tabela do contrato
    sql = render_query("rj-iplanrio-dia.nota_carioca_v2.paridade_nc2__resumo")

    # Then: lê as colunas do contrato da tabela, sem filtro nem tolerância
    assert "`rj-iplanrio-dia.nota_carioca_v2.paridade_nc2__resumo`" in sql
    for column in ("regra", "classe", "quantidade", "corte_nsu", "gerado_em"):
        assert column in sql
    assert "WHERE" not in sql.upper()


@pytest.mark.parametrize("table", ["a.b", "a.b.c; DROP TABLE x", "a.b.`c`", "a b.c.d", ""])
def test_table_reference_must_be_project_dataset_table(table: str) -> None:
    # Given / When / Then: referência fora de projeto.dataset.tabela é recusada antes de virar SQL
    with pytest.raises(ValueError, match="projeto.dataset.tabela"):
        render_query(table)


def test_reader_returns_one_mapping_per_row_using_the_injected_client() -> None:
    # Given: um cliente que devolve linhas (o BigQuery real nunca é tocado)
    class FakeJob:
        def result(self):
            return [{"regra": "R-01", "classe": "igual", "quantidade": 3, "corte_nsu": 1, "gerado_em": None}]

    class FakeClient:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def query(self, sql: str) -> FakeJob:
            self.queries.append(sql)
            return FakeJob()

    client = FakeClient()

    # When: o leitor lê
    rows = BigQueryResumoReader("rj-iplanrio-dia", "p.d.t", client=client).read()

    # Then: devolve as linhas como dicionários e fez exatamente uma consulta
    assert rows == [{"regra": "R-01", "classe": "igual", "quantidade": 3, "corte_nsu": 1, "gerado_em": None}]
    assert len(client.queries) == 1

"""Índice do BigQuery: tabelas e consultas do ambiente saem do dataset recebido, nunca do de outro ambiente."""

from datetime import UTC, datetime
from typing import Any

import pytest

from pipelines.rj_smfp__nota_carioca_adn.utils.indice import BigQueryIndice

PROJETO = "rj-iplanrio-dia"
DATASET_STAGING = "raw_nfse_nacional_staging"
TABELAS = ("documentos", "adn_cursor", "adn_nsu_inexistente")


class _Resultado:
    def result(self) -> list[dict[str, Any]]:
        return []


class ClienteBigQueryFalso:
    """Dublê do cliente do BigQuery: grava o nome das tabelas criadas e o SQL de cada consulta."""

    def __init__(self) -> None:
        self.tabelas_criadas: list[str] = []
        self.consultas: list[str] = []

    def create_table(self, tabela: Any, exists_ok: bool) -> None:  # noqa: ARG002
        self.tabelas_criadas.append(f"{tabela.project}.{tabela.dataset_id}.{tabela.table_id}")

    def query(self, sql: str, job_config: object = None) -> _Resultado:  # noqa: ARG002
        self.consultas.append(sql)
        return _Resultado()


def test_garantir_tabelas_cria_as_tabelas_no_dataset_recebido() -> None:
    cliente = ClienteBigQueryFalso()

    BigQueryIndice(cliente, PROJETO, DATASET_STAGING).garantir_tabelas()  # type: ignore[arg-type]

    assert cliente.tabelas_criadas == [f"{PROJETO}.{DATASET_STAGING}.{tabela}" for tabela in TABELAS]


@pytest.mark.parametrize(
    "consulta",
    [
        lambda indice: indice.ler_cursor("staging"),
        lambda indice: indice.gravar_cursor("staging", 1, datetime(2026, 1, 1, tzinfo=UTC)),
        lambda indice: indice.nsus_indexados("staging", 1, 10),
        lambda indice: indice.documentos_na_faixa("staging", 1, 10),
        lambda indice: indice.lacunas("staging", 10),
    ],
    ids=["ler_cursor", "gravar_cursor", "nsus_indexados", "documentos_na_faixa", "lacunas"],
)
def test_consultas_do_indice_leem_e_gravam_so_no_dataset_recebido(consulta: Any) -> None:
    cliente = ClienteBigQueryFalso()

    consulta(BigQueryIndice(cliente, PROJETO, DATASET_STAGING))  # type: ignore[arg-type]

    assert cliente.consultas
    for sql in cliente.consultas:
        assert f"{PROJETO}.{DATASET_STAGING}." in sql
        assert f"{PROJETO}.raw_nfse_nacional." not in sql

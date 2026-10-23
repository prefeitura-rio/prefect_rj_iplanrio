"""Leitura do resumo de paridade no BigQuery, atrás de um adaptador que os testes substituem."""

import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Protocol

from google.cloud import bigquery

from prefect_rj_iplanrio.sql import load_query

# load_query resolve queries/ no diretório pai do caminho recebido; a pasta utils/ aponta para a raiz da pipeline.
QUERIES_ANCHOR = str(Path(__file__).parent)
TABLE_REFERENCE = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


class ResumoReader(Protocol):
    """Fonte das linhas do resumo de paridade."""

    def read(self) -> list[dict[str, object]]:
        """Lê todas as linhas do resumo, uma por (regra, classe)."""
        ...


class _Job(Protocol):
    def result(self) -> Iterable[Mapping[str, object]]: ...


class _QueryClient(Protocol):
    def query(self, query: str) -> _Job: ...


def render_query(table: str) -> str:
    """Renderiza ``queries/get_resumo.sql`` para a tabela de resumo.

    :param table: Tabela no formato ``projeto.dataset.tabela``.
    :returns: SQL do BigQuery.
    :raises ValueError: Se a referência não for ``projeto.dataset.tabela``.
    """
    if not TABLE_REFERENCE.fullmatch(table):
        raise ValueError(f"Tabela de resumo fora do formato projeto.dataset.tabela: {table!r}")
    return load_query(QUERIES_ANCHOR, "get_resumo", table=table)


class BigQueryResumoReader:
    """Lê o resumo de paridade com uma única consulta ao BigQuery.

    :param project: Projeto que executa a consulta.
    :param table: Tabela de resumo, ``projeto.dataset.tabela``.
    :param client: Cliente do BigQuery; o padrão é ``bigquery.Client(project=project)``.
    """

    def __init__(self, project: str, table: str, client: _QueryClient | None = None) -> None:
        self._sql = render_query(table)
        self._project = project
        self._client = client

    def read(self) -> list[dict[str, object]]:
        """Executa a consulta e devolve cada linha como dicionário.

        :returns: Linhas com ``regra``, ``classe``, ``quantidade``, ``corte_nsu`` e ``gerado_em``.
        """
        client = self._client if self._client is not None else bigquery.Client(project=self._project)
        return [dict(row.items()) for row in client.query(self._sql).result()]

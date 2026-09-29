"""Features de telefone: renderiza a query única usada pelo treino e pelo scoring diário.

A lógica das features vive num único arquivo, ``queries/features_telefone.sql``. Treino e
scoring só diferem em três pedaços (a CTE ``amostra``, as CTEs de rótulo e o ``SELECT``
final), que ficam em fragmentos ``.sql`` ao lado do template. Assim as duas pontas nunca
calculam uma feature de jeitos diferentes.
"""

from collections.abc import Iterable, Iterator
from typing import Literal

import pandas as pd
from google.cloud import bigquery, bigquery_storage_v1
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

from pipelines.rj_crm__modelo_qualidade_telefone import constants

logger = get_logger(__name__)

ModoFeatures = Literal["treino", "agora"]

# `load_query` procura `queries/` ao lado do arquivo recebido; módulos em `tasks/`
# passam `constants.__file__` para chegar no `queries/` da raiz da pipeline.
QUERY_ANCHOR = constants.__file__


def renderiza_features_sql(
    modo: ModoFeatures,
    random_state: int = constants.RANDOM_STATE,
    janela_futuro_dias: int = constants.JANELA_FUTURO_DIAS,
    limiar_high_delivery: float = constants.LIMIAR_HIGH_DELIVERY,
) -> str:
    """Monta a query de features para o modo pedido.

    :param modo: ``"treino"`` sorteia um ``data_corte`` por telefone e devolve
        ``telefone, data_corte, high_delivery`` + features (só telefones com rótulo).
        ``"agora"`` usa ``CURRENT_DATETIME('America/Sao_Paulo')`` como corte e devolve
        uma linha por telefone: ``cpfs`` (lista), ``telefone``, ``data_corte`` + features, sem rótulo.
    :param random_state: Semente do sorteio de ``data_corte`` (só ``modo="treino"``).
    :param janela_futuro_dias: Janela do rótulo HighDelivery (só ``modo="treino"``).
    :param limiar_high_delivery: Taxa de entrega acima da qual o telefone é
        HighDelivery (só ``modo="treino"``).
    :returns: SQL renderizada, sem placeholders pendentes.
    :raises ValueError: Se ``modo`` não for ``"treino"`` nem ``"agora"``.
    """
    if modo == "treino":
        params = {
            "random_state": random_state,
            "janela_futuro_dias": janela_futuro_dias,
            "limiar_high_delivery": limiar_high_delivery,
        }
        fragmentos = {
            "amostra_ctes": load_query(QUERY_ANCHOR, "amostra_treino", **params),
            "rotulo_ctes": load_query(QUERY_ANCHOR, "rotulo_treino", **params),
            "select_final": load_query(QUERY_ANCHOR, "select_treino"),
        }
    elif modo == "agora":
        fragmentos = {
            "amostra_ctes": load_query(QUERY_ANCHOR, "amostra_agora"),
            "rotulo_ctes": "",
            "select_final": load_query(QUERY_ANCHOR, "select_agora"),
        }
    else:
        raise ValueError(f"modo inválido: {modo!r} (esperado 'treino' ou 'agora')")

    return load_query(QUERY_ANCHOR, "features_telefone", **fragmentos)


def itera_features_agora(
    client: bigquery.Client,
    bqstorage_client: bigquery_storage_v1.BigQueryReadClient,
    tamanho_lote: int = 500_000,
) -> Iterator[pd.DataFrame]:
    """Roda a query de features do scoring e devolve o resultado em lotes.

    São ~9 milhões de linhas por dia; em lotes a memória fica limitada ao tamanho do lote
    (o resultado inteiro, com features e SHAP, ocuparia vários GB).

    :param client: Cliente do BigQuery autenticado.
    :param bqstorage_client: Cliente da API de Storage do BigQuery. É obrigatório: sem ele o
        resultado vem pela API REST em JSON, mais de 10 vezes mais lenta.
    :param tamanho_lote: Linhas por DataFrame devolvido.
    :returns: Iterador de DataFrames com ``cpfs``, ``telefone``, ``data_corte`` e as features.
    """
    job = client.query(renderiza_features_sql("agora"))
    logger.info("Query de features (scoring) enviada: job %s", job.job_id)
    # a API de leitura devolve páginas de ~10 mil linhas; junta até chegar no tamanho do lote
    yield from agrupa_lotes(job.result().to_dataframe_iterable(bqstorage_client=bqstorage_client), tamanho_lote)
    logger.info("Features lidas: %.1f GB faturados", (job.total_bytes_billed or 0) / 1e9)


def agrupa_lotes(paginas: Iterable[pd.DataFrame], tamanho_lote: int) -> Iterator[pd.DataFrame]:
    """Junta DataFrames pequenos em lotes de pelo menos ``tamanho_lote`` linhas.

    :param paginas: DataFrames com as mesmas colunas.
    :param tamanho_lote: Tamanho mínimo de cada lote devolvido (o último pode ser menor).
    :returns: Iterador de DataFrames concatenados, com índice reiniciado.
    """
    acumulado: list[pd.DataFrame] = []
    linhas = 0
    for pagina in paginas:
        acumulado.append(pagina)
        linhas += len(pagina)
        if linhas >= tamanho_lote:
            yield pd.concat(acumulado, ignore_index=True)
            acumulado, linhas = [], 0
    if acumulado:
        yield pd.concat(acumulado, ignore_index=True)

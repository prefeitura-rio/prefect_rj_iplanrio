"""Orquestra o scoring diário: baixa as features do BigQuery e pontua com o champion.

Universo, lógica de pontuação e formato de saída ficam em ``tasks/features.py`` e
``tasks/calcula_scores.py`` (funções puras, testadas sem BigQuery). Este módulo só
autentica e liga as duas pontas.
"""

from pathlib import Path

from google.cloud import bigquery, bigquery_storage_v1
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from prefect import task
from prefect_rj_iplanrio.logging import get_logger

from pipelines.rj_crm__modelo_qualidade_telefone import constants
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.calcula_scores import EstatisticasScores, gera_parquet_scores
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.features import itera_features_agora
from pipelines.rj_crm__modelo_qualidade_telefone.utils.modelo_store import ModeloCarregado

logger = get_logger(__name__)

CAMINHO_PARQUET = Path("/tmp/rj_crm__modelo_qualidade_telefone/scores.parquet")


def pontua_telefones(
    champion: ModeloCarregado,
    environment: str,
    tamanho_lote: int = 300_000,
    caminho: Path = CAMINHO_PARQUET,
) -> tuple[Path, EstatisticasScores]:
    """Baixa as features do universo do scoring e pontua com o champion.

    :param champion: Modelo carregado por ``tasks/carrega_modelo.py``.
    :param environment: ``"prod"`` ou ``"staging"`` — credenciais do secret do work pool.
    :param tamanho_lote: Telefones por lote pontuado (ver ``calcula_scores.gera_parquet_scores``).
    :param caminho: Onde gravar o parquet de saída (sobrescrito).
    :returns: O caminho do parquet e as estatísticas da rodada.
    :raises ValueError: Se algum lote for inválido, ou não houver telefone a pontuar.
    """
    credentials = get_bd_credentials_from_env(mode=environment)
    bq = bigquery.Client(credentials=credentials, project=constants.PROJECT_ID)
    bqstorage = bigquery_storage_v1.BigQueryReadClient(credentials=credentials)

    lotes = itera_features_agora(bq, bqstorage, tamanho_lote=tamanho_lote)
    stats = gera_parquet_scores(lotes, champion.booster, champion.versao, caminho)
    logger.info("Scoring pontuado (versão %s): %s", champion.versao, stats)
    return caminho, stats


@task
def pontua_telefones_task(
    champion: ModeloCarregado,
    environment: str,
    tamanho_lote: int = 300_000,
    caminho: Path = CAMINHO_PARQUET,
) -> tuple[Path, EstatisticasScores]:
    """Task-wrapper fina de :func:`pontua_telefones`."""
    return pontua_telefones(champion, environment, tamanho_lote, caminho)

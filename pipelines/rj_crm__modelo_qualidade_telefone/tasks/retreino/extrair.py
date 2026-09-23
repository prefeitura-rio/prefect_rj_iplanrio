"""Extrai os dois datasets do retreino mensal: o treino (``extrai_treino``) e o pool de
eventos candidatos pra simulação (``extrai_eventos``)."""

import pandas as pd
from google.cloud import bigquery, bigquery_storage_v1
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from prefect import task
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

from pipelines.rj_crm__modelo_qualidade_telefone import constants
from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.features import renderiza_features_sql

logger = get_logger(__name__)

JANELA_RECENTE_DIAS = 30
# Hoje o pool tem ~107 mil disparos avaliáveis em 30 dias — ver README de
# qualidade_telefone_modelo. Limite bem abaixo, só alarme cedo (fonte quebrada, JOIN que
# esvaziou o pool), não um limite ajustado fino.
MINIMO_EVENTOS = 1_000

COLUNAS_OBRIGATORIAS_EVENTOS = (*FEATURES, "id_interacao", "telefone", "data_corte", "telefone_usado", "falhou")

# Hoje o treino tem ~50 mil linhas e ~11% de high_delivery=0 (a classe minoritária) — ver
# README de qualidade_telefone_modelo. Limites bem abaixo disso: um alarme cedo (fonte
# quebrada, JOIN que esvaziou o universo), não um limite ajustado fino.
MINIMO_LINHAS_TREINO = 10_000
MINIMO_EXEMPLOS_CLASSE_MINORITARIA = 200

COLUNAS_OBRIGATORIAS = (*FEATURES, "telefone", "data_corte", "high_delivery")


def valida_treino(df: pd.DataFrame) -> None:
    """Recusa um dataset de treino claramente quebrado, antes de gastar tempo treinando.

    :param df: Resultado de ``renderiza_features_sql("treino")``.
    :raises ValueError: Se faltar coluna, vier vazio, ou uma das duas classes de
        ``high_delivery`` tiver poucos exemplos (o split estratificado e a CV do Optuna
        ficam instáveis com poucos positivos/negativos).
    """
    faltando = [c for c in COLUNAS_OBRIGATORIAS if c not in df.columns]
    if faltando:
        raise ValueError(f"colunas ausentes no resultado do treino: {faltando}")
    if len(df) < MINIMO_LINHAS_TREINO:
        raise ValueError(f"treino com {len(df):,} linhas, abaixo do mínimo de {MINIMO_LINHAS_TREINO:,}")

    contagem_classes = df["high_delivery"].value_counts()
    n_classe_minoritaria = contagem_classes.min()
    if len(contagem_classes) < 2 or n_classe_minoritaria < MINIMO_EXEMPLOS_CLASSE_MINORITARIA:
        raise ValueError(
            f"classe minoritária com {n_classe_minoritaria} exemplo(s), abaixo do mínimo de "
            f"{MINIMO_EXEMPLOS_CLASSE_MINORITARIA} — split estratificado/CV ficaria instável"
        )


def extrai_treino(environment: str) -> pd.DataFrame:
    """Roda a query de treino no BigQuery e valida o resultado.

    :param environment: ``"prod"`` ou ``"staging"`` — credenciais do secret do work pool.
    :returns: 1 linha por telefone: ``telefone``, ``data_corte``, ``high_delivery`` e as
        colunas de ``constants.FEATURES``.
    :raises ValueError: Ver :func:`valida_treino`.
    """
    credentials = get_bd_credentials_from_env(mode=environment)
    bq = bigquery.Client(credentials=credentials, project=constants.PROJECT_ID)
    bqstorage = bigquery_storage_v1.BigQueryReadClient(credentials=credentials)

    df = bq.query(renderiza_features_sql("treino")).to_dataframe(bqstorage_client=bqstorage)
    valida_treino(df)
    logger.info(
        "Treino extraído: %d linhas, %.1f%% HighDelivery.", len(df), 100 * df["high_delivery"].mean()
    )
    return df


@task
def extrai_treino_task(environment: str) -> pd.DataFrame:
    """Task-wrapper fina de :func:`extrai_treino`."""
    return extrai_treino(environment)


def renderiza_eventos_sql(janela_recente_dias: int = JANELA_RECENTE_DIAS) -> str:
    """Renderiza ``queries/eventos_candidatos.sql``.

    :param janela_recente_dias: Quantos dias de disparos recentes entram na simulação.
    """
    return load_query(constants.__file__, "eventos_candidatos", janela_recente_dias=janela_recente_dias)


def valida_eventos(df: pd.DataFrame) -> None:
    """Recusa um pool de eventos claramente quebrado.

    :param df: Resultado de :func:`renderiza_eventos_sql`.
    :raises ValueError: Se faltar coluna, ou o pool tiver poucos disparos distintos.
    """
    faltando = [c for c in COLUNAS_OBRIGATORIAS_EVENTOS if c not in df.columns]
    if faltando:
        raise ValueError(f"colunas ausentes no resultado dos eventos: {faltando}")
    n_eventos = df["id_interacao"].nunique()
    if n_eventos < MINIMO_EVENTOS:
        raise ValueError(f"pool com {n_eventos:,} disparos distintos, abaixo do mínimo de {MINIMO_EVENTOS:,}")


def extrai_eventos(environment: str, janela_recente_dias: int = JANELA_RECENTE_DIAS) -> pd.DataFrame:
    """Roda a query de eventos candidatos no BigQuery e valida o resultado.

    :param environment: ``"prod"`` ou ``"staging"`` — credenciais do secret do work pool.
    :param janela_recente_dias: Quantos dias de disparos recentes entram na simulação.
    :returns: 1 linha por (id_interacao, telefone candidato): ``id_interacao``,
        ``telefone``, ``data_corte``, ``telefone_usado``, ``status_disparo``, ``falhou`` e
        as colunas de ``constants.FEATURES``.
    :raises ValueError: Ver :func:`valida_eventos`.
    """
    credentials = get_bd_credentials_from_env(mode=environment)
    bq = bigquery.Client(credentials=credentials, project=constants.PROJECT_ID)
    bqstorage = bigquery_storage_v1.BigQueryReadClient(credentials=credentials)

    df = bq.query(renderiza_eventos_sql(janela_recente_dias)).to_dataframe(bqstorage_client=bqstorage)
    valida_eventos(df)
    logger.info(
        "Eventos extraídos: %d linhas, %d disparos distintos, %.1f%% falharam.",
        len(df), df["id_interacao"].nunique(), 100 * df.drop_duplicates("id_interacao")["falhou"].mean(),
    )
    return df


@task
def extrai_eventos_task(environment: str, janela_recente_dias: int = JANELA_RECENTE_DIAS) -> pd.DataFrame:
    """Task-wrapper fina de :func:`extrai_eventos`."""
    return extrai_eventos(environment, janela_recente_dias)

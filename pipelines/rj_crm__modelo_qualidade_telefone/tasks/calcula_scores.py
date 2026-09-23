"""Pontua telefones com o champion e grava o resultado em parquet.

Cada linha do parquet é um par (cpf, telefone) com a probabilidade de HighDelivery, as
features avaliadas e o valor SHAP de cada uma (STRUCTs com os mesmos nomes de campo).
A probabilidade só depende do telefone: cada telefone é pontuado uma vez e o resultado é
repetido para cada CPF ligado a ele. Um load job desse arquivo popula a tabela do dia.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from prefect_rj_iplanrio.logging import get_logger

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES, FEATURES_FLOAT

logger = get_logger(__name__)

QUANTIS_PROB = (0.01, 0.25, 0.50, 0.75, 0.99)


@dataclass(frozen=True)
class EstatisticasScores:
    """Resumo de uma rodada de scoring, para validar antes de publicar a tabela."""

    n_telefones: int
    n_linhas: int
    prob_media: float
    prob_quantis: dict[str, float]  # sobre telefones distintos, sem pesar por CPF
    n_probs_invalidas: int


def pontua_lote(features: pd.DataFrame, booster: lgb.Booster) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Calcula probabilidade, SHAP e valor base de um lote de features.

    O SHAP vem em log-odds (``pred_contrib`` do LightGBM): ``base + soma(shap)`` é o score
    bruto e o sigmoide dele é a probabilidade. A probabilidade não é calibrada
    (``is_unbalance=True``): serve para ranquear telefones, não para corte absoluto.

    :param features: Lote com todas as colunas de ``FEATURES`` (a ordem não importa).
    :param booster: Modelo cujas features estão exatamente na ordem de ``FEATURES``.
    :returns: ``(prob, shap, base)`` com formas ``(n,)``, ``(n, len(FEATURES))`` em float32 e ``(n,)``.
    :raises ValueError: Se faltar feature, houver valor nulo, ou o modelo esperar outra ordem.
    """
    if booster.feature_name() != FEATURES:
        raise ValueError("as features do modelo não estão na ordem de FEATURES")
    faltando = [f for f in FEATURES if f not in features.columns]
    if faltando:
        raise ValueError(f"features ausentes no lote: {faltando}")

    X = features[FEATURES].to_numpy(dtype="float64")  # posição = ordem de FEATURES
    if np.isnan(X).any():
        raise ValueError("lote de features com valores nulos")

    contrib = booster.predict(X, pred_contrib=True)
    score_bruto = contrib.sum(axis=1)
    prob = 1.0 / (1.0 + np.exp(-score_bruto))
    return prob, contrib[:, :-1].astype("float32"), contrib[:, -1]


def gera_parquet_scores(
    lotes: Iterable[pd.DataFrame],
    booster: lgb.Booster,
    versao_modelo: str,
    caminho: Path,
    score_datahora: datetime | None = None,
) -> EstatisticasScores:
    """Pontua os lotes e grava o parquet do scoring, um lote por vez.

    :param lotes: DataFrames com uma linha por telefone: ``cpfs`` (lista de CPFs),
        ``telefone`` e as colunas de ``FEATURES``.
    :param booster: Modelo (champion) a aplicar.
    :param versao_modelo: Versão do champion, gravada em cada linha.
    :param caminho: Arquivo parquet de saída (sobrescrito).
    :param score_datahora: Momento do scoring; por padrão, agora (UTC).
    :returns: Estatísticas da rodada.
    :raises ValueError: Se algum lote for inválido (ver :func:`pontua_lote`) ou não houver linhas.
    """
    score_datahora = score_datahora or datetime.now(timezone.utc)
    caminho.parent.mkdir(parents=True, exist_ok=True)
    probs: list[np.ndarray] = []
    n_linhas = 0
    escritor: pq.ParquetWriter | None = None
    try:
        for i, lote in enumerate(lotes, start=1):
            prob, shap, base = pontua_lote(lote, booster)
            tabela = monta_tabela(lote, prob, shap, base, versao_modelo, score_datahora)
            if escritor is None:
                escritor = pq.ParquetWriter(caminho, tabela.schema, compression="zstd")
            escritor.write_table(tabela)
            probs.append(prob)
            n_linhas += tabela.num_rows
            logger.info("Lote %d pontuado (%d telefones, %d pares)", i, len(lote), tabela.num_rows)
    finally:
        if escritor is not None:
            escritor.close()

    if not probs:
        raise ValueError("nenhuma linha para pontuar")
    todas = np.concatenate(probs)
    return EstatisticasScores(
        n_telefones=len(todas),
        n_linhas=n_linhas,
        prob_media=float(todas.mean()),
        prob_quantis={f"p{int(q * 100):02d}": float(np.quantile(todas, q)) for q in QUANTIS_PROB},
        n_probs_invalidas=int((~np.isfinite(todas) | (todas < 0) | (todas > 1)).sum()),
    )


def monta_tabela(
    lote: pd.DataFrame,
    prob: np.ndarray,
    shap: np.ndarray,
    base: np.ndarray,
    versao_modelo: str,
    score_datahora: datetime,
) -> pa.Table:
    """Monta a tabela arrow do lote: 1 linha por (cpf, telefone), com ``features`` e ``shap`` em STRUCT.

    O lote tem uma linha por telefone; cada uma é repetida para cada CPF da sua lista ``cpfs``.
    """
    n = len(lote)
    features = pa.StructArray.from_arrays(
        [pa.array(lote[f].to_numpy(dtype="float64" if f in FEATURES_FLOAT else "int64")) for f in FEATURES],
        names=FEATURES,
    )
    shaps = pa.StructArray.from_arrays([pa.array(shap[:, i]) for i in range(len(FEATURES))], names=FEATURES)
    por_telefone = pa.table(
        {
            "telefone": pa.array(lote["telefone"].astype(str).to_numpy()),
            "prob_high_delivery": pa.array(prob, type=pa.float64()),
            "versao_modelo": pa.array([versao_modelo] * n),
            "score_datahora": pa.array([score_datahora] * n, type=pa.timestamp("us", tz="UTC")),
            "shap_base": pa.array(base, type=pa.float64()),
            "features": features,
            "shap": shaps,
        }
    )
    cpfs = pa.array([list(c) for c in lote["cpfs"]], type=pa.list_(pa.string()))
    por_par = por_telefone.take(pc.list_parent_indices(cpfs))
    return por_par.add_column(0, "cpf", pc.list_flatten(cpfs))

"""Cobertura de dados por feature: % de linhas preenchidas, por tipo de feature
(sentinela/cold_start/categórica — ver regra em :func:`mascara_preenchida`).

Compartilhado pelo treino (dataset inteiro cabe em memória, calculado de uma vez — ver
``tasks/retreino/extrair.py``) e pelo scoring (dataset grande demais pra caber inteiro,
acumulado lote a lote — ver ``tasks/calcula_scores.py::gera_parquet_scores``), pra não ter
a mesma regra ("o que é preenchido") escrita duas vezes.
"""

from dataclasses import dataclass, field

import pandas as pd

from pipelines.rj_crm__modelo_qualidade_telefone.constants import (
    FEATURES_COM_SENTINELA,
    FEATURES_NUMERICAS,
    GRUPOS_DUMMIES,
)

# Nomes de "feature" pro relatório de cobertura: as numéricas 1 a 1, e os 2 grupos de
# dummies como 1 pergunta cada ("telefone tem DDD preenchido?", não 6 perguntas por dummy).
NOMES_FEATURES_COBERTURA = [*FEATURES_NUMERICAS, *GRUPOS_DUMMIES]


def tipo_da_feature(feature: str) -> str:
    """``"categorica"``, ``"sentinela"`` ou ``"cold_start"`` — ver :func:`mascara_preenchida`."""
    if feature in GRUPOS_DUMMIES:
        return "categorica"
    if feature in FEATURES_COM_SENTINELA:
        return "sentinela"
    return "cold_start"


def mascara_preenchida(df: pd.DataFrame, feature: str) -> pd.Series:
    """Máscara booleana de "preenchido" pra 1 feature, 3 regras:

    - "sentinela": features numéricas com -1 = "sem dado" (``FEATURES_COM_SENTINELA``) —
      preenchido = ``!= -1``.
    - "cold_start": as demais features numéricas (contagens que usam 0 como valor REAL de
      cold start, não "sem dado") — preenchido = ``!= 0``, ou seja, quanto da feature
      carrega sinal de verdade em vez do default.
    - "categorica": ``GRUPOS_DUMMIES`` (ddd_categoria, telefone_qualidade) — já vêm como
      dummies (one-hot em SQL); preenchido = soma das dummies do grupo ``> 0`` (todas 0 =
      o LEFT JOIN não achou essa informação pro telefone).
    """
    if feature in GRUPOS_DUMMIES:
        return df[GRUPOS_DUMMIES[feature]].sum(axis=1) > 0
    if feature in FEATURES_COM_SENTINELA:
        return df[feature] != -1
    return df[feature] != 0


@dataclass
class AcumuladorCobertura:
    """Acumula ``(n_preenchido, n_total)`` por feature ao longo de vários lotes — guarda só
    os contadores, não as linhas (memória ``O(features)``, não ``O(linhas)``; dá pra rodar
    sobre um dataset de 9 milhões de linhas sem materializar nada a mais)."""

    n_preenchido: dict[str, int] = field(default_factory=lambda: dict.fromkeys(NOMES_FEATURES_COBERTURA, 0))
    n_total: int = 0

    def atualiza(self, lote: pd.DataFrame) -> None:
        """Soma este lote aos contadores acumulados."""
        self.n_total += len(lote)
        for feature in NOMES_FEATURES_COBERTURA:
            self.n_preenchido[feature] += int(mascara_preenchida(lote, feature).sum())

    def tabela(self) -> pd.DataFrame:
        """Tabela final: ``feature, tipo, pct_preenchido, n_preenchido, n_total``, ordenada
        da menos pra mais preenchida."""
        linhas = [
            {
                "feature": feature,
                "tipo": tipo_da_feature(feature),
                "pct_preenchido": round(100 * self.n_preenchido[feature] / self.n_total, 1) if self.n_total else 0.0,
                "n_preenchido": self.n_preenchido[feature],
                "n_total": self.n_total,
            }
            for feature in NOMES_FEATURES_COBERTURA
        ]
        return pd.DataFrame(linhas).sort_values("pct_preenchido").reset_index(drop=True)


def calcula_cobertura(df: pd.DataFrame) -> pd.DataFrame:
    """Atalho pra quando o dataset inteiro já está em memória (treino) — 1 lote só."""
    acumulador = AcumuladorCobertura()
    acumulador.atualiza(df)
    return acumulador.tabela()

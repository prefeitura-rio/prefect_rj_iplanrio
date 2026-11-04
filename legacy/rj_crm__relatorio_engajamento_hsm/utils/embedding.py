# -*- coding: utf-8 -*-
"""Embeddings locais (sentence-transformers, torch só-CPU, sem chamada externa) e busca
dos exemplos mais parecidos — base do few-shot da classificação recorrente (RAG).

O texto embedado é só a fala do CIDADÃO (utils/texto.py::falas_cidadao), nunca a
conversa inteira — ver gera_embeddings_conversas.

A busca é em memória (numpy), não no BigQuery: o banco de exemplos revisados é
filtrado por campanha e fica na casa das centenas/poucos milhares de linhas, então
comparar tudo contra tudo custa milissegundos e dispensa índice vetorial."""

from __future__ import annotations

from functools import lru_cache

import numpy as np
from sentence_transformers import SentenceTransformer

from pipelines.rj_crm__relatorio_engajamento_hsm.config import EMBEDDING_MODEL
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.texto import falas_cidadao

_TAMANHO_LOTE_EMBEDDING = 32


@lru_cache(maxsize=2)
def _modelo(nome: str) -> SentenceTransformer:
    # Carregar o modelo custa alguns segundos — cacheado pra durar o flow run inteiro.
    # A imagem já traz o modelo baixado (HF_HOME, ver Dockerfile).
    return SentenceTransformer(nome, device="cpu")


def gera_embeddings(textos: list[str], modelo: str = EMBEDDING_MODEL) -> np.ndarray:
    """Matriz (n, dim) de embeddings normalizados (norma 1) — com isso o produto
    escalar já é a similaridade de cosseno."""
    if not textos:
        return np.empty((0, 0), dtype=np.float32)
    return _modelo(modelo).encode(
        [t or "" for t in textos],
        batch_size=_TAMANHO_LOTE_EMBEDDING,
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=False,
    ).astype(np.float32)


def gera_embeddings_conversas(conversas: list[str], modelo: str = EMBEDDING_MODEL) -> np.ndarray:
    """Embeddings de conversas no formato conversa_completa (tasks/extract.py), usando
    só as falas do cidadão — é o que tanto a ingestão do banco RAG quanto a busca na
    classificação devem chamar, pros dois lados ficarem comparáveis."""
    return gera_embeddings([falas_cidadao(c) for c in conversas], modelo)


def top_k_similares(consultas: np.ndarray, base: np.ndarray, k: int) -> tuple[np.ndarray, np.ndarray]:
    """Pra cada linha de `consultas`, os índices das k linhas mais parecidas de `base`
    (ordem decrescente de similaridade) e as similaridades correspondentes. Os dois
    lados precisam vir de gera_embeddings (normalizados, mesmo modelo). k maior que a
    base é limitado ao tamanho da base."""
    k = min(k, len(base))
    if k == 0 or len(consultas) == 0:
        vazio = np.empty((len(consultas), 0))
        return vazio.astype(int), vazio
    similaridade = consultas @ base.T
    # argpartition acha o top-k sem ordenar a base inteira; depois ordena só esses k.
    candidatos = np.argpartition(-similaridade, k - 1, axis=1)[:, :k]
    sim_candidatos = np.take_along_axis(similaridade, candidatos, axis=1)
    ordem = np.argsort(-sim_candidatos, axis=1)
    indices = np.take_along_axis(candidatos, ordem, axis=1)
    return indices, np.take_along_axis(sim_candidatos, ordem, axis=1)

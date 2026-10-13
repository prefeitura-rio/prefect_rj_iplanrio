# -*- coding: utf-8 -*-
"""Julgamento (LLM as a judge) — audita a classificação das conversas classificadas
com sucesso, em lotes de tamanho_lote_juiz. Julga o CONJUNTO de categorias de cada
conversa (a ordem não importa): CORRETO, ou INCORRETO com o conjunto esperado.

Avalia TODAS até teto_juiz conversas. Acima disso (só acontece numa campanha liberada
acima do TETO_CONVERSAS), avalia uma amostra estratificada por categoria — ver
seleciona_para_juiz.

Lote que falha (rede, JSON inválido) é tentado mais 1 vez; se falhar de novo, as
conversas dele ficam sem veredito (fora do cálculo de precisão — o relatório mostra
quantas foram julgadas de fato)."""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    CATEGORIA_SEM_CATALOGO,
    MAX_OUTPUT_TOKENS_LOTE,
    SEED_AMOSTRA_JUIZ,
    SENTINELA_SEM_CATEGORIA,
    SEP_CATEGORIAS,
    VEREDITOS_VALIDOS,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.extract import textos_hsm_prompt
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bifrost import BifrostClient, max_output_tokens_para
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.catalogo import CatalogoCategorias
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.texto import strip_code_fence

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
_PROMPT_JUIZ = (_PROMPTS_DIR / "juiz.txt").read_text(encoding="utf-8")
_TENTATIVAS_LOTE = 2
COLUNAS_JUIZ = ["juiz_veredito", "juiz_categorias_esperadas", "juiz_justificativa"]


def _para_prompt(categorias: list[str]) -> str:
    """Categorias internas -> como aparecem pra LLM (CATEGORIA_SEM_CATALOGO volta a ser
    o sentinela que os prompts conhecem)."""
    return SEP_CATEGORIAS.join(SENTINELA_SEM_CATEGORIA if c == CATEGORIA_SEM_CATALOGO else c for c in categorias)


def _da_resposta(nomes: object, catalogo: CatalogoCategorias) -> list[str]:
    """categorias_esperadas da LLM -> nomes canônicos; descarta o que não está no
    catálogo; sentinela vira CATEGORIA_SEM_CATALOGO (só se vier sozinho)."""
    if isinstance(nomes, str):
        nomes = [nomes]
    validas: list[str] = []
    nenhuma = False
    for bruto in nomes if isinstance(nomes, list) else []:
        nome = str(bruto).strip()
        if nome.upper() == SENTINELA_SEM_CATEGORIA:
            nenhuma = True
            continue
        canonica = catalogo.busca(nome)
        if canonica and canonica not in validas:
            validas.append(canonica)
    return validas or ([CATEGORIA_SEM_CATALOGO] if nenhuma else [])


def interpreta_resposta_lote(parsed: object, lote: pd.DataFrame, catalogo: CatalogoCategorias) -> dict[str, dict]:
    """JSON de 1 lote -> {id_sessao_48h: {juiz_veredito, juiz_categorias_esperadas,
    juiz_justificativa}}. Item sem veredito válido é só pulado (conversa sem veredito)."""
    if not isinstance(parsed, list):
        raise ValueError(f"resposta não é uma lista JSON (veio {type(parsed).__name__})")
    por_id = {item.get("id"): item for item in parsed if isinstance(item, dict)}
    resultados: dict[str, dict] = {}
    for i, linha in enumerate(lote.itertuples(), start=1):
        item = por_id.get(i)
        veredito = str(item.get("veredito") or "").strip().upper() if item else ""
        if veredito not in VEREDITOS_VALIDOS:
            continue
        esperadas = _da_resposta(item.get("categorias_esperadas"), catalogo) if veredito == "INCORRETO" else []
        resultados[linha.id_sessao_48h] = {
            "juiz_veredito": veredito,
            "juiz_categorias_esperadas": esperadas,
            "juiz_justificativa": str(item.get("justificativa") or "").strip(),
        }
    return resultados


def _julga_lote(lote: pd.DataFrame, bifrost: BifrostClient, catalogo: CatalogoCategorias) -> dict[str, dict]:
    conversas_texto = "\n\n".join(
        f"### CONVERSA {i} (id_sessao_48h={row.id_sessao_48h})\n"
        f"{row.conversa_completa}\n"
        f"Categorias propostas: {_para_prompt(row.categorias)}\n"
        f"Justificativa proposta: {row.categoria_justificativa}"
        for i, row in enumerate(lote.itertuples(), start=1)
    )
    prompt = (
        _PROMPT_JUIZ.replace("<<HSM_TEXTO>>", textos_hsm_prompt(lote))
        .replace("<<CATALOGO_CATEGORIAS>>", catalogo.texto_prompt())
        .replace("<<CONVERSAS>>", conversas_texto)
    )
    ultimo_erro: Exception | None = None
    for tentativa in range(1, _TENTATIVAS_LOTE + 1):
        try:
            resposta = bifrost.ask(
                prompt, max_output_tokens=max(MAX_OUTPUT_TOKENS_LOTE, max_output_tokens_para(len(lote)))
            )
            texto = BifrostClient.extract_text(resposta)
            return interpreta_resposta_lote(json.loads(strip_code_fence(texto), strict=False), lote, catalogo)
        except Exception as exc:  # tenta o lote de novo; depois desiste só deste lote
            ultimo_erro = exc
            print(f"  [FALHA LOTE JUIZ] tentativa {tentativa}/{_TENTATIVAS_LOTE}, {len(lote)} conversa(s): {exc}")
    print(f"  [JUIZ] lote de {len(lote)} conversa(s) ficou sem veredito: {ultimo_erro}")
    return {}


def seleciona_para_juiz(df: pd.DataFrame, teto: int, piso: int) -> pd.DataFrame:
    """Conversas que o juiz avalia. Até `teto`, todas. Acima, amostra estratificada:
    cada conversa entra no estrato da sua categoria MAIS RARA (a ordem das categorias
    não tem significado; assim as categorias pequenas ficam bem representadas); cada
    estrato recebe primeiro min(tamanho, piso) e o resto até `teto` é dividido
    proporcionalmente ao que sobrou em cada estrato (maiores restos de divisão levam as
    vagas que sobram do arredondamento). Se nem os pisos cabem no teto, todos os
    estratos são reduzidos na mesma proporção (mínimo 1)."""
    if len(df) <= teto:
        return df
    frequencia = Counter(c for cats in df["categorias"] for c in cats)
    estrato = df["categorias"].map(lambda cats: min(cats, key=lambda c: (frequencia[c], c)))
    tamanhos = estrato.value_counts().sort_index()

    cota = tamanhos.clip(upper=piso)
    if cota.sum() > teto:
        cota = (cota * teto / cota.sum()).apply(math.floor).clip(lower=1)
    sobra = teto - cota.sum()
    restante = tamanhos - cota
    if sobra > 0 and restante.sum() > 0:
        ideal = restante * sobra / restante.sum()
        extra = ideal.apply(math.floor)
        vagas = int(sobra - extra.sum())
        for nome in (ideal - extra).sort_values(ascending=False).index[:vagas]:
            extra[nome] += 1
        cota = cota + extra.clip(upper=restante)

    partes = [
        df[estrato == nome].sample(n=int(min(n, tamanhos[nome])), random_state=SEED_AMOSTRA_JUIZ)
        for nome, n in cota.items()
        if n > 0
    ]
    return pd.concat(partes).sort_index()


@task(log_prints=True)
def julga_classificacoes(
    df: pd.DataFrame,
    catalogo: CatalogoCategorias,
    bifrost: BifrostClient,
    tamanho_lote_juiz: int,
    teto_juiz: int,
    piso_juiz_por_categoria: int,
) -> pd.DataFrame:
    """df da classificação + COLUNAS_JUIZ. Conversa com falha de classificação, fora da
    amostra do juiz (acima do teto) ou de lote que falhou fica com juiz_veredito vazio."""
    df = df.copy()
    df["juiz_veredito"] = ""
    df["juiz_categorias_esperadas"] = [[] for _ in range(len(df))]
    df["juiz_justificativa"] = ""
    classificadas = df[df["falha_classificacao"] == ""]
    a_julgar = seleciona_para_juiz(classificadas, teto_juiz, piso_juiz_por_categoria)
    if len(a_julgar) < len(classificadas):
        print(
            f"[JUIZ] {len(classificadas)} conversa(s) acima do teto ({teto_juiz}) — "
            f"amostra estratificada de {len(a_julgar)}."
        )
    if a_julgar.empty:
        print("[JUIZ] nada a julgar.")
        return df

    vereditos: dict[str, dict] = {}
    n_lotes = -(-len(a_julgar) // tamanho_lote_juiz)
    for n, i in enumerate(range(0, len(a_julgar), tamanho_lote_juiz), start=1):
        resultado = _julga_lote(a_julgar.iloc[i : i + tamanho_lote_juiz], bifrost, catalogo)
        vereditos.update(resultado)
        print(f"[JUIZ] lote {n}/{n_lotes}: {len(resultado)} veredito(s).")

    posicao = {id_sessao: i for i, id_sessao in enumerate(df["id_sessao_48h"])}
    for id_sessao, valores in vereditos.items():
        linha = posicao[id_sessao]
        for coluna, valor in valores.items():
            df.at[df.index[linha], coluna] = valor

    n_corretos = sum(1 for v in vereditos.values() if v["juiz_veredito"] == "CORRETO")
    print(f"[JUIZ] {len(vereditos)} de {len(a_julgar)} conversa(s) julgada(s); {n_corretos} CORRETO.")
    return df


def metricas_por_categoria(df: pd.DataFrame) -> pd.DataFrame:
    """Volume e precisão por categoria, a partir do df julgado. Conta a conversa em
    CADA categoria dela (por isso a soma de pct_do_engajamento passa de 100% quando há
    conversa com mais de uma). Precisão da categoria X = entre as conversas julgadas que
    têm X, fração em que X está certa: veredito CORRETO, ou INCORRETO com X entre as
    categorias esperadas pelo juiz. Conversas com falha de classificação ficam de fora.

    Colunas: categoria, qtd, pct_do_engajamento, n_avaliadas_juiz, n_corretas, precisao
    (None se nenhuma foi julgada). Ordenado por qtd decrescente."""
    classificadas = df[df["falha_classificacao"] == ""]
    total = len(classificadas)
    contagem: dict[str, list[int]] = {}
    for linha in classificadas.itertuples():
        julgada = linha.juiz_veredito in VEREDITOS_VALIDOS
        for categoria in linha.categorias:
            qtd, avaliadas, corretas = contagem.setdefault(categoria, [0, 0, 0])
            certa = julgada and (linha.juiz_veredito == "CORRETO" or categoria in linha.juiz_categorias_esperadas)
            contagem[categoria] = [qtd + 1, avaliadas + int(julgada), corretas + int(certa)]
    linhas = [
        {
            "categoria": categoria,
            "qtd": qtd,
            "pct_do_engajamento": 100 * qtd / total if total else 0.0,
            "n_avaliadas_juiz": avaliadas,
            "n_corretas": corretas,
            "precisao": corretas / avaliadas if avaliadas else None,
        }
        for categoria, (qtd, avaliadas, corretas) in contagem.items()
    ]
    colunas = ["categoria", "qtd", "pct_do_engajamento", "n_avaliadas_juiz", "n_corretas", "precisao"]
    return (
        pd.DataFrame(linhas, columns=colunas).sort_values("qtd", ascending=False, kind="stable").reset_index(drop=True)
    )

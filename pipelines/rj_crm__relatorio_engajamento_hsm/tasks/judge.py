# -*- coding: utf-8 -*-
"""Julgamento (LLM as a judge) — audita a classificação feita por classify.py.
Amostragem por categoria: categoria pequena (< LIMIAR_JULGA_TUDO) julga 100% dela
(amostrar uma fração de pouca gente só deixaria a margem de erro enorme à toa);
categoria grande julga só uma fração (a margem de erro já fica pequena bem antes de
julgar tudo)."""

from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    MAX_OUTPUT_TOKENS_LOTE,
    SEED_AMOSTRA_JUIZ,
    VEREDITOS_VALIDOS,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bifrost import BifrostClient
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.texto import strip_code_fence

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
_PROMPT_JUIZ = (_PROMPTS_DIR / "juiz.txt").read_text(encoding="utf-8")


def _catalogo_texto(catalogo: dict[str, str]) -> str:
    if not catalogo:
        return "(catálogo vazio)"
    return "\n".join(f"- {nome}: {descricao}" for nome, descricao in sorted(catalogo.items()))


def _julga_lote(lote: pd.DataFrame, bifrost: BifrostClient, catalogo_texto: str) -> tuple[dict[str, dict], int]:
    conversas_texto = "\n\n".join(
        f"### CONVERSA {i} (id_sessao_48h={row.id_sessao_48h})\n"
        f"{row.conversa_completa}\n"
        f"Categoria proposta: {row.categoria}\n"
        f"Justificativa proposta: {row.categoria_justificativa}"
        for i, row in enumerate(lote.itertuples(), start=1)
    )
    hsm_texto = next((t for t in lote["hsm_texto"] if t), "")
    prompt = (
        _PROMPT_JUIZ.replace("<<HSM_TEXTO>>", hsm_texto)
        .replace("<<CATALOGO_CATEGORIAS>>", catalogo_texto)
        .replace("<<CONVERSAS>>", conversas_texto)
    )
    try:
        resposta = bifrost.ask(prompt, max_output_tokens=MAX_OUTPUT_TOKENS_LOTE)
        texto = BifrostClient.extract_text(resposta)
        parsed = json.loads(strip_code_fence(texto), strict=False)
        if not isinstance(parsed, list):
            raise ValueError(f"resposta não é uma lista JSON (bruto: {texto[:200]!r})")
    except Exception as exc:
        log(f"  [FALHA LOTE JUIZ] {len(lote)} conversa(s): {exc}")
        return {}, len(lote)

    por_id = {item.get("id"): item for item in parsed if isinstance(item, dict)}
    resultados: dict[str, dict] = {}
    n_falhas = 0
    for i, row in enumerate(lote.itertuples(), start=1):
        item = por_id.get(i)
        veredito = str(item.get("veredito") or "").strip().upper() if item else ""
        if veredito not in VEREDITOS_VALIDOS:
            n_falhas += 1
            continue
        resultados[row.id_sessao_48h] = {
            "juiz_veredito": veredito,
            "juiz_categoria_esperada": str(item.get("categoria_esperada") or "") if veredito == "INCORRETO" else "",
            "juiz_justificativa": item.get("justificativa") or "",
        }
    return resultados, n_falhas


def _alvo_amostra_juiz(total_cat: int, limiar_julga_tudo: int, pct_amostra_categoria_grande: float) -> int:
    if total_cat < limiar_julga_tudo:
        return total_cat
    return math.ceil(pct_amostra_categoria_grande / 100 * total_cat)


@task
def julga_classificacoes(
    df: pd.DataFrame,
    catalogo: dict[str, str],
    bf_key: str,
    bifrost_model: str,
    tamanho_lote_juiz: int,
    limiar_julga_tudo: int,
    pct_amostra_categoria_grande: float,
) -> pd.DataFrame:
    if df.empty:
        return df
    bifrost = BifrostClient(api_key=bf_key, model=bifrost_model)
    catalogo_texto = _catalogo_texto(catalogo)
    df = df.set_index("id_sessao_48h", drop=False)

    a_julgar = []
    for _categoria, grupo in df.groupby("categoria"):
        alvo = _alvo_amostra_juiz(len(grupo), limiar_julga_tudo, pct_amostra_categoria_grande)
        if alvo == 0:
            continue
        a_julgar.append(grupo.sample(n=min(alvo, len(grupo)), random_state=SEED_AMOSTRA_JUIZ))

    if not a_julgar:
        log("[JUIZ] nada a julgar.")
        return df.reset_index(drop=True)

    df_a_julgar = pd.concat(a_julgar)
    log(f"[JUIZ] {len(df_a_julgar)} conversa(s) selecionada(s) pra julgamento (categoria < {limiar_julga_tudo}: 100%; senão {pct_amostra_categoria_grande:.0f}%).")

    total_julgadas = total_falhas = 0
    for i in range(0, len(df_a_julgar), tamanho_lote_juiz):
        lote = df_a_julgar.iloc[i : i + tamanho_lote_juiz]
        resultados, n_falhas = _julga_lote(lote, bifrost, catalogo_texto)
        for id_sessao_48h, veredito in resultados.items():
            for col, val in veredito.items():
                df.loc[id_sessao_48h, col] = val
        total_julgadas += len(resultados)
        total_falhas += n_falhas

    log(f"[JUIZ] {total_julgadas} julgada(s), {total_falhas} falha(s).")
    return df.reset_index(drop=True)

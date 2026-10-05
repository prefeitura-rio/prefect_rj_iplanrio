# -*- coding: utf-8 -*-
"""Descoberta de categorias (passada 1 de 2) — estabiliza o catálogo com uma amostra
ALEATÓRIA do disparo antes de qualquer conversa ser classificada de verdade, pra evitar
viés de ordem: numa classificação direta, quem é classificado cedo ficaria preso ao
catálogo como ele estava naquele momento, mesmo que uma categoria melhor devesse existir.
Só esta etapa pode CRIAR categoria — classify.py só escolhe entre as já existentes."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from iplanrio.pipelines_utils.logging import log

from pipelines.rj_crm__relatorio_engajamento_hsm.config import LOTES_SEM_NOVIDADE_LIMITE, SEED_DESCOBERTA
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bifrost import BifrostClient, max_output_tokens_para
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.catalogo import CatalogoCategorias
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.texto import strip_code_fence

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
_PROMPT_DESCOBERTA = (_PROMPTS_DIR / "descoberta.txt").read_text(encoding="utf-8")


def _descobre_lote(lote: pd.DataFrame, bifrost: BifrostClient, catalogo: CatalogoCategorias, max_output_tokens: int) -> tuple[int, int]:
    conversas_texto = "\n\n".join(
        f"### CONVERSA {i} (id_sessao_48h={row.id_sessao_48h})\n{row.conversa_completa}"
        for i, row in enumerate(lote.itertuples(), start=1)
    )
    hsm_texto = next((t for t in lote["hsm_texto"] if t), "")
    prompt = (
        _PROMPT_DESCOBERTA.replace("<<HSM_TEXTO>>", hsm_texto)
        .replace("<<CATALOGO_CATEGORIAS>>", catalogo.texto_prompt())
        .replace("<<CONVERSAS>>", conversas_texto)
    )
    try:
        resposta = bifrost.ask(prompt, max_output_tokens=max_output_tokens)
        texto = BifrostClient.extract_text(resposta)
        parsed = json.loads(strip_code_fence(texto), strict=False)
        if not isinstance(parsed, list):
            raise ValueError(f"resposta não é uma lista JSON (bruto: {texto[:200]!r})")
    except Exception as exc:
        log(f"  [FALHA LOTE DESCOBERTA] {len(lote)} conversa(s): {exc}")
        return 0, len(lote)

    n_novas = n_falhas = 0
    for item in parsed:
        if not isinstance(item, dict):
            n_falhas += 1
            continue
        nome = str(item.get("categoria_nova") or "").strip()
        if not nome:
            n_falhas += 1
            continue
        n_antes = len(catalogo.como_dict())
        catalogo.registra(nome, str(item.get("categoria_nova_descricao") or "").strip())
        if len(catalogo.como_dict()) > n_antes:
            n_novas += 1
    return n_novas, n_falhas


def descobre_categorias(
    df_pendentes: pd.DataFrame,
    bifrost: BifrostClient,
    catalogo: CatalogoCategorias,
    tamanho_lote_descoberta: int,
    teto_amostra_descoberta: int,
) -> None:
    if len(df_pendentes) > teto_amostra_descoberta:
        amostra = df_pendentes.sample(n=teto_amostra_descoberta, random_state=SEED_DESCOBERTA)
    else:
        amostra = df_pendentes.sample(frac=1, random_state=SEED_DESCOBERTA)

    max_output_tokens = max_output_tokens_para(tamanho_lote_descoberta)
    lotes_sem_novidade = 0
    for i in range(0, len(amostra), tamanho_lote_descoberta):
        lote = amostra.iloc[i : i + tamanho_lote_descoberta]
        n_novas, n_falhas = _descobre_lote(lote, bifrost, catalogo, max_output_tokens)
        log(f"[DESCOBERTA] lote {i // tamanho_lote_descoberta + 1}: {n_novas} categoria(s) nova(s), {n_falhas} falha(s). Catálogo com {len(catalogo.como_dict())}.")
        if n_novas == 0:
            lotes_sem_novidade += 1
            if lotes_sem_novidade >= LOTES_SEM_NOVIDADE_LIMITE:
                log(f"[DESCOBERTA] {lotes_sem_novidade} lote(s) seguido(s) sem novidade — catálogo estável, parando.")
                break
        else:
            lotes_sem_novidade = 0

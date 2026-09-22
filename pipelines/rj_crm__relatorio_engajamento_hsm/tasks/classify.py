# -*- coding: utf-8 -*-
"""Classificação (passada 2 de 2) — só ESCOLHE entre categorias já existentes no
catálogo (nunca cria; isso é trabalho exclusivo de discovery.py). Roda sempre depois da
descoberta terminar de estabilizar o catálogo pra esse disparo."""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path

import pandas as pd
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import SENTINELA_SEM_CATEGORIA
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.discovery import descobre_categorias
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bifrost import BifrostClient, max_output_tokens_para
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.catalogo import CatalogoCategorias
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.texto import strip_code_fence

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
_PROMPT_CLASSIFICACAO = (_PROMPTS_DIR / "classificacao.txt").read_text(encoding="utf-8")


def _classifica_lote(lote: pd.DataFrame, bifrost: BifrostClient, catalogo: CatalogoCategorias, max_output_tokens: int) -> tuple[list[dict], Counter]:
    conversas_texto = "\n\n".join(
        f"### CONVERSA {i} (id_sessao_48h={row.id_sessao_48h})\n{row.conversa_completa}"
        for i, row in enumerate(lote.itertuples(), start=1)
    )
    hsm_texto = next((t for t in lote["hsm_texto"] if t), "")
    prompt = (
        _PROMPT_CLASSIFICACAO.replace("<<HSM_TEXTO>>", hsm_texto)
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
        log(f"  [FALHA LOTE] {len(lote)} conversa(s): {exc}")
        return [], Counter({"lote_inteiro_falhou": len(lote)})

    por_id = {item.get("id"): item for item in parsed if isinstance(item, dict)}
    resultados: list[dict] = []
    falhas: Counter = Counter()
    for i, row in enumerate(lote.itertuples(), start=1):
        item = por_id.get(i)
        categoria_bruta = str(item.get("categoria") or "").strip() if item else ""
        if not item or not categoria_bruta:
            falhas["sem_categoria_na_resposta"] += 1
            continue
        if categoria_bruta.upper() == SENTINELA_SEM_CATEGORIA:
            falhas["nenhuma_do_catalogo"] += 1
            continue
        nome_categoria = catalogo.busca(categoria_bruta)
        if nome_categoria is None:
            falhas["categoria_nao_existe_no_catalogo"] += 1
            continue
        resultados.append(
            {
                "id_sessao_48h": row.id_sessao_48h,
                "cpf": row.cpf,
                "telefone": row.telefone,
                "nome_campanha": row.nome_campanha,
                "nome_eixo": row.nome_eixo,
                "resumo_gerado": str(item.get("resumo") or "").strip(),
                "hsm_texto": row.hsm_texto,
                "conversa_completa": row.conversa_completa,
                "categoria": nome_categoria,
                "categoria_justificativa": item.get("justificativa") or "",
            }
        )
    return resultados, falhas


@task
def classifica_conversas(
    df_sessoes: pd.DataFrame,
    bf_key: str,
    bifrost_model: str,
    tamanho_lote_classificacao: int,
    tamanho_lote_descoberta: int,
    teto_amostra_descoberta: int,
) -> tuple[pd.DataFrame, dict[str, str]]:
    bifrost = BifrostClient(api_key=bf_key, model=bifrost_model)
    catalogo = CatalogoCategorias()

    log(f"[DESCOBERTA] estabilizando catálogo a partir de {len(df_sessoes)} conversa(s) engajada(s)...")
    descobre_categorias(df_sessoes, bifrost, catalogo, tamanho_lote_descoberta, teto_amostra_descoberta)
    log(f"[DESCOBERTA] catálogo com {len(catalogo.como_dict())} categoria(s). Iniciando classificação.")

    max_output_tokens = max_output_tokens_para(tamanho_lote_classificacao)
    resultados_totais: list[dict] = []
    falhas_totais: Counter = Counter()
    n_lotes = math.ceil(len(df_sessoes) / tamanho_lote_classificacao)
    for n, i in enumerate(range(0, len(df_sessoes), tamanho_lote_classificacao), start=1):
        lote = df_sessoes.iloc[i : i + tamanho_lote_classificacao]
        resultados, falhas_lote = _classifica_lote(lote, bifrost, catalogo, max_output_tokens)
        resultados_totais.extend(resultados)
        falhas_totais.update(falhas_lote)
        log(f"[CLASSIFICACAO] lote {n}/{n_lotes}: {len(resultados)} classificada(s), {sum(falhas_lote.values())} falha(s).")

    detalhe_falhas = ", ".join(f"{motivo}={qtd}" for motivo, qtd in falhas_totais.most_common()) or "nenhuma"
    log(f"[CLASSIFICACAO] {len(resultados_totais)} classificada(s), {sum(falhas_totais.values())} falha(s) [{detalhe_falhas}].")

    df_classificada = pd.DataFrame(resultados_totais)
    for col in ["juiz_veredito", "juiz_categoria_esperada", "juiz_justificativa"]:
        df_classificada[col] = ""
    return df_classificada, catalogo.como_dict()

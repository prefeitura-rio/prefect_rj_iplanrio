# -*- coding: utf-8 -*-
"""Descoberta de categorias — monta o catálogo a partir de uma amostra ALEATÓRIA das
conversas, em lotes. Usada pelo material de validação (o catálogo vira o Sheet
Catálogo que a secretaria valida), pelo relatório rápido (catálogo automático, sem
validação) e pelo relatório de engajamento, pra SUGERIR categorias novas a partir do
Catálogo existente (sugere_categorias — só aparecem no relatório, não entram no
Catálogo nem na classificação). Só esta etapa pode CRIAR categoria — a classificação
só escolhe entre as existentes.

Amostra aleatória pra evitar viés de ordem: numa classificação direta, quem é
classificado cedo ficaria preso ao catálogo como ele estava naquele momento, mesmo
que uma categoria melhor devesse existir.

Cada categoria nova vem com até N_EXEMPLOS_DESCOBERTA conversas do lote em que foi
criada, que a LLM aponta como exemplos claros (a evidência que a fez criar a
categoria) — vão pra coluna "Exemplos de conversas" do Catálogo. Lotes seguintes não
completam exemplos de categoria antiga: tema raro fica com poucos exemplos, o que também
informa a secretaria."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    LOTES_SEM_NOVIDADE_LIMITE,
    N_EXEMPLOS_DESCOBERTA,
    SEED_DESCOBERTA,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.extract import textos_hsm_prompt
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bifrost import BifrostClient, max_output_tokens_para
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.catalogo import CatalogoCategorias
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.texto import strip_code_fence

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
_PROMPT_DESCOBERTA = (_PROMPTS_DIR / "descoberta.txt").read_text(encoding="utf-8")


def interpreta_resposta_lote(
    parsed: object, lote: pd.DataFrame, catalogo: CatalogoCategorias
) -> tuple[dict[str, list[str]], int]:
    """Resposta JSON de 1 lote -> registra as categorias novas no catálogo e devolve
    ({categoria nova canônica: [id_sessao_48h dos exemplos]}, nº de itens inválidos).

    Exemplos só de categoria criada NESTE lote — se a LLM "propõe" de novo uma que já
    existe (mesmo nome, outra grafia), ela não é recriada e os exemplos dela são
    ignorados. Número de conversa fora do lote (a LLM às vezes inventa) é descartado;
    no máximo N_EXEMPLOS_DESCOBERTA por categoria, sem repetir."""
    if not isinstance(parsed, list):
        raise ValueError(f"resposta não é uma lista JSON (veio {type(parsed).__name__})")

    ids_lote = lote["id_sessao_48h"].tolist()
    novas: dict[str, list[str]] = {}
    n_invalidos = 0
    for item in parsed:
        nome = str(item.get("categoria") or "").strip() if isinstance(item, dict) else ""
        if not nome:
            n_invalidos += 1
            continue
        if catalogo.busca(nome) is not None:
            continue  # já existia: não recria nem pega exemplos
        canonica = catalogo.registra(nome, str(item.get("descricao") or "").strip())
        ids: list[str] = []
        for bruto in item.get("exemplos") or []:
            try:
                posicao = int(bruto)
            except (TypeError, ValueError):
                n_invalidos += 1
                continue
            if not 1 <= posicao <= len(ids_lote):
                n_invalidos += 1
                continue
            if ids_lote[posicao - 1] not in ids and len(ids) < N_EXEMPLOS_DESCOBERTA:
                ids.append(ids_lote[posicao - 1])
        novas[canonica] = ids
    return novas, n_invalidos


def _descobre_lote(
    lote: pd.DataFrame, bifrost: BifrostClient, catalogo: CatalogoCategorias, max_output_tokens: int
) -> tuple[dict[str, list[str]], int]:
    conversas_texto = "\n\n".join(
        f"### CONVERSA {i} (id_sessao_48h={row.id_sessao_48h})\n{row.conversa_completa}"
        for i, row in enumerate(lote.itertuples(), start=1)
    )
    prompt = (
        _PROMPT_DESCOBERTA.replace("<<HSM_TEXTO>>", textos_hsm_prompt(lote))
        .replace("<<CATALOGO_CATEGORIAS>>", catalogo.texto_prompt())
        .replace("<<CONVERSAS>>", conversas_texto)
    )
    try:
        resposta = bifrost.ask(prompt, max_output_tokens=max_output_tokens)
        texto = BifrostClient.extract_text(resposta)
        return interpreta_resposta_lote(json.loads(strip_code_fence(texto), strict=False), lote, catalogo)
    except Exception as exc:  # lote que falha não derruba a descoberta — os outros seguem
        print(f"  [FALHA LOTE DESCOBERTA] {len(lote)} conversa(s): {exc}")
        return {}, len(lote)


def descobre_categorias(
    df_sessoes: pd.DataFrame,
    bifrost: BifrostClient,
    catalogo: CatalogoCategorias,
    tamanho_lote_descoberta: int,
    teto_amostra_descoberta: int,
) -> dict[str, list[str]]:
    """Preenche `catalogo` com as categorias descobertas e devolve os exemplos:
    {categoria: [id_sessao_48h, ...]} (do lote em que cada uma foi criada), na ordem em
    que as categorias foram criadas. Para cedo quando LOTES_SEM_NOVIDADE_LIMITE
    lotes seguidos não trazem categoria nova."""
    if len(df_sessoes) > teto_amostra_descoberta:
        amostra = df_sessoes.sample(n=teto_amostra_descoberta, random_state=SEED_DESCOBERTA)
    else:
        amostra = df_sessoes.sample(frac=1, random_state=SEED_DESCOBERTA)

    max_output_tokens = max_output_tokens_para(tamanho_lote_descoberta)
    exemplos: dict[str, list[str]] = {}
    lotes_sem_novidade = 0
    for i in range(0, len(amostra), tamanho_lote_descoberta):
        lote = amostra.iloc[i : i + tamanho_lote_descoberta]
        novas, n_invalidos = _descobre_lote(lote, bifrost, catalogo, max_output_tokens)
        exemplos.update(novas)
        n_novas = len(novas)
        print(
            f"[DESCOBERTA] lote {i // tamanho_lote_descoberta + 1}: {n_novas} categoria(s) nova(s), "
            f"{sum(len(ids) for ids in novas.values())} exemplo(s), {n_invalidos} item(ns) inválido(s). "
            f"Catálogo com {len(catalogo.como_dict())}."
        )
        if n_novas == 0:
            lotes_sem_novidade += 1
            if lotes_sem_novidade >= LOTES_SEM_NOVIDADE_LIMITE:
                print(f"[DESCOBERTA] {lotes_sem_novidade} lote(s) seguido(s) sem novidade — catálogo estável, parando.")
                break
        else:
            lotes_sem_novidade = 0

    # Ordem de criação das categorias (como_dict preserva a ordem de registro).
    return {nome: exemplos.get(nome, []) for nome in catalogo.como_dict()}


@dataclass(frozen=True)
class SugestaoCategoria:
    nome: str
    descricao: str
    ids_exemplo: tuple[str, ...]  # id_sessao_48h das conversas que levaram a LLM a sugerir


def sugere_categorias(
    df_sessoes: pd.DataFrame,
    bifrost: BifrostClient,
    ja_no_catalogo: CatalogoCategorias,
    tamanho_lote_descoberta: int,
    teto_amostra_descoberta: int,
) -> list[SugestaoCategoria]:
    """Sugestões de categorias NOVAS pro relatório de engajamento: roda a descoberta
    partindo de todas as categorias que já estão no Sheet do Catálogo (a LLM só propõe
    o que não está lá) e devolve só as que surgiram. Não altera `ja_no_catalogo` nem o
    Catálogo — as sugestões só aparecem no relatório."""
    partida = CatalogoCategorias()
    for nome, descricao in ja_no_catalogo.como_dict().items():
        partida.registra(nome, descricao)
    existentes = set(partida.como_dict())
    exemplos = descobre_categorias(df_sessoes, bifrost, partida, tamanho_lote_descoberta, teto_amostra_descoberta)
    descricoes = partida.como_dict()
    return [
        SugestaoCategoria(nome, descricoes[nome], tuple(ids))
        for nome, ids in exemplos.items()
        if nome not in existentes
    ]

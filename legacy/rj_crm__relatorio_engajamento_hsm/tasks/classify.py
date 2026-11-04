# -*- coding: utf-8 -*-
"""Classificação — 1 chamada de LLM por conversa, em paralelo. Só ESCOLHE entre as
categorias do catálogo recebido (nunca cria): o Catálogo validado no relatório de
engajamento, o catálogo automático da descoberta no relatório rápido.

Uma conversa pode ter mais de uma categoria (temas centrais; a ordem não importa).
Quando nenhuma serve, a conversa recebe CATEGORIA_SEM_CATALOGO — aparece no relatório
como categoria própria (sinal pra secretaria criar categoria Nova). Falha da LLM (ou
resposta só com categorias fora do catálogo) deixa a conversa sem categoria, com o
motivo em `falha_classificacao` — fica fora das métricas, mas vai pra planilha.

Few-shot (só no relatório de engajamento): cada conversa vai com os N exemplos
revisados mais parecidos da MESMA campanha (banco do RAG, ver tasks/rag.py), comparando
só as falas do cidadão (utils/embedding.py). Banco vazio = zero-shot.

Na mesma chamada, a LLM devolve o sentimento do cidadão (config.SENTIMENTOS); valor
inválido fica vazio, sem marcar a conversa como falha.

Ordem do prompt: instruções + catálogo (iguais em todas as chamadas) primeiro, o que
varia (HSM, exemplos, conversa) no fim — aproveita o cache de prefixo do Gemini."""

from __future__ import annotations

import json
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    CATEGORIA_SEM_CATALOGO,
    MAX_CHARS_EXEMPLO_RAG,
    MAX_OUTPUT_TOKENS_CONVERSA,
    SENTIMENTOS,
    SENTINELA_SEM_CATEGORIA,
    SEP_CATEGORIAS,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.tasks.rag import BancoExemplos
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bifrost import BifrostClient
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.catalogo import CatalogoCategorias
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.embedding import gera_embeddings_conversas, top_k_similares
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.texto import strip_code_fence

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"
_PROMPT_CLASSIFICACAO = (_PROMPTS_DIR / "classificacao.txt").read_text(encoding="utf-8")


@dataclass(frozen=True)
class ResultadoClassificacao:
    categorias: tuple[str, ...] = ()
    justificativa: str = ""
    resumo: str = ""
    sentimento: str = ""  # um de SENTIMENTOS, ou "" se a LLM não devolveu um válido
    falha: str = ""  # motivo, se a classificação falhou (categorias vazio)


_SENTIMENTO_POR_CHAVE = {s.lower(): s for s in SENTIMENTOS}


def interpreta_resposta(parsed: object, catalogo: CatalogoCategorias) -> ResultadoClassificacao:
    """JSON de 1 conversa -> categorias canônicas do catálogo. Nome fora do catálogo é
    descartado; o sentinela "nenhuma do catálogo" só vale se não vier categoria válida
    junto. ValueError se não sobrar nada utilizável."""
    if not isinstance(parsed, dict):
        raise ValueError(f"resposta não é um objeto JSON (veio {type(parsed).__name__})")
    brutas = parsed.get("categorias")
    if isinstance(brutas, str):
        brutas = [brutas]
    if not isinstance(brutas, list):
        raise ValueError("resposta sem a lista 'categorias'")

    validas: list[str] = []
    fora_do_catalogo: list[str] = []
    nenhuma = False
    for bruta in brutas:
        nome = str(bruta).strip()
        if not nome:
            continue
        if nome.upper() == SENTINELA_SEM_CATEGORIA:
            nenhuma = True
            continue
        canonica = catalogo.busca(nome)
        if canonica is None:
            fora_do_catalogo.append(nome)
        elif canonica not in validas:
            validas.append(canonica)

    if not validas and not nenhuma:
        raise ValueError(f"nenhuma categoria do catálogo na resposta (fora do catálogo: {fora_do_catalogo})")
    return ResultadoClassificacao(
        categorias=tuple(validas) if validas else (CATEGORIA_SEM_CATALOGO,),
        justificativa=str(parsed.get("justificativa") or "").strip(),
        resumo=str(parsed.get("resumo") or "").strip(),
        sentimento=_SENTIMENTO_POR_CHAVE.get(str(parsed.get("sentimento") or "").strip().lower(), ""),
    )


def _trunca(texto: str, limite: int) -> str:
    texto = texto or ""
    return texto if len(texto) <= limite else texto[:limite].rstrip() + " […]"


SEM_EXEMPLOS = (
    "(Esta campanha ainda não tem conversas revisadas pela equipe — não há exemplos. "
    "Classifique só pelo catálogo e pelas regras acima.)"
)


def bloco_exemplos(exemplos: pd.DataFrame) -> str:
    """Conteúdo de <<EXEMPLOS>> (o cabeçalho da seção está no prompt). Sem exemplo, o
    aviso SEM_EXEMPLOS — a LLM sabe que a seção vazia é proposital."""
    if exemplos is None or exemplos.empty:
        return SEM_EXEMPLOS
    partes = []
    for i, ex in enumerate(exemplos.itertuples(), start=1):
        partes.append(f"### EXEMPLO {i}\n{_trunca(ex.conversa, MAX_CHARS_EXEMPLO_RAG)}")
        partes.append(f"Categorias corretas: {SEP_CATEGORIAS.join(ex.categorias)}")
        if str(ex.justificativa_revisao or "").strip():
            partes.append(f"Observação da revisão: {ex.justificativa_revisao}")
        partes.append("")
    return "\n".join(partes).strip()


def monta_prompt(catalogo: CatalogoCategorias, hsm_texto: str, exemplos: str, conversa: str) -> str:
    return (
        _PROMPT_CLASSIFICACAO.replace("<<CATALOGO_CATEGORIAS>>", catalogo.texto_prompt())
        .replace("<<HSM_TEXTO>>", (hsm_texto or "").strip() or "(texto do HSM indisponível)")
        .replace("<<EXEMPLOS>>", exemplos)
        .replace("<<CONVERSA>>", conversa or "")
    )


def exemplos_por_conversa(df_sessoes: pd.DataFrame, banco: BancoExemplos | None, n_exemplos: int) -> list[str]:
    """Bloco de exemplos de cada conversa (mesma ordem de df_sessoes): os n_exemplos
    revisados mais parecidos, pela fala do cidadão."""
    if banco is None or banco.vazio or n_exemplos <= 0:
        return [SEM_EXEMPLOS] * len(df_sessoes)
    consultas = gera_embeddings_conversas(df_sessoes["conversa_completa"].tolist())
    indices, _ = top_k_similares(consultas, banco.embeddings, n_exemplos)
    return [bloco_exemplos(banco.exemplos.iloc[linha]) for linha in indices]


def _classifica_uma(bifrost: BifrostClient, prompt: str, catalogo: CatalogoCategorias) -> ResultadoClassificacao:
    try:
        resposta = bifrost.ask(prompt, max_output_tokens=MAX_OUTPUT_TOKENS_CONVERSA)
        texto = BifrostClient.extract_text(resposta)
        return interpreta_resposta(json.loads(strip_code_fence(texto), strict=False), catalogo)
    except Exception as exc:  # 1 conversa que falha não derruba as outras
        return ResultadoClassificacao(falha=f"{type(exc).__name__}: {exc}"[:300])


@task(log_prints=True)
def classifica_conversas(
    df_sessoes: pd.DataFrame,
    catalogo: CatalogoCategorias,
    bifrost: BifrostClient,
    concorrencia_llm: int,
    banco: BancoExemplos | None = None,
    n_exemplos_rag: int = 0,
) -> pd.DataFrame:
    """df_sessoes (saída de extrai_conversas) + colunas `categorias` (lista),
    `categoria_justificativa`, `resumo_gerado`, `sentimento` e `falha_classificacao` ("" se ok)."""
    blocos = exemplos_por_conversa(df_sessoes, banco, n_exemplos_rag)
    prompts = [
        monta_prompt(catalogo, linha.hsm_texto, bloco, linha.conversa_completa)
        for linha, bloco in zip(df_sessoes.itertuples(), blocos, strict=True)
    ]

    por_thread = threading.local()

    def classifica(prompt: str) -> ResultadoClassificacao:
        if not hasattr(por_thread, "cliente"):
            por_thread.cliente = bifrost.clone()
        return _classifica_uma(por_thread.cliente, prompt, catalogo)

    modo = "zero-shot" if all(b == SEM_EXEMPLOS for b in blocos) else "few-shot"
    print(f"[CLASSIFICACAO] {len(prompts)} conversa(s), {modo}, até {concorrencia_llm} chamada(s) em paralelo...")
    with ThreadPoolExecutor(max_workers=max(1, concorrencia_llm)) as executor:
        resultados = list(executor.map(classifica, prompts))

    df = df_sessoes.copy()
    df["categorias"] = [list(r.categorias) for r in resultados]
    df["categoria_justificativa"] = [r.justificativa for r in resultados]
    df["resumo_gerado"] = [r.resumo for r in resultados]
    df["sentimento"] = [r.sentimento for r in resultados]
    df["falha_classificacao"] = [r.falha for r in resultados]

    n_falhas = sum(1 for r in resultados if r.falha)
    por_categoria = Counter(c for r in resultados for c in r.categorias)
    print(
        f"[CLASSIFICACAO] {len(resultados) - n_falhas} classificada(s), {n_falhas} falha(s). "
        f"Por categoria: {dict(por_categoria.most_common())}. "
        f"Por sentimento: {dict(Counter(r.sentimento or '(vazio)' for r in resultados if not r.falha))}"
    )
    return df

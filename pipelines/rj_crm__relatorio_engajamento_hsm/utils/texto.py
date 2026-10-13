# -*- coding: utf-8 -*-
"""Helpers de texto puros, sem estado — usados por várias etapas (descoberta,
classificação, juiz, relatório)."""

from __future__ import annotations

import re
import unicodedata

from pipelines.rj_crm__relatorio_engajamento_hsm.config import ROTULO_FONTE


def normaliza_rotulo(texto) -> str:
    """Normaliza nome de categoria pra comparação (evita duplicar por acento/maiúscula/
    espaço vs underscore) — usado por CatalogoCategorias (utils/catalogo.py)."""
    texto = str(texto).strip().lower().replace("_", " ")
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", texto)


def strip_code_fence(texto: str) -> str:
    """Remove ```json ... ``` (ou variante sem linguagem) que a LLM às vezes envolve a
    resposta com, mesmo pedindo JSON puro no prompt."""
    texto = texto.strip()
    if texto.startswith("```"):
        texto = re.sub(r"^```(\w+)?\s*|\s*```$", "", texto, flags=re.IGNORECASE).strip()
    return texto


_PREFIXOS_FALA = tuple(f"{rotulo}:" for rotulo in ROTULO_FONTE.values())
_PREFIXO_CIDADAO = f"{ROTULO_FONTE['AI_AGENT_CIDADAO']}:"


def falas_cidadao(conversa: str) -> str:
    """Só as falas do CIDADÃO de uma conversa_completa (formato de tasks/extract.py) —
    é o texto usado no embedding do RAG: a categoria é definida pela fala do cidadão, e
    as respostas do agente são longas e parecidas entre si (no benchmark de
    tmp/avaliacao_embeddings, embedar a conversa inteira derrubou a precisão dos
    exemplos recuperados em 10-24 pontos).

    Segue quem está falando linha a linha: numa mensagem com quebra de linha só a 1ª
    linha tem o prefixo, as seguintes continuam sendo do mesmo falante. Conversa sem
    nenhuma fala do cidadão volta inteira (não deveria acontecer — extract.py só mantém
    sessão com pelo menos 1 fala dele)."""
    falas: list[str] = []
    do_cidadao = False
    for linha in (conversa or "").split("\n"):
        conteudo = linha
        if linha.startswith(_PREFIXOS_FALA):
            do_cidadao = linha.startswith(_PREFIXO_CIDADAO)
            conteudo = linha.split(":", 1)[1]
        if do_cidadao and conteudo.strip():
            falas.append(conteudo.strip())
    return "\n".join(falas) if falas else (conversa or "")

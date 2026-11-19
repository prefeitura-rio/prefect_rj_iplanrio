# -*- coding: utf-8 -*-
"""
Juiz de tema/motivo: confirma, por LLM, cada match das regras de taxonomia_regras.

As regras (regex) leem palavras, não sentido — ex.: o `motivo` "A responsável pela
associação passou mal e fechou o local" casava "Problema de saúde próprio" (`passou mal`)
e "Indisponibilidade do posto" (`fech` + `local`). O juiz lê o mesmo texto que a regra leu
(`resumo` pra tema, `motivo` pra motivo) e responde se ele pertence à categoria. Só tira
rótulo, nunca põe: a cobertura continua sendo da regra.

Prompt (prompts/juiz_taxonomia.txt) e parâmetros são os do juiz que mediu a precisão das
regras no clustering_v3 (prompts/coleta.txt, 04_coleta.julga / 08_precisao): mesma
definição completa da categoria (descricao, inclui, nao_inclui), lotes de textos de UMA
categoria, `[n] texto` por linha, resposta {"julgamentos": [{numero, pertence, por_que}]}.

Sem decisão do juiz -> pertence None (o rótulo da regra fica, ver tasks/taxonomia.py):
- lote falhou (Bifrost fora, timeout, JSON inválido depois de JUIZ_TENTATIVAS_JSON);
- item ausente ou fora do formato na resposta;
- regra sem inclui/nao_inclui no catálogo (o prompt não seria o validado — nem chama).
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

_TEMPLATE_JUIZ = (Path(__file__).resolve().parent.parent / "prompts" / "juiz_taxonomia.txt").read_text(encoding="utf-8")


@dataclass(frozen=True)
class ConfigJuiz:
    """Parâmetros do juiz — só valores simples (a task que recebe isto é NO_CACHE)."""

    bf_key: str
    base_url: str
    modelo: str
    prompt_versao: str
    tamanho_lote: int
    thinking_budget: int
    max_output_tokens: int
    tentativas_json: int
    max_workers: int
    max_tentativas: int
    espera_inicial: int


def _valor(texto) -> str:
    return "" if texto is None else str(texto)


def monta_prompt(escopo: str, regra: dict, textos: list[str]) -> str:
    """Mesma substituição de clustering_v3/utils/llm_client.carregar_prompt (None -> '')."""
    valores = {
        "ESCOPO": escopo,
        "NOME": regra["nome"],
        "NOME_FUNCAO": regra["nome_funcao"],
        "DESCRICAO": regra["descricao"],
        "INCLUI": regra["inclui"],
        "NAO_INCLUI": regra["nao_inclui"],
        "TEXTOS": "\n".join(f"[{n}] {_valor(t).strip()}" for n, t in enumerate(textos, start=1)),
    }
    prompt = _TEMPLATE_JUIZ
    for chave, valor in valores.items():
        prompt = prompt.replace(f"<<{chave}>>", _valor(valor))
    return prompt


def le_julgamentos(resposta: str, n_textos: int) -> dict[int, tuple[bool, str]]:
    """{numero: (pertence, por_que)} — mesma validação de 04_coleta.julga: numero inteiro
    dentro do lote e pertence booleano; o resto é ignorado (vira None em quem chama).
    JSONDecodeError sobe (quem chama repete a chamada)."""
    from pipelines.rj_crm__agentforce_classificacao_llm.tasks.classify import _strip_code_fence

    bruto = json.loads(_strip_code_fence(resposta), strict=False)
    julgamentos = {}
    for j in bruto.get("julgamentos", []) if isinstance(bruto, dict) else []:
        n = j.get("numero")
        if (
            isinstance(n, int)
            and not isinstance(n, bool)
            and 1 <= n <= n_textos
            and isinstance(j.get("pertence"), bool)
        ):
            julgamentos[n] = (j["pertence"], str(j.get("por_que", "")).strip())
    return julgamentos


def _julga_lote(cliente, cfg: ConfigJuiz, escopo: str, regra: dict, textos: list[str]) -> dict[int, tuple[bool, str]]:
    from pipelines.rj_crm__agentforce_classificacao_llm.tasks.classify import BifrostClient

    prompt = monta_prompt(escopo, regra, textos)
    for tentativa in range(1, cfg.tentativas_json + 1):
        resposta = BifrostClient.extract_text(
            cliente.ask(
                prompt,
                system_instruction="Responda em PT-BR.",
                max_output_tokens=cfg.max_output_tokens,
                temperature=0,
                thinking_budget=cfg.thinking_budget,
                max_tentativas=cfg.max_tentativas,
                espera_inicial=cfg.espera_inicial,
            )
        )
        try:
            return le_julgamentos(resposta, len(textos))
        except json.JSONDecodeError:
            if tentativa == cfg.tentativas_json:
                raise
            print(f"[JUIZ] '{regra['nome_funcao']}': JSON inválido — repetindo ({tentativa}/{cfg.tentativas_json})")
    return {}


def julga(cfg: ConfigJuiz, pedidos: list[dict]) -> list[tuple[bool | None, str | None]]:
    """Julga cada pedido {"escopo", "regra", "texto"} e devolve (pertence, por_que) na MESMA
    ordem. Pedidos da mesma categoria (mesmo escopo + regra) vão juntos em lotes de
    cfg.tamanho_lote; os lotes rodam em paralelo (cfg.max_workers)."""
    from pipelines.rj_crm__agentforce_classificacao_llm.tasks.classify import BifrostClient

    resultado: list[tuple[bool | None, str | None]] = [(None, None)] * len(pedidos)
    if not pedidos:
        return resultado

    grupos: dict[tuple, list[int]] = {}
    for i, p in enumerate(pedidos):
        regra = p["regra"]
        if not regra.get("inclui") or not regra.get("nao_inclui"):
            continue  # definição incompleta: não é o prompt validado -> fica None
        chave = (p["escopo"], regra["agrupamento"], regra["categoria_pai"], regra["nome_funcao"])
        grupos.setdefault(chave, []).append(i)

    lotes = [
        (chave, indices[k : k + cfg.tamanho_lote])
        for chave, indices in grupos.items()
        for k in range(0, len(indices), cfg.tamanho_lote)
    ]
    cliente = BifrostClient(api_key=cfg.bf_key, base=cfg.base_url, model=cfg.modelo)

    def _roda(lote):
        chave, indices = lote
        regra = pedidos[indices[0]]["regra"]
        try:
            return indices, _julga_lote(cliente, cfg, chave[0], regra, [pedidos[i]["texto"] for i in indices]), None
        except Exception as e:  # noqa: BLE001 — lote quebrado fica None, não derruba a carga
            return indices, {}, f"{type(e).__name__}: {e}"

    n_falhas = 0
    with ThreadPoolExecutor(max_workers=cfg.max_workers) as executor:
        for indices, julgamentos, erro in executor.map(_roda, lotes):
            if erro:
                n_falhas += 1
                print(
                    f"[JUIZ] lote de {len(indices)} de '{pedidos[indices[0]]['regra']['nome_funcao']}' falhou: {erro}"
                )
            for n, i in enumerate(indices, start=1):
                if n in julgamentos:
                    resultado[i] = julgamentos[n]

    n_sem_definicao = len(pedidos) - sum(len(v) for v in grupos.values())
    print(
        f"[JUIZ] {len(pedidos)} par(es) em {len(lotes)} lote(s): {n_falhas} lote(s) com falha, "
        f"{n_sem_definicao} par(es) de regra sem inclui/nao_inclui (não julgados)."
    )
    return resultado

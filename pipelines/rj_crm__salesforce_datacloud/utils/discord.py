# -*- coding: utf-8 -*-
"""
Montagem das mensagens do Discord (puro — o envio fica em tasks/notify.py).

Texto simples com markdown do Discord no campo `content` do webhook, mesmo
formato de pipelines/rj_crm__agentforce_classificacao_llm/tasks/notify.py.
"""

from __future__ import annotations

NOME = "salesforce-datacloud"
LIMITE_CARACTERES = 2000  # teto do `content` num webhook do Discord


def _corta(mensagem: str) -> str:
    if len(mensagem) <= LIMITE_CARACTERES:
        return mensagem
    return mensagem[: LIMITE_CARACTERES - 20].rstrip() + "\n… (cortado)"


def mensagem_falha_tabela(tabela: str, grupo: str, janela: str, erro: str) -> str:
    """Tabela não-crítica falhou — o flow seguiu pras próximas."""
    return _corta(
        f"⚠️ **{NOME}: tabela `{tabela}` falhou** ({grupo}) — o resto do flow seguiu\n\n"
        f"**Janela:** {janela}\n"
        f"**Erro:** ```{erro[:1500]}```"
    )


def mensagem_falha_flow(flow_run: str, erro: str | None) -> str:
    """Flow abortou (tabela crítica, auth, BigQuery...)."""
    return _corta(
        f"🚨 **{NOME}: FALHOU**\n\n"
        f"**Flow run:** {flow_run}\n"
        f"**Erro:** ```{(erro or 'sem mensagem')[:1500]}```"
    )


def mensagem_resumo(
    rotulo: str,
    resultados: dict[str, dict[str, int]],
    falhas: list[str],
    minutos: float,
) -> str:
    """Resumo de fim de run: linhas (inseridas + atualizadas) por tabela."""
    icone = "⚠️" if falhas else "✅"
    linhas = [f"{icone} **{NOME}: concluído** — {rotulo}", ""]
    for grupo, tabelas in resultados.items():
        linhas.append(f"**{grupo}**")
        linhas += [f"• `{t}`: {n:,} linhas".replace(",", ".") for t, n in tabelas.items()]
    if falhas:
        linhas += ["", f"❌ **Falharam:** {', '.join(f'`{t}`' for t in falhas)}"]
    linhas += ["", f"⏱️ {minutos:.1f} min"]
    return _corta("\n".join(linhas))

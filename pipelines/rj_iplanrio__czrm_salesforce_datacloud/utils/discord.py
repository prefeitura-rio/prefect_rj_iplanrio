"""Mensagens de notificação (texto puro, sem I/O)."""

from __future__ import annotations

_LIMITE = 1900  # Discord corta em 2000


def _corta(texto: str) -> str:
    return texto if len(texto) <= _LIMITE else texto[: _LIMITE - 3] + "..."


def mensagem_falha_tabela(tabela: str, grupo: str, janela: str, erro: str) -> str:
    """Mensagem de falha de uma tabela."""
    return _corta(f"❌ **{tabela}** ({grupo}) falhou\njanela: {janela}\n```{erro[:900]}```")


def mensagem_falha_flow(flow_run) -> str:
    """Mensagem de falha que abortou o flow."""
    return (
        f"🛑 **czrm-salesforce-datacloud** abortou\n"
        f"run: `{flow_run.name}`\n"
        f"estado: `{flow_run.state.name if flow_run.state else '?'}`"
    )


def mensagem_resumo(rotulo: str, resultados: dict[str, dict[str, int]], falhas: list[str], minutos: float) -> str:
    """Resumo de fim de run (só nos modos frios e na carga inicial)."""
    linhas = [f"✅ **czrm-salesforce-datacloud** — {rotulo} ({minutos:.1f} min)"]
    for grupo, tabelas in resultados.items():
        detalhe = ", ".join(f"{nome}: {qtd}" for nome, qtd in tabelas.items())
        linhas.append(f"- **{grupo}**: {detalhe}")
    if falhas:
        linhas.append(f"- ⚠️ falharam: {', '.join(falhas)}")
    return _corta("\n".join(linhas))

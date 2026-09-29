# -*- coding: utf-8 -*-
"""
Leitura das queries de extração, que ficam em sql/ — nunca inline no .py.

  - .sql  : Data Cloud Query API (DMOs/DLLs). Aceita comentário '--'.
  - .soql : CRM REST (SOQL). SOQL não tem sintaxe de comentário — explicação
            fica no .py que usa a query.

Todas usam {data_inicio}/{data_fim} (e, se precisar, outros placeholders),
preenchidos em flow.py (processar_tabela) via str.format.
"""

from __future__ import annotations

from pathlib import Path

_SQL_DIR = Path(__file__).parent.parent / "sql"


def ler_query(nome_arquivo: str) -> str:
    """Conteúdo de sql/<nome_arquivo> (ex.: 'ai_agent_session.sql')."""
    return (_SQL_DIR / nome_arquivo).read_text()

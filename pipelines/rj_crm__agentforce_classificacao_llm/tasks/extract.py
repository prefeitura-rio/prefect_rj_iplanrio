# -*- coding: utf-8 -*-
"""
Extração incremental das sessões do agente (id_sessao_receptivo) ainda não classificadas.

Portado de clustering/classificacao_inicial.ipynb (seções 1-2) — mesma lógica, adaptada
para produção:
  - janela de busca fixa (constants.LOOKBACK_DAYS), não a data fixa do notebook original;
  - anti-join contra a tabela destino já embutido na query (ver queries/extract_sessoes.sql);
  - HSM ligado à sessão (com o texto já renderizado) e resposta atrasada a botão vêm
    prontos do dbt, na própria query.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.rj_crm__agentforce_classificacao_llm.utils.bigquery import get_bq_client

_QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"


@task(log_prints=True, retries=2, retry_delay_seconds=30)
def extrai_sessoes_nao_classificadas(
    project_id: str,
    source_table: str,
    destino_full_table_id: str,
    lookback_days: int,
) -> pd.DataFrame:
    """Roda a query de extração incremental — janela fixa + DOIS filtros de "já
    classificada": llm.classificacao_datahora IS NULL na fonte (pré-filtro barato, pode
    estar até 15min desatualizado) e o anti-join contra a tabela destino (garantia final,
    cobre esse desincronismo — ver cabeçalho da query). Retorna 1 linha por sessão do agente
    (id_sessao_receptivo) pendente de classificação, com o HSM que o dbt ligou a ela (se
    houver) e a flag resposta_atrasada_btn."""
    sql_template = (_QUERIES_DIR / "extract_sessoes.sql").read_text()
    query = sql_template.format(
        lookback_days=lookback_days,
        source_table=source_table,
        destino_full_table_id=destino_full_table_id,
    )

    client = get_bq_client(project_id)
    df = client.query(query).to_dataframe()
    n_atrasada = int(df["resposta_atrasada_btn"].sum()) if not df.empty else 0
    print(
        f"[EXTRACT] {len(df)} sessão(ões) pendente(s) de classificação (janela: {lookback_days}d), "
        f"{n_atrasada} resposta(s) atrasada(s) a botão (não vão pra LLM)."
    )
    return df

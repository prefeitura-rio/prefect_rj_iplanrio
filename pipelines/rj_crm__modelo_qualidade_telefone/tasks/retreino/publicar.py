"""Monta a tabela longa de avaliação do retreino (treino/held-out + SHAP + simulação +
gate, 1 linha por métrica) e publica no BigQuery — delete da mesma versão + append, nunca
upsert (uma execução nunca é reprocessada parcialmente).
"""

import json

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from prefect import task
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

from pipelines.rj_crm__modelo_qualidade_telefone import constants
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.avaliar_simulacao import (
    NOME_HEURISTICA,
    NOME_MODELO_NOVO,
    ResultadoSimulacao,
)
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.promover import DecisaoGate
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.treinar import ResultadoTreino

logger = get_logger(__name__)

# Cabeçalho das colunas do resumo da simulação (ver avaliar_simulacao.tabela_resumo) pro
# nome de métrica que vira a tabela longa — snake_case, sem o "%"/"|" do cabeçalho original.
COLUNAS_RESUMO_SIMULACAO = {
    "% telefone real avaliado": "pct_avaliado",
    "% mudaria | disparo falhou": "pct_mudaria_falhou",
    "% manteria | disparo falhou": "pct_manteria_falhou",
    "% mudaria | disparo sucesso": "pct_mudaria_sucesso",
    "% manteria | disparo sucesso": "pct_manteria_sucesso",
    "gap": "gap",
}

SCHEMA_AVALIACAO = [
    bigquery.SchemaField("data_execucao", "DATE"),
    bigquery.SchemaField("versao", "STRING"),
    bigquery.SchemaField("origem", "STRING"),
    bigquery.SchemaField("algoritmo", "STRING"),
    bigquery.SchemaField("metrica", "STRING"),
    bigquery.SchemaField("valor", "FLOAT64"),
    bigquery.SchemaField("detalhe", "STRING"),
]


def _linha(origem: str, metrica: str, valor: float, algoritmo: str | None = None, detalhe: str | None = None) -> dict:
    return {"origem": origem, "algoritmo": algoritmo, "metrica": metrica, "valor": float(valor), "detalhe": detalhe}


def linhas_treino_holdout(resultado_treino: ResultadoTreino) -> list[dict]:
    """1 linha por métrica do held-out (auc_roc, precisao, recall, f1, fbeta) — sempre do
    ``modelo_novo``, é o único avaliado no held-out."""
    return [
        _linha("treino_holdout", nome, valor, algoritmo=NOME_MODELO_NOVO)
        for nome, valor in resultado_treino.metricas_held_out.items()
    ]


def linhas_shap(resultado_treino: ResultadoTreino) -> list[dict]:
    """1 linha por feature (a métrica é o NOME da feature, o valor é a importância média)."""
    return [_linha("shap", feature, valor) for feature, valor in resultado_treino.importancia_shap.items()]


def linhas_simulacao(resultado_simulacao: ResultadoSimulacao) -> list[dict]:
    """1 linha por (algoritmo, métrica) do resumo + os p-valores dos testes pareados +
    2 linhas de contexto do pool (fração descartada, total de eventos)."""
    linhas = []
    for algoritmo, linha in resultado_simulacao.resumo.iterrows():
        for coluna, metrica in COLUNAS_RESUMO_SIMULACAO.items():
            linhas.append(_linha("simulacao", metrica, linha[coluna], algoritmo=algoritmo))
        teste_interno = resultado_simulacao.testes_gap_interno.get(algoritmo)
        if teste_interno is not None:
            linhas.append(_linha("simulacao", "p_valor_gap_interno", teste_interno.p_valor, algoritmo=algoritmo))

    if resultado_simulacao.teste_vs_aleatorio is not None:
        linhas.append(
            _linha("simulacao", "p_valor_vs_aleatorio", resultado_simulacao.teste_vs_aleatorio.p_valor, algoritmo=NOME_MODELO_NOVO)
        )
    if resultado_simulacao.teste_heuristica_vs_novo is not None:
        linhas.append(
            _linha(
                "simulacao", "p_valor_vs_modelo_novo", resultado_simulacao.teste_heuristica_vs_novo.p_valor,
                algoritmo=NOME_HEURISTICA,
            )
        )
    linhas.append(_linha("simulacao", "fracao_eventos_descartados", resultado_simulacao.fracao_eventos_descartados))
    linhas.append(_linha("simulacao", "n_eventos", resultado_simulacao.n_eventos))
    return linhas


def linhas_gate(decisao: DecisaoGate) -> list[dict]:
    """1 linha por valor medido pelo gate + a decisão final (com os motivos, se reprovado,
    no campo ``detalhe`` como JSON)."""
    linhas = [
        _linha("gate", "promovido", float(decisao.promovido), detalhe=json.dumps(decisao.motivos, ensure_ascii=False)),
        _linha("gate", "probabilidade_degenerada", float(decisao.probabilidade_degenerada)),
        _linha("gate", "gap_novo", decisao.gap_novo),
    ]
    if decisao.gap_producao is not None:
        linhas.append(_linha("gate", "gap_producao", decisao.gap_producao))
    if decisao.p_valor_vs_aleatorio is not None:
        linhas.append(_linha("gate", "p_valor_vs_aleatorio", decisao.p_valor_vs_aleatorio))
    return linhas


def monta_tabela_avaliacao(
    resultado_treino: ResultadoTreino,
    resultado_simulacao: ResultadoSimulacao,
    decisao: DecisaoGate,
    versao: str,
    data_execucao: str,
) -> pd.DataFrame:
    """Junta as 4 origens (treino_holdout, shap, simulacao, gate) na tabela longa.

    :param data_execucao: Data no formato ``YYYY-MM-DD`` (normalmente igual a ``versao``).
    :returns: Colunas ``data_execucao, versao, origem, algoritmo, metrica, valor, detalhe``.
    """
    linhas = [
        *linhas_treino_holdout(resultado_treino),
        *linhas_shap(resultado_treino),
        *linhas_simulacao(resultado_simulacao),
        *linhas_gate(decisao),
    ]
    df = pd.DataFrame(linhas, columns=["origem", "algoritmo", "metrica", "valor", "detalhe"])
    df.insert(0, "versao", versao)
    df.insert(0, "data_execucao", pd.to_datetime(data_execucao).date())
    return df


def _tabela_existe(client: bigquery.Client, dataset_id: str, table_id: str) -> bool:
    try:
        client.get_table(f"{client.project}.{dataset_id}.{table_id}")
        return True
    except NotFound:
        return False


def publica_avaliacao_bq(
    tabela: pd.DataFrame,
    environment: str,
    dataset_id: str = constants.DATASET_ID,
    table_id: str = constants.TABLE_ID_AVALIACAO,
) -> None:
    """Publica a tabela: apaga as linhas da mesma versão (se a tabela já existir) e faz
    append — nunca sobrescreve a tabela inteira, cada versão é independente.

    :param tabela: Saída de :func:`monta_tabela_avaliacao`.
    :param environment: ``"prod"`` ou ``"staging"`` — credenciais do secret do work pool.
    """
    credentials = get_bd_credentials_from_env(mode=environment)
    client = bigquery.Client(credentials=credentials, project=constants.PROJECT_ID)
    versao = str(tabela["versao"].iloc[0])

    if _tabela_existe(client, dataset_id, table_id):
        sql = load_query(
            constants.__file__, "deleta_avaliacao_versao", project=constants.PROJECT_ID,
            dataset_id=dataset_id, table_id=table_id,
        )
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("versao", "STRING", versao)]
        )
        client.query(sql, job_config=job_config).result()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=SCHEMA_AVALIACAO)
    job = client.load_table_from_dataframe(
        tabela, f"{client.project}.{dataset_id}.{table_id}", job_config=job_config
    )
    job.result()
    logger.info("%d linhas publicadas em %s.%s (versão %s).", len(tabela), dataset_id, table_id, versao)


@task
def monta_tabela_avaliacao_task(
    resultado_treino: ResultadoTreino,
    resultado_simulacao: ResultadoSimulacao,
    decisao: DecisaoGate,
    versao: str,
    data_execucao: str,
) -> pd.DataFrame:
    """Task-wrapper fina de :func:`monta_tabela_avaliacao`."""
    return monta_tabela_avaliacao(resultado_treino, resultado_simulacao, decisao, versao, data_execucao)


@task
def publica_avaliacao_bq_task(
    tabela: pd.DataFrame,
    environment: str,
    dataset_id: str = constants.DATASET_ID,
    table_id: str = constants.TABLE_ID_AVALIACAO,
) -> None:
    """Task-wrapper fina de :func:`publica_avaliacao_bq`."""
    publica_avaliacao_bq(tabela, environment, dataset_id, table_id)

"""Monta e publica os artefatos de UM retreino (treino/SHAP + simulação + gate) numa
subpasta do Drive nomeada pela versão do modelo (data e hora do treino, BRT — mesma
convenção do GCS, ver ``utils/modelo_store.py``).

Os números oficiais vivem no BigQuery (``tasks/retreino/publicar.py``); os arquivos daqui
são pra leitura humana — comparar rapidamente sem escrever uma query.
"""

import csv
import io

import matplotlib
import pandas as pd

matplotlib.use("Agg")  # sem display — só gera a imagem em memória
import matplotlib.pyplot as plt
import shap
from prefect import task
from prefect_rj_iplanrio.logging import get_logger

from pipelines.rj_crm__modelo_qualidade_telefone.constants import FEATURES
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.cobertura import calcula_cobertura
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.avaliar_simulacao import ResultadoSimulacao
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.promover import DecisaoGate
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.treinar import ResultadoTreino
from pipelines.rj_crm__modelo_qualidade_telefone.utils import drive

logger = get_logger(__name__)

# Slot 1 (azul) de references/palette.md do skill dataviz — validado (CVD/contraste) no
# tema claro, que é o único que interessa aqui (PNG estático, sem alternância de tema).
COR_BARRA = "#2a78d6"


def gera_csv_metricas(resultado: ResultadoTreino) -> bytes:
    """CSV de 2 colunas (metrica, valor): as 5 métricas do held-out + volume de treino."""
    linhas = [
        *resultado.metricas_held_out.items(),
        ("n_treino", resultado.n_treino),
        ("n_positivos", resultado.n_positivos),
        ("taxa_base_high_delivery", resultado.taxa_base_high_delivery),
    ]
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["metrica", "valor"])
    writer.writerows(linhas)
    return buffer.getvalue().encode("utf-8")


def gera_csv_importancia_shap(resultado: ResultadoTreino) -> bytes:
    """CSV de 1 linha por feature (feature, importancia_media_abs), da mais pra menos
    importante — mesmo formato de ``shap_importance.csv`` do repo do modelo."""
    ordenado = sorted(resultado.importancia_shap.items(), key=lambda item: -item[1])
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["feature", "importancia_media_abs"])
    writer.writerows(ordenado)
    return buffer.getvalue().encode("utf-8")


def gera_grafico_ranking_shap(resultado: ResultadoTreino, versao: str) -> bytes:
    """Barras horizontais com a importância média (|SHAP|) de cada feature, da menor (embaixo)
    pra maior (em cima) — mesmo gráfico de ``shap_importance.png`` do repo do modelo, com a
    cor do palette validado (skill dataviz)."""
    ordenado = sorted(resultado.importancia_shap.items(), key=lambda item: item[1])
    nomes = [nome for nome, _ in ordenado]
    valores = [valor for _, valor in ordenado]

    fig, ax = plt.subplots(figsize=(8, max(4, 0.3 * len(nomes))))
    ax.barh(nomes, valores, color=COR_BARRA)
    ax.set_xlabel("|valor SHAP| médio (impacto na previsão de HighDelivery)")
    ax.set_title(f"Importância das features — versão {versao}")
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return buffer.getvalue()


def gera_grafico_beeswarm_shap(resultado: ResultadoTreino, versao: str) -> bytes:
    """Beeswarm: 1 ponto por (feature, linha da amostra), no valor SHAP daquela linha,
    colorido pelo valor da feature — mostra a DISTRIBUIÇÃO do impacto, não só a média (é
    o que o ranking em barra não mostra). Usa o renderizador do pacote ``shap`` só pra
    plotagem (os valores em si vêm do LightGBM nativo — ver ``treinar.amostra_shap`` —,
    sem o pacote ``shap`` calcular nada)."""
    amostra = resultado.amostra_shap
    explicacao = shap.Explanation(
        values=amostra.shap_values,
        data=amostra.X.to_numpy(),
        feature_names=list(FEATURES),
    )

    fig = plt.figure(figsize=(9, max(4, 0.3 * len(FEATURES))))
    shap.plots.beeswarm(explicacao, max_display=len(FEATURES), show=False)
    plt.title(f"Distribuição do impacto de cada feature — versão {versao}")
    plt.tight_layout()

    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    return buffer.getvalue()


def gera_csv_simulacao(resultado_simulacao: ResultadoSimulacao) -> bytes:
    """CSV da tabela de resumo da simulação (1 linha por algoritmo) — mesmo formato de
    ``resultados_avaliacao.csv`` do repo do modelo."""
    buffer = io.StringIO()
    resultado_simulacao.resumo.round(4).to_csv(buffer)
    return buffer.getvalue().encode("utf-8")


def gera_texto_gate(decisao: DecisaoGate) -> bytes:
    """Texto simples com a decisão do gate e, se reprovado, os motivos — pra quem abrir a
    pasta entender o resultado sem consultar o BigQuery."""
    linhas = [
        f"promovido: {decisao.promovido}",
        f"gap do modelo novo: {decisao.gap_novo:.2f}",
        "gap do modelo em produção: "
        + (f"{decisao.gap_producao:.2f}" if decisao.gap_producao is not None else "(sem champion ainda)"),
        "p-valor vs. aleatório: "
        + (f"{decisao.p_valor_vs_aleatorio:.4f}" if decisao.p_valor_vs_aleatorio is not None else "(não calculado)"),
        f"probabilidade degenerada: {decisao.probabilidade_degenerada}",
    ]
    if decisao.motivos:
        linhas.append("")
        linhas.append("motivos da reprovação:")
        linhas.extend(f"  - {motivo}" for motivo in decisao.motivos)
    return ("\n".join(linhas) + "\n").encode("utf-8")


def gera_csv_cobertura(df_treino: pd.DataFrame) -> bytes:
    """CSV com o % de linhas preenchidas por feature (``tasks/cobertura.py``), calculado
    sobre o DataFrame CRU do treino — antes de virar ``X`` pro modelo, pra pegar fonte
    quebrada (uma feature virando sentinela/0 pra quase todo mundo não aparece só olhando
    o modelo já treinado).

    :param df_treino: Saída de ``tasks/retreino/extrair.py::extrai_treino`` (antes do split
        held-out/fit final — a cobertura é sobre o dataset inteiro).
    """
    buffer = io.StringIO()
    calcula_cobertura(df_treino).to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


def publica_relatorio(
    resultado_treino: ResultadoTreino,
    versao: str,
    drive_pasta_raiz_id: str,
    environment: str,
    resultado_simulacao: ResultadoSimulacao | None = None,
    decisao: DecisaoGate | None = None,
    df_treino: pd.DataFrame | None = None,
) -> None:
    """Sobe os artefatos do retreino pra subpasta ``versao`` da pasta raiz: sempre os 4 de
    treino/SHAP; simulação, gate e cobertura entram quando fornecidos (o retreino sempre
    passa os 3 — os parâmetros são opcionais só pra permitir montar/testar o relatório de
    treino sozinho).

    :param resultado_treino: Saída de ``treinar.treina``.
    :param versao: Nome da subpasta (data e hora do treino, BRT — mesma versão publicada no GCS).
    :param drive_pasta_raiz_id: ID da pasta raiz no Drive (precisa estar compartilhada,
        Editor, com a service account de ``BASEDOSDADOS_CREDENTIALS_<PROD|STAGING>``).
    :param environment: ``"prod"`` ou ``"staging"``.
    :param resultado_simulacao: Saída de ``avaliar_simulacao.avalia_simulacao``.
    :param decisao: Saída de ``promover.avalia_gate``.
    :param df_treino: Saída de ``extrair.extrai_treino`` (o DataFrame cru, não o
        ``ResultadoTreino``) — pra ``gera_csv_cobertura``.
    :raises RuntimeError: Se a pasta raiz não existir/não estiver acessível.
    """
    servico = drive.get_drive_service(environment)
    drive.confirma_pasta_raiz(servico, drive_pasta_raiz_id)
    pasta_id = drive.pasta_por_nome(servico, drive_pasta_raiz_id, versao)

    drive.upload_bytes(servico, pasta_id, "metricas_treino.csv", gera_csv_metricas(resultado_treino), "text/csv")
    drive.upload_bytes(
        servico, pasta_id, "importancia_shap.csv", gera_csv_importancia_shap(resultado_treino), "text/csv"
    )
    drive.upload_bytes(
        servico, pasta_id, "shap_ranking.png", gera_grafico_ranking_shap(resultado_treino, versao), "image/png"
    )
    drive.upload_bytes(
        servico, pasta_id, "shap_beeswarm.png", gera_grafico_beeswarm_shap(resultado_treino, versao), "image/png"
    )
    if resultado_simulacao is not None:
        drive.upload_bytes(servico, pasta_id, "simulacao.csv", gera_csv_simulacao(resultado_simulacao), "text/csv")
    if decisao is not None:
        drive.upload_bytes(servico, pasta_id, "gate.txt", gera_texto_gate(decisao), "text/plain")
    if df_treino is not None:
        drive.upload_bytes(servico, pasta_id, "cobertura.csv", gera_csv_cobertura(df_treino), "text/csv")

    logger.info("Relatório da versão %s publicado no Drive (pasta %s).", versao, pasta_id)


@task
def publica_relatorio_task(
    resultado_treino: ResultadoTreino,
    versao: str,
    drive_pasta_raiz_id: str,
    environment: str,
    resultado_simulacao: ResultadoSimulacao | None = None,
    decisao: DecisaoGate | None = None,
    df_treino: pd.DataFrame | None = None,
) -> None:
    """Task-wrapper fina de :func:`publica_relatorio`."""
    publica_relatorio(
        resultado_treino, versao, drive_pasta_raiz_id, environment, resultado_simulacao, decisao, df_treino
    )

"""Flow for rj_crm__modelo_qualidade_telefone.

Estima a probabilidade de cada telefone ser HighDelivery no WhatsApp (LightGBM) e, uma vez
por mês, retreina o modelo. Um único flow, dois modos — ``score`` roda a cada 3 dias e só
executa o modelo já treinado; ``retreino`` roda uma vez por mês e treina, avalia contra o
modelo em produção/heurística/aleatório e (se passar no gate) promove uma versão nova.
Ver TODO do projeto pra detalhe de cada parte.
"""

from datetime import date

from prefect import flow
from prefect_rj_iplanrio.logging import get_logger

from pipelines.rj_crm__modelo_qualidade_telefone import constants
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.carrega_modelo import (
    carrega_champion_opcional_task,
    carrega_champion_task,
)
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino import extrair, treinar
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.avaliar_simulacao import avalia_simulacao_task
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.promover import avalia_gate_task, promove_se_aprovado_task
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.publicar import (
    monta_tabela_avaliacao_task,
    publica_avaliacao_bq_task,
)
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.retreino.relatorio import publica_relatorio_task
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.score.pontua_telefones import pontua_telefones_task
from pipelines.rj_crm__modelo_qualidade_telefone.tasks.score.publica_tabela import publica_scores_task

logger = get_logger(__name__)

MODOS_VALIDOS = ("score", "retreino")


@flow(log_prints=True)
def rj_crm__modelo_qualidade_telefone(
    modo: str = "score",
    environment: str = "prod",
    dataset_id: str = constants.DATASET_ID,
    table_id_score: str = constants.TABLE_ID_SCORE,
    table_id_avaliacao: str = constants.TABLE_ID_AVALIACAO,
    raiz_modelos: str = constants.RAIZ_MODELOS,
    tamanho_lote: int = 300_000,
    n_trials: int = treinar.N_TRIALS_PADRAO,
    janela_recente_dias: int = extrair.JANELA_RECENTE_DIAS,
    exigir_gate: bool = True,
    promocao_automatica: bool = True,
    drive_pasta_raiz_id: str = constants.DRIVE_PASTA_RAIZ_ID,
) -> None:
    """Roda o modo ``score`` (a cada 3 dias) ou ``retreino`` (mensal) da pipeline.

    ``score``: carrega o champion do GCS, calcula a probabilidade de HighDelivery de todo
    telefone elegível associado a um CPF vivo (~6,75M telefones, ~9,4M pares — ver
    ``queries/amostra_agora.sql``) e publica a tabela do dia com truncate atômico. Se a
    rodada falhar a validação (sem linha, probabilidade inválida, volume fora do esperado),
    a tabela de ontem fica intacta.

    ``retreino``: treina um LightGBM novo (Optuna semeado pelos hiperparâmetros do champion,
    se houver), simula a escolha de telefone em disparos recentes comparando modelo novo x
    modelo em produção x heurística x aleatório, decide promoção pelo gate (ver
    ``tasks/retreino/promover.py``), publica a versão no GCS (sempre — aprovada ou não) e
    promove o champion só se aprovada. Publica os números na tabela de avaliação do BigQuery
    e um relatório legível (CSVs + gráficos de SHAP) numa subpasta do Drive nomeada pela
    versão (a data do treino).

    :param modo: ``"score"`` ou ``"retreino"``.
    :param environment: ``"prod"`` ou ``"staging"`` — decide o secret de credenciais.
    :param dataset_id: Dataset de destino das 2 tabelas.
    :param table_id_score: Nome da tabela do dia (modo ``score``).
    :param table_id_avaliacao: Nome da tabela de avaliação (modo ``retreino``).
    :param raiz_modelos: Raiz das versões do modelo no GCS.
    :param tamanho_lote: Telefones por lote pontuado no scoring (memória do container).
    :param n_trials: Trials do Optuna na busca de hiperparâmetros (modo ``retreino``).
    :param janela_recente_dias: Dias de disparos recentes que entram na simulação (modo
        ``retreino``).
    :param exigir_gate: Se ``False``, promove o modelo novo mesmo reprovando os critérios
        do gate (os motivos continuam registrados, só não bloqueiam) — modo ``retreino``.
    :param promocao_automatica: Se ``False``, publica a versão no GCS mas não troca o
        champion mesmo aprovada — fica pra alguém promover manualmente depois (modo
        ``retreino``).
    :param drive_pasta_raiz_id: ID da pasta raiz no Drive onde o relatório é publicado
        (modo ``retreino``) — precisa estar compartilhada (Editor) com a service account.
    :raises ValueError: Se ``modo`` não for ``"score"`` nem ``"retreino"``.
    """
    if modo not in MODOS_VALIDOS:
        raise ValueError(f"modo inválido: {modo!r} (esperado um de {MODOS_VALIDOS})")

    if modo == "score":
        champion = carrega_champion_task(environment=environment, raiz=raiz_modelos)
        caminho_parquet, stats = pontua_telefones_task(champion, environment=environment, tamanho_lote=tamanho_lote)
        publica_scores_task(
            caminho_parquet, stats, environment=environment, dataset_id=dataset_id, table_id=table_id_score
        )
        logger.info("Scoring publicado: versão %s, %d linhas.", champion.versao, stats.n_linhas)
        return

    # modo == "retreino"
    versao = date.today().isoformat()
    champion = carrega_champion_opcional_task(environment=environment, raiz=raiz_modelos)
    hiperparametros_champion = champion.metadata.get("hiperparametros") if champion else None
    booster_producao = champion.booster if champion else None

    df_treino = extrair.extrai_treino_task(environment=environment)
    resultado_treino = treinar.treina_task(
        df_treino, n_trials=n_trials, hiperparametros_champion=hiperparametros_champion
    )

    df_eventos = extrair.extrai_eventos_task(environment=environment, janela_recente_dias=janela_recente_dias)
    resultado_simulacao = avalia_simulacao_task(
        df_eventos, resultado_treino.telefones_treino, resultado_treino.booster, booster_producao
    )

    decisao = avalia_gate_task(resultado_treino, resultado_simulacao, exigir_gate=exigir_gate)
    promove_se_aprovado_task(
        resultado_treino, decisao, raiz_modelos, versao, environment=environment,
        promocao_automatica=promocao_automatica,
    )

    tabela_avaliacao = monta_tabela_avaliacao_task(resultado_treino, resultado_simulacao, decisao, versao, versao)
    publica_avaliacao_bq_task(
        tabela_avaliacao, environment=environment, dataset_id=dataset_id, table_id=table_id_avaliacao
    )
    publica_relatorio_task(
        resultado_treino, versao, drive_pasta_raiz_id, environment, resultado_simulacao, decisao
    )

    logger.info("Retreino %s concluído: promovido=%s (motivos: %s).", versao, decisao.promovido, decisao.motivos)

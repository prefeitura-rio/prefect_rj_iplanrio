"""Simulação: para cada disparo recente (``extrair.extrai_eventos``), compara 4 estratégias
de escolha de telefone no mesmo pool de candidatos — modelo novo, modelo em produção
(champion), heurística e aleatório.

A métrica que importa é o "gap" (``% mudaria | disparo falhou`` − ``% mudaria | disparo
sucesso``): quanto maior, mais o algoritmo troca de telefone quando deveria (o disparo real
falhou) e mantém quando não deveria mexer (o disparo teve sucesso). O ranking aleatório
compartilha o mesmo pool que os demais, e o gap dele é o piso de ruído pra comparar —
se o gap de um algoritmo não bater o do aleatório com significância, ele não está
discriminando sinal real.

Porta pra cá ``avaliacao/{base,modelos,aleatorio,heuristica_limiar,executar_avaliacao}.py``
de ``qualidade_telefone_modelo``, com 3 adaptações:
  - o one-hot já vem pronto em SQL (``eventos_candidatos.sql``) — sem ``codificar()`` aqui;
  - só 2 modelos entram no ranking (modelo novo + modelo em produção), não 3 — regressão
    logística/random forest do repo original nunca venceram e não entram em produção aqui;
  - ``telefones_treino`` vem em memória (``ResultadoTreino.telefones_treino``), não de um
    CSV (decisão do projeto: nunca persistir lista de telefones — ver TODO).
"""

from dataclasses import dataclass

import lightgbm as lgb
import numpy as np
import pandas as pd
from prefect import task
from prefect_rj_iplanrio.logging import get_logger
from scipy import stats
from statsmodels.stats.proportion import proportions_ztest

from pipelines.rj_crm__modelo_qualidade_telefone.constants import (
    FEATURES,
    LIMIAR_TAXA_ENTREGA,
    RANDOM_STATE,
    RANKING_CONFIABILIDADE,
    SISTEMAS,
)

logger = get_logger(__name__)

NOME_MODELO_NOVO = "modelo_novo"
NOME_MODELO_PRODUCAO = "modelo_producao"
NOME_HEURISTICA = "heuristica_limiar"
NOME_ALEATORIO = "ranking_aleatorio"


@dataclass(frozen=True)
class TesteSignificancia:
    """Resultado de 1 teste de hipótese sobre o gap."""

    __test__ = False  # o nome começa com "Teste" — sem isso o pytest tenta coletar a classe

    estatistica: float
    p_valor: float


@dataclass(frozen=True)
class ResultadoSimulacao:
    """Saída completa da simulação — o que o gate (passo 8) e a tabela do BQ (passo 9) usam."""

    resumo: pd.DataFrame  # 1 linha por algoritmo (ver tabela_resumo)
    testes_gap_interno: dict[str, TesteSignificancia]  # gap de cada algoritmo é > 0 de verdade?
    teste_vs_aleatorio: TesteSignificancia | None  # modelo novo bate o aleatório?
    teste_heuristica_vs_novo: TesteSignificancia | None  # heurística é mesmo diferente do modelo novo?
    fracao_eventos_descartados: float  # contaminação (ver remover_eventos_contaminados)
    n_eventos: int


def remover_eventos_contaminados(
    feat_eventos: pd.DataFrame, telefones_treino: frozenset[str]
) -> tuple[pd.DataFrame, float]:
    """Descarta os eventos (``id_interacao`` inteiro) em que o telefone usado ou QUALQUER
    candidato está no conjunto de treino do modelo novo.

    O modelo novo é treinado com 100% dos dados, e o rótulo HighDelivery de um telefone de
    treino agrega uma janela de dias que pode conter o próprio disparo avaliado aqui — o
    desfecho vazaria pro modelo. Descartar o evento inteiro (não só a linha do candidato)
    mantém o pool idêntico pra todos os algoritmos, inclusive heurística e aleatório.

    :returns: ``(feat_eventos limpo, fração de eventos descartados)``.
    """
    telefone = feat_eventos["telefone"].astype(str)
    telefone_usado = feat_eventos["telefone_usado"].astype(str)
    contaminada = telefone.isin(telefones_treino) | telefone_usado.isin(telefones_treino)
    evento_contaminado = contaminada.groupby(feat_eventos["id_interacao"]).transform("any")
    fracao = feat_eventos.loc[evento_contaminado, "id_interacao"].nunique() / feat_eventos["id_interacao"].nunique()
    return feat_eventos[~evento_contaminado].reset_index(drop=True), float(fracao)


def base_disparos(feat_eventos: pd.DataFrame) -> pd.DataFrame:
    """1 linha por ``id_interacao`` com o desfecho real do disparo."""
    return feat_eventos[["id_interacao", "telefone_usado", "falhou"]].drop_duplicates("id_interacao")


def avaliado_mask(feat_eventos: pd.DataFrame, base_disp: pd.DataFrame) -> np.ndarray:
    """O telefone realmente usado estava entre os candidatos pontuados?

    Todos os algoritmos compartilham o mesmo pool (``feat_eventos``), então usam a mesma
    máscara — só muda a regra de escolha do ``telefone_sugerido``.
    """
    idx_avaliados = pd.MultiIndex.from_frame(feat_eventos[["id_interacao", "telefone"]].drop_duplicates())
    return pd.MultiIndex.from_frame(
        base_disp[["id_interacao", "telefone_usado"]].rename(columns={"telefone_usado": "telefone"})
    ).isin(idx_avaliados)


def matriz_confusao_pct(
    base_disp: pd.DataFrame, avaliado: np.ndarray, ranking_df: pd.DataFrame, chave: str = "id_interacao"
) -> tuple[float, pd.DataFrame]:
    """``ranking_df``: colunas ``[chave, "telefone_sugerido"]``, 1 linha por chave.

    :returns: ``(% avaliado, matriz % por linha: falhou/sucesso x mudaria/manteria)``.
    """
    df = base_disp.copy()
    df["avaliado"] = avaliado
    df = df.merge(ranking_df[[chave, "telefone_sugerido"]], on=chave, how="left")
    df["mudaria"] = df["telefone_sugerido"] != df["telefone_usado"]

    pct_avaliado = df["avaliado"].mean()

    sub = df[df["avaliado"]]
    contagem = pd.crosstab(sub["falhou"], sub["mudaria"]).reindex(
        index=[False, True], columns=[False, True], fill_value=0
    )
    matriz_pct = contagem.div(contagem.sum(axis=1), axis=0) * 100
    matriz_pct.index = matriz_pct.index.map({False: "disparo teve sucesso", True: "disparo falhou"})
    matriz_pct.columns = matriz_pct.columns.map({False: "manteria telefone", True: "mudaria telefone"})
    return pct_avaliado, matriz_pct


def tabela_resumo(
    feat_eventos: pd.DataFrame, base_disp: pd.DataFrame, rankings: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Os 5 números por algoritmo que importam (% avaliado e, dentro disso, % mudaria/
    manteria por desfecho real) + "gap" (``% mudaria|falhou`` − ``% mudaria|sucesso``)."""
    avaliado = avaliado_mask(feat_eventos, base_disp)
    linhas = []
    for nome, ranking_df in rankings.items():
        pct, matriz = matriz_confusao_pct(base_disp, avaliado, ranking_df)
        mudaria_falhou = matriz.loc["disparo falhou", "mudaria telefone"]
        mudaria_sucesso = matriz.loc["disparo teve sucesso", "mudaria telefone"]
        linhas.append(
            {
                "algoritmo": nome,
                "% telefone real avaliado": pct * 100,
                "% mudaria | disparo falhou": mudaria_falhou,
                "% manteria | disparo falhou": matriz.loc["disparo falhou", "manteria telefone"],
                "% mudaria | disparo sucesso": mudaria_sucesso,
                "% manteria | disparo sucesso": matriz.loc["disparo teve sucesso", "manteria telefone"],
                "gap": mudaria_falhou - mudaria_sucesso,
            }
        )
    return pd.DataFrame(linhas).set_index("algoritmo")


def rankear_modelo(feat_eventos: pd.DataFrame, booster: lgb.Booster) -> pd.DataFrame:
    """Top-1 por ``id_interacao`` pela probabilidade prevista (LightGBM nativo —
    probabilidade não calibrada, serve só pra ranquear, ver ``tasks/calcula_scores.py``)."""
    probs = booster.predict(feat_eventos[FEATURES].to_numpy(dtype="float64"))
    df = feat_eventos[["id_interacao", "telefone"]].copy()
    df["prob"] = probs
    return (
        df.sort_values("prob", ascending=False)
        .groupby("id_interacao")
        .head(1)
        .rename(columns={"telefone": "telefone_sugerido"})[["id_interacao", "telefone_sugerido", "prob"]]
    )


def rankear_aleatorio(feat_eventos: pd.DataFrame, random_state: int = RANDOM_STATE) -> pd.DataFrame:
    """Ranking de controle: escolhe 1 candidato ao acaso por ``id_interacao``, do mesmo
    pool dos demais — isola o que é mérito genuíno do ranking aprendido/heurístico vs. só
    "tentar outro número"."""
    rng = np.random.default_rng(random_state)
    df = feat_eventos[["id_interacao", "telefone"]].copy()
    df["_sorteio"] = rng.random(len(df))
    return (
        df.sort_values("_sorteio")
        .groupby("id_interacao")
        .head(1)
        .rename(columns={"telefone": "telefone_sugerido"})[["id_interacao", "telefone_sugerido"]]
    )


def rankear_heuristica(
    feat_eventos: pd.DataFrame, confiabilidade: dict[str, float] = RANKING_CONFIABILIDADE
) -> pd.DataFrame:
    """Ranking heurístico sem ML: se algum candidato tem ``taxa_sucesso_anterior`` acima do
    limiar, ele é escolhido (desempate pela própria taxa). Quem não passa cai no fallback:
    maior confiabilidade de sistema (``RANKING_CONFIABILIDADE``) entre os sistemas em que o
    candidato está registrado. Duas camadas de propósito — taxa individual e confiabilidade
    de sistema não são a mesma unidade, então nunca comparadas direto (quem passa do limiar
    sempre vem antes de quem caiu no fallback, independente do valor)."""
    df = feat_eventos.copy()
    scores_sistema = pd.DataFrame(
        {
            sistema: np.where(
                df[f"qtd_aparicoes_{sistema}"] > 0, confiabilidade.get(sistema, np.nan), np.nan
            )
            for sistema in SISTEMAS
        },
        index=df.index,
    )
    df["acima_limiar"] = df["taxa_sucesso_anterior"] > LIMIAR_TAXA_ENTREGA
    df["score_heuristica"] = np.where(df["acima_limiar"], df["taxa_sucesso_anterior"], scores_sistema.max(axis=1))
    return (
        df.sort_values(["acima_limiar", "score_heuristica"], ascending=[False, False])
        .groupby("id_interacao")
        .head(1)
        .rename(columns={"telefone": "telefone_sugerido"})[["id_interacao", "telefone_sugerido"]]
    )


def monta_rankings(
    feat_eventos: pd.DataFrame, booster_novo: lgb.Booster, booster_producao: lgb.Booster | None
) -> dict[str, pd.DataFrame]:
    """1 ranking por algoritmo, todos no mesmo pool (``feat_eventos``).

    :param booster_producao: O champion atual. ``None`` na 1ª execução (sem champion ainda)
        — nesse caso o ranking ``modelo_producao`` fica de fora.
    """
    rankings = {NOME_MODELO_NOVO: rankear_modelo(feat_eventos, booster_novo)}
    if booster_producao is not None:
        rankings[NOME_MODELO_PRODUCAO] = rankear_modelo(feat_eventos, booster_producao)
    rankings[NOME_HEURISTICA] = rankear_heuristica(feat_eventos)
    rankings[NOME_ALEATORIO] = rankear_aleatorio(feat_eventos)
    return rankings


def teste_gap_interno(mudaria_sub: np.ndarray, falhou_sub: np.ndarray) -> TesteSignificancia:
    """O gap de 1 algoritmo é maior que zero de verdade, ou é ruído amostral (proporção Z)?"""
    x_falhou, n_falhou = mudaria_sub[falhou_sub].sum(), falhou_sub.sum()
    x_sucesso, n_sucesso = mudaria_sub[~falhou_sub].sum(), (~falhou_sub).sum()
    z, p = proportions_ztest([x_falhou, x_sucesso], [n_falhou, n_sucesso])
    return TesteSignificancia(estatistica=float(z), p_valor=float(p))


def teste_pareado(
    mudarias: dict[str, np.ndarray], falhou_sub: np.ndarray, nome_a: str, nome_b: str
) -> TesteSignificancia | None:
    """A diferença dos gaps de ``nome_a`` e ``nome_b`` é significativa, ou é ruído amostral?

    Pareado porque os dois rankings compartilham o mesmo pool e os mesmos disparos — cada
    evento vira 1 observação emparelhada (``d = mudaria_a - mudaria_b``), testada
    separadamente entre quem falhou e quem teve sucesso (Welch, variâncias desiguais).

    :returns: ``None`` se um dos dois nomes não estiver em ``mudarias`` (ex.: sem champion
        ainda, então não há ``modelo_producao`` pra comparar).
    """
    if nome_a not in mudarias or nome_b not in mudarias:
        return None
    d = mudarias[nome_a].astype(int) - mudarias[nome_b].astype(int)
    d_falhou, d_sucesso = d[falhou_sub], d[~falhou_sub]
    t_stat, p = stats.ttest_ind(d_falhou, d_sucesso, equal_var=False)
    return TesteSignificancia(estatistica=float(t_stat), p_valor=float(p))


def avalia_simulacao(
    feat_eventos_brutos: pd.DataFrame,
    telefones_treino: frozenset[str],
    booster_novo: lgb.Booster,
    booster_producao: lgb.Booster | None = None,
) -> ResultadoSimulacao:
    """Orquestra a simulação inteira: descontamina o pool, ranqueia os 4 algoritmos, monta
    a tabela de resumo e roda os testes de significância.

    :param feat_eventos_brutos: Saída de ``tasks/retreino/extrair.py::extrai_eventos``.
    :param telefones_treino: ``ResultadoTreino.telefones_treino`` do modelo novo.
    :param booster_novo: Modelo recém-treinado (``ResultadoTreino.booster``).
    :param booster_producao: O champion atual, ou ``None`` na 1ª execução (sem champion).
    :returns: :class:`ResultadoSimulacao`.
    """
    feat_eventos, fracao = remover_eventos_contaminados(feat_eventos_brutos, telefones_treino)
    logger.info("%.1f%% dos eventos descartados (telefone deles está no treino do modelo novo).", fracao * 100)

    base_disp = base_disparos(feat_eventos)
    rankings = monta_rankings(feat_eventos, booster_novo, booster_producao)
    resumo = tabela_resumo(feat_eventos, base_disp, rankings)
    logger.info("Resumo da simulação:\n%s", resumo.round(1).to_string())

    avaliado = avaliado_mask(feat_eventos, base_disp)
    falhou_sub = base_disp["falhou"].to_numpy()[avaliado]

    def mudaria_de(ranking_df: pd.DataFrame) -> np.ndarray:
        df = base_disp.merge(ranking_df[["id_interacao", "telefone_sugerido"]], on="id_interacao", how="left")
        return (df["telefone_sugerido"] != df["telefone_usado"]).to_numpy()[avaliado]

    mudarias = {nome: mudaria_de(ranking_df) for nome, ranking_df in rankings.items()}
    testes_gap_interno = {nome: teste_gap_interno(mudaria, falhou_sub) for nome, mudaria in mudarias.items()}

    return ResultadoSimulacao(
        resumo=resumo,
        testes_gap_interno=testes_gap_interno,
        teste_vs_aleatorio=teste_pareado(mudarias, falhou_sub, NOME_MODELO_NOVO, NOME_ALEATORIO),
        teste_heuristica_vs_novo=teste_pareado(mudarias, falhou_sub, NOME_HEURISTICA, NOME_MODELO_NOVO),
        fracao_eventos_descartados=fracao,
        n_eventos=int(feat_eventos["id_interacao"].nunique()),
    )


@task
def avalia_simulacao_task(
    feat_eventos_brutos: pd.DataFrame,
    telefones_treino: frozenset[str],
    booster_novo: lgb.Booster,
    booster_producao: lgb.Booster | None = None,
) -> ResultadoSimulacao:
    """Task-wrapper fina de :func:`avalia_simulacao`."""
    return avalia_simulacao(feat_eventos_brutos, telefones_treino, booster_novo, booster_producao)

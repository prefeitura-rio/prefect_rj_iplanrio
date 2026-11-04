"""Extrai e reconstrói as conversas de uma campanha num período (1 solicitação)."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    CHATBOT_DATASET,
    CHATBOT_TABLE,
    FONTE_DISPAROS_CAMPANHA,
    FONTE_DISPAROS_HSM,
    ORDEM_TIPO_ENGAJAMENTO,
    PROJECT_ID,
    ROTULO_FONTE,
    TIPO_ENGAJAMENTO_OUTRO,
    TIPO_ENGAJAMENTO_POR_MSG_TIPO,
    TIPO_ENGAJAMENTO_REACAO,
    TIPO_ENGAJAMENTO_TEXTO,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.bigquery import get_bq_client

_QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"
_SQL_TEMPLATE = (_QUERIES_DIR / "extrai_conversas.sql").read_text(encoding="utf-8")

COLUNAS_SESSOES = [
    "id_sessao_48h",
    "id_sessao_24h",
    "cpf",
    "telefone",
    "nome_campanha",
    "nome_eixo",
    "nome_hsm",
    "hsm_texto",
    "conversa_completa",
]

_RE_REACAO = re.compile(r"^\[reação: (.+)\]$")
# tom de pele e seletor de variação: 👍🏻 e 👍 contam como a mesma reação
_RE_MODIFICADOR_EMOJI = re.compile("[\U0001F3FB-\U0001F3FF\uFE0F]")
_SELETOR_EMOJI = "\uFE0F"
_INICIO_BLOCO_EMOJI = 0x1F000  # abaixo disso, símbolo que sem o seletor vira texto (❤ U+2764)


@dataclass
class Engajamento:
    """Contagens das sessões da extração, pro relatório. por_tipo: sessões engajadas
    por tipo de engajamento — exclusivo (ver config.ORDEM_TIPO_ENGAJAMENTO), soma =
    total_engajados. reacoes: emoji -> nº de reações (mensagens) nas sessões engajadas."""

    total_disparos: int = 0
    por_tipo: dict[str, int] = field(default_factory=dict)
    reacoes: Counter = field(default_factory=Counter)

    @property
    def total_engajados(self) -> int:
        return sum(self.por_tipo.values())

    @property
    def total_texto(self) -> int:
        """Sessões com fala em texto do cidadão — as únicas que vão pra LLM."""
        return self.por_tipo.get(TIPO_ENGAJAMENTO_TEXTO, 0)


def _monta_sql(filtro_expr: str, data_inicio_expr: str, data_fim_expr: str, fonte_disparos: str) -> str:
    return _SQL_TEMPLATE.format(
        tabela_chatbot=f"{PROJECT_ID}.{CHATBOT_DATASET}.{CHATBOT_TABLE}",
        campo_disparo="nome_hsm" if fonte_disparos == FONTE_DISPAROS_HSM else "nome_campanha",
        filtro=filtro_expr,
        data_inicio=data_inicio_expr,
        data_fim=data_fim_expr,
    )


def sql_query_utilizada(
    campanha: str, data_inicio: date, data_fim: date, fonte_disparos: str = FONTE_DISPAROS_CAMPANHA
) -> str:
    """Mesma consulta de _busca_mensagens, com parâmetros já substituídos por valor
    literal — só pra exibir no relatório final como referência (nunca roda contra o BQ)."""
    campanha_literal = "'" + str(campanha).replace("'", "''") + "'"
    inicio_literal, fim_literal = f"date '{data_inicio.isoformat()}'", f"date '{data_fim.isoformat()}'"
    return _monta_sql(campanha_literal, inicio_literal, fim_literal, fonte_disparos)


def _busca_mensagens(
    client: bigquery.Client, campanha: str, data_inicio: date, data_fim: date, fonte_disparos: str
) -> pd.DataFrame:
    """`campanha` é o nome da campanha ou, com FONTE_DISPAROS_HSM, o nome do template."""
    sql = _monta_sql("@filtro", "@data_inicio", "@data_fim", fonte_disparos)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("filtro", "STRING", campanha),
            bigquery.ScalarQueryParameter("data_inicio", "DATE", data_inicio),
            bigquery.ScalarQueryParameter("data_fim", "DATE", data_fim),
        ]
    )
    return client.query(sql, job_config=job_config).to_dataframe()


def _primeiro_preenchido(serie: pd.Series):
    valores = serie.dropna()
    valores = valores[valores.astype(str).str.strip() != ""]
    return valores.iloc[0] if not valores.empty else None


def _tipo_engajamento(msg_tipo) -> str:
    if pd.isna(msg_tipo) or not str(msg_tipo).strip():
        return TIPO_ENGAJAMENTO_TEXTO
    return TIPO_ENGAJAMENTO_POR_MSG_TIPO.get(str(msg_tipo).strip().lower(), TIPO_ENGAJAMENTO_OUTRO)


def _emoji_reacao(texto) -> str:
    texto = "" if pd.isna(texto) else str(texto).strip()
    casamento = _RE_REACAO.match(texto)
    emoji = _RE_MODIFICADOR_EMOJI.sub("", casamento.group(1) if casamento else texto)
    # símbolo antigo (ex.: ❤ U+2764) sem o seletor aparece como texto no Docs: devolve
    if len(emoji) == 1 and ord(emoji) < _INICIO_BLOCO_EMOJI:
        emoji += _SELETOR_EMOJI
    return emoji or "(sem emoji)"


def _monta_conversas(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, Engajamento]:
    """Agrupa mensagens por id_sessao_48h em 1 conversa formatada. `conversa_completa`
    NÃO inclui a fala 'HSM: ...' — o texto do HSM vai à parte em `hsm_texto` (por
    sessão: uma campanha pode ter mais de um HSM, e cada sessão guarda o que recebeu)
    e é enviado separado no prompt de cada etapa e no relatório; incluí-lo de novo em
    conversa_completa só duplicaria o texto.

    nome_hsm/nome_campanha vêm da linha da fala HSM quando existe (é ela que carrega o
    struct `hsm`); senão, do primeiro valor preenchido da sessão.

    Só entram no df as sessões com PELO MENOS 1 fala em texto do cidadão (texto, botão
    ou menu) — as únicas que vão pra LLM; a conversa leva também as demais falas
    (reação, mídia) como contexto. O Engajamento cobre TODAS as sessões do período: as
    só de reação/mídia entram nas contagens por tipo do relatório, sem classificação."""
    if df_raw.empty:
        return pd.DataFrame(columns=COLUNAS_SESSOES), Engajamento()

    df_raw = df_raw.sort_values(["id_sessao_48h", "msg_data"])
    engajamento = Engajamento(total_disparos=df_raw["id_sessao_48h"].nunique())
    por_tipo: Counter = Counter()

    linhas = []
    for id_sessao_48h, grupo in df_raw.groupby("id_sessao_48h", sort=False):
        falas_cidadao = grupo[grupo["msg_fonte"] == "AI_AGENT_CIDADAO"]
        if falas_cidadao.empty:
            continue
        tipos = set(falas_cidadao["msg_tipo"].map(_tipo_engajamento))
        por_tipo[next(t for t in ORDEM_TIPO_ENGAJAMENTO if t in tipos)] += 1
        reacoes = falas_cidadao[falas_cidadao["msg_tipo"].map(_tipo_engajamento) == TIPO_ENGAJAMENTO_REACAO]
        engajamento.reacoes.update(reacoes["msg_texto"].map(_emoji_reacao))
        if TIPO_ENGAJAMENTO_TEXTO not in tipos:
            continue
        hsm_rows = grupo[grupo["msg_fonte"] == "HSM"]
        origem_hsm = hsm_rows if not hsm_rows.empty else grupo
        conversa = "\n".join(
            f"{ROTULO_FONTE.get(row.msg_fonte, row.msg_fonte)}: {row.msg_texto or ''}"
            for row in grupo.itertuples()
            if row.msg_fonte != "HSM"
        )
        primeira = grupo.iloc[0]
        linhas.append(
            {
                "id_sessao_48h": id_sessao_48h,
                "id_sessao_24h": primeira["id_sessao_24h"],
                "cpf": _primeiro_preenchido(grupo["cpf"]),
                "telefone": _primeiro_preenchido(grupo["telefone"]),
                "nome_campanha": _primeiro_preenchido(origem_hsm["nome_campanha"]),
                "nome_eixo": _primeiro_preenchido(grupo["nome_eixo"]),
                "nome_hsm": _primeiro_preenchido(origem_hsm["nome_hsm"]),
                "hsm_texto": hsm_rows["msg_texto"].iloc[0] if not hsm_rows.empty else "",
                "conversa_completa": conversa,
            }
        )

    engajamento.por_tipo = {t: por_tipo[t] for t in ORDEM_TIPO_ENGAJAMENTO if por_tipo[t]}
    return pd.DataFrame(linhas, columns=COLUNAS_SESSOES), engajamento


def textos_hsm(df_sessoes: pd.DataFrame) -> dict[str, str]:
    """{nome_hsm: texto de exemplo} — 1 por HSM distinto de um conjunto de sessões, pros
    prompts em lote (descoberta/juiz), que podem misturar HSMs da mesma campanha, e pro
    relatório. Deduplica pelo NOME do HSM, não pelo texto: hoje a chatbot guarda o
    template (com ${...} sem preencher), mas se algum dia guardar o texto já
    personalizado, deduplicar pelo texto daria quase 1 texto por sessão."""
    if df_sessoes.empty:
        return {}
    com_texto = df_sessoes[df_sessoes["hsm_texto"].fillna("").str.strip() != ""].copy()
    com_texto["_chave"] = com_texto["nome_hsm"].fillna("").astype(str).str.strip()
    com_texto.loc[com_texto["_chave"] == "", "_chave"] = "(HSM sem nome)"
    return {chave: grupo["hsm_texto"].iloc[0] for chave, grupo in com_texto.groupby("_chave", sort=True)}


def textos_hsm_prompt(df_sessoes: pd.DataFrame) -> str:
    """Bloco <<HSM_TEXTO>> dos prompts em lote: 1 texto de exemplo por HSM do lote."""
    textos = textos_hsm(df_sessoes)
    if not textos:
        return "(texto do HSM indisponível)"
    return "\n\n".join(f"[{nome}]\n{texto}" for nome, texto in textos.items())


@task(log_prints=True)
def extrai_conversas(
    campanha: str, data_inicio: date, data_fim: date, fonte_disparos: str = FONTE_DISPAROS_CAMPANHA
) -> tuple[pd.DataFrame, Engajamento]:
    """Sessões com fala em texto do cidadão no período (1 linha por sessão, colunas em
    COLUNAS_SESSOES — o que vai pra LLM) e as contagens de engajamento de todas as sessões.
    Com fonte_disparos = FONTE_DISPAROS_HSM, `campanha` é o nome do template e cada
    disparo vira uma sessão (ver queries/extrai_conversas.sql)."""
    client = get_bq_client()
    print(f"[EXTRACAO] {campanha} (por {fonte_disparos}): buscando sessões de {data_inicio} a {data_fim}...")
    df_raw = _busca_mensagens(client, campanha, data_inicio, data_fim, fonte_disparos)
    df_sessoes, engajamento = _monta_conversas(df_raw)
    taxa = 100 * engajamento.total_engajados / engajamento.total_disparos if engajamento.total_disparos else 0.0
    n_hsms = len(textos_hsm(df_sessoes))
    print(
        f"[EXTRACAO] {campanha}: {engajamento.total_disparos} sessão(ões) com disparo, "
        f"{engajamento.total_engajados} engajada(s) ({taxa:.1f}%) — por tipo: {engajamento.por_tipo}; "
        f"{len(df_sessoes)} com texto vão pra classificação; {n_hsms} HSM(s) distinto(s)."
    )
    return df_sessoes, engajamento

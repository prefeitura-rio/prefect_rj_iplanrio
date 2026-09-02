# -*- coding: utf-8 -*-
"""
Extração incremental das sessões do Agentforce ainda não classificadas + enriquecimento
com o catálogo de HSM.

Portado de clustering/classificacao_inicial.ipynb (seções 1-2) e
clustering/modules/hsm_enrichment.py — mesma lógica, adaptada para produção:
  - janela de busca fixa (constants.LOOKBACK_DAYS), não a data fixa do notebook original;
  - anti-join contra a tabela destino já embutido na query (ver queries/extract_sessoes.sql).
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import pandas as pd
from prefect import task

from pipelines.rj_crm__agentforce_classificacao_llm.utils.bigquery import get_bq_client

_QUERIES_DIR = Path(__file__).resolve().parent.parent / "queries"


# ---------------------------------------------------------------------------
# Helpers de texto/HSM — portados de clustering/modules/text_utils.py e
# clustering/modules/hsm_enrichment.py (mesmas regras, sem alteração de comportamento)
# ---------------------------------------------------------------------------


def _normaliza(texto) -> str:
    """Baixa a caixa, remove acentos e colapsa espaços — usado pra comparar texto do
    usuário com o título de um botão sem falso negativo por acentuação/espaçamento."""
    texto = str(texto).strip().lower()
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", texto)


def _extrai_titulos_botoes(botoes_json) -> list[str]:
    """Extrai os títulos dos botões de um HSM a partir do JSON bruto de hsm_botoes_json."""
    if pd.isna(botoes_json) or not str(botoes_json).strip():
        return []
    try:
        botoes = json.loads(botoes_json)
    except (TypeError, ValueError):
        return []
    return [b.get("title", "") for b in botoes.values() if isinstance(b, dict)]


def _escolhe_hsm(grupo: pd.DataFrame) -> pd.Series:
    """Escolhe 1 linha do catálogo por (jornada_nome, hsm_nome) quando há mais de uma
    candidata (ex.: template republicado) — só pra resolver hsm_botoes_json, já que
    jornada_nome + hsm_nome (o par exato do disparo, não mais uma escolha por jornada
    inteira) já veio resolvido de v2_chatbot_conversas. Prioriza a versão que não é
    anacrônica (não veio depois de uma mudança de versão do template) e desempata por nome
    de atividade.

    Chamada via groupby(["jornada_nome", "hsm_nome"]).apply() — no pandas >= 2.2
    (obrigatório a partir do 3.0, ver caller) as colunas de agrupamento não são mais
    passadas pra cá (include_groups=False virou o único comportamento possível), então
    `grupo` aqui dentro NÃO tem jornada_nome/hsm_nome. Isso é ok: o pandas ainda usa o
    valor do grupo (agora uma tupla) como índice do resultado, e o caller recupera as
    colunas de lá com reset_index()."""
    nao_anacronico = grupo[grupo["template_pos_versao_indicador"] == False]  # noqa: E712
    candidatos = nao_anacronico if len(nao_anacronico) > 0 else grupo
    return candidatos.sort_values("atividade_nome").iloc[0]


# ---------------------------------------------------------------------------
# Renderização de hsm_texto — substitui ${variavel} pelos valores de hsm_variaveis_json
# (JSON com as variáveis de personalização do disparo específico, ver hsm_candidatas em
# extract_sessoes.sql). Sem isso, a LLM recebia o template cru (ex.: "sua solicitação de
# ${solicitacao} foi finalizada") sem nenhuma pista de qual era o assunto de verdade —
# tornando impossível avaliar relacao_hsm corretamente pra esses casos.
# ---------------------------------------------------------------------------
_PLACEHOLDER_HSM = re.compile(r"\$\{(\w+)\}")


def _lcs_len(a: str, b: str) -> int:
    """Tamanho da maior subsequência comum entre a e b (programação dinâmica clássica, uma
    linha da matriz por vez). Usado por _busca_valor_fuzzy pra medir o quão parecida uma
    chave do JSON é do nome do placeholder."""
    anterior = [0] * (len(b) + 1)
    for i in range(1, len(a) + 1):
        atual = [0] * (len(b) + 1)
        for j in range(1, len(b) + 1):
            atual[j] = anterior[j - 1] + 1 if a[i - 1] == b[j - 1] else max(anterior[j], atual[j - 1])
        anterior = atual
    return anterior[len(b)]


_LCS_RATIO_MINIMO = 0.7  # fração do tamanho do placeholder que o LCS precisa cobrir pra aceitar o match


def _busca_valor_fuzzy(chave_placeholder: str, variaveis: dict):
    """Quando a chave do placeholder não bate exatamente com nenhuma chave do JSON — ex.:
    placeholder ${solicitacao}, chave real 'cc_wt_solicitacao' (prefixo de sistema variando
    por campanha/canal, ex. cc_wt_*) — escolhe a chave do JSON com maior LCS (longest common
    subsequence, comparação case-insensitive) em relação ao placeholder. Exige LCS >= 70% do
    tamanho do placeholder pra aceitar, pra não casar com uma chave só remotamente parecida.
    Retorna o valor da melhor candidata, ou None se nenhuma bater o suficiente."""
    alvo = chave_placeholder.lower()
    melhor_chave, melhor_score = None, 0
    for chave in variaveis:
        score = _lcs_len(alvo, str(chave).lower())
        if score > melhor_score:
            melhor_chave, melhor_score = chave, score
    if melhor_chave is not None and melhor_score >= _LCS_RATIO_MINIMO * len(alvo):
        return variaveis[melhor_chave]
    return None


def _renderiza_hsm(hsm_texto, hsm_variaveis_json):
    """Substitui ${chave} em hsm_texto pelo valor correspondente em hsm_variaveis_json —
    primeiro por match exato, senão pelo mais parecido via LCS (ver _busca_valor_fuzzy,
    necessário porque as chaves reais do JSON costumam ter um prefixo de sistema que não
    está no placeholder, ex. ${canal} -> 'cc_wt_canal'). Nem exato nem fuzzy encontrado:
    mantém o placeholder original (melhor deixar visível que a variável não foi resolvida
    do que assumir um valor errado). Sem hsm_texto, sem JSON, ou JSON inválido: retorna
    hsm_texto como veio — inclui o caso de session_texto (Session, não Template), que já
    chega completo, sem placeholder nenhum, e a substituição vira no-op."""
    if hsm_texto is None or (isinstance(hsm_texto, float) and pd.isna(hsm_texto)):
        return hsm_texto
    if hsm_variaveis_json is None or (isinstance(hsm_variaveis_json, float) and pd.isna(hsm_variaveis_json)):
        return hsm_texto

    if isinstance(hsm_variaveis_json, dict):
        variaveis = hsm_variaveis_json
    elif isinstance(hsm_variaveis_json, str):
        try:
            variaveis = json.loads(hsm_variaveis_json)
        except (TypeError, ValueError):
            return hsm_texto
    else:
        return hsm_texto

    if not isinstance(variaveis, dict):
        return hsm_texto

    def _substitui(match: re.Match) -> str:
        chave = match.group(1)
        valor = variaveis[chave] if chave in variaveis else _busca_valor_fuzzy(chave, variaveis)
        return str(valor) if valor is not None else match.group(0)

    return _PLACEHOLDER_HSM.sub(_substitui, hsm_texto)


def _resposta_e_botao_atrasado(row) -> bool:
    """True quando a única mensagem do usuário é idêntica ao texto de um botão do HSM —
    sinal de que é uma resposta a um botão que chegou fora da janela de 24h (não foi
    capturada como clique). Só considera sessões de 1 única mensagem, pra não dar falso
    positivo numa conversa longa que por acaso contém o texto do botão em algum ponto."""
    if row["qtd_mensagens_usuario"] != 1:
        return False
    titulos = _extrai_titulos_botoes(row["hsm_botoes_json"])
    if not titulos:
        return False
    msg = _normaliza(row["mensagens_usuario_concatenadas"])
    return any(_normaliza(t) == msg for t in titulos)


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(log_prints=True, retries=2, retry_delay_seconds=30)
def extrai_sessoes_nao_classificadas(
    project_id: str,
    source_table: str,
    destino_full_table_id: str,
    lookback_days: int,
    hsm_max_dias_antes: int,
) -> pd.DataFrame:
    """Roda a query de extração incremental — janela fixa + DOIS filtros de "já
    classificada": classificacao_llm_datahora IS NULL na fonte (pré-filtro barato, pode
    estar até 15min desatualizado) e o anti-join contra a tabela destino (garantia final,
    cobre esse desincronismo — ver cabeçalho da query). Retorna 1 linha por sessão pendente
    de classificação, com o ponteiro pra HSM disparada (se houver) — o texto do HSM em si
    vem depois, via enriquece_com_catalogo_hsm."""
    sql_template = (_QUERIES_DIR / "extract_sessoes.sql").read_text()
    query = sql_template.format(
        lookback_days=lookback_days,
        hsm_max_dias_antes=hsm_max_dias_antes,
        source_table=source_table,
        destino_full_table_id=destino_full_table_id,
    )

    client = get_bq_client(project_id)
    df = client.query(query).to_dataframe()
    print(f"[EXTRACT] {len(df)} sessão(ões) pendente(s) de classificação (janela: {lookback_days}d).")
    return df


@task(log_prints=True, retries=2, retry_delay_seconds=30)
def enriquece_com_catalogo_hsm(
    df_sessoes: pd.DataFrame,
    project_id: str,
    hsm_catalog_table: str,
) -> pd.DataFrame:
    """Renderiza hsm_texto (substitui ${variavel} pelos valores de hsm_variaveis_json —
    ambos já vieram de v2_chatbot_conversas, resolvidos por disparo específico, ver
    hsm_candidatas em extract_sessoes.sql) e junta ao catálogo (jornada_nome + hsm_nome)
    só pra pegar hsm_botoes_json — o único dado que não chega até v2_chatbot_conversas
    (descartado no intermediate antes do mart, ver hsm_por_jornada.sql). Calcula a flag
    resposta_atrasada_btn — sessão que só respondeu com o texto de um botão do HSM fora da
    janela de 24h, e por isso não deve ir pra LLM (não carrega demanda real)."""
    if df_sessoes.empty:
        df_sessoes["hsm_botoes_json"] = pd.Series(dtype="object")
        df_sessoes["resposta_atrasada_btn"] = pd.Series(dtype="bool")
        return df_sessoes

    df_sessoes = df_sessoes.copy()
    df_sessoes["hsm_texto"] = df_sessoes.apply(
        lambda row: _renderiza_hsm(row["hsm_texto"], row.get("hsm_variaveis_json")), axis=1
    )
    df_sessoes = df_sessoes.drop(columns=["hsm_variaveis_json"])

    jornadas_distintas = df_sessoes["jornada_nome"].dropna()
    jornadas_distintas = jornadas_distintas[jornadas_distintas.astype(str).str.strip() != ""].unique()

    if len(jornadas_distintas) == 0:
        print("[EXTRACT] Nenhuma jornada_nome preenchida no lote — pulando enriquecimento de botões.")
        df_sessoes["hsm_botoes_json"] = None
        df_sessoes["resposta_atrasada_btn"] = False
        return df_sessoes

    def _sql_quote_list(values):
        escaped = [str(v).replace("'", "''") for v in values]
        return ", ".join(f"'{v}'" for v in escaped)

    sql_template = (_QUERIES_DIR / "hsm_por_jornada.sql").read_text()
    query = sql_template.format(hsm_catalog_table=hsm_catalog_table, jornadas=_sql_quote_list(jornadas_distintas))

    client = get_bq_client(project_id)
    df_hsm = client.query(query).to_dataframe()

    if df_hsm.empty:
        df_sessoes["hsm_botoes_json"] = None
        df_sessoes["resposta_atrasada_btn"] = False
        return df_sessoes

    hsm_por_jornada_hsm = df_hsm.drop_duplicates(subset=["jornada_nome", "hsm_nome", "hsm_botoes_json"])

    hsm_lookup = (
        hsm_por_jornada_hsm.groupby(["jornada_nome", "hsm_nome"], group_keys=False)
        .apply(_escolhe_hsm)
        # jornada_nome/hsm_nome não vêm mais como coluna (ver docstring de _escolhe_hsm),
        # mas o pandas ainda usa o valor do grupo (tupla) como índice do resultado —
        # reset_index() sem drop transforma esse índice de volta em colunas antes de
        # selecionar.
        .reset_index()[["jornada_nome", "hsm_nome", "hsm_botoes_json"]]
        .reset_index(drop=True)
    )

    df_enriquecido = df_sessoes.merge(hsm_lookup, on=["jornada_nome", "hsm_nome"], how="left")
    df_enriquecido["resposta_atrasada_btn"] = df_enriquecido.apply(_resposta_e_botao_atrasado, axis=1)

    n_atrasada = int(df_enriquecido["resposta_atrasada_btn"].sum())
    print(f"[EXTRACT] {n_atrasada} sessão(ões) marcada(s) como resposta atrasada a botão (não vão pra LLM).")
    return df_enriquecido

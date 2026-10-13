# -*- coding: utf-8 -*-
"""Gatilho do flow — as solicitações do Google Form "Solicitação de Relatório de
Engajamento", lidas e atualizadas (status) direto no Sheet de respostas pela API.

Regra de quando processar: toda solicitação com status vazio e "Data de geração" até
hoje (America/Sao_Paulo). Como o cron roda às 8h e às 18h (prefect.yaml), isso já dá:
data futura sai às 8h do dia escolhido; data de hoje sai na próxima execução (8h se
enviada antes das 8h, 18h se entre 8h e 18h, 8h de amanhã se depois das 18h).

Ciclo de status de uma linha: vazio -> processando -> concluido | pulado: ... | erro: ...
"processando" que passa de HORAS_LIMITE_PROCESSANDO (processo morto de fora, sem chegar
no except do flow) vira erro na execução seguinte — sem reprocessar sozinho: quem
pediu envia o Form de novo, igual a qualquer outro erro."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
from googleapiclient.discovery import Resource
from prefect import task

from pipelines.rj_crm__relatorio_engajamento_hsm.config import (
    COL_SHEET_CAMPANHA,
    COL_SHEET_CARIMBO,
    COL_SHEET_EMAIL,
    COL_SHEET_FIM,
    COL_SHEET_GERACAO,
    COL_SHEET_INICIO,
    COL_SHEET_LINK,
    COL_SHEET_PROCESSADO_EM,
    COL_SHEET_STATUS,
    COL_SHEET_TIPO,
    FORMATO_PROCESSADO_EM,
    FORMATOS_DATA_HORA_SHEET,
    FORMATOS_DATA_SHEET,
    HORAS_LIMITE_PROCESSANDO,
    PREFIXO_STATUS_AGUARDANDO,
    PREFIXO_STATUS_ERRO,
    PREFIXO_STATUS_PULADO,
    SOLICITACOES_ABA,
    SOLICITACOES_SHEET_ID,
    STATUS_PROCESSANDO,
    TIMEZONE,
    TIPOS_SOLICITACAO,
)
from pipelines.rj_crm__relatorio_engajamento_hsm.utils.sheets import (
    COL_LINHA,
    atualiza_linha,
    get_sheets_service,
    le_sheet,
)

_COLUNAS_OBRIGATORIAS = (
    COL_SHEET_CARIMBO,
    COL_SHEET_EMAIL,
    COL_SHEET_CAMPANHA,
    COL_SHEET_TIPO,
    COL_SHEET_INICIO,
    COL_SHEET_FIM,
    COL_SHEET_GERACAO,
    COL_SHEET_STATUS,
    COL_SHEET_LINK,
    COL_SHEET_PROCESSADO_EM,
)
_MAX_CHARS_MOTIVO = 300


@dataclass(frozen=True)
class _Alvo:
    """O que identifica uma resposta no Sheet: carimbo + e-mail (texto exato das
    células) e a linha onde estava na leitura (pode mudar — ver _localiza_linha)."""

    carimbo: str
    email: str
    linha: int

    @classmethod
    def da_linha(cls, linha: pd.Series) -> _Alvo:
        return cls(str(linha[COL_SHEET_CARIMBO]).strip(), str(linha[COL_SHEET_EMAIL]).strip(), int(linha[COL_LINHA]))


@dataclass(frozen=True)
class Solicitacao:
    linha: int  # nº da linha no Sheet no momento da leitura (pode mudar — ver _localiza_linha)
    carimbo: str  # texto exato da célula — identifica a resposta junto com o e-mail
    email: str
    campanha: str
    tipo: str
    data_inicio: date
    data_fim: date
    data_geracao: date
    # True só pra solicitação que estava "aguardando liberação" e cuja campanha está em
    # `campanhas_liberadas` nesta execução: pode passar do TETO_CONVERSAS.
    liberada: bool = False

    @property
    def alvo(self) -> _Alvo:
        return _Alvo(self.carimbo, self.email, self.linha)

    def descricao(self) -> str:
        return f"[{self.campanha} | {self.tipo} | {self.data_inicio}..{self.data_fim} | linha {self.linha}]"


class SolicitacaoError(Exception):
    """Erro "esperado" de uma solicitação (ex.: Catálogo não encontrado) — a mensagem
    vai como está pro status ("erro: <mensagem>"), pra quem pediu entender o que fazer."""


@dataclass(frozen=True)
class ResultadoSolicitacao:
    """O que o processamento de 1 solicitação devolve pro flow gravar no Sheet."""

    status: str
    link: str = ""


# ============================================================================
# Funções puras (sem rede) — regras de leitura, validação e quando processar
# ============================================================================


def agora_brt() -> datetime:
    """Agora em America/Sao_Paulo, sem tzinfo — mesmo referencial dos textos do Sheet."""
    return datetime.now(ZoneInfo(TIMEZONE)).replace(tzinfo=None)


def _parse(texto: str, formatos: tuple[str, ...], campo: str) -> datetime:
    texto = (texto or "").strip()
    if not texto:
        raise ValueError(f"'{campo}' vazio")
    for formato in formatos:
        try:
            return datetime.strptime(texto, formato)  # noqa: DTZ007 — naive BRT, igual a agora_brt()
        except ValueError:
            continue
    raise ValueError(f"'{campo}' em formato não reconhecido: {texto!r}")


def parse_data(texto: str, campo: str) -> date:
    return _parse(texto, FORMATOS_DATA_SHEET, campo).date()


def parse_data_hora(texto: str, campo: str) -> datetime:
    return _parse(texto, FORMATOS_DATA_HORA_SHEET, campo)


def interpreta_linha(linha: pd.Series) -> Solicitacao:
    """Linha do Sheet -> Solicitacao validada. ValueError com o motivo (em português,
    vai pro status da linha) se algo estiver inválido."""
    campanha = str(linha[COL_SHEET_CAMPANHA]).strip()
    if not campanha:
        raise ValueError("campanha vazia")
    tipo = str(linha[COL_SHEET_TIPO]).strip()
    if tipo not in TIPOS_SOLICITACAO:
        raise ValueError(f"tipo de solicitação desconhecido: {tipo!r}")
    data_inicio = parse_data(linha[COL_SHEET_INICIO], COL_SHEET_INICIO)
    data_fim = parse_data(linha[COL_SHEET_FIM], COL_SHEET_FIM)
    if data_inicio > data_fim:
        raise ValueError(f"data de início ({data_inicio:%d/%m/%Y}) depois da data de fim ({data_fim:%d/%m/%Y})")
    return Solicitacao(
        linha=int(linha[COL_LINHA]),
        carimbo=str(linha[COL_SHEET_CARIMBO]).strip(),
        email=str(linha[COL_SHEET_EMAIL]).strip(),
        campanha=campanha,
        tipo=tipo,
        data_inicio=data_inicio,
        data_fim=data_fim,
        data_geracao=parse_data(linha[COL_SHEET_GERACAO], COL_SHEET_GERACAO),
    )


def esta_pronta(solicitacao: Solicitacao, hoje: date) -> bool:
    """A regra inteira de agendamento — ver docstring do módulo."""
    return solicitacao.data_geracao <= hoje


def processando_travada(status: str, processado_em: str, agora: datetime) -> bool:
    """True se a linha está em 'processando' há mais de HORAS_LIMITE_PROCESSANDO.
    processado_em ilegível conta como travada (não tem como saber quando começou)."""
    if status.strip() != STATUS_PROCESSANDO:
        return False
    try:
        inicio = parse_data_hora(processado_em, COL_SHEET_PROCESSADO_EM)
    except ValueError:
        return True
    return (agora - inicio).total_seconds() > HORAS_LIMITE_PROCESSANDO * 3600


def status_pulado(motivo: str) -> str:
    return f"{PREFIXO_STATUS_PULADO}: {motivo}"


def status_aguardando(n_conversas: int, teto: int) -> str:
    return f"{PREFIXO_STATUS_AGUARDANDO}: {n_conversas} conversas com resposta no período (teto: {teto})"


def aguardando_liberacao(status: str) -> bool:
    return status.strip().startswith(PREFIXO_STATUS_AGUARDANDO)


def status_erro(motivo: str, flow_run_id: str | None = None) -> str:
    """Motivo curto (cabe numa célula legível) + id curto do flow run pra achar o log
    completo no Prefect."""
    motivo = " ".join(str(motivo).split())
    if len(motivo) > _MAX_CHARS_MOTIVO:
        motivo = motivo[: _MAX_CHARS_MOTIVO - 1] + "…"
    sufixo = f" (run {flow_run_id[:8]})" if flow_run_id else ""
    return f"{PREFIXO_STATUS_ERRO}: {motivo}{sufixo}"


def _localiza_linha(df: pd.DataFrame, carimbo: str, email: str, linha_esperada: int) -> int | None:
    """Linha atual da solicitação no Sheet. Prefere a linha lida antes (caso normal);
    se ela mudou (alguém ordenou/apagou linhas no meio da execução), procura de novo
    pelo par carimbo + e-mail. None se não achar ou se houver mais de uma — melhor não
    gravar do que gravar status na resposta de outra pessoa."""
    mesma = (df[COL_SHEET_CARIMBO].str.strip() == carimbo) & (df[COL_SHEET_EMAIL].str.strip() == email)
    candidatas = df.loc[mesma, COL_LINHA].tolist()
    if linha_esperada in candidatas:
        return linha_esperada
    return candidatas[0] if len(candidatas) == 1 else None


# ============================================================================
# Leitura e escrita no Sheet
# ============================================================================


def _le_solicitacoes(sheets: Resource) -> pd.DataFrame:
    df = le_sheet(sheets, SOLICITACOES_SHEET_ID, aba=SOLICITACOES_ABA, com_linha=True)
    faltando = [c for c in _COLUNAS_OBRIGATORIAS if c not in df.columns]
    if faltando:
        raise ValueError(
            f"Coluna(s) {faltando} não encontrada(s) na aba '{SOLICITACOES_ABA}' do Sheet de solicitações "
            f"({SOLICITACOES_SHEET_ID}) — conferir títulos das perguntas do Form e as colunas de status."
        )
    return df


def _grava(sheets: Resource, alvo: _Alvo, status: str, link: str, agora: datetime) -> bool:
    linha_atual = _localiza_linha(_le_solicitacoes(sheets), alvo.carimbo, alvo.email, alvo.linha)
    if linha_atual is None:
        print(
            f"[SOLICITACOES] linha de '{alvo.carimbo}' / '{alvo.email}' não encontrada (ou duplicada) — "
            f"status '{status}' NÃO gravado."
        )
        return False
    atualiza_linha(
        sheets,
        SOLICITACOES_SHEET_ID,
        SOLICITACOES_ABA,
        linha_atual,
        {
            COL_SHEET_STATUS: status,
            COL_SHEET_LINK: link,
            COL_SHEET_PROCESSADO_EM: agora.strftime(FORMATO_PROCESSADO_EM),
        },
    )
    return True


@task(log_prints=True)
def registra_status(solicitacao: Solicitacao, status: str, link: str = "") -> bool:
    """Grava status/link/processado_em na linha da solicitação. Nunca levanta exceção:
    falhar ao gravar status de uma solicitação não pode derrubar as outras da rodada
    (o erro fica no log). Devolve se gravou."""
    try:
        gravou = _grava(get_sheets_service(), solicitacao.alvo, status, link, agora_brt())
    except Exception as exc:  # ver docstring: status de uma solicitação não derruba as outras
        print(f"[SOLICITACOES] {solicitacao.descricao()} falha ao gravar status '{status}': {exc}")
        return False
    if gravou:
        print(f"[SOLICITACOES] {solicitacao.descricao()} status -> {status}")
    return gravou


@task(log_prints=True)
def carrega_solicitacoes(
    hoje: date, flow_run_id: str | None = None, campanhas_liberadas: list[str] | None = None
) -> list[Solicitacao]:
    """Solicitações a processar nesta execução, já marcadas como 'processando' no Sheet:
    as de status vazio com data de geração até hoje e, das campanhas em
    `campanhas_liberadas`, também as que estavam 'aguardando liberação' — só ESSAS podem
    passar do TETO_CONVERSAS. Solicitação nova de uma campanha liberada segue a regra
    normal (se passar do teto, também vai pra 'aguardando liberação'): a liberação vale
    só pro que já foi barrado e visto.

    Antes, fecha as pendências da planilha:
      - 'processando' travado há mais de HORAS_LIMITE_PROCESSANDO -> erro (interrompida);
      - status vazio com dado inválido -> erro com o motivo (não espera a data de geração:
        quem pediu fica sabendo logo que precisa reenviar)."""
    print("[SOLICITACOES] iniciando — obtendo serviço do Sheets...")
    sheets = get_sheets_service()
    print("[SOLICITACOES] serviço obtido — lendo sheet de solicitações...")
    df = _le_solicitacoes(sheets)
    print(f"[SOLICITACOES] sheet lido — {len(df)} linha(s) encontrada(s).")
    agora = agora_brt()

    for _, linha in df.iterrows():
        if processando_travada(linha[COL_SHEET_STATUS], linha[COL_SHEET_PROCESSADO_EM], agora):
            status = status_erro("execução interrompida, envie a solicitação novamente", flow_run_id)
            _grava(sheets, _Alvo.da_linha(linha), status, "", agora)
            print(
                f"[SOLICITACOES] linha {linha[COL_LINHA]} estava 'processando' desde "
                f"{linha[COL_SHEET_PROCESSADO_EM]} — marcada como erro."
            )

    liberadas = {c.strip() for c in campanhas_liberadas or [] if c.strip()}
    pendentes = df[
        (df[COL_SHEET_STATUS].str.strip() == "")
        | (df[COL_SHEET_STATUS].map(aguardando_liberacao) & df[COL_SHEET_CAMPANHA].str.strip().isin(liberadas))
    ]
    prontas: list[Solicitacao] = []
    n_aguardando = 0
    for _, linha in pendentes.iterrows():
        try:
            solicitacao = interpreta_linha(linha)
        except ValueError as exc:
            _grava(sheets, _Alvo.da_linha(linha), status_erro(str(exc), flow_run_id), "", agora)
            print(f"[SOLICITACOES] linha {linha[COL_LINHA]} inválida: {exc}")
            continue
        if not esta_pronta(solicitacao, hoje):
            n_aguardando += 1
            continue
        if aguardando_liberacao(linha[COL_SHEET_STATUS]):
            solicitacao = replace(solicitacao, liberada=True)
        if _grava(sheets, solicitacao.alvo, STATUS_PROCESSANDO, "", agora):
            prontas.append(solicitacao)

    print(
        f"[SOLICITACOES] {len(prontas)} solicitação(ões) a processar hoje ({hoje}); "
        f"{n_aguardando} aguardando a data de geração."
    )
    return prontas

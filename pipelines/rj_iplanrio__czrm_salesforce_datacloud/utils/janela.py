"""Cálculo das janelas de extração.

A fonte (DLO do conector Salesforce) grava ``SystemModstamp`` em **UTC real**.
As janelas aqui são calculadas em hora-parede de São Paulo — porque "ontem" é
o dia do Rio, não o dia UTC — e convertidas para UTC antes de irem para a SQL
(:func:`para_utc`).

Quatro recortes:

- ``hora``          : últimos 60 minutos, a cada 15min. A sobreposição de 1h
                      faz um tick perdido ser coberto pelos três seguintes.
- ``dia``           : um dia inteiro, 1x/dia. Rede de segurança do modo hora.
- ``reconciliacao`` : últimos N dias (padrão 7), 1x/semana. Cobre o caso em que
                      o Data Cloud ingere com atraso um registro alterado há
                      dias — o relógio do registro é o da origem, não o da
                      chegada.
- ``carga_inicial`` : sem filtro (primeira carga). Não passa por aqui.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

SP = ZoneInfo("America/Sao_Paulo")
_FMT = "%Y-%m-%dT%H:%M:%SZ"  # formato aceito pela SQL do Data Cloud

DIAS_RECONCILIACAO = 7


def hoje() -> date:
    """Data corrente em São Paulo (o ``date.today()`` do SO usaria UTC)."""
    return datetime.now(tz=SP).date()


def _parse(texto: str) -> datetime:
    """Interpreta o formato da API. O ``Z`` é convenção de formato, não fuso —
    quem aplica o fuso é o chamador (:func:`para_utc`)."""
    return datetime.strptime(texto, _FMT)  # noqa: DTZ007


def janela_hora(agora: datetime | None = None) -> tuple[str, str]:
    """Janela rolante de 1h, clampada no início do dia corrente.

    :param agora: instante de referência. Padrão: agora, em hora de SP.
    :returns: ``(data_inicio, data_fim)`` em hora de SP, formato da API.
    """
    agora = agora or datetime.now(tz=SP)
    inicio_dia = agora.replace(hour=0, minute=0, second=0, microsecond=0)
    inicio = max(agora - timedelta(hours=1), inicio_dia)
    return inicio.strftime(_FMT), agora.strftime(_FMT)


def janela_dia(dia: date, agora: datetime | None = None) -> tuple[str, str]:
    """Janela de um dia inteiro (até agora, se ``dia`` for hoje).

    :param dia: dia a processar.
    :param agora: instante de referência. Padrão: agora, em hora de SP.
    :returns: ``(data_inicio, data_fim)`` em hora de SP, formato da API.
    """
    agora = agora or datetime.now(tz=SP)
    inicio = datetime(dia.year, dia.month, dia.day, 0, 0, 0, tzinfo=SP)
    fim_dia = inicio + timedelta(days=1)
    fim = min(fim_dia, agora) if dia == agora.date() else fim_dia
    return inicio.strftime(_FMT), fim.strftime(_FMT)


def janela_reconciliacao(
    dias: int = DIAS_RECONCILIACAO,
    agora: datetime | None = None,
) -> tuple[str, str]:
    """Janela dos últimos ``dias`` dias completos, mais o dia corrente.

    :param dias: quantos dias para trás (mínimo 1).
    :param agora: instante de referência. Padrão: agora, em hora de SP.
    :returns: ``(data_inicio, data_fim)`` em hora de SP, formato da API.
    :raises ValueError: Se ``dias`` for menor que 1.
    """
    if dias < 1:
        raise ValueError(f"dias precisa ser >= 1, veio {dias}.")
    agora = agora or datetime.now(tz=SP)
    inicio = (agora - timedelta(days=dias)).replace(hour=0, minute=0, second=0, microsecond=0)
    return inicio.strftime(_FMT), agora.strftime(_FMT)


def para_utc(data_inicio: str, data_fim: str) -> tuple[str, str]:
    """Converte uma janela em hora de SP para UTC real.

    :param data_inicio: início em hora de SP (formato da API).
    :param data_fim: fim (exclusivo) em hora de SP.
    :returns: ``(inicio_utc, fim_utc)`` no formato da API.
    """
    inicio = _parse(data_inicio).replace(tzinfo=SP).astimezone(timezone.utc)
    fim = _parse(data_fim).replace(tzinfo=SP).astimezone(timezone.utc)
    return inicio.strftime(_FMT), fim.strftime(_FMT)


def janelas_do_run(
    modo: str,
    dia: date | None,
    reprocessar: bool,
    reprocessar_de: date | None,
    reprocessar_ate: date | None,
) -> list[tuple[str, str]]:
    """Lista de janelas ``(inicio, fim)`` que o run deve processar.

    :param modo: ``hora``, ``dia`` ou ``reconciliacao``.
    :param dia: dia a processar no modo ``dia``. Padrão: ontem.
    :param reprocessar: reprocessa dia a dia de ``reprocessar_de`` a ``reprocessar_ate``.
    :param reprocessar_de: primeiro dia (inclusivo).
    :param reprocessar_ate: último dia (inclusivo). Padrão: igual a ``de``.
    :returns: janelas em hora de SP, na ordem de execução.
    :raises ValueError: Se o reprocessamento vier incompleto, invertido ou no futuro.
    """
    if not reprocessar:
        if modo == "dia":
            return [janela_dia(dia or (hoje() - timedelta(days=1)))]
        if modo == "reconciliacao":
            return [janela_reconciliacao()]
        return [janela_hora()]

    if reprocessar_de is None:
        raise ValueError("reprocessar=True exige reprocessar_de.")
    ate = reprocessar_ate or reprocessar_de
    if ate < reprocessar_de:
        raise ValueError(f"reprocessar_ate ({ate}) antes de reprocessar_de ({reprocessar_de}).")
    if ate > hoje():
        raise ValueError(f"reprocessar_ate ({ate}) no futuro.")

    dias = (ate - reprocessar_de).days + 1
    return [janela_dia(reprocessar_de + timedelta(days=i)) for i in range(dias)]


def filtro_sql(coluna: str, data_inicio: str, data_fim: str) -> str:
    """Cláusula WHERE da janela, já em UTC, pronta para a SQL.

    :param coluna: coluna de modificação da DLO (ex.: ``SystemModstamp__c``).
    :param data_inicio: início da janela em hora de SP.
    :param data_fim: fim (exclusivo) em hora de SP.
    :returns: ``WHERE col >= '...' AND col < '...'`` em UTC, ou string vazia
        quando a janela é aberta (carga inicial).
    """
    if not data_inicio or not data_fim:
        return ""
    inicio, fim = para_utc(data_inicio, data_fim)
    return f"WHERE {coluna} >= '{inicio}' AND {coluna} < '{fim}'"


def fmt_janela(data_inicio: str, data_fim: str) -> str:
    """Formata a janela para log: ``'2026-10-23 14:00 → 15:00 (SP)'``."""
    ini = _parse(data_inicio).strftime("%Y-%m-%d %H:%M")
    fim = _parse(data_fim).strftime("%Y-%m-%d %H:%M")
    return f"{ini} → {fim} (SP)"

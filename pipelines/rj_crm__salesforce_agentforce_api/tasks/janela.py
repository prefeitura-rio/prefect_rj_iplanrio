# -*- coding: utf-8 -*-
"""
Janelas de extração — substituem o watermark único (lê >= watermark, nunca
revisita) por duas passadas com sobreposição intencional. Motivo: watermark
forward-only não tem cura automática pra falha pontual (schedule pausado,
erro transitório, rajada de volume) — o tick seguinte só olha pra frente,
então qualquer coisa perdida num tick fica perdida pra sempre até alguém
notar e fazer backfill manual (como aconteceu com 02/09/2026, achado nesta
investigação).

  - janela_hora(): últimos 60 minutos, nunca cruzando a virada de dia. Roda a
    cada 15min. Como a janela sempre se sobrepõe à da rodada anterior (60min
    de range, 15min entre execuções), um tick que falhar é coberto pelos 3
    seguintes dentro da mesma hora — auto-cura de falha pontual.
  - janela_dia(data): o dia inteiro (ou até agora, se for hoje). Roda 1x/dia,
    reconciliação funda — pega o que nem a sobreposição de 1h alcançou (ex.:
    pipeline parado por mais de 1h seguida).

Ambas retornam limites já formatados pra WHERE >= ... AND < ..., e o
partition_date correto pra carimbar data_particao — cada chamada é sempre
uma janela de no máximo 1 dia, então data_particao = 1 valor único continua
seguro (diferente de um backfill de range largo, que precisaria de
data_particao por linha).

Tudo em hora-parede de São Paulo — mesmo formato que a fonte usa (dígitos
batem com a UI da Salesforce, apesar do rótulo UTC do BigQuery/DLO; ver notas
em extract_data_cloud.py e raw_salesforce_ai_agent_session.sql). Usar
datetime.now(tz=timezone.utc) aqui, como o checkpoint.py antigo fazia,
introduziria ~3h de descompasso com o "agora" da fonte.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

SP = ZoneInfo("America/Sao_Paulo")
_FMT = "%Y-%m-%dT%H:%M:%SZ"  # 'Z' é convenção de formato da fonte, não fuso real


def janela_hora(agora: datetime | None = None) -> tuple[str, str, date]:
    """
    Janela rolante de 1h, clampada no início do dia corrente (nunca cruza
    meia-noite) — garante 1 único data_particao por chamada.

    Args:
        agora: instante de referência. Default: agora, em hora de SP.

    Returns:
        (data_inicio, data_fim, partition_date) — as duas primeiras prontas
        pra query ('YYYY-MM-DDTHH:MM:SSZ'); a terceira é o date() do
        partition_date a passar pro load.
    """
    agora = agora or datetime.now(tz=SP)
    inicio_dia = agora.replace(hour=0, minute=0, second=0, microsecond=0)
    inicio = max(agora - timedelta(hours=1), inicio_dia)
    return inicio.strftime(_FMT), agora.strftime(_FMT), agora.date()


def janela_dia(dia: date, agora: datetime | None = None) -> tuple[str, str, date]:
    """
    Janela do dia inteiro — reconciliação funda, 1x/dia. Se `dia` for hoje,
    o fim é limitado ao instante atual (não pede dado do futuro).

    Args:
        dia   : dia a reconciliar (normalmente "ontem", rodando logo após a
                virada — mas aceita qualquer dia, inclusive hoje).
        agora : instante de referência. Default: agora, em hora de SP.

    Returns:
        Mesmo formato de janela_hora.
    """
    agora = agora or datetime.now(tz=SP)
    inicio = datetime(dia.year, dia.month, dia.day, 0, 0, 0, tzinfo=SP)
    fim_dia = inicio + timedelta(days=1)
    fim = min(fim_dia, agora) if dia == agora.date() else fim_dia
    return inicio.strftime(_FMT), fim.strftime(_FMT), dia

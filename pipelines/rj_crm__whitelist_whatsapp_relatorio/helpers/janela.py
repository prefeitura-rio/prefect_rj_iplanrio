"""Compute the 24 hour window of the report.

It does not depend on Prefect: `tasks.py` reads the run context and passes the
resolved values here, so the rule of the window is testable alone.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

FORMATO_SQL = "%Y-%m-%d %H:%M:%S"
FORMATO_EXIBICAO = "%d/%m/%Y %H:%M"
FORMATO_DATA = "%d/%m/%Y"

TIMEZONE = "America/Sao_Paulo"
"""Fuso de referência. ``inicio_datahora`` é DATETIME em hora-parede de São Paulo."""

HORA_CORTE = 9
"""Hora do agendamento diário, espelhando o ``cron`` de produção no ``prefect.yaml``."""


@dataclass(frozen=True)
class ConfigJanela:
    """Parâmetros que definem o recorte diário."""

    timezone: str
    hora_corte: int
    duracao_horas: int = 24


CONFIG_PADRAO = ConfigJanela(timezone=TIMEZONE, hora_corte=HORA_CORTE)
"""Configuração de produção, montada a partir das constantes acima."""


def agora_no_fuso() -> datetime:
    """Return the current time in the reference time zone.

    It lets `tasks.py` skip `datetime` and `ZoneInfo`. `resolver` still gets the instant
    by injection, which keeps the rule of the window testable without a frozen clock.
    """
    return datetime.now(tz=ZoneInfo(TIMEZONE))


@dataclass(frozen=True)
class Janela:
    """Recorte temporal semiaberto `[inicio, fim)` do relatório."""

    inicio: datetime
    fim: datetime
    origem: str

    @property
    def inicio_sql(self) -> str:
        """Return the start in the format that `DATETIME()` of BigQuery accepts."""
        return self.inicio.strftime(FORMATO_SQL)

    @property
    def fim_sql(self) -> str:
        """Return the end in the format that `DATETIME()` of BigQuery accepts."""
        return self.fim.strftime(FORMATO_SQL)

    @property
    def inicio_exibicao(self) -> str:
        """Início no formato mostrado ao leitor."""
        return self.inicio.strftime(FORMATO_EXIBICAO)

    @property
    def fim_exibicao(self) -> str:
        """Fim no formato mostrado ao leitor."""
        return self.fim.strftime(FORMATO_EXIBICAO)

    @property
    def data_relatorio(self) -> str:
        """Return the date of the report, which is the date of the end of the window."""
        return self.fim.strftime(FORMATO_DATA)


def para_fuso(momento: datetime, timezone: str) -> datetime:
    """Return a datetime in the given time zone.

    A naive `momento` counts as already in the target zone. `timezone` is an IANA name
    such as `America/Sao_Paulo`.
    """
    fuso = ZoneInfo(timezone)
    if momento.tzinfo is None:
        return momento.replace(tzinfo=fuso)
    return momento.astimezone(fuso)


def interpretar(texto: str, timezone: str) -> datetime:
    """Read a date and time from text, in the time zone of the report.

    `texto` is in ISO format, such as `2026-09-15 09:00:00`. Raise `ValueError` if it is
    not a valid ISO date and time.
    """
    return para_fuso(datetime.fromisoformat(texto), timezone)


def ultimo_corte(agora: datetime, hora_corte: int) -> datetime:
    """Return the latest cut time that has passed.

    `agora` has a time zone, and `hora_corte` is the hour of the daily schedule, from 0
    to 23. The result is never after `agora`.
    """
    corte = agora.replace(hour=hora_corte, minute=0, second=0, microsecond=0)
    if corte > agora:
        corte -= timedelta(days=1)
    return corte


def resolver(
    start_datetime: str | None,
    end_datetime: str | None,
    ancora_agendada: datetime | None,
    agora: datetime,
    config: ConfigJanela,
) -> Janela:
    """Resolve the window of the report by the order of precedence.

    The order is: the explicit parameters, the anchor of the schedule, and last the
    latest cut before `agora`. The last case covers a manual or local run, which has no
    reliable scheduled time.

    `ancora_agendada` is the scheduled time of a run from a deployment, and `None` for a
    manual or local run. `config` has the time zone, the cut hour and the length of the
    window. Return the window with its origin. Raise `ValueError` if only one of the
    explicit ends is given, or if the start is not before the end.
    """
    if (start_datetime is None) != (end_datetime is None):
        raise ValueError("start_datetime e end_datetime devem ser informados juntos ou ambos omitidos.")

    duracao = timedelta(hours=config.duracao_horas)

    if start_datetime is not None and end_datetime is not None:
        inicio = interpretar(start_datetime, config.timezone)
        fim = interpretar(end_datetime, config.timezone)
        if inicio >= fim:
            raise ValueError(f"Início {inicio} não é anterior ao fim {fim}.")
        return Janela(inicio=inicio, fim=fim, origem="parametros")

    if ancora_agendada is not None:
        fim = para_fuso(ancora_agendada, config.timezone)
        return Janela(inicio=fim - duracao, fim=fim, origem="agendamento")

    fim = ultimo_corte(para_fuso(agora, config.timezone), config.hora_corte)
    return Janela(inicio=fim - duracao, fim=fim, origem="corte_local")

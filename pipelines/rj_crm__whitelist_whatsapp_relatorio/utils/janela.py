"""Cálculo da janela de 24h do relatório.

Sem dependência do Prefect: quem lê o contexto de execução é `tasks.py`, que passa os
valores já resolvidos para cá. Isso mantém a regra da janela testável isoladamente.
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
    """Momento atual no fuso de referência.

    Existe para que ``tasks.py`` não precise importar ``datetime`` nem ``ZoneInfo`` só
    para montar o argumento de :func:`resolver`, que continua recebendo o instante por
    injeção — é o que mantém a regra da janela testável sem congelar o relógio.

    :returns: Instante atual com fuso.
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
        """Início no formato aceito por ``DATETIME()`` do BigQuery."""
        return self.inicio.strftime(FORMATO_SQL)

    @property
    def fim_sql(self) -> str:
        """Fim no formato aceito por ``DATETIME()`` do BigQuery."""
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
        """Data de referência do relatório, que é a do fim da janela."""
        return self.fim.strftime(FORMATO_DATA)


def para_fuso(momento: datetime, timezone: str) -> datetime:
    """Converte um datetime para o fuso indicado.

    :param momento: Datetime ingênuo, assumido já no fuso alvo, ou com fuso definido.
    :param timezone: Nome IANA do fuso, por exemplo ``America/Sao_Paulo``.
    :returns: Datetime com fuso definido.
    """
    fuso = ZoneInfo(timezone)
    if momento.tzinfo is None:
        return momento.replace(tzinfo=fuso)
    return momento.astimezone(fuso)


def interpretar(texto: str, timezone: str) -> datetime:
    """Interpreta uma data-hora textual no fuso do relatório.

    :param texto: Data-hora em formato ISO, por exemplo ``2026-09-15 09:00:00``.
    :param timezone: Nome IANA do fuso.
    :returns: Datetime com fuso definido.
    :raises ValueError: Se o texto não for uma data-hora ISO válida.
    """
    return para_fuso(datetime.fromisoformat(texto), timezone)


def ultimo_corte(agora: datetime, hora_corte: int) -> datetime:
    """Devolve o horário de corte mais recente que já passou.

    :param agora: Momento de referência, com fuso.
    :param hora_corte: Hora do agendamento diário, de 0 a 23.
    :returns: Datetime do corte mais recente, sempre menor ou igual a ``agora``.
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
    """Resolve a janela do relatório pela cadeia de precedência.

    A ordem é: parâmetros explícitos, âncora do agendamento e, por último, o corte mais
    recente anterior a ``agora``. O último caso cobre execução manual ou local, em que
    não existe horário agendado confiável.

    :param start_datetime: Início explícito, ou ``None``.
    :param end_datetime: Fim explícito, ou ``None``.
    :param ancora_agendada: Horário agendado, quando a execução vem de um deployment.
        ``None`` em execução manual ou local.
    :param agora: Momento atual, com fuso.
    :param config: Fuso, hora de corte e duração da janela.
    :returns: A janela resolvida, com a origem registrada.
    :raises ValueError: Se apenas um dos extremos explícitos for informado, ou se o
        início não for anterior ao fim.
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

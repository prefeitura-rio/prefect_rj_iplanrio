"""Janela horária do backfill, em horário de São Paulo; pode atravessar a meia-noite (``20:00-06:00``)."""

from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo

FUSO_SAO_PAULO = ZoneInfo("America/Sao_Paulo")


@dataclass(frozen=True, slots=True)
class JanelaHoraria:
    """Intervalo ``[inicio, fim)`` do dia; com ``inicio > fim`` ele atravessa a meia-noite.

    :param inicio: Hora de abertura (inclusive).
    :param fim: Hora de fechamento (exclusive).
    """

    inicio: time
    fim: time

    @classmethod
    def de_texto(cls, texto: str) -> "JanelaHoraria":
        """Lê ``HH:MM-HH:MM``.

        :raises ValueError: Se o texto não for uma janela válida ou ela for vazia (início igual ao fim).
        """
        try:
            abre, fecha = texto.split("-")
            janela = cls(time.fromisoformat(abre), time.fromisoformat(fecha))
        except ValueError:
            raise ValueError(f"janela horária inválida: {texto!r}; use HH:MM-HH:MM, por exemplo 20:00-06:00") from None
        if janela.inicio == janela.fim:
            raise ValueError(f"janela horária inválida: {texto!r} começa e termina na mesma hora")
        return janela

    def contem(self, momento: datetime) -> bool:
        """Diz se o instante, visto em São Paulo, cai dentro da janela."""
        hora = momento.astimezone(FUSO_SAO_PAULO).time().replace(second=0, microsecond=0)
        if self.inicio < self.fim:
            return self.inicio <= hora < self.fim
        return hora >= self.inicio or hora < self.fim

    def __str__(self) -> str:
        return f"{self.inicio:%H:%M}-{self.fim:%H:%M}"

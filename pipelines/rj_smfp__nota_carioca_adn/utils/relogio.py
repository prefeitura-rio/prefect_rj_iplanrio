"""Relógio injetável: o extrator e o backfill nunca chamam ``time`` diretamente."""

import time
from datetime import UTC, datetime
from typing import Protocol


class Relogio(Protocol):
    """Tempo monotônico, hora de parede em UTC e espera."""

    def monotonic(self) -> float: ...

    def agora_utc(self) -> datetime: ...

    def dormir(self, segundos: float) -> None: ...


class RelogioReal:
    """Relógio do sistema."""

    def monotonic(self) -> float:
        """Segundos desde um ponto qualquer, sempre crescente."""
        return time.monotonic()

    def agora_utc(self) -> datetime:
        """Agora, em UTC."""
        return datetime.now(UTC)

    def dormir(self, segundos: float) -> None:
        """Espera de verdade."""
        time.sleep(segundos)

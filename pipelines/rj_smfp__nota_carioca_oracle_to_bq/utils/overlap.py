"""Contagem do Oracle em segundo plano, sobreposta à extração."""

import contextvars
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import format_duration


@dataclass(frozen=True)
class CountOutcome:
    """Resultado de uma contagem.

    :param rows: Linhas contadas.
    :param seconds: Duração da contagem.
    """

    rows: int
    seconds: float


class BackgroundCount:
    """Executa uma contagem numa thread própria; quem a inicia a espera com :meth:`result` ou :meth:`abandon`.

    A thread herda o contexto de quem chama :meth:`start`, para que ``report`` (log do Prefect) funcione nela. O
    python-oracledb em modo thick solta o GIL durante as chamadas ao banco, então a thread principal segue ativa.
    """

    def __init__(self, label: str, count: Callable[[], int], report: Callable[[str], None]) -> None:
        """Prepara a contagem sem iniciá-la.

        :param label: Nome exibido nos logs (a tabela).
        :param count: Executa a contagem, com a sua própria conexão.
        :param report: Publica uma linha de log.
        """
        self.label = label
        self.count = count
        self.report = report
        self.outcome: CountOutcome | None = None
        self.error: Exception | None = None
        self.thread = threading.Thread(target=self.run_in_context, name=f"count-{label}")
        self.context = contextvars.copy_context()

    def start(self) -> None:
        """Inicia a thread de contagem."""
        self.thread.start()

    def run_in_context(self) -> None:
        """Roda :meth:`run` no contexto copiado em :meth:`__init__`."""
        self.context.run(self.run)

    def run(self) -> None:
        """Conta, guarda o resultado ou a falha e, com sucesso, registra a duração no log."""
        started = time.monotonic()
        try:
            rows = self.count()
        except Exception as error:
            self.error = error
            return
        self.outcome = CountOutcome(rows=rows, seconds=time.monotonic() - started)
        self.report(
            f"{self.label}: contagem no Oracle concluída (em segundo plano): {rows:,} linhas "
            f"em {format_duration(self.outcome.seconds)}"
        )

    def result(self) -> CountOutcome:
        """Espera a thread terminar e entrega a contagem.

        :returns: Linhas e duração.
        :raises Exception: A falha da contagem, a mesma levantada na thread.
        :raises RuntimeError: Se a thread terminou sem resultado nem falha.
        """
        self.thread.join()
        if self.error is not None:
            raise self.error
        if self.outcome is None:
            raise RuntimeError(f"{self.label}: a contagem terminou sem resultado.")
        return self.outcome

    def abandon(self) -> None:
        """Espera a thread terminar e descarta o resultado ou a falha; usado quando a extração já falhou."""
        self.thread.join()

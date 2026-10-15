"""Contrapressão por memória: pausa o início de novas faixas quando o pod chega perto do limite do cgroup.

Os workers ficam abertos durante toda a extração e o RSS de cada um tende a crescer (fragmentação do alocador, buffers
do driver). Perto do limite o pod é morto pelo OOM killer e a tabela inteira se perde; o gate deixa as faixas em leitura
terminarem e seus envios acabarem, sem iniciar outras, até a memória baixar.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.cgroup_memory import MemoryReader, PodMemory
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.memory import MIB
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import format_size

# A extração só volta a iniciar faixas quando o uso cai esta fração do limite abaixo do limiar de pausa; sem a margem o
# gate oscilaria a cada faixa que termina.
RESUME_MARGIN = 0.05


@dataclass(frozen=True)
class GateSummary:
    """O que o gate fez numa extração.

    :param pauses: Vezes que o início de faixas foi pausado.
    :param paused_seconds: Tempo total pausado, incluindo uma pausa ainda aberta ao fim.
    :param forced_chunks: Faixas iniciadas com o gate pausado porque nenhuma outra estava em leitura (sem isso a
        extração travaria); cada uma anda uma faixa mesmo com a memória alta.
    :param peak_used_bytes: Maior working set observado; ``None`` se a memória nunca foi lida.
    """

    pauses: int = 0
    paused_seconds: float = 0.0
    forced_chunks: int = 0
    peak_used_bytes: int | None = None

    def describe(self, cgroup_peak_bytes: int | None) -> str:
        """Resumo para o log da extração: picos de memória e pausas.

        :param cgroup_peak_bytes: ``memory.peak`` do cgroup (inclui cache de arquivo), se disponível.
        :returns: Texto como ``memória do pod: pico de 4.1 GB, memory.peak 4.6 GB (com cache de arquivo); nenhuma
            pausa por memória``.
        """
        if self.peak_used_bytes is None and cgroup_peak_bytes is None:
            return (
                "memória do pod: sem leitura do cgroup (fora de um container ou sem faixas); nenhuma pausa por memória"
            )
        peaks = []
        if self.peak_used_bytes is not None:
            peaks.append(f"pico de {format_size(self.peak_used_bytes)}")
        if cgroup_peak_bytes is not None:
            peaks.append(f"memory.peak {format_size(cgroup_peak_bytes)} (com cache de arquivo)")
        if not self.pauses:
            return f"memória do pod: {', '.join(peaks)}; nenhuma pausa por memória"
        forced = f", {self.forced_chunks} faixas liberadas com a memória ainda alta" if self.forced_chunks else ""
        return (
            f"memória do pod: {', '.join(peaks)}; {self.pauses} pausas por memória, "
            f"{self.paused_seconds:.0f} s no total{forced}"
        )


def describe(memory: PodMemory | None) -> str:
    """Texto do uso para o log: ``1234 MiB de 6144 MiB (20%)``, ou só o uso se não há limite."""
    if memory is None:
        return "memória do pod indisponível"
    used = f"{memory.used_bytes / MIB:.0f} MiB"
    if memory.limit_bytes is None or memory.ratio is None:
        return f"{used}, sem limite"
    return f"{used} de {memory.limit_bytes / MIB:.0f} MiB ({memory.ratio:.0%})"


class MemoryGate:
    """Decide se uma nova faixa pode começar, com histerese; só a thread principal do agendador o usa.

    Pausa quando o uso chega a ``pause_ratio`` do limite e só retoma abaixo de ``pause_ratio - RESUME_MARGIN``.
    Pausado, nunca deixa o agendador sem nenhuma faixa em leitura: com a memória ainda alta libera uma faixa por vez,
    para a extração seguir. Sem limite (``max`` no cgroup), sem cgroup ou com ``pause_ratio`` nulo ou zero, não pausa
    nunca; a leitura segue alimentando o progresso.

    :param pause_ratio: Fração do limite a partir da qual pausa; ``None`` ou ``0`` desliga a pausa.
    :param read: Lê a memória do pod; devolve ``None`` fora de um cgroup.
    :param report: Publica uma linha de log, já com o nome da tabela.
    """

    def __init__(self, pause_ratio: float | None, read: MemoryReader, report: Callable[[str], None]) -> None:
        self.pause_ratio = pause_ratio or 0.0
        self.read = read
        self.report = report
        self.last: PodMemory | None = None
        self.paused = False
        self.pauses = 0
        self.forced_chunks = 0
        self.paused_seconds = 0.0
        self.peak_used_bytes: int | None = None
        self._paused_since = 0.0

    @classmethod
    def off(cls) -> "MemoryGate":
        """Gate que não lê nem pausa nada."""
        return cls(None, lambda: None, lambda _: None)

    def refresh(self) -> None:
        """Lê a memória do pod e pausa ou retoma conforme os limiares."""
        self.last = self.read()
        if self.last is not None:
            self.peak_used_bytes = max(self.peak_used_bytes or 0, self.last.used_bytes)
        ratio = self.last.ratio if self.last is not None else None
        if self.paused:
            if ratio is None or ratio < self.pause_ratio - RESUME_MARGIN:
                self._resume()
        elif self.pause_ratio and ratio is not None and ratio >= self.pause_ratio:
            self._pause()

    def admit(self, in_flight: int) -> bool:
        """Autoriza o início de uma faixa.

        :param in_flight: Faixas em leitura agora.
        :returns: ``True`` se não está pausado, ou se está e nenhuma faixa está em leitura (a faixa liberada conta em
            ``forced_chunks``).
        """
        if not self.paused:
            return True
        if in_flight == 0:
            self.forced_chunks += 1
            return True
        return False

    def summary(self) -> GateSummary:
        """Totais até agora; uma pausa ainda aberta entra com o tempo decorrido."""
        open_seconds = time.monotonic() - self._paused_since if self.paused else 0.0
        return GateSummary(self.pauses, self.paused_seconds + open_seconds, self.forced_chunks, self.peak_used_bytes)

    def _pause(self) -> None:
        self.paused = True
        self.pauses += 1
        self._paused_since = time.monotonic()
        self.report(
            f"memória do pod em {describe(self.last)} atingiu o limiar de pausa de {self.pause_ratio:.0%}; pausando o "
            f"início de novas faixas até cair abaixo de {self.pause_ratio - RESUME_MARGIN:.0%} "
            "(as faixas em leitura e os envios continuam)"
        )

    def _resume(self) -> None:
        elapsed = time.monotonic() - self._paused_since
        self.paused = False
        self.paused_seconds += elapsed
        self.report(
            f"memória do pod em {describe(self.last)}: retomando o início de faixas após {elapsed:.0f} s de pausa"
        )

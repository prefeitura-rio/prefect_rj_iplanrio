"""Configuração do backfill e percurso das faixas de NSU com janela horária e linhas por minuto.

Compartilhado pelo backfill e pela conferência: ambos só leem o Oracle dentro de ``janela_horaria`` (São Paulo) e no
máximo ``linhas_por_minuto``; fora da janela o percurso termina sem entregar faixa e registra ``fora_da_janela``.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Literal

from pipelines.rj_smfp__nota_carioca_adn.utils.armazenamento import Armazenamento
from pipelines.rj_smfp__nota_carioca_adn.utils.fonte import FonteOracle
from pipelines.rj_smfp__nota_carioca_adn.utils.indice import Indice
from pipelines.rj_smfp__nota_carioca_adn.utils.janela import FUSO_SAO_PAULO, JanelaHoraria
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import FaixaNsu
from pipelines.rj_smfp__nota_carioca_adn.utils.relogio import Relogio
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ConfigBackfill:
    """Parâmetros do backfill e da conferência.

    :param ambiente: ``staging`` ou ``prod``.
    :param nsu_inicio: Primeiro NSU da faixa a processar.
    :param nsu_fim: Último NSU da faixa a processar (inclusive).
    :param linhas_por_minuto: Teto de linhas lidas por minuto (20.000 na etapa 1 do pedido à DBA).
    :param janela_horaria: ``HH:MM-HH:MM`` em São Paulo; só lê dentro dela.
    :param faixa_nsus: NSUs por consulta ao Oracle (5.000 no pedido à DBA, 1.000 por padrão para poupar memória).
    :param linhas_por_objeto: Linhas por objeto gravado.
    """

    ambiente: str
    nsu_inicio: int
    nsu_fim: int
    linhas_por_minuto: int = 20_000
    janela_horaria: str = "20:00-06:00"
    faixa_nsus: int = 1000
    linhas_por_objeto: int = 1000

    def __post_init__(self) -> None:
        if not 0 <= self.nsu_inicio <= self.nsu_fim:
            raise ValueError(f"Faixa de NSU inválida: {self.nsu_inicio}..{self.nsu_fim}")
        if min(self.linhas_por_minuto, self.faixa_nsus, self.linhas_por_objeto) < 1:
            raise ValueError("linhas_por_minuto, faixa_nsus e linhas_por_objeto devem ser positivos")

    def faixas(self) -> Iterator[FaixaNsu]:
        """A faixa total repartida em consultas de ``faixa_nsus`` NSUs."""
        for inicio in range(self.nsu_inicio, self.nsu_fim + 1, self.faixa_nsus):
            yield FaixaNsu(inicio, min(inicio + self.faixa_nsus - 1, self.nsu_fim))


@dataclass(frozen=True, slots=True)
class ServicosBackfill:
    """As portas do backfill: fonte do histórico, bucket, índice e relógio."""

    fonte: FonteOracle
    armazenamento: Armazenamento
    indice: Indice
    relogio: Relogio


class Percurso:
    """Percorre as faixas respeitando a janela e as linhas por minuto; base do backfill e da conferência."""

    def __init__(self, config: ConfigBackfill, relogio: Relogio) -> None:
        self._config = config
        self._relogio = relogio
        self._janela = JanelaHoraria.de_texto(config.janela_horaria)
        self.motivo_fim: Literal["concluido", "fora_da_janela"] = "concluido"
        self.linhas_da_faixa = 0
        """Linhas completas lidas na faixa em curso; quem consome a faixa preenche, e a pausa seguinte depende disso."""

    def faixas_liberadas(self) -> Iterator[FaixaNsu]:
        """Entrega as faixas uma a uma; antes de cada uma (depois da pausa da anterior) exige estar na janela."""
        alvo_s, inicio_s = 0.0, self._relogio.monotonic()
        for faixa in self._config.faixas():
            espera = alvo_s - (self._relogio.monotonic() - inicio_s)
            if espera > 0:
                self._relogio.dormir(espera)
            agora = self._relogio.agora_utc()
            if not self._janela.contem(agora):
                logger.warning(
                    "fora_da_janela: %s fora de %s; nada foi lido a partir da faixa %d-%d",
                    agora.astimezone(FUSO_SAO_PAULO).strftime("%H:%M"),
                    self._janela,
                    faixa.ini,
                    faixa.fim,
                )
                self.motivo_fim = "fora_da_janela"
                return
            inicio_s = self._relogio.monotonic()
            self.linhas_da_faixa = 0
            yield faixa
            alvo_s = self.linhas_da_faixa / (self._config.linhas_por_minuto / 60)
            logger.info(
                "faixa %d-%d: %d linhas lidas (teto %d/min)",
                faixa.ini,
                faixa.fim,
                self.linhas_da_faixa,
                self._config.linhas_por_minuto,
            )

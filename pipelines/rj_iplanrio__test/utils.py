"""Funções utilitárias puras da pipeline de teste.

Este módulo não importa nada do Prefect — toda lógica de negócio fica aqui,
de modo que possa ser testada unitariamente sem depender de um servidor Prefect.
"""

import math
import random
import time
from dataclasses import dataclass

from prefect_rj_iplanrio.log import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Tipos de domínio
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Medicao:
    """Representa uma única leitura de sensor simulada.

    :param sensor_id: Identificador do sensor (ex: ``"sensor-01"``).
    :param valor: Valor bruto lido pelo sensor.
    :param unidade: Unidade de medida do valor (ex: ``"°C"``, ``"mm"``).
    :param timestamp: Epoch Unix do momento da leitura.
    """

    sensor_id: str
    valor: float
    unidade: str
    timestamp: float


@dataclass(frozen=True)
class Relatorio:
    """Resumo estatístico de um conjunto de medições.

    :param sensor_id: Identificador do sensor.
    :param total: Número de leituras processadas.
    :param media: Média aritmética dos valores.
    :param minimo: Menor valor registrado.
    :param maximo: Maior valor registrado.
    :param desvio_padrao: Desvio padrão amostral dos valores.
    """

    sensor_id: str
    total: int
    media: float
    minimo: float
    maximo: float
    desvio_padrao: float


# ---------------------------------------------------------------------------
# Geração de dados simulados
# ---------------------------------------------------------------------------


def gerar_medicoes(
    sensor_id: str,
    n: int,
    media: float = 25.0,
    desvio: float = 3.0,
    unidade: str = "°C",
) -> list[Medicao]:
    """Gera ``n`` medições simuladas com distribuição normal.

    :param sensor_id: Identificador do sensor.
    :param n: Quantidade de medições a gerar.
    :param media: Média da distribuição (valor esperado).
    :param desvio: Desvio padrão da distribuição.
    :param unidade: Unidade de medida dos valores gerados.
    :returns: Lista de :class:`Medicao` com timestamps crescentes.
    :raises ValueError: Se ``n`` for menor ou igual a zero.
    """
    logger.debug("[utils] DEBUG — iniciando geração de %d medições para '%s'", n, sensor_id)

    if n <= 0:
        msg = f"O número de medições deve ser positivo, recebeu: {n}"
        logger.critical("[utils] CRITICAL — parâmetro inválido: n=%d para sensor '%s'", n, sensor_id)
        raise ValueError(msg)

    agora = time.time()
    medicoes = [
        Medicao(
            sensor_id=sensor_id,
            valor=round(random.gauss(media, desvio), 2),
            unidade=unidade,
            timestamp=agora - (n - i) * 60,
        )
        for i in range(n)
    ]

    logger.info("[utils] INFO — %d medições geradas para sensor '%s'", n, sensor_id)
    return medicoes


# ---------------------------------------------------------------------------
# Validação
# ---------------------------------------------------------------------------


def validar_medicoes(medicoes: list[Medicao]) -> list[str]:
    """Identifica medições com valores fora do intervalo aceitável.

    Considera suspeita qualquer medição cujo valor absoluto seja maior que
    ``1000``. Útil para detectar leituras de sensor corrompidas ou saturadas.

    :param medicoes: Lista de medições a validar.
    :returns: Lista de ``sensor_id`` com leituras suspeitas (pode ser vazia).
    """
    logger.info("[utils] INFO — iniciando validação de %d medições", len(medicoes))
    suspeitos: list[str] = []
    for m in medicoes:
        if abs(m.valor) > 1000:
            logger.error(
                "[utils] ERROR — leitura extrema detectada: sensor=%s, valor=%f",
                m.sensor_id,
                m.valor,
            )
            suspeitos.append(m.sensor_id)
    if not suspeitos:
        logger.debug("[utils] DEBUG — nenhuma leitura suspeita encontrada")
    else:
        logger.warning("[utils] WARNING — %d sensor(es) com leituras suspeitas: %s", len(suspeitos), suspeitos)
    return suspeitos


# ---------------------------------------------------------------------------
# Cálculos estatísticos
# ---------------------------------------------------------------------------


def calcular_relatorio(medicoes: list[Medicao]) -> Relatorio:
    """Calcula estatísticas descritivas de uma lista de medições.

    :param medicoes: Lista de medições do mesmo sensor.
    :returns: :class:`Relatorio` com média, mínimo, máximo e desvio padrão.
    :raises ValueError: Se a lista estiver vazia.
    :raises ValueError: Se as medições pertencerem a mais de um sensor.
    """
    logger.debug("[utils] DEBUG — iniciando cálculo estatístico de %d medições", len(medicoes))

    if not medicoes:
        msg = "Não é possível calcular relatório de lista vazia."
        logger.critical("[utils] CRITICAL — tentativa de calcular relatório com lista vazia")
        raise ValueError(msg)

    sensor_ids = {m.sensor_id for m in medicoes}
    if len(sensor_ids) > 1:
        msg = f"Todas as medições devem ser do mesmo sensor. Encontrados: {sensor_ids}"
        logger.error("[utils] ERROR — medições de múltiplos sensores misturadas: %s", sensor_ids)
        raise ValueError(msg)

    valores = [m.valor for m in medicoes]
    n = len(valores)
    media = sum(valores) / n
    minimo = min(valores)
    maximo = max(valores)
    variancia = sum((v - media) ** 2 for v in valores) / (n - 1) if n > 1 else 0.0
    desvio_padrao = math.sqrt(variancia)

    relatorio = Relatorio(
        sensor_id=medicoes[0].sensor_id,
        total=n,
        media=round(media, 4),
        minimo=minimo,
        maximo=maximo,
        desvio_padrao=round(desvio_padrao, 4),
    )

    if desvio_padrao > 10.0:
        logger.warning(
            "[utils] WARNING — desvio padrão elevado (%.2f) para sensor '%s'",
            desvio_padrao,
            relatorio.sensor_id,
        )

    logger.info(
        "[utils] INFO — relatório calculado: sensor=%s, n=%d, media=%.2f, dp=%.2f",
        relatorio.sensor_id,
        relatorio.total,
        relatorio.media,
        relatorio.desvio_padrao,
    )
    return relatorio


# ---------------------------------------------------------------------------
# Formatação de saída
# ---------------------------------------------------------------------------


def formatar_relatorio(relatorio: Relatorio) -> str:
    """Formata um :class:`Relatorio` como texto legível para logs.

    :param relatorio: Relatório com estatísticas calculadas.
    :returns: String multilinha com o resumo formatado.
    """
    return (
        f"Sensor: {relatorio.sensor_id}\n"
        f"  Total de leituras : {relatorio.total}\n"
        f"  Média             : {relatorio.media}\n"
        f"  Mínimo            : {relatorio.minimo}\n"
        f"  Máximo            : {relatorio.maximo}\n"
        f"  Desvio padrão     : {relatorio.desvio_padrao}"
    )

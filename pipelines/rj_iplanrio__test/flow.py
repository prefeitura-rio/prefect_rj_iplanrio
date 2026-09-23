"""Flow de teste para validar o funcionamento do logger, tasks e utils.

Esta pipeline não consome dados reais. Ela simula a coleta de medições de
sensores, aplica validação e cálculo estatístico, e publica o resultado nos
logs do Prefect — servindo de referência de arquitetura para novas pipelines.
"""

from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from prefect_rj_iplanrio.log import get_logger

from tasks import (
    calcular_relatorio_task,
    gerar_medicoes_task,
    publicar_relatorio_task,
    validar_medicoes_task,
)

logger = get_logger(__name__)


@flow(log_prints=True)
def rj_iplanrio__test(
    sensor_id: str = "sensor-01",
    n_medicoes: int = 20,
    media: float = 25.0,
    desvio: float = 3.0,
    unidade: str = "°C",
) -> None:
    """Executa o pipeline de teste com dados simulados de sensor.

    Gera medições aleatórias com distribuição normal, valida os valores,
    calcula estatísticas descritivas e publica o relatório nos logs.

    :param sensor_id: Identificador do sensor a simular
        (padrão: ``"sensor-01"``).
    :param n_medicoes: Número de leituras a gerar (padrão: ``20``).
    :param media: Média da distribuição dos valores simulados (padrão: ``25.0``).
    :param desvio: Desvio padrão da distribuição (padrão: ``3.0``).
    :param unidade: Unidade de medida dos valores (padrão: ``"°C"``).
    """
    rename_current_flow_run_task(new_name=f"test--{sensor_id}")

    logger.debug(
        "[flow] DEBUG — parâmetros recebidos: sensor=%s, n=%d, media=%.1f, desvio=%.1f, unidade=%s",
        sensor_id,
        n_medicoes,
        media,
        desvio,
        unidade,
    )
    logger.info("[flow] INFO — iniciando pipeline de teste para sensor '%s'", sensor_id)
    logger.warning("[flow] WARNING — pipeline de teste em execução; não usar em produção")
    logger.error("[flow] ERROR — mensagem de nível ERROR emitida intencionalmente para validação do logger")
    logger.critical("[flow] CRITICAL — mensagem de nível CRITICAL emitida intencionalmente para validação do logger")

    medicoes = gerar_medicoes_task(
        sensor_id=sensor_id,
        n=n_medicoes,
        media=media,
        desvio=desvio,
        unidade=unidade,
    )

    suspeitos = validar_medicoes_task(medicoes=medicoes)

    if suspeitos:
        logger.warning(
            "Pipeline concluida com avisos: %d sensor(es) com leituras suspeitas.",
            len(suspeitos),
        )

    relatorio = calcular_relatorio_task(medicoes=medicoes)

    publicar_relatorio_task(relatorio=relatorio)

    logger.info("[flow] INFO — pipeline de teste finalizada com sucesso.")

rj_iplanrio__test()
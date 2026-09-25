"""Tasks da pipeline de teste.

Cada task é um wrapper fino sobre uma função de ``utils``. A única lógica
permitida aqui é a configuração de retry, cache e instrumentação do Prefect.
"""

from prefect import task

from prefect_rj_iplanrio.log import get_logger

from pipelines.rj_iplanrio__test.utils import (
    Medicao,
    Relatorio,
    calcular_relatorio,
    formatar_relatorio,
    gerar_medicoes,
    validar_medicoes,
)

logger = get_logger(__name__)


@task(name="gerar-medicoes", retries=2)
def gerar_medicoes_task(
    sensor_id: str,
    n: int,
    media: float = 25.0,
    desvio: float = 3.0,
    unidade: str = "°C",
) -> list[Medicao]:
    """Gera medições simuladas de sensor e registra o início da coleta.

    :param sensor_id: Identificador do sensor.
    :param n: Quantidade de medições a gerar.
    :param media: Média da distribuição normal dos valores.
    :param desvio: Desvio padrão da distribuição.
    :param unidade: Unidade de medida (ex: ``"°C"``, ``"mm"``).
    :returns: Lista de medições geradas.
    """
    logger.debug("[tasks] DEBUG — gerar_medicoes_task chamada: sensor=%s, n=%d", sensor_id, n)
    logger.info("[tasks] INFO — iniciando coleta de %d medições para sensor '%s'", n, sensor_id)
    medicoes = gerar_medicoes(sensor_id=sensor_id, n=n, media=media, desvio=desvio, unidade=unidade)
    logger.info("[tasks] INFO — coleta concluída: %d leituras obtidas", len(medicoes))
    return medicoes


@task(name="validar-medicoes", retries=1)
def validar_medicoes_task(medicoes: list[Medicao]) -> list[str]:
    """Valida as medições e retorna os IDs de sensores com leituras suspeitas.

    :param medicoes: Lista de medições a inspecionar.
    :returns: Lista de ``sensor_id`` com valores fora do intervalo esperado.
    """
    logger.info("[tasks] INFO — iniciando validação de %d medições", len(medicoes))
    suspeitos = validar_medicoes(medicoes)

    if suspeitos:
        logger.error(
            "[tasks] ERROR — validação reprovada: %d sensor(es) com leituras fora do intervalo: %s",
            len(suspeitos),
            suspeitos,
        )
    else:
        logger.warning("[tasks] WARNING — validação concluída sem suspeitos (verifique se os dados são reais)")

    return suspeitos


@task(name="calcular-relatorio", retries=1)
def calcular_relatorio_task(medicoes: list[Medicao]) -> Relatorio:
    """Calcula estatísticas descritivas das medições de um sensor.

    :param medicoes: Lista de medições válidas do mesmo sensor.
    :returns: :class:`Relatorio` com média, mínimo, máximo e desvio padrão.
    """
    logger.info("[tasks] INFO — calculando relatório para %d medições", len(medicoes))
    relatorio = calcular_relatorio(medicoes)
    logger.debug(
        "[tasks] DEBUG — relatório retornado: sensor=%s, media=%.2f, dp=%.2f",
        relatorio.sensor_id,
        relatorio.media,
        relatorio.desvio_padrao,
    )
    return relatorio


@task(name="publicar-relatorio")
def publicar_relatorio_task(relatorio: Relatorio) -> None:
    """Publica o relatório formatado nos logs da pipeline.

    Em produção, este passo poderia enviar o relatório para um webhook,
    BigQuery, ou sistema de alertas.

    :param relatorio: Relatório com estatísticas a publicar.
    """
    logger.critical("[tasks] CRITICAL — publicando relatório final (nível crítico de visibilidade intencional)")
    texto = formatar_relatorio(relatorio)
    logger.info("[tasks] INFO — relatório final:\n%s", texto)

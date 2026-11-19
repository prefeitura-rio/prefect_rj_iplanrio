"""Tasks do extrator do ADN e do backfill do histórico."""

from typing import Literal

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.rj_smfp__nota_carioca_adn.utils.backfill import ConfigBackfill, conferir, executar_backfill
from pipelines.rj_smfp__nota_carioca_adn.utils.extrator import executar_extracao
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ConfigExtracao, ResultadoExtracao
from pipelines.rj_smfp__nota_carioca_adn.utils.servicos import (
    ParametrosBackfill,
    ParametrosServicos,
    construir_servicos,
    construir_servicos_backfill,
)


@task(cache_policy=NO_CACHE)
def extrair_task(config: ConfigExtracao, parametros: ParametrosServicos) -> ResultadoExtracao:
    """Roda um run do extrator com as portas reais; fecha o cliente do ADN ao terminar, com sucesso ou não."""
    servicos = construir_servicos(parametros)
    try:
        return executar_extracao(config, servicos)
    finally:
        servicos.cliente.close()


@task(cache_policy=NO_CACHE)
def backfill_task(
    config: ConfigBackfill, modo: Literal["backfill", "conferir"], parametros: ParametrosBackfill
) -> dict[str, object]:
    """Roda o backfill ou a conferência com as portas reais; fecha a sessão do Oracle ao terminar.

    :returns: Resumo do que foi feito (contagens e motivo do fim), também registrado no log.
    """
    servicos = construir_servicos_backfill(parametros)
    try:
        if modo == "conferir":
            return conferir(config, servicos).resumo()
        resultado = executar_backfill(config, servicos)
        return {
            "faixa": [config.nsu_inicio, config.nsu_fim],
            "faixas": resultado.faixas,
            "linhas_lidas": resultado.linhas_lidas,
            "linhas_gravadas": resultado.linhas_gravadas,
            "linhas_puladas": resultado.linhas_puladas,
            "ultimo_nsu": resultado.ultimo_nsu,
            "motivo_fim": resultado.motivo_fim,
        }
    finally:
        servicos.fonte.close()

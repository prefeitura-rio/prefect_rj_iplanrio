"""Tasks do extrator do ADN."""

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.rj_smfp__nota_carioca_adn.utils.extrator import executar_extracao
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ConfigExtracao, ResultadoExtracao
from pipelines.rj_smfp__nota_carioca_adn.utils.servicos import ParametrosServicos, construir_servicos


@task(cache_policy=NO_CACHE)
def extrair_task(config: ConfigExtracao, parametros: ParametrosServicos) -> ResultadoExtracao:
    """Roda um run do extrator com as portas reais; fecha o cliente do ADN ao terminar, com sucesso ou não."""
    servicos = construir_servicos(parametros)
    try:
        return executar_extracao(config, servicos)
    finally:
        servicos.cliente.close()

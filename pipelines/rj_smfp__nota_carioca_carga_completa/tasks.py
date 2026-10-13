"""Tasks da carga completa: conferir se o deployment está livre, disparar e esperar, pausar."""

import time
from collections.abc import Sequence
from datetime import UTC, datetime

from prefect import task
from prefect.cache_policies import NO_CACHE

from iplanrio.pipelines_utils.logging import log
from pipelines.rj_smfp__nota_carioca_carga_completa.utils import guard, runs
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.chain import ChildRun, Step


@task(cache_policy=NO_CACHE)
def ensure_idle_task(deployment: str) -> None:
    """Recusa seguir se o deployment tiver execução ativa ou agendada para os próximos 10 minutos.

    Os deployments já se serializam sozinhos, mas um disparo manual pela UI não passa pelo bot.

    :param deployment: ``<flow>/<deployment>``.
    :raises guard.ActiveRunsError: Se houver execução que impeça, nomeando-a com o link.
    """
    blocking = guard.blocking_runs(runs.list_deployment_runs(deployment), datetime.now(UTC))
    if blocking:
        raise guard.ActiveRunsError(guard.describe_blocking(blocking, runs.run_link), blocking)
    log(f"{deployment}: nenhuma execução ativa; liberado para disparar.")


@task(cache_policy=NO_CACHE)
def run_deployment_task(step: Step, tags: Sequence[str]) -> ChildRun:
    """Dispara o deployment (padrões do deployment) como subflow e espera o término ou o tempo limite.

    :param step: Deployment e tempo limite.
    :param tags: Tags do flow run criado.
    :returns: O flow run filho no último estado lido.
    """
    return runs.run_and_wait(step.deployment, step.timeout_seconds, tags)


@task(cache_policy=NO_CACHE)
def pause_task(seconds: int) -> None:
    """Espera entre as duas cargas, para o primeiro deployment terminar de soltar seus recursos.

    :param seconds: Segundos de espera.
    """
    log(f"Aguardando {seconds} s antes do próximo disparo.")
    time.sleep(seconds)

"""Tasks da carga completa: conferir se o deployment está livre, disparar e esperar, retomar, pausar."""

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
def find_own_child_task(step: Step, parent_run_id: str | None) -> ChildRun | None:
    """O filho que este flow run já disparou no deployment do passo (antes de um reinício), ou ``None``.

    :param step: Passo da cadeia.
    :param parent_run_id: Id do flow run da carga completa.
    :returns: O filho no estado atual, ou ``None``.
    """
    child = runs.find_own_child(step.deployment, parent_run_id)
    if child is not None:
        log(f"{step.label}: este run já disparou o flow run {child.name} ({child.state_name}) antes de reiniciar.")
    return child


@task(cache_policy=NO_CACHE)
def wait_for_child_task(step: Step, child: ChildRun) -> ChildRun:
    """Espera um filho já existente terminar ou o tempo limite do passo (sem cancelar no limite).

    :param step: Passo da cadeia (tempo limite).
    :param child: Filho disparado antes do reinício.
    :returns: O filho no último estado lido.
    """
    return runs.wait_for_run(child.run_id, step.timeout_seconds)


@task(cache_policy=NO_CACHE)
def pause_task(seconds: int) -> None:
    """Espera entre as duas cargas, para o primeiro deployment terminar de soltar seus recursos.

    :param seconds: Segundos de espera.
    """
    log(f"Aguardando {seconds} s antes do próximo disparo.")
    time.sleep(seconds)

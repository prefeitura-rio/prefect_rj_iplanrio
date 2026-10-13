"""Flow for rj_smfp__nota_carioca_carga_completa.

Encadeia os dois deployments de prod da Nota Carioca: dispara Oracle → BigQuery e, só se terminar em ``Completed``,
espera ``pause_seconds`` e dispara BigQuery → Oracle. Cada filho usa os parâmetros padrão do próprio deployment (o mesmo
que um disparo rápido) e é ligado a este run como subflow. O pod só espera: a carga roda nos pods dos filhos.
Se qualquer parte não concluir, o orquestrador falha e o segundo deployment não é disparado; um filho que ainda corria
quando o tempo limite acabou NÃO é cancelado.
"""

import time

from prefect import flow
from prefect.runtime import flow_run

from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    BQ_TO_ORACLE_DEPLOYMENT,
    BQ_TO_ORACLE_LABEL,
    ORACLE_TO_BQ_DEPLOYMENT,
    ORACLE_TO_BQ_LABEL,
    SECONDS_PER_HOUR,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.tasks import ensure_idle_task, pause_task, run_deployment_task
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.chain import (
    ChainActions,
    ChainPlan,
    Step,
    build_run_tags,
    run_chain,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.notify import make_notifier
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.runs import run_link


@flow(log_prints=True)
def rj_smfp__nota_carioca_carga_completa(  # noqa: PLR0913, PLR0917
    oracle_to_bq_deployment: str = ORACLE_TO_BQ_DEPLOYMENT,
    bq_to_oracle_deployment: str = BQ_TO_ORACLE_DEPLOYMENT,
    pause_seconds: int = 30,
    oracle_to_bq_timeout_hours: int = 4,
    bq_to_oracle_timeout_hours: int = 8,
    discord_notifications: bool = True,
) -> None:
    rename_current_flow_run_task(new_name="carga-completa")
    plan = ChainPlan(
        first=Step(ORACLE_TO_BQ_LABEL, oracle_to_bq_deployment, oracle_to_bq_timeout_hours * SECONDS_PER_HOUR),
        second=Step(BQ_TO_ORACLE_LABEL, bq_to_oracle_deployment, bq_to_oracle_timeout_hours * SECONDS_PER_HOUR),
        pause_seconds=pause_seconds,
        tags=tuple(build_run_tags(flow_run.tags)),
    )
    actions = ChainActions(
        run=run_deployment_task,
        ensure_idle=ensure_idle_task,
        pause=pause_task,
        notify=make_notifier(discord_notifications),
        log=log,
        link=run_link,
        clock=time.monotonic,
    )
    run_chain(plan, actions)

"""Flow for rj_smfp__nota_carioca_carga_completa.

Encadeia os deployments da Nota Carioca: a extração XML (documentos fiscais nacionais → DFEN, antes acionada à mão),
Oracle → BigQuery e BigQuery → Oracle. Cada passo só roda se o anterior terminar em ``Completed``, depois de
``pause_seconds``. ``incluir_extracao_xml=false`` pula a extração (por exemplo, se ela já foi rodada à mão). Os padrões
dos parâmetros são os deployments de prod; o deployment de staging passa os de staging. Cada filho usa os parâmetros
padrão do próprio deployment (o mesmo que um disparo rápido) e é ligado a este run como subflow. O pod só espera: o
trabalho roda nos pods dos filhos. O deployment de prod roda todo dia às 17h (America/Sao_Paulo) e também pelo bot.
Se o pod deste orquestrador for encerrado (SIGTERM), o Prefect reenvia o mesmo flow run: ele recomeça, acha os filhos
que já disparou, não repete os passos concluídos e volta a esperar o que ficou rodando.
O ambiente (prod ou staging) vem do nome deste deployment e só escolhe o webhook e o marcador das mensagens do Discord.
Se qualquer parte não concluir, o orquestrador falha, os passos seguintes não são disparados e o alerta no Discord
menciona os cargos de ``discord_alert_role_ids``; um filho que ainda corria quando o tempo limite acabou NÃO é
cancelado.
"""

import time

from prefect import flow
from prefect.runtime import flow_run

from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from pipelines.rj_smfp__nota_carioca_carga_completa.constants import (
    BQ_TO_ORACLE_DEPLOYMENT,
    BQ_TO_ORACLE_LABEL,
    EXTRACTION_DEPLOYMENT,
    EXTRACTION_LABEL,
    ORACLE_TO_BQ_DEPLOYMENT,
    ORACLE_TO_BQ_LABEL,
    SECONDS_PER_HOUR,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.tasks import (
    ensure_idle_task,
    find_own_child_task,
    pause_task,
    run_deployment_task,
    wait_for_child_task,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.chain import (
    ChainActions,
    ChainPlan,
    Step,
    build_run_tags,
    run_chain,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.discord import current_environment
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
    extracao_xml_deployment: str = EXTRACTION_DEPLOYMENT,
    incluir_extracao_xml: bool = True,
    extracao_xml_timeout_hours: int = 9,
    discord_alert_role_ids: list[str] | None = None,
) -> None:
    rename_current_flow_run_task(new_name="carga-completa")
    extraction = (
        Step(EXTRACTION_LABEL, extracao_xml_deployment, extracao_xml_timeout_hours * SECONDS_PER_HOUR, "concluída")
        if incluir_extracao_xml
        else None
    )
    plan = ChainPlan(
        first=Step(ORACLE_TO_BQ_LABEL, oracle_to_bq_deployment, oracle_to_bq_timeout_hours * SECONDS_PER_HOUR),
        second=Step(BQ_TO_ORACLE_LABEL, bq_to_oracle_deployment, bq_to_oracle_timeout_hours * SECONDS_PER_HOUR),
        pause_seconds=pause_seconds,
        tags=tuple(build_run_tags(flow_run.tags)),
        extraction=extraction,
    )
    environment = current_environment()
    current_run_id = str(flow_run.id) if flow_run.id else None
    actions = ChainActions(
        run=run_deployment_task,
        ensure_idle=ensure_idle_task,
        pause=pause_task,
        notify=make_notifier(discord_notifications, environment),
        alert=make_notifier(discord_notifications, environment, mention_role_ids=tuple(discord_alert_role_ids or ())),
        find_own=lambda step: find_own_child_task(step, current_run_id),
        wait=wait_for_child_task,
        log=log,
        link=run_link,
        clock=time.monotonic,
    )
    run_chain(plan, actions)

"""Flow for rj_smfp__nota_carioca_paridade_f1.

Paridade F1 da Nota Carioca (tarefa 33 do plano do gateway nacional): roda o ``dbt build`` de ``tag:paridade_nc2`` (os
models que comparam o v2 com o Oracle, cortados no maior NSU já extraído) pelo deployment de dbt compartilhado, lê
``nota_carioca_v2.paridade_nc2__resumo``, publica o relatório JSON com contagens por regra e por classe (log e artefato)
e avisa no Discord. Falha se qualquer regra tiver ``divergente > 0`` ou ``comparados == 0``: tolerância zero.
O ambiente (prod ou staging) vem do nome do deployment e só escolhe o webhook e o marcador das mensagens do Discord.
"""

from datetime import UTC, datetime

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from prefect import flow
from prefect.runtime import flow_run
from prefect.settings import PREFECT_UI_URL

from pipelines.rj_smfp__nota_carioca_paridade_f1.constants import DBT_DEPLOYMENT, RESUMO_TABLE
from pipelines.rj_smfp__nota_carioca_paridade_f1.tasks import publish_report_task, read_resumo_task, run_dbt_task
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.dbt import build_dbt_parameters
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.discord import current_environment
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.notify import make_notifier
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.paridade import (
    ParidadeReport,
    ParidadeReprovadaError,
    build_report,
    parse_resumo,
)
from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.render import approved_line, failed_line, rejected_line


def _run_link() -> str:
    ui_url = PREFECT_UI_URL.value()
    return f" · <{ui_url.rstrip('/')}/runs/flow-run/{flow_run.id}>" if ui_url and flow_run.id else ""


@flow(log_prints=True)
def rj_smfp__nota_carioca_paridade_f1(  # noqa: PLR0913, PLR0917
    project: str = "rj-iplanrio-dia",
    resumo_table: str = RESUMO_TABLE,
    corte_nsu: int | None = None,
    dbt_deployment: str = DBT_DEPLOYMENT,
    dbt_select: str = "tag:paridade_nc2",
    dbt_timeout_minutes: int = 120,
    discord_notifications: bool = True,
) -> None:
    notify = make_notifier(discord_notifications, current_environment())
    try:
        report = _build_report(project, resumo_table, corte_nsu, dbt_deployment, dbt_select, dbt_timeout_minutes)
    except Exception as error:
        notify(failed_line(error) + _run_link())
        raise
    if report.resultado == "aprovado":
        notify(approved_line(report) + _run_link())
        return
    notify(rejected_line(report) + _run_link())
    raise ParidadeReprovadaError("Paridade F1 reprovada: " + "; ".join(report.motivos))


def _build_report(  # noqa: PLR0913, PLR0917
    project: str, resumo_table: str, corte_nsu: int | None, dbt_deployment: str, dbt_select: str, timeout_minutes: int
) -> ParidadeReport:
    inject_bd_credentials_task(environment="prod")
    started_at = datetime.now(UTC)
    run_dbt_task(
        deployment=dbt_deployment,
        parameters=build_dbt_parameters(dbt_select, corte_nsu),
        timeout_minutes=timeout_minutes,
    )
    rows = read_resumo_task(project=project, table=resumo_table)
    report = build_report(parse_resumo(rows), requested_corte_nsu=corte_nsu, not_before=started_at)
    publish_report_task(report)
    return report

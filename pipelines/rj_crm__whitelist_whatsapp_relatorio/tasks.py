"""Tasks of rj_crm__whitelist_whatsapp_relatorio.

Each task reads what only the run knows, the environment variables and the flow run
context, and passes the work to the helpers. No business rule lives here.

`Settings` holds the configuration that the Kubernetes Secret puts in the container.
"""

from datetime import datetime
from typing import TypedDict, cast

from prefect import task
from prefect.runtime import deployment, flow_run
from toolkit.telemetry import logger

from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers import bigquery, discord, mailman
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers import janela as janela_utils
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.configuracao import ler_destinatarios
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.janela import Janela
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.ocorrencias import (
    Ocorrencia,
    ler_ultima_ocorrencia,
    normalizar,
)
from pipelines.rj_crm__whitelist_whatsapp_relatorio.settings import Settings
from utils.utils import render_query


class QueryParam(TypedDict):
    """A structured query parameter.

    `name` is the file in `queries/` with no extension, and `replacements` are the
    values of the `$placeholder` of the template. No SQL goes through the
    `prefect.yaml`.
    """

    name: str
    replacements: dict[str, str]


FONTE_PADRAO: dict[str, str] = {
    "project": "rj-crm-registry",
    "dataset_id": "brutos_salesforce",
    "table_id": "ai_agent_interaction_step",
}
"""Fonte usada quando o flow roda fora de um deployment.

Em staging e prod estes valores vêm do ``prefect.yaml``; aqui eles só existem para que
uma execução local não precise repetir a configuração inteira na mão.
"""

QUERY_OCORRENCIAS_PADRAO: QueryParam = {"name": "get_redacted_urls", "replacements": FONTE_PADRAO}
QUERY_ULTIMA_OCORRENCIA_PADRAO: QueryParam = {"name": "get_ultima_ocorrencia", "replacements": FONTE_PADRAO}


@task
def validar_configuracao_task() -> None:
    """Check at the start of the flow that every environment variable exists.

    Checking before any send avoids the bad case: to find out that a recipient is
    missing after the Discord message has gone out. Raise `pydantic.ValidationError` if
    a required variable is missing or empty. The message names all of them at once.
    """
    Settings()
    logger.info("Configuração validada")


@task
def resolver_janela_task(start_datetime: str | None, end_datetime: str | None) -> Janela:
    """Resolve the 24 hour window of the report.

    It reads the Prefect run context and passes the computation on. `deployment.id`
    tells a scheduled run from a manual one: `flow_run.scheduled_start_time` gives the
    current time outside a schedule too, and would be a false anchor.
    """
    agendada = cast("str | None", deployment.id) is not None
    janela = janela_utils.resolver(
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        ancora_agendada=cast("datetime | None", flow_run.scheduled_start_time) if agendada else None,
        agora=janela_utils.agora_no_fuso(),
        config=janela_utils.CONFIG_PADRAO,
    )
    if janela.origem == "corte_local":
        logger.warning(
            "Execução sem agendamento: janela deduzida do último corte das %dh. Informe start_datetime e end_datetime.",
            janela_utils.CONFIG_PADRAO.hora_corte,
        )
    logger.info("Janela [%s, %s) definida por %s", janela.inicio_sql, janela.fim_sql, janela.origem)
    return janela


@task(retries=2, retry_delay_seconds=60)
def buscar_ocorrencias_task(query: QueryParam, janela: Janela) -> list[Ocorrencia]:
    """Query BigQuery and return the occurrences of the period, possibly none.

    The `replacements` of the deployment describe the source, and the window is a
    runtime value that goes on top. In that order, a `start_datetime` set by mistake in
    the `prefect.yaml` is replaced by the real window and does not become a duplicate
    argument.
    """
    replacements = {
        **query["replacements"],
        "start_datetime": janela.inicio_sql,
        "end_datetime": janela.fim_sql,
    }
    return normalizar(bigquery.baixar(query=render_query(__file__, query["name"], **replacements)))


@task
def buscar_ultima_ocorrencia_task(query: QueryParam, ocorrencias: list[Ocorrencia]) -> datetime | None:
    """Find the time of the last known occurrence when the period comes back empty.

    It queries BigQuery only on a day with no occurrence, the one day that the number
    matters, so the extra scan does not run on days that have content.

    The failure is swallowed on purpose. This query is an extra diagnostic (D37), and if
    it stopped the flow, a BigQuery outage would hide the daily message that it
    qualifies. Without it the report goes out the same, with no line of context.

    Return `None` when there was an occurrence in the period, when the source never had
    one, or when the query failed.
    """
    if ocorrencias:
        return None

    try:
        dados = bigquery.baixar(query=render_query(__file__, query["name"], **query["replacements"]))
    except Exception:
        logger.warning("Não foi possível consultar a última ocorrência conhecida", exc_info=True)
        return None

    ultima = ler_ultima_ocorrencia(dados)
    logger.info("Última ocorrência conhecida na fonte: %s", ultima or "nenhuma")
    return ultima


@task(retries=2, retry_delay_seconds=60)
def enviar_email_task(janela: Janela, ocorrencias: list[Ocorrencia]) -> list[str]:
    """Send the detailed report when there are occurrences.

    A day with no occurrence has no e-mail, and only Discord goes out (D16). Return the
    recipients that got the report, and none when nothing was sent. Raise
    `pydantic.ValidationError` if a variable of the Data Relay is not set.
    """
    if not ocorrencias:
        logger.info("Sem ocorrências no período: e-mail não será enviado")
        return []

    settings = Settings()
    destinatarios = ler_destinatarios(settings.DATA_RELAY_TO_ADDRESSES)
    mailman.enviar_relatorio(
        conexao=mailman.ConexaoDataRelay(
            url=settings.DATA_RELAY_URL,
            api_key=settings.DATA_RELAY_API_KEY.get_secret_value(),
        ),
        destinatarios=destinatarios,
        janela=janela,
        ocorrencias=ocorrencias,
        run_url=cast("str | None", flow_run.ui_url) or "",
    )
    return destinatarios


@task(retries=2, retry_delay_seconds=30)
def enviar_discord_task(
    janela: Janela,
    ocorrencias: list[Ocorrencia],
    destinatarios: list[str],
    ambiente: str,
    ultima_ocorrencia: datetime | None,
) -> None:
    """Post the daily summary to the Discord channel.

    It runs after the e-mail and gets the recipients that were really notified, so the
    message tells what happened and does not promise (D29). `ultima_ocorrencia` is shown
    on an empty day. Raise `pydantic.ValidationError` if the variable of the webhook is
    not set.
    """
    mensagem = discord.montar_mensagem(
        janela=janela,
        ocorrencias=ocorrencias,
        destinatarios=destinatarios,
        contexto=discord.Contexto(ambiente=ambiente, run_url=cast("str | None", flow_run.ui_url) or ""),
        ultima_ocorrencia=ultima_ocorrencia,
    )
    discord.enviar(webhook_url=Settings().DISCORD_WEBHOOK_URL.get_secret_value(), mensagem=mensagem)

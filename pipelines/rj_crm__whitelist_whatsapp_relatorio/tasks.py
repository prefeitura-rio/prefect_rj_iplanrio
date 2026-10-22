"""Tasks de rj_crm__whitelist_whatsapp_relatorio.

Fronteira entre o Prefect e o Python puro (§4.2 do styleguide): cada task lê o que só o
ambiente de execução sabe — variáveis de ambiente e contexto do flow run — e delega o
trabalho para ``utils``. Nenhuma regra de negócio mora aqui.

A configuração vem de ``Settings``, que lê o que o Kubernetes Secret injeta no container.
"""

from datetime import datetime
from typing import TypedDict

from prefect import task
from prefect.runtime import deployment, flow_run

from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers import bigquery, discord, mailman
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers import janela as janela_utils
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.configuracao import ler_destinatarios
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.janela import Janela
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.log import logger_da_pipeline
from pipelines.rj_crm__whitelist_whatsapp_relatorio.helpers.ocorrencias import (
    Ocorrencia,
    ler_ultima_ocorrencia,
    normalizar,
)
from pipelines.rj_crm__whitelist_whatsapp_relatorio.settings import Settings
from utils.utils import render_query

logger = logger_da_pipeline(__name__)


class QueryParam(TypedDict):
    """Parâmetro estruturado de query, no formato do §7.5 do styleguide.

    ``name`` é o arquivo em ``queries/`` sem a extensão; ``replacements`` são os valores
    dos ``$placeholder`` do template. Nenhum SQL trafega pelo ``prefect.yaml``.
    """

    name: str
    replacements: dict[str, object]


FONTE_PADRAO: dict[str, object] = {
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
    """Confere no início do flow se todas as variáveis de ambiente existem.

    Validar antes de qualquer envio evita o caso ruim: descobrir que falta destinatário
    depois de a mensagem do Discord já ter saído.

    :raises pydantic.ValidationError: Se alguma variável obrigatória estiver ausente ou
        vazia. A mensagem nomeia todas de uma vez.
    """
    Settings()
    logger.info("Configuração validada")


@task
def resolver_janela_task(start_datetime: str | None, end_datetime: str | None) -> Janela:
    """Resolve a janela de 24h do relatório.

    Lê o contexto de execução do Prefect e delega o cálculo. ``deployment.id`` é o que
    distingue execução agendada de manual: ``flow_run.scheduled_start_time`` devolve a
    hora atual mesmo fora de agendamento, e serviria de âncora enganosa.

    :param start_datetime: Início explícito, ou ``None``.
    :param end_datetime: Fim explícito, ou ``None``.
    :returns: A janela resolvida.
    """
    agendada = deployment.id is not None
    janela = janela_utils.resolver(
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        ancora_agendada=flow_run.scheduled_start_time if agendada else None,
        agora=janela_utils.agora_no_fuso(),
        config=janela_utils.CONFIG_PADRAO,
    )
    if janela.origem == "corte_local":
        logger.warning(
            "Execução sem agendamento: janela deduzida do último corte das %dh. "
            "Para um recorte exato, informe start_datetime e end_datetime.",
            janela_utils.CONFIG_PADRAO.hora_corte,
        )
    logger.info("Janela [%s, %s) definida por %s", janela.inicio_sql, janela.fim_sql, janela.origem)
    return janela


@task(retries=2, retry_delay_seconds=60)
def buscar_ocorrencias_task(query: QueryParam, janela: Janela) -> list[Ocorrencia]:
    """Consulta o BigQuery e normaliza as ocorrências do período.

    Os ``replacements`` do deployment descrevem a fonte; a janela é runtime e entra por
    cima. Nessa ordem, um ``start_datetime`` cadastrado por engano no ``prefect.yaml``
    é sobrescrito pelo recorte real em vez de virar argumento duplicado.

    :param query: Nome do arquivo ``.sql`` e valores dos ``$placeholder`` da fonte.
    :param janela: Recorte do relatório.
    :returns: Ocorrências encontradas, possivelmente vazia.
    """
    replacements = {
        **query["replacements"],
        "start_datetime": janela.inicio_sql,
        "end_datetime": janela.fim_sql,
    }
    return normalizar(bigquery.baixar(query=render_query(__file__, query["name"], **replacements)))


@task
def buscar_ultima_ocorrencia_task(query: QueryParam, ocorrencias: list[Ocorrencia]) -> datetime | None:
    """Descobre quando foi a última ocorrência conhecida, quando o período volta vazio.

    Só consulta o BigQuery em dia sem ocorrência: é o único dia em que o número importa,
    e assim a varredura extra não incide nos dias em que o relatório tem conteúdo.

    A falha é engolida de propósito. Esta consulta é um diagnóstico auxiliar (D37); se
    ela derrubasse o flow, uma indisponibilidade do BigQuery suprimiria justamente a
    mensagem diária que o diagnóstico existe para qualificar. Sem ela o relatório sai
    igual, só sem a linha de contexto.

    :param query: Nome do arquivo ``.sql`` e valores dos ``$placeholder`` da fonte.
    :param ocorrencias: Ocorrências do período.
    :returns: Instante da última ocorrência conhecida; ``None`` quando houve ocorrência
        no período, quando a fonte nunca registrou nenhuma ou quando a consulta falhou.
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
    """Envia o relatório detalhado, quando há ocorrências.

    Em dia sem ocorrência não há e-mail: só o Discord sai, conforme a decisão D16.

    :param janela: Recorte do relatório.
    :param ocorrencias: Ocorrências do período.
    :returns: Destinatários que receberam o relatório; vazio quando não houve envio.
    :raises pydantic.ValidationError: Se alguma das variáveis do Data Relay não estiver definida.
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
        run_url=flow_run.ui_url or "",
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
    """Publica o resumo diário no canal do Discord.

    Roda depois do e-mail e recebe os destinatários de fato notificados, para a mensagem
    relatar o que aconteceu em vez de prometer (decisão D29).

    :param janela: Recorte do relatório.
    :param ocorrencias: Ocorrências do período.
    :param destinatarios: Quem recebeu o e-mail detalhado.
    :param ambiente: Ambiente de execução, ``staging`` ou ``prod``.
    :param ultima_ocorrencia: Última ocorrência conhecida na fonte, mostrada em dia vazio.
    :raises pydantic.ValidationError: Se a variável do webhook não estiver definida.
    """
    mensagem = discord.montar_mensagem(
        janela=janela,
        ocorrencias=ocorrencias,
        destinatarios=destinatarios,
        contexto=discord.Contexto(ambiente=ambiente, run_url=flow_run.ui_url or ""),
        ultima_ocorrencia=ultima_ocorrencia,
    )
    discord.enviar(webhook_url=Settings().DISCORD_WEBHOOK_URL.get_secret_value(), mensagem=mensagem)

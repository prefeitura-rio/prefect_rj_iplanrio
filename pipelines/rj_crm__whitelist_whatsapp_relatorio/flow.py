"""Flow for rj_crm__whitelist_whatsapp_relatorio."""

from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__whitelist_whatsapp_relatorio.tasks import (
    buscar_ocorrencias_task,
    buscar_ultima_ocorrencia_task,
    enviar_discord_task,
    enviar_email_task,
    preparar_credenciais_task,
    resolver_janela_task,
    validar_configuracao_task,
)


@flow(log_prints=True)
def rj_crm__whitelist_whatsapp_relatorio(
    environment: str = "prod",
    start_datetime: str | None = None,
    end_datetime: str | None = None,
) -> None:
    """Relatar as URLs redigidas pelo agente de IA nas últimas 24 horas.

    O e-mail roda antes do Discord e o Discord consome o resultado dele (D29), mas a
    falha do e-mail **não** impede a publicação: ``return_state=True`` deixa o flow
    seguir com a task marcada como falha, o Discord sai em modo degradado e o flow
    termina em falha no fim (D41). Sem isso, um mailman fora do ar suprimiria justamente
    a entrega diária que confirma que a pipeline está viva.

    :param environment: Ambiente de execução — ``staging`` ou ``prod``.
    :param start_datetime: Início explícito da janela, no formato ``YYYY-MM-DD HH:MM:SS``.
        Quando omitido, a janela vem do horário agendado.
    :param end_datetime: Fim explícito da janela, no mesmo formato.
    :raises RuntimeError: Se o relatório foi publicado no Discord mas o e-mail falhou.
    """
    rename_current_flow_run_task(new_name=f"whitelist_whatsapp_relatorio--{environment}")
    validar_configuracao_task()
    preparar_credenciais_task()

    janela = resolver_janela_task(start_datetime=start_datetime, end_datetime=end_datetime)
    ocorrencias = buscar_ocorrencias_task(janela=janela)
    ultima_ocorrencia = buscar_ultima_ocorrencia_task(ocorrencias=ocorrencias)

    estado_email = enviar_email_task(janela=janela, ocorrencias=ocorrencias, return_state=True)
    email_entregue = estado_email.is_completed()

    enviar_discord_task(
        janela=janela,
        ocorrencias=ocorrencias,
        destinatarios=estado_email.result() if email_entregue else [],
        ambiente=environment,
        ultima_ocorrencia=ultima_ocorrencia,
    )

    if not email_entregue:
        raise RuntimeError("Relatório publicado no Discord, mas o envio do e-mail falhou.")

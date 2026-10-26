"""Flow for rj_crm__whitelist_whatsapp_relatorio."""

from prefect import flow

from pipelines.rj_crm__whitelist_whatsapp_relatorio.tasks import (
    QUERY_OCORRENCIAS_PADRAO,
    QUERY_ULTIMA_OCORRENCIA_PADRAO,
    QueryParam,
    buscar_ocorrencias_task,
    buscar_ultima_ocorrencia_task,
    enviar_discord_task,
    enviar_email_task,
    resolver_janela_task,
    validar_configuracao_task,
)


@flow(log_prints=True, flow_run_name="whitelist_whatsapp_relatorio--{environment}")
def rj_crm__whitelist_whatsapp_relatorio(
    environment: str = "prod",
    start_datetime: str | None = None,
    end_datetime: str | None = None,
    query_ocorrencias: QueryParam = QUERY_OCORRENCIAS_PADRAO,
    query_ultima_ocorrencia: QueryParam = QUERY_ULTIMA_OCORRENCIA_PADRAO,
) -> None:
    """Report the URLs that the AI agent redacted in the last 24 hours.

    The e-mail runs before Discord, and Discord uses its result (D29). A failed e-mail
    does not stop the post: `return_state=True` lets the flow go on with the task marked
    as failed, Discord gets the degraded message, and the flow ends as failed (D41).
    Without this, a mailman that is down would hide the daily post that shows the
    pipeline is alive.

    - `environment` is `staging` or `prod`.
    - `start_datetime` is the start of the window as `YYYY-MM-DD HH:MM:SS`. Without it
      the window follows the scheduled time.
    - `end_datetime` is the end of the window, in the same format.
    - `query_ocorrencias` is the query of the window: the `name` of the file in
      `queries/` and the `replacements` with the source, from the `prefect.yaml`.
    - `query_ultima_ocorrencia` is the diagnostic query, in the same format.

    Raise `RuntimeError` if the report went to Discord but the e-mail failed.
    """
    validar_configuracao_task()

    janela = resolver_janela_task(start_datetime=start_datetime, end_datetime=end_datetime)
    ocorrencias = buscar_ocorrencias_task(query=query_ocorrencias, janela=janela)
    ultima_ocorrencia = buscar_ultima_ocorrencia_task(query=query_ultima_ocorrencia, ocorrencias=ocorrencias)

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

from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from utils import send_discord_webhook_message, download_data_from_bigquery
from prefect import flow
import os


@flow(log_prints=True)
def rj_crm__webhook_wetalkie(
    query: str,
    test_mode: bool = False,
):

    rename_flow_run = rename_current_flow_run_task(new_name=f"rj_crm__webhook_wetalkie")
    crd = inject_bd_credentials_task(environment="prod")
    disc_webhook_url = os.getenv("discord_wetalkie_notifications_hook")

    data = download_data_from_bigquery(
        query=query, billing_project_id="rj-iplanrio", bucket_name="rj-iplanrio"
    )
    data_json = data.to_dict("records")

    if len(data) > 0:
        for i in range(0, len(data_json)):
            fmt_msg = """⚠️ Atenção:
O template {content_message_template_name} teve a sua categoria atualizada de {content_previous_category} para
{content_new_category}""".format(
                content_message_template_name=data_json[i][
                    "content_message_template_name"
                ],
                content_previous_category=data_json[i]["content_previous_category"],
                content_new_category=data_json[i]["content_new_category"],
            )

            if not test_mode:
                send_discord_webhook_message(
                    webhook_url=disc_webhook_url, message=fmt_msg
                )
            else:
                log("Execução de Teste:")
                log(fmt_msg)
        return

    log("Nenhum alerta encontrado. Finalizando.")
    return

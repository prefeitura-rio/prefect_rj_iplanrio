# -*- coding: utf-8 -*-
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from utils import send_discord_webhook_message, download_data_from_bigquery
from prefect import flow
import os


@flow(log_prints=True)
def rj_crm__webhook_wetalkie_template_quality_update(
    query: str,
    test_mode: bool = False,
):

    rename_flow_run = rename_current_flow_run_task(new_name=f"rj_crm__webhook_wetalkie_template_quality_update")
    crd = inject_bd_credentials_task(environment="prod")
    disc_webhook_url = os.getenv("discord_wetalkie_notifications_hook")

    data = download_data_from_bigquery(
        query=query, billing_project_id="rj-iplanrio", bucket_name="rj-iplanrio"
    )
    data_json = data.to_dict("records")

    if len(data) > 0:
        quality_map = {
            "GREEN": "🟢 Alta qualidade",
            "YELLOW": "🟡 Média qualidade",
            "RED": "🔴 Baixa qualidade",
            "UNKNOWN": "❓ Desconhecido",
        }

        for row in data_json:
            content_message_template_name = row["content_message_template_name"]
            
            raw_prev_score = row.get("content_previous_quality_score", "UNKNOWN")
            raw_new_score = row.get("content_new_quality_score", "UNKNOWN")

            content_previous_quality_score = quality_map.get(raw_prev_score, "❓ Desconhecido")
            content_new_quality_score = quality_map.get(raw_new_score, "❓ Desconhecido")

            fmt_msg = f"⚠️ Atenção:\nO template **`{content_message_template_name}`** teve a sua qualidade atualizada de **`{content_previous_quality_score}`** para **`{content_new_quality_score}`**"

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

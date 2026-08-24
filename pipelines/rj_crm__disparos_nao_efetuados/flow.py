# -*- coding: utf-8 -*-
# flake8: noqa:E501
# pylint: disable='line-too-long'

"""
Flow que verifica campanhas ativas sem disparo no dia corrente,
salva o resultado no BigQuery e notifica via Discord no canal de falhas.
"""
import os
from datetime import datetime
from pathlib import Path

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.env import inject_bd_credentials_task  # pylint: disable=E0611, E0401
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task  # pylint: disable=E0611, E0401
from prefect import flow  # pylint: disable=E0611, E0401
from prefect.client.schemas.objects import Flow, FlowRun, State  # pylint: disable=E0611, E0401
from pytz import timezone

from pipelines.rj_crm__disparo_template.constants import TemplateConstants  # pylint: disable=E0611, E0401
from pipelines.rj_crm__disparo_template.utils.discord import (  # pylint: disable=E0611, E0401
    send_discord_notification,
)
from pipelines.rj_crm__disparo_template.utils.tasks import (  # pylint: disable=E0611, E0401
    create_date_partitions,
    task_download_data_from_bigquery,
)

QUERIES_DIR = Path(__file__).parent / "queries"

DATASET_ID = "brutos_salesforce"
TABLE_ID = "campanhas_sem_disparo"
DUMP_MODE = "append"


def send_discord_notification_on_failure(flow: Flow, flow_run: FlowRun, state: State):
    """
    Sends a Discord notification when a flow run fails.
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_ERRORS")
    if not webhook_url:
        print("DISCORD_WEBHOOK_URL_ERRORS environment variable not set on Infisical. Cannot send notification.")
        return

    message = (
        "<@821121576455634955> <@1458456241683824744> <@302518123066556426>\n"
        "Prefect flow de verificação de disparos não efetuados falhou! 🚨\n"
        f"Flow: {flow.name}\n"
        f"Flow Run: {flow_run.name}\n"
        f"State: {state.name}"
    )
    send_discord_notification(webhook_url, message)


@flow(log_prints=True, on_failure=[send_discord_notification_on_failure])
def rj_crm__disparos_nao_efetuados(
    dataset_id: str = DATASET_ID,
    table_id: str = TABLE_ID,
    dump_mode: str = DUMP_MODE,
):
    """
    Verifica diariamente campanhas ativas que não tiveram disparo no dia corrente.
    - Salva o resultado particionado no BigQuery (sempre, para histórico).
    - Envia alerta no canal de falhas do Discord quando há campanhas sem disparo.
    """

    billing_project_id = TemplateConstants.BILLING_PROJECT_ID.value

    rename_current_flow_run_task(new_name=f"{dataset_id}__{table_id}")
    inject_bd_credentials_task(environment="prod")

    query = (QUERIES_DIR / "campanhas_sem_disparo.sql").read_text(encoding="utf-8")

    df = task_download_data_from_bigquery(
        query=query,
        billing_project_id=billing_project_id,
        bucket_name=billing_project_id,
    )

    if df is None or df.empty:
        print("Todas as campanhas ativas tiveram disparo hoje. Nenhuma notificação necessária.")
        return

    # Notifica no canal de falhas
    hoje = datetime.now(timezone("America/Sao_Paulo")).strftime("%d/%m/%Y")
    linhas = "\n".join(
        f"• `{row['campanha_nome']}` — {row['nome_campanha_limpo']}"
        for _, row in df.iterrows()
    )
    message = (
        f"⚠️ **Campanhas ativas SEM disparo hoje ({hoje}):**\n\n"
        f"{linhas}\n\n"
        f"Total: **{len(df)}** campanha(s) sem disparo."
    )
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_ERRORS")
    send_discord_notification(webhook_url, message)

    # Salva resultado particionado no BigQuery para histórico
    partitions_path = create_date_partitions(
        dataframe=df,
        partition_column=None,  # usa data_particao = hoje
        file_format="csv",
        root_folder="./data_campanhas_sem_disparo/",
    )

    if not partitions_path:
        raise ValueError("partitions_path is None - partition creation failed")

    if not os.path.exists(partitions_path):
        raise ValueError(f"partitions_path does not exist: {partitions_path}")

    print(f"Generated partitions_path: {partitions_path}")
    files_in_path = []
    for root, dirs, files in os.walk(partitions_path):  # pylint: disable=unused-variable
        files_in_path.extend([os.path.join(root, f) for f in files])
    print(f"Files in partitions path: {files_in_path}")

    create_table_and_upload_to_gcs_task(
        data_path=partitions_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=False,
    )

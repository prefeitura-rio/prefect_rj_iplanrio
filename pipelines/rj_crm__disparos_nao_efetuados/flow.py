"""
Flow que verifica campanhas ativas sem disparo no dia corrente,
salva o resultado no BigQuery e notifica via Discord no canal de falhas.
"""
import os
from datetime import datetime
from pathlib import Path

from prefect import flow
from prefect.client.schemas.objects import Flow, FlowRun, State
from pytz import timezone

from pipelines.rj_crm__disparos_nao_efetuados.helpers import create_date_partitions
from utils.tasks import notify_discord, publish_table_in_bq, read_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options

QUERIES_DIR = Path(__file__).parent / "queries"

BILLING_PROJECT_ID = "rj-crm-registry"
DATASET_ID = "brutos_salesforce"
TABLE_ID = "campanhas_sem_disparo"
DUMP_MODE = "append"


async def send_discord_notification_on_failure(flow: Flow, flow_run: FlowRun, state: State):
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
    await notify_discord.fn(message, webhook=webhook_url)


@flow(log_prints=True, flow_run_name="{dataset_id}__{table_id}", on_failure=[send_discord_notification_on_failure])
async def rj_crm__disparos_nao_efetuados(
    *,
    cloud: Cloud,
    dataset_id: str = DATASET_ID,
    table_id: str = TABLE_ID,
    dump_mode: str = DUMP_MODE,
):
    """
    Verifica diariamente campanhas ativas que não tiveram disparo no dia corrente.
    - Salva o resultado particionado no BigQuery (sempre, para histórico).
    - Envia alerta no canal de falhas do Discord quando há campanhas sem disparo.
    """

    query = (QUERIES_DIR / "campanhas_sem_disparo.sql").read_text(encoding="utf-8")

    df = (await read_bq(query, project=BILLING_PROJECT_ID)).to_pandas()

    hoje = datetime.now(timezone("America/Sao_Paulo")).strftime("%d/%m/%Y")

    if df is None or df.empty:
        print("Todas as campanhas ativas tiveram disparo hoje.")
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL_DISPAROS")
        if webhook_url:
            await notify_discord(
                f"<@821121576455634955> <@1458456241683824744> <@302518123066556426> <@1524151359765479475>\n✅ **Todas as campanhas ativas tiveram disparo hoje ({hoje}).**",
                webhook=webhook_url,
            )
        return

    # Notifica no canal de falhas
    linhas = "\n".join(
        f"• `{row['nome_campanha']} - {row['nome_hsm']}`"
        for _, row in df.iterrows()
    )
    message = (
        "<@821121576455634955> <@1458456241683824744> <@302518123066556426> <@1524151359765479475>\n"
        f"⚠️ **Campanhas ativas SEM disparo hoje ({hoje}):**\n\n"
        f"{linhas}\n\n"
        f"Total: **{len(df)}** campanha(s) sem disparo."
    )
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL_ERRORS")
    if webhook_url:
        await notify_discord(message, webhook=webhook_url)

    # Salva resultado particionado no BigQuery para histórico
    partitions_path = create_date_partitions(
        dataframe=df,
        partition_column="data_particao",
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

    table = Table(dataset_id=dataset_id, table_id=table_id)
    await upload_files_to_gcs(
        Path(partitions_path),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="csv",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)

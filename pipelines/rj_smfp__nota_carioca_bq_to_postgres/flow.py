"""Flow for rj_smfp__nota_carioca_bq_to_postgres.

Carrega no Postgres (slots A/B de ``exigiveis_a``/``exigiveis_b``) as tabelas exigíveis que o ``bq_to_oracle``
exportou para o GCS e preservou (``preservar_export_para_postgres``): lê o manifesto, carrega o slot inativo com
``COPY``, confere as contagens, publica as views com ``publicar_exigiveis`` e só então apaga o export. Se algo falhar,
as views continuam no slot anterior e o export fica para uma nova tentativa. O ambiente (prod ou staging) vem do nome do
deployment (``--staging``) e escolhe o webhook e o marcador das mensagens do Discord; ``ambiente`` o força.
"""

from typing import Literal

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow
from prefect.runtime import flow_run

from pipelines.rj_smfp__nota_carioca_bq_to_postgres.constants import DEFAULT_PROJECT, PG_DSN_ENV
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.tasks import (
    delete_export_task,
    load_and_publish_task,
    read_manifest_task,
)
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import manifest as manifest_module
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils import postgres
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.discord import Environment, current_environment
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.notify import NotifierConfig, PublishNotifier, Step
from pipelines.rj_smfp__nota_carioca_bq_to_postgres.utils.publication import required_exports


@flow(log_prints=True)
def rj_smfp__nota_carioca_bq_to_postgres(
    manifesto_uri: str,
    ambiente: Literal["staging", "prod"] | None = None,
    project: str = DEFAULT_PROJECT,
    pg_dsn_env: str = PG_DSN_ENV,
    discord_notifications: bool = True,
) -> None:
    location = manifest_module.parse_manifest_uri(manifesto_uri)
    rename_current_flow_run_task(new_name=location.dataset_id)
    inject_bd_credentials_task(environment="prod")
    notifier = PublishNotifier.create(
        NotifierConfig(
            dataset_id=location.dataset_id,
            run_id=str(flow_run.id),
            run_name=location.dataset_id,
            enabled=discord_notifications,
            environment=Environment(ambiente) if ambiente else current_environment(),
        )
    )
    with notifier.guard():
        notifier.begin()
        postgres.read_dsn(pg_dsn_env)
        manifest = read_manifest_task(manifesto_uri=manifesto_uri, project=project)
        notifier.set_tables({table.bq_name: export.rows for table, export in required_exports(manifest).items()})
        load_and_publish_task(manifesto_uri=manifesto_uri, manifest=manifest, project=project, pg_dsn_env=pg_dsn_env)
        notifier.enter(Step.CLEANUP)
        delete_export_task(manifesto_uri=manifesto_uri, manifest=manifest, project=project)

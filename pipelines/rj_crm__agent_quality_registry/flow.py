"""Orquestre a ingestão diária de artefatos de qualidade Agentforce."""

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__agent_quality_registry.constants import BQ_DATASET_ID, BQ_PROJECT_ID
from pipelines.rj_crm__agent_quality_registry.tasks import (
    discover_artifacts_task,
    ensure_tables_task,
    load_artifacts_task,
)


@flow(log_prints=True)
def rj_crm__agent_quality_registry(
    project_id: str = BQ_PROJECT_ID,
    dataset_id: str = BQ_DATASET_ID,
    environment: str = "prod",
) -> None:
    """Ingira artefatos de qualidade do GitLab Registry no BigQuery.

    :param project_id: Identificador do projeto Google Cloud de destino.
    :param dataset_id: Identificador do dataset BigQuery de destino.
    :param environment: Ambiente cujas credenciais devem ser injetadas.
    """
    rename_current_flow_run_task(new_name="agent-quality-registry-daily")
    credentials = inject_bd_credentials_task(environment=environment)
    tables_ready = ensure_tables_task(
        project_id=project_id,
        dataset_id=dataset_id,
        environment=environment,
        wait_for=[credentials],
    )
    artifacts = discover_artifacts_task()
    load_artifacts_task(
        artifacts=artifacts,
        project_id=project_id,
        dataset_id=dataset_id,
        environment=environment,
        wait_for=[tables_ready],
    )

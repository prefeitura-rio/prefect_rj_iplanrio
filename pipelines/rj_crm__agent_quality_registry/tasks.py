"""Exponha operações da ingestão como tasks do Prefect."""

from prefect import task

from pipelines.rj_crm__agent_quality_registry.utils import ingestion
from pipelines.rj_crm__agent_quality_registry.utils.schemas import Artifact, IngestionConfig
from prefect_rj_iplanrio.sql import load_query


@task(retries=3, retry_delay_seconds=[30, 60, 120])
def discover_artifacts_task() -> list[Artifact]:
    """Descubra e baixe os artefatos publicados no GitLab Registry."""
    return ingestion.discover_artifacts()


@task
def ensure_tables_task(project_id: str, dataset_id: str, environment: str) -> None:
    """Garanta a existência das tabelas de destino no BigQuery."""
    ingestion.ensure_tables(
        config=IngestionConfig(
            project_id=project_id,
            dataset_id=dataset_id,
            environment=environment,
        )
    )


@task
def load_artifacts_task(
    artifacts: list[Artifact],
    project_id: str,
    dataset_id: str,
    environment: str,
) -> dict[str, int]:
    """Persista os artefatos descobertos e seus detalhes no BigQuery."""
    merge_template = load_query(
        __file__,
        "upsert_rows",
        target="$target",
        staging="$staging",
        key="$key",
        updates="$updates",
        inserts="$inserts",
        values="$values",
    )
    return ingestion.load_artifacts(
        artifacts=artifacts,
        config=IngestionConfig(
            project_id=project_id,
            dataset_id=dataset_id,
            environment=environment,
        ),
        merge_template=merge_template,
    )

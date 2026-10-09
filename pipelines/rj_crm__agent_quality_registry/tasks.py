"""Exponha operações da ingestão como tasks do Prefect."""

from typing import Any

from prefect import task

from pipelines.rj_crm__agent_quality_registry.utils import gitlab, ingestion
from pipelines.rj_crm__agent_quality_registry.utils.schemas import IngestionConfig, RegistryFile
from prefect_rj_iplanrio.sql import load_query


@task(retries=3, retry_delay_seconds=[30, 60, 120])
def discover_artifacts_task() -> list[RegistryFile]:
    """Descubra os arquivos publicados sem baixá-los antecipadamente."""
    return gitlab.discover_registry_files()


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
    artifacts: list[RegistryFile],
    project_id: str,
    dataset_id: str,
    environment: str,
    full_refresh: bool = False,
) -> dict[str, Any]:
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
    return ingestion.load_registry_files(
        registry_files=artifacts,
        config=IngestionConfig(
            project_id=project_id,
            dataset_id=dataset_id,
            environment=environment,
            full_refresh=full_refresh,
        ),
        merge_template=merge_template,
    )

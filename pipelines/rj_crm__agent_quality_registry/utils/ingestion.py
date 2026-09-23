"""Coordene a leitura, normalização e persistência dos artefatos."""

from datetime import datetime, timezone

import requests

from pipelines.rj_crm__agent_quality_registry.constants import (
    AGGREGATE_FIELDS,
    BQ_DETAIL_TABLE,
    BQ_PROD_TABLE,
    BQ_QA_TABLE,
    DETAIL_FIELDS,
)
from pipelines.rj_crm__agent_quality_registry.utils import bigquery, grid, parser
from pipelines.rj_crm__agent_quality_registry.utils.schemas import Artifact, IngestionConfig, TableSpec
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def table_specs() -> tuple[TableSpec, TableSpec, TableSpec]:
    """Monte as definições das três tabelas de destino.

    :returns: Tabelas de QA, baseline de produção e detalhes de testes.
    """
    return (
        TableSpec(BQ_QA_TABLE, AGGREGATE_FIELDS, "tested_at", "release_key"),
        TableSpec(BQ_PROD_TABLE, AGGREGATE_FIELDS, "promoted_at", "release_key"),
        TableSpec(BQ_DETAIL_TABLE, DETAIL_FIELDS, "tested_at", "result_key"),
    )


def ensure_tables(config: IngestionConfig) -> None:
    """Garanta que todas as tabelas de destino existam.

    :param config: Configuração de destino da ingestão.
    """
    client = bigquery.get_client(config)
    for table in table_specs():
        bigquery.ensure_table(client, config, table)


def load_artifacts(
    artifacts: list[Artifact],
    config: IngestionConfig,
    merge_template: str,
) -> dict[str, int]:
    """Normalize e persista artefatos e detalhes no BigQuery.

    :param artifacts: Pares de metadados do Registry e conteúdo do artefato.
    :param config: Configuração de destino da ingestão.
    :param merge_template: Template SQL usado para o upsert no BigQuery.
    :returns: Contagem de registros persistidos por tipo.
    :raises requests.HTTPError: Se o enriquecimento obrigatório do Grid responder com erro HTTP.
    :raises ValueError: Se algum artefato tiver schema não suportado.
    """
    client = bigquery.get_client(config)
    qa_table, prod_table, detail_table = table_specs()
    grid_client = grid.create_grid_client()
    loaded = {"qa_versions": 0, "prod_baselines": 0, "details": 0, "grid_enriched": 0}
    for registry_file, artifact in artifacts:
        aggregate, details = parser.parse_artifact(
            registry_file=registry_file,
            artifact=artifact,
            ingested_at=datetime.now(timezone.utc),
        )
        enrich_grid_details(details, grid_client, loaded)
        aggregate_table = qa_table if registry_file.environment == "qa" else prod_table
        bigquery.upsert_rows(client, config, aggregate_table, [aggregate], merge_template)
        bigquery.upsert_rows(client, config, detail_table, details, merge_template)
        loaded["qa_versions" if registry_file.environment == "qa" else "prod_baselines"] += 1
        loaded["details"] += len(details)
    logger.info("Ingestão de qualidade concluída: %s", loaded)
    return loaded


def enrich_grid_details(
    details: list[dict[str, object]],
    grid_client: grid.TestGridClient,
    loaded: dict[str, int],
) -> None:
    """Enriqueça detalhes de suites com IDs de workbook e planilha.

    :param details: Linhas detalhadas já normalizadas.
    :param grid_client: Cliente opcional do Salesforce Test Grid.
    :param loaded: Contadores acumulados da ingestão.
    """
    for detail in details:
        run_id = detail.get("grid_run_id")
        if detail.get("test_source") != "test_suite" or not isinstance(run_id, str):
            continue
        suite_name = detail.get("suite_name")
        if not isinstance(suite_name, str):
            continue
        try:
            enrichment = grid_client.enrich(suite_name, run_id)
        except requests.RequestException as error:
            logger.warning("Enriquecimento Grid falhou para run_id=%s: %s", run_id, error)
            continue
        if enrichment and enrichment.get("enrichment_status") == "SUCCESS":
            detail["grid_workbook_id"] = enrichment["workbook_id"]
            detail["grid_worksheet_id"] = enrichment["worksheet_id"]
            loaded["grid_enriched"] += 1

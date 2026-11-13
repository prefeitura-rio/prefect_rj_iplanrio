"""Coordene a leitura, normalização e persistência dos artefatos."""

from datetime import datetime, timezone
from typing import Any

import requests

from pipelines.rj_crm__agent_quality_registry.constants import (
    AGGREGATE_FIELDS,
    BQ_DETAIL_TABLE,
    BQ_PROD_TABLE,
    BQ_QA_TABLE,
    BQ_REJECTED_TABLE,
    DETAIL_FIELDS,
    REJECTED_FIELDS,
)
from pipelines.rj_crm__agent_quality_registry.utils import bigquery, grid, notifications, parser
from pipelines.rj_crm__agent_quality_registry.utils.gitlab import create_gitlab_client
from pipelines.rj_crm__agent_quality_registry.utils.schemas import (
    Artifact,
    IngestionConfig,
    RegistryFile,
    TableSpec,
)
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def table_specs() -> tuple[TableSpec, TableSpec, TableSpec, TableSpec]:
    """Monte as definições das tabelas de destino e rejeitados."""
    aggregate_clustering = ("agent_version_number", "environment_scope", "release_key")
    return (
        TableSpec(BQ_QA_TABLE, AGGREGATE_FIELDS, "tested_at", "release_key", aggregate_clustering),
        TableSpec(BQ_PROD_TABLE, AGGREGATE_FIELDS, "promoted_at", "release_key", aggregate_clustering),
        TableSpec(
            BQ_DETAIL_TABLE,
            DETAIL_FIELDS,
            "tested_at",
            "result_key",
            ("agent_version_number", "environment_scope", "release_key"),
        ),
        TableSpec(
            BQ_REJECTED_TABLE,
            REJECTED_FIELDS,
            "failed_at",
            "rejection_key",
            ("environment_scope", "stage", "package_file_id"),
        ),
    )


def ensure_tables(config: IngestionConfig) -> None:
    """Valide o dataset e garanta que todas as tabelas existam."""
    client = bigquery.get_client(config)
    bigquery.ensure_dataset(client, config)
    for table in table_specs():
        bigquery.ensure_table(client, config, table)


def _empty_summary(discovered: int) -> dict[str, Any]:
    return {
        "status": "SUCCESS",
        "discovered": discovered,
        "processed": 0,
        "skipped": 0,
        "rejected": 0,
        "rejection_persist_failures": 0,
        "qa_versions": 0,
        "prod_baselines": 0,
        "details": 0,
        "grid_requests": 0,
        "grid_enriched": 0,
        "grid_recovered": 0,
        "grid_failures": 0,
        "grid_initialization_failures": 0,
    }


def _rejection_row(
    registry_file: RegistryFile,
    stage: str,
    error: Exception,
    retryable: bool,
) -> dict[str, Any]:
    return {
        "rejection_key": parser.create_key(
            registry_file.package_file_id,
            stage,
            type(error).__name__,
        ),
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "environment_scope": registry_file.environment,
        "registry_package_name": registry_file.package_name,
        "registry_package_version": registry_file.package_version,
        "package_id": registry_file.package_id,
        "package_file_id": registry_file.package_file_id,
        "artifact_sha256": registry_file.file_sha256,
        "stage": stage,
        "error_type": type(error).__name__,
        "error_message": str(error)[:8000],
        "retryable": retryable,
    }


def _persist_rejection(  # noqa: PLR0913 - contexto completo é necessário para dead-letter
    client: Any,
    config: IngestionConfig,
    rejected_table: TableSpec,
    registry_file: RegistryFile,
    stage: str,
    error: Exception,
    retryable: bool,
    merge_template: str,
    summary: dict[str, Any],
) -> None:
    summary["rejected"] += 1
    summary["status"] = "PARTIAL"
    row = _rejection_row(registry_file, stage, error, retryable)
    logger.error(
        "Artefato rejeitado package_file_id=%s stage=%s error_type=%s error=%s",
        registry_file.package_file_id,
        stage,
        type(error).__name__,
        error,
    )
    try:
        bigquery.upsert_rows(client, config, rejected_table, [row], merge_template)
    except Exception as persistence_error:
        summary["rejection_persist_failures"] += 1
        logger.exception(
            "Falha ao persistir rejeição package_file_id=%s: %s",
            registry_file.package_file_id,
            persistence_error,
        )


def _json_or_none(value: Any) -> str | None:
    return parser.serialize_json(value) if value is not None else None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return parser.serialize_json(value)
    return str(value)


def _number_or_none(value: Any, cast: type[int] | type[float]) -> int | float | None:
    if value is None or value == "":
        return None
    try:
        return cast(value)
    except (TypeError, ValueError):
        return None


def _matching_grid_row(
    detail: dict[str, Any],
    normalized_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    case_number = _string_or_none(detail.get("case_number"))
    assertion = _string_or_none(detail.get("assertion"))
    for row in normalized_rows:
        if (
            _string_or_none(row.get("case_number")) == case_number
            and _string_or_none(row.get("assertion")) == assertion
        ):
            return row
    return None


def _grid_detail(
    aggregate: dict[str, Any],
    suite: dict[str, str | None],
    enrichment: dict[str, Any],
    row: dict[str, Any],
    row_index: int,
) -> dict[str, Any]:
    row_identity = row.get("worksheet_row_id") or row.get("case_number") or row.get("assertion") or row_index
    return {
        "result_key": parser.create_key(
            aggregate["release_key"],
            "test_suite_grid",
            suite["suite_name"],
            suite["run_id"],
            row_identity,
            row_index,
        ),
        "release_key": aggregate["release_key"],
        "environment_scope": aggregate["environment_scope"],
        "test_source": "test_suite",
        "tested_at": aggregate["tested_at"],
        "agent_version_number": aggregate["agent_version_number"],
        "grid_run_id": suite["run_id"],
        "grid_workbook_id": enrichment.get("workbook_id"),
        "grid_worksheet_id": enrichment.get("worksheet_id"),
        "grid_enrichment_status": enrichment.get("enrichment_status"),
        "grid_run_status_json": _json_or_none(enrichment.get("run_status")),
        "grid_worksheet_data_json": _json_or_none(row),
        "result_origin": "grid",
        "suite_name": suite["suite_name"],
        "runtime_suite_name": suite.get("runtime_suite_name"),
        "case_number": _string_or_none(row.get("case_number")),
        "worksheet_row_id": _string_or_none(row.get("worksheet_row_id")),
        "assertion": _string_or_none(row.get("assertion")),
        "scenario_id": None,
        "service_name": _string_or_none(row.get("service_name")),
        "turn_number": None,
        "status": _string_or_none(row.get("status")),
        "score": _number_or_none(row.get("score"), float),
        "latency_ms": _number_or_none(row.get("latency_ms"), int),
        "expected_value": _string_or_none(row.get("expected_value")),
        "actual_value": _string_or_none(row.get("actual_value")),
        "message": _string_or_none(row.get("message")),
        "judge_provider": None,
        "judge_pass": None,
        "judge_rationale": None,
        "safety_severity": None,
        "safety_type": None,
        "utterance": _string_or_none(row.get("utterance")),
        "response": _string_or_none(row.get("response")),
    }


def enrich_grid_details(  # noqa: PLR0913 - enriquecimento compartilha resumo e cache do lote
    details: list[dict[str, Any]],
    suite_runs: list[dict[str, str | None]],
    aggregate: dict[str, Any],
    grid_client: grid.TestGridClient,
    summary: dict[str, Any],
    cache: dict[tuple[str, str], dict[str, Any] | None],
) -> None:
    """Enriqueça uma vez por suite/run e recupere detalhes ausentes pelo Grid."""
    if not grid_client.enabled:
        return
    for suite in suite_runs:
        suite_name = suite.get("suite_name")
        run_id = suite.get("run_id")
        if not suite_name or not run_id:
            continue
        cache_key = (suite_name, run_id)
        if cache_key not in cache:
            summary["grid_requests"] += 1
            try:
                cache[cache_key] = grid_client.enrich(suite_name, run_id)
            except (requests.RequestException, RuntimeError, ValueError) as error:
                summary["grid_failures"] += 1
                cache[cache_key] = {
                    "enrichment_status": "ERROR",
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                }
                logger.warning("Enriquecimento Grid falhou para run_id=%s: %s", run_id, error)
        enrichment = cache[cache_key]
        if not enrichment:
            continue

        related = [
            detail
            for detail in details
            if detail.get("test_source") == "test_suite"
            and detail.get("suite_name") == suite_name
            and detail.get("grid_run_id") == run_id
        ]
        normalized_rows = [
            grid_client.normalize_worksheet_row(row)
            for row in grid_client.worksheet_rows(enrichment.get("worksheet_data"))
        ]
        for detail in related:
            detail["grid_enrichment_status"] = enrichment.get("enrichment_status")
            detail["grid_run_status_json"] = _json_or_none(enrichment.get("run_status"))
            if enrichment.get("enrichment_status") != "SUCCESS":
                continue
            detail["grid_workbook_id"] = enrichment.get("workbook_id")
            detail["grid_worksheet_id"] = enrichment.get("worksheet_id")
            detail["result_origin"] = "artifact+grid"
            matching_row = _matching_grid_row(detail, normalized_rows)
            if matching_row:
                detail["worksheet_row_id"] = _string_or_none(matching_row.get("worksheet_row_id"))
                detail["grid_worksheet_data_json"] = _json_or_none(matching_row)
            summary["grid_enriched"] += 1

        if not related and enrichment.get("enrichment_status") == "SUCCESS":
            recovered = [
                _grid_detail(aggregate, suite, enrichment, row, index) for index, row in enumerate(normalized_rows)
            ]
            details.extend(recovered)
            summary["grid_recovered"] += len(recovered)


def _safe_grid_client(summary: dict[str, Any]) -> grid.TestGridClient:
    try:
        return grid.create_grid_client()
    except (requests.RequestException, RuntimeError, ValueError) as error:
        summary["grid_initialization_failures"] += 1
        summary["grid_failures"] += 1
        logger.warning("Test Grid desabilitado após falha de inicialização: %s", error)
        return grid.create_disabled_grid_client()


def load_registry_files(  # noqa: PLR0915 - orquestra etapas isoladas e contabiliza o lote
    registry_files: list[RegistryFile],
    config: IngestionConfig,
    merge_template: str,
) -> dict[str, Any]:
    """Baixe e carregue arquivos isoladamente, com incrementalidade e dead-letter."""
    client = bigquery.get_client(config)
    qa_table, prod_table, detail_table, rejected_table = table_specs()
    processed_ids = (
        set()
        if config.full_refresh
        else bigquery.get_processed_package_file_ids(client, config, (qa_table, prod_table))
    )
    summary = _empty_summary(len(registry_files))
    grid_client = _safe_grid_client(summary)
    grid_cache: dict[tuple[str, str], dict[str, Any] | None] = {}
    seen_ids: set[int] = set()

    try:
        gitlab_client = create_gitlab_client()
    except Exception as error:
        for registry_file in registry_files:
            if registry_file.package_file_id in processed_ids:
                summary["skipped"] += 1
                continue
            _persist_rejection(
                client,
                config,
                rejected_table,
                registry_file,
                "gitlab_client",
                error,
                False,
                merge_template,
                summary,
            )
        logger.error("Cliente GitLab não pôde ser inicializado: %s", error)
        notifications.notify_partial_load(summary)
        return summary

    for registry_file in registry_files:
        if registry_file.package_file_id in seen_ids or registry_file.package_file_id in processed_ids:
            summary["skipped"] += 1
            continue
        seen_ids.add(registry_file.package_file_id)

        try:
            artifact = gitlab_client.download(registry_file)
        except Exception as error:
            _persist_rejection(
                client,
                config,
                rejected_table,
                registry_file,
                "download",
                error,
                isinstance(error, requests.RequestException),
                merge_template,
                summary,
            )
            continue

        try:
            aggregate, details = parser.parse_artifact(
                registry_file=registry_file,
                artifact=artifact,
                ingested_at=datetime.now(timezone.utc),
            )
            suite_runs = parser.extract_suite_runs(artifact)
        except Exception as error:
            _persist_rejection(
                client,
                config,
                rejected_table,
                registry_file,
                "parse",
                error,
                False,
                merge_template,
                summary,
            )
            continue

        try:
            enrich_grid_details(details, suite_runs, aggregate, grid_client, summary, grid_cache)
        except Exception as error:
            summary["grid_failures"] += 1
            summary["status"] = "PARTIAL"
            logger.exception(
                "Enriquecimento Grid ignorado para package_file_id=%s: %s",
                registry_file.package_file_id,
                error,
            )
        aggregate_table = qa_table if registry_file.environment == "qa" else prod_table
        try:
            _, detail_count = bigquery.upsert_artifact(
                client,
                config,
                aggregate_table,
                detail_table,
                aggregate,
                details,
                merge_template,
            )
        except Exception as error:
            _persist_rejection(
                client,
                config,
                rejected_table,
                registry_file,
                "load",
                error,
                False,
                merge_template,
                summary,
            )
            continue

        summary["processed"] += 1
        summary["qa_versions" if registry_file.environment == "qa" else "prod_baselines"] += 1
        summary["details"] += detail_count

    if summary["rejected"] or summary["grid_failures"] or summary["rejection_persist_failures"]:
        summary["status"] = "PARTIAL"
        logger.warning("Ingestão de qualidade concluída com carga parcial: %s", summary)
        notifications.notify_partial_load(summary)
    else:
        logger.info("Ingestão de qualidade concluída: %s", summary)
    return summary


def load_artifacts(
    artifacts: list[Artifact],
    config: IngestionConfig,
    merge_template: str,
) -> dict[str, Any]:
    """Carregue artefatos já baixados; mantido para integrações e testes locais."""
    client = bigquery.get_client(config)
    qa_table, prod_table, detail_table, rejected_table = table_specs()
    summary = _empty_summary(len(artifacts))
    grid_client = _safe_grid_client(summary)
    grid_cache: dict[tuple[str, str], dict[str, Any] | None] = {}
    for registry_file, artifact in artifacts:
        try:
            aggregate, details = parser.parse_artifact(
                registry_file,
                artifact,
                datetime.now(timezone.utc),
            )
        except Exception as error:
            _persist_rejection(
                client,
                config,
                rejected_table,
                registry_file,
                "parse",
                error,
                False,
                merge_template,
                summary,
            )
            continue
        try:
            enrich_grid_details(
                details,
                parser.extract_suite_runs(artifact),
                aggregate,
                grid_client,
                summary,
                grid_cache,
            )
        except Exception as error:
            summary["grid_failures"] += 1
            summary["status"] = "PARTIAL"
            logger.exception(
                "Enriquecimento Grid ignorado para package_file_id=%s: %s",
                registry_file.package_file_id,
                error,
            )
        try:
            aggregate_table = qa_table if registry_file.environment == "qa" else prod_table
            _, detail_count = bigquery.upsert_artifact(
                client,
                config,
                aggregate_table,
                detail_table,
                aggregate,
                details,
                merge_template,
            )
        except Exception as error:
            _persist_rejection(
                client,
                config,
                rejected_table,
                registry_file,
                "load",
                error,
                False,
                merge_template,
                summary,
            )
            continue
        summary["processed"] += 1
        summary["qa_versions" if registry_file.environment == "qa" else "prod_baselines"] += 1
        summary["details"] += detail_count
    if summary["rejected"] or summary["grid_failures"]:
        summary["status"] = "PARTIAL"
        notifications.notify_partial_load(summary)
    return summary

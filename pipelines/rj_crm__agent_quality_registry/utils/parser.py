"""Normalize os artefatos de qualidade para o esquema BigQuery."""

import hashlib
import json
import re
from datetime import datetime
from typing import Any

from pipelines.rj_crm__agent_quality_registry.constants import ARTIFACT_SCHEMA
from pipelines.rj_crm__agent_quality_registry.utils.schemas import RegistryFile


def serialize_json(value: Any) -> str:
    """Serialize um valor para JSON compacto.

    :param value: Valor serializável em JSON.
    :returns: Representação JSON compacta sem escape de caracteres Unicode.
    :raises TypeError: Se ``value`` não for serializável em JSON.
    """
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def string_or_none(value: Any) -> str | None:
    """Normalize valores escalares ou estruturados para campos STRING."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return serialize_json(value)
    return str(value)


def int_or_none(value: Any) -> int | None:
    """Normalize um valor para INT64 ou retorne nulo quando inválido."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def float_or_none(value: Any) -> float | None:
    """Normalize um valor para FLOAT64 ou retorne nulo quando inválido."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_run_id(result_file: str | None) -> str | None:
    """Extraia o ID da execução a partir do nome de arquivo de resultado.

    :param result_file: Nome do arquivo de resultado da suite.
    :returns: ID da execução ou ``None`` quando não for possível extraí-lo.
    """
    if not result_file:
        return None
    match = re.search(r"-([A-Za-z0-9_-]+)\.json$", result_file)
    return match.group(1) if match else None


def create_key(*parts: Any) -> str:
    """Crie uma chave estável SHA-256 a partir de partes de um registro.

    :param parts: Valores que identificam unicamente o registro.
    :returns: Hash hexadecimal SHA-256 das partes concatenadas.
    """
    return hashlib.sha256("|".join(str(part or "") for part in parts).encode()).hexdigest()


def parse_artifact(
    registry_file: RegistryFile,
    artifact: dict[str, Any],
    ingested_at: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Converta um artefato publicado nas linhas agregada e detalhadas.

    :param registry_file: Metadados do arquivo no GitLab Registry.
    :param artifact: Conteúdo JSON do artefato.
    :param ingested_at: Horário UTC em que a ingestão começou.
    :returns: Linha agregada e linhas detalhadas normalizadas.
    :raises ValueError: Se o schema do artefato não for suportado.
    """
    if artifact.get("schema") != ARTIFACT_SCHEMA:
        raise ValueError(f"schema não suportado: {artifact.get('schema')}")

    gitlab = artifact.get("gitlab") or {}
    agent = artifact.get("agentVersion") or {}
    release_key = create_key(
        registry_file.package_name,
        registry_file.package_version,
        registry_file.package_file_id,
    )
    source = registry_file.environment
    tested_at = artifact.get("generatedAt") or registry_file.created_at
    routing = artifact.get("routing") or {}
    routing_summary = routing.get("summary") or {}
    harness = artifact.get("harness") or {}
    harness_summary = harness.get("summary") or {}
    row = {
        "release_key": release_key,
        "environment_scope": source,
        "registry_package_name": registry_file.package_name,
        "registry_package_version": registry_file.package_version,
        "package_id": registry_file.package_id,
        "package_file_id": registry_file.package_file_id,
        "artifact_sha256": registry_file.file_sha256,
        "artifact_generated_at": artifact.get("generatedAt"),
        "tested_at": tested_at,
        "promoted_at": registry_file.created_at if source == "prod_baseline" else None,
        "agent_version_id": string_or_none(agent.get("id")),
        "agent_version_number": int_or_none(agent.get("versionNumber")),
        "agent_version_status": string_or_none(agent.get("status")),
        "gitlab_project_id": string_or_none(gitlab.get("projectId")),
        "gitlab_project_path": string_or_none(gitlab.get("projectPath")),
        "pipeline_id": string_or_none(gitlab.get("pipelineId")),
        "pipeline_url": string_or_none(gitlab.get("pipelineUrl")),
        "job_id": string_or_none(gitlab.get("jobId")),
        "job_url": string_or_none(gitlab.get("jobUrl")),
        "commit_sha": string_or_none(gitlab.get("commitSha")),
        "merge_request_iid": string_or_none(gitlab.get("mergeRequestIid")),
        "source_branch": string_or_none(gitlab.get("sourceBranch")),
        "target_branch": string_or_none(gitlab.get("targetBranch")),
        "routing_total": int_or_none(routing_summary.get("total")),
        "routing_pass": int_or_none(routing_summary.get("pass")),
        "routing_fail": int_or_none(routing_summary.get("fail")),
        "routing_pass_rate": float_or_none(routing_summary.get("passRate")),
        "harness_scenarios_total": int_or_none((harness_summary.get("scenarios") or {}).get("total")),
        "harness_scenarios_pass": int_or_none((harness_summary.get("scenarios") or {}).get("pass")),
        "harness_scenarios_fail": int_or_none((harness_summary.get("scenarios") or {}).get("fail")),
        "harness_scenarios_pass_rate": float_or_none((harness_summary.get("scenarios") or {}).get("passRate")),
        "harness_turns_total": int_or_none((harness_summary.get("turns") or {}).get("total")),
        "harness_turns_pass": int_or_none((harness_summary.get("turns") or {}).get("pass")),
        "harness_turns_fail": int_or_none((harness_summary.get("turns") or {}).get("fail")),
        "harness_turns_pass_rate": float_or_none((harness_summary.get("turns") or {}).get("passRate")),
        "metric_counters_json": serialize_json(routing.get("metricCounters") or {}),
        "quality_summary_json": serialize_json((artifact.get("qualityReport") or {}).get("summary")),
        "ingested_at": ingested_at.isoformat(),
    }
    details = parse_suite_details(release_key, source, tested_at, agent, routing_summary)
    details.extend(parse_harness_details(release_key, source, tested_at, agent, harness))
    return row, details


def extract_suite_runs(artifact: dict[str, Any]) -> list[dict[str, str | None]]:
    """Extraia as suites e seus runs mesmo quando o artefato não trouxer casos.

    :param artifact: Conteúdo JSON do artefato.
    :returns: Referências únicas de suite e run encontradas no resumo de roteamento.
    """
    routing_summary = (artifact.get("routing") or {}).get("summary") or {}
    runs: list[dict[str, str | None]] = []
    seen: set[tuple[str, str]] = set()
    for suite in routing_summary.get("suites") or []:
        suite_name = suite.get("suite")
        run_id = extract_run_id(suite.get("resultFile"))
        if not isinstance(suite_name, str) or not suite_name or not run_id:
            continue
        key = (suite_name, run_id)
        if key in seen:
            continue
        seen.add(key)
        runs.append(
            {
                "suite_name": suite_name,
                "runtime_suite_name": suite.get("runtimeSuite"),
                "run_id": run_id,
            }
        )
    return runs


def parse_suite_details(
    release_key: str,
    source: str,
    tested_at: str | None,
    agent: dict[str, Any],
    routing_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    """Normalize os resultados das suites de roteamento.

    :param release_key: Chave da versão publicada.
    :param source: Escopo de ambiente do artefato.
    :param tested_at: Horário em que o artefato foi testado.
    :param agent: Metadados da versão do agente.
    :param routing_summary: Resumo de roteamento do artefato.
    :returns: Linhas detalhadas de asserts de suites.
    """
    details: list[dict[str, Any]] = []
    for suite in routing_summary.get("suites") or []:
        suite_name = suite.get("suite")
        run_id = extract_run_id(suite.get("resultFile"))
        for case in suite.get("cases") or []:
            for assertion in case.get("assertions") or []:
                details.append(
                    {
                        "result_key": create_key(
                            release_key,
                            "test_suite",
                            suite_name,
                            case.get("caseNumber"),
                            assertion.get("assertion"),
                        ),
                        "release_key": release_key,
                        "environment_scope": source,
                        "test_source": "test_suite",
                        "tested_at": tested_at,
                        "agent_version_number": int_or_none(agent.get("versionNumber")),
                        "grid_run_id": run_id,
                        "grid_workbook_id": None,
                        "grid_worksheet_id": None,
                        "grid_enrichment_status": None,
                        "grid_run_status_json": None,
                        "grid_worksheet_data_json": None,
                        "result_origin": "artifact",
                        "suite_name": string_or_none(suite_name),
                        "runtime_suite_name": string_or_none(suite.get("runtimeSuite")),
                        "case_number": string_or_none(case.get("caseNumber")),
                        "worksheet_row_id": None,
                        "assertion": string_or_none(assertion.get("assertion")),
                        "scenario_id": None,
                        "service_name": string_or_none(case.get("expectedSubagent")),
                        "turn_number": None,
                        "status": string_or_none(assertion.get("result")),
                        "score": float_or_none(assertion.get("score")),
                        "latency_ms": int_or_none(case.get("latencyMs")),
                        "expected_value": string_or_none(assertion.get("expectedValue")),
                        "actual_value": string_or_none(assertion.get("actualValue")),
                        "message": string_or_none(assertion.get("message")),
                        "judge_provider": None,
                        "judge_pass": None,
                        "judge_rationale": None,
                        "safety_severity": None,
                        "safety_type": None,
                        "utterance": string_or_none(case.get("utterance")),
                        "response": None,
                    }
                )
    return details


def parse_harness_details(
    release_key: str,
    source: str,
    tested_at: str | None,
    agent: dict[str, Any],
    harness: dict[str, Any],
) -> list[dict[str, Any]]:
    """Normalize os resultados do harness de conversação.

    :param release_key: Chave da versão publicada.
    :param source: Escopo de ambiente do artefato.
    :param tested_at: Horário em que o artefato foi testado.
    :param agent: Metadados da versão do agente.
    :param harness: Dados de harness do artefato.
    :returns: Linhas detalhadas de cenários e turnos.
    """
    details: list[dict[str, Any]] = []
    for report in harness.get("reports") or []:
        contents = report.get("contents") or {}
        for scenario in contents.get("scenarios") or []:
            for turn in scenario.get("transcript") or []:
                judge = turn.get("judge") or {}
                safety = turn.get("safetyAlert") or {}
                status = "PASS"
                if not turn.get("response") or turn.get("httpError") or judge.get("pass") is False or safety:
                    status = "FAIL"
                details.append(
                    {
                        "result_key": create_key(release_key, "harness", scenario.get("id"), turn.get("turn")),
                        "release_key": release_key,
                        "environment_scope": source,
                        "test_source": "harness",
                        "tested_at": tested_at,
                        "agent_version_number": int_or_none(agent.get("versionNumber")),
                        "grid_run_id": None,
                        "grid_workbook_id": None,
                        "grid_worksheet_id": None,
                        "grid_enrichment_status": None,
                        "grid_run_status_json": None,
                        "grid_worksheet_data_json": None,
                        "result_origin": "artifact",
                        "suite_name": None,
                        "runtime_suite_name": None,
                        "case_number": None,
                        "worksheet_row_id": None,
                        "assertion": None,
                        "scenario_id": string_or_none(scenario.get("id")),
                        "service_name": string_or_none(scenario.get("serviceName")),
                        "turn_number": int_or_none(turn.get("turn")),
                        "status": status,
                        "score": float_or_none(judge.get("score")),
                        "latency_ms": int_or_none(turn.get("latencyMs")),
                        "expected_value": None,
                        "actual_value": None,
                        "message": string_or_none(
                            turn.get("httpError") or judge.get("rationale") or safety.get("message")
                        ),
                        "judge_provider": string_or_none(judge.get("provider")),
                        "judge_pass": judge.get("pass"),
                        "judge_rationale": string_or_none(judge.get("rationale")),
                        "safety_severity": string_or_none(safety.get("severity")),
                        "safety_type": string_or_none(safety.get("type")),
                        "utterance": string_or_none(turn.get("sent")),
                        "response": string_or_none(turn.get("response")),
                    }
                )
    return details

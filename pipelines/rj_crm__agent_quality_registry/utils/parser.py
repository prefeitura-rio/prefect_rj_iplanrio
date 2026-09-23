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
        "agent_version_id": agent.get("id"),
        "agent_version_number": agent.get("versionNumber"),
        "agent_version_status": agent.get("status"),
        "gitlab_project_id": gitlab.get("projectId"),
        "gitlab_project_path": gitlab.get("projectPath"),
        "pipeline_id": gitlab.get("pipelineId"),
        "pipeline_url": gitlab.get("pipelineUrl"),
        "job_id": gitlab.get("jobId"),
        "job_url": gitlab.get("jobUrl"),
        "commit_sha": gitlab.get("commitSha"),
        "merge_request_iid": gitlab.get("mergeRequestIid"),
        "source_branch": gitlab.get("sourceBranch"),
        "target_branch": gitlab.get("targetBranch"),
        "routing_total": routing_summary.get("total"),
        "routing_pass": routing_summary.get("pass"),
        "routing_fail": routing_summary.get("fail"),
        "routing_pass_rate": routing_summary.get("passRate"),
        "harness_scenarios_total": (harness_summary.get("scenarios") or {}).get("total"),
        "harness_scenarios_pass": (harness_summary.get("scenarios") or {}).get("pass"),
        "harness_scenarios_fail": (harness_summary.get("scenarios") or {}).get("fail"),
        "harness_scenarios_pass_rate": (harness_summary.get("scenarios") or {}).get("passRate"),
        "harness_turns_total": (harness_summary.get("turns") or {}).get("total"),
        "harness_turns_pass": (harness_summary.get("turns") or {}).get("pass"),
        "harness_turns_fail": (harness_summary.get("turns") or {}).get("fail"),
        "harness_turns_pass_rate": (harness_summary.get("turns") or {}).get("passRate"),
        "metric_counters_json": serialize_json(routing.get("metricCounters") or {}),
        "quality_summary_json": serialize_json((artifact.get("qualityReport") or {}).get("summary")),
        "ingested_at": ingested_at.isoformat(),
    }
    details = parse_suite_details(release_key, source, tested_at, agent, routing_summary)
    details.extend(parse_harness_details(release_key, source, tested_at, agent, harness))
    return row, details


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
                        "agent_version_number": agent.get("versionNumber"),
                        "grid_run_id": run_id,
                        "grid_workbook_id": None,
                        "grid_worksheet_id": None,
                        "suite_name": suite_name,
                        "runtime_suite_name": suite.get("runtimeSuite"),
                        "case_number": case.get("caseNumber"),
                        "worksheet_row_id": None,
                        "assertion": assertion.get("assertion"),
                        "scenario_id": None,
                        "service_name": case.get("expectedSubagent"),
                        "turn_number": None,
                        "status": assertion.get("result"),
                        "score": assertion.get("score"),
                        "latency_ms": case.get("latencyMs"),
                        "expected_value": assertion.get("expectedValue"),
                        "actual_value": assertion.get("actualValue"),
                        "message": assertion.get("message"),
                        "judge_provider": None,
                        "judge_pass": None,
                        "judge_rationale": None,
                        "safety_severity": None,
                        "safety_type": None,
                        "utterance": case.get("utterance"),
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
                        "agent_version_number": agent.get("versionNumber"),
                        "grid_run_id": None,
                        "grid_workbook_id": None,
                        "grid_worksheet_id": None,
                        "suite_name": None,
                        "runtime_suite_name": None,
                        "case_number": None,
                        "worksheet_row_id": None,
                        "assertion": None,
                        "scenario_id": scenario.get("id"),
                        "service_name": scenario.get("serviceName"),
                        "turn_number": turn.get("turn"),
                        "status": status,
                        "score": judge.get("score"),
                        "latency_ms": turn.get("latencyMs"),
                        "expected_value": None,
                        "actual_value": None,
                        "message": turn.get("httpError") or judge.get("rationale") or safety.get("message"),
                        "judge_provider": judge.get("provider"),
                        "judge_pass": judge.get("pass"),
                        "judge_rationale": judge.get("rationale"),
                        "safety_severity": safety.get("severity"),
                        "safety_type": safety.get("type"),
                        "utterance": turn.get("sent"),
                        "response": turn.get("response"),
                    }
                )
    return details

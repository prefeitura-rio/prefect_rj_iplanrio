# -*- coding: utf-8 -*-
"""
Tasks para pipeline de geracao de dados de teste COR
"""

import uuid
from datetime import datetime
from time import sleep
from typing import Any, Dict, List

from basedosdados import Base
from google.cloud import bigquery
from prefect import task

from iplanrio.pipelines_utils.logging import log

from pipelines.rj_iplanrio__cor_alerts_test_writer.constants import (
    CORTestWriterConstants,
    TEST_SCENARIOS,
)


def validate_environment(environment: str) -> str:
    """
    Valida environment contra whitelist para prevenir SQL injection.

    Args:
        environment: Ambiente a validar

    Returns:
        Environment validado (lowercase)

    Raises:
        ValueError: Se environment nao estiver na whitelist
    """
    valid_environments = CORTestWriterConstants.VALID_ENVIRONMENTS.value
    env_lower = environment.strip().lower()

    if env_lower not in valid_environments:
        raise ValueError(
            f"Environment '{environment}' invalido. "
            f"Valores permitidos: {valid_environments}"
        )

    return env_lower


def get_bigquery_client(billing_project_id: str) -> bigquery.Client:
    """Obtem cliente BigQuery com credenciais"""
    credentials = Base(bucket_name=billing_project_id)._load_credentials(mode="prod")
    return bigquery.Client(credentials=credentials, project=billing_project_id)


def execute_bigquery_dml(query: str, billing_project_id: str) -> int:
    """Executa DML (INSERT/UPDATE/DELETE) no BigQuery e retorna linhas afetadas"""
    log(f"Executando DML: {query[:200]}...")
    client = get_bigquery_client(billing_project_id)
    job = client.query(query)
    while not job.done():
        sleep(1)
    rows_affected = job.num_dml_affected_rows or 0
    log(f"DML afetou {rows_affected} linhas")
    return rows_affected


@task
def generate_mock_alerts(
    scenario: str,
    environment: str = "staging",
) -> List[Dict[str, Any]]:
    """
    Gera alertas mockados baseado em cenario pre-definido.

    Args:
        scenario: Nome do cenario ("single", "small_cluster", "large_cluster", "mixed", "edge_cases")
        environment: Ambiente alvo (staging ou prod)

    Returns:
        Lista de dicionarios com dados de alertas
    """
    env_validated = validate_environment(environment)

    if scenario not in TEST_SCENARIOS:
        available = list(TEST_SCENARIOS.keys())
        raise ValueError(
            f"Cenario '{scenario}' nao encontrado. Disponiveis: {available}"
        )

    scenario_config = TEST_SCENARIOS[scenario]
    log(f"Gerando alertas para cenario: {scenario_config['name']}")
    log(f"Descricao: {scenario_config['description']}")

    alerts = []
    prefix = CORTestWriterConstants.TEST_ALERT_PREFIX.value
    user_id = CORTestWriterConstants.TEST_USER_ID.value
    now = datetime.now()

    for i, alert_config in enumerate(scenario_config["alerts"]):
        alert_id = f"{prefix}{scenario}_{uuid.uuid4().hex[:8]}"

        alert = {
            "alert_id": alert_id,
            "user_id": user_id,
            "alert_type": alert_config["alert_type"],
            "severity": alert_config.get("severity", "alta"),
            "description": alert_config.get("description", f"Alerta de teste {i+1}"),
            "address": alert_config.get("address", "Endereco de teste"),
            "latitude": alert_config["lat"],
            "longitude": alert_config["lng"],
            "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "environment": env_validated,
            "status": "pending",
        }
        alerts.append(alert)

    log(f"Gerados {len(alerts)} alertas mockados")
    return alerts


def insert_single_alert_to_bigquery(
    alert: Dict[str, Any],
    environment: str,
) -> bool:
    """
    Insere um unico alerta no BigQuery.

    Args:
        alert: Alerta a inserir
        environment: Ambiente (staging ou prod)

    Returns:
        True se inserido com sucesso
    """
    env_validated = validate_environment(environment)
    billing_project = CORTestWriterConstants.BILLING_PROJECT_ID.value
    dataset = CORTestWriterConstants.DATASET_ID.value
    table = CORTestWriterConstants.QUEUE_TABLE_ID.value

    # Escape single quotes
    alert_id = alert["alert_id"].replace("'", "''")
    user_id = alert["user_id"].replace("'", "''")
    alert_type = alert["alert_type"].replace("'", "''")
    severity = alert["severity"].replace("'", "''")
    description = alert["description"][:500].replace("'", "''")
    address = alert["address"].replace("'", "''")

    query = f"""
    INSERT INTO `{billing_project}.{dataset}.{table}`
    (alert_id, user_id, alert_type, severity, description, address,
     latitude, longitude, created_at, environment, status,
     aggregation_group_id, sent_at)
    VALUES
    ('{alert_id}', '{user_id}', '{alert_type}', '{severity}',
     '{description}', '{address}', {alert['latitude']}, {alert['longitude']},
     CURRENT_DATETIME('America/Sao_Paulo'), '{env_validated}', 'pending',
     NULL, NULL)
    """

    execute_bigquery_dml(query, billing_project)
    return True


@task
def insert_alerts_to_bigquery(
    alerts: List[Dict[str, Any]],
    environment: str,
    delay_seconds: int = 10,
) -> int:
    """
    Insere alertas mockados no BigQuery, um a um com delay entre eles.

    Args:
        alerts: Lista de alertas a inserir
        environment: Ambiente (staging ou prod)
        delay_seconds: Delay em segundos entre cada alerta (default: 10s)

    Returns:
        Numero de alertas inseridos
    """
    if not alerts:
        log("Nenhum alerta para inserir")
        return 0

    inserted_count = 0
    total = len(alerts)

    for i, alert in enumerate(alerts):
        try:
            insert_single_alert_to_bigquery(alert, environment)
            inserted_count += 1
            log(
                f"[{i+1}/{total}] Inserido alerta: {alert['alert_type']} - "
                f"{alert['address'][:50]}"
            )

            # Delay entre alertas (exceto no ultimo)
            if i < total - 1:
                log(f"Aguardando {delay_seconds}s antes do proximo alerta...")
                sleep(delay_seconds)

        except Exception as e:
            log(f"Erro ao inserir alerta {alert['alert_id']}: {e}", level="error")

    log(f"Total inseridos: {inserted_count}/{total} alertas")
    return inserted_count


@task
def cleanup_test_alerts(
    environment: str,
    older_than_hours: int = None,
) -> int:
    """
    Remove dados de teste do BigQuery.

    Args:
        environment: Ambiente (staging ou prod)
        older_than_hours: Se especificado, remove apenas alertas mais antigos que X horas

    Returns:
        Numero de alertas removidos
    """
    env_validated = validate_environment(environment)
    billing_project = CORTestWriterConstants.BILLING_PROJECT_ID.value
    dataset = CORTestWriterConstants.DATASET_ID.value
    table = CORTestWriterConstants.QUEUE_TABLE_ID.value
    prefix = CORTestWriterConstants.TEST_ALERT_PREFIX.value

    # Construir condicao de tempo se especificado
    time_condition = ""
    if older_than_hours is not None:
        time_condition = f"""
        AND created_at < DATETIME_SUB(
            CURRENT_DATETIME('America/Sao_Paulo'),
            INTERVAL {older_than_hours} HOUR
        )
        """

    query = f"""
    DELETE FROM `{billing_project}.{dataset}.{table}`
    WHERE environment = '{env_validated}'
        AND alert_id LIKE '{prefix}%'
        {time_condition}
    """

    try:
        removed = execute_bigquery_dml(query, billing_project)
        log(f"Removidos {removed} alertas de teste")
        return removed
    except Exception as e:
        log(f"Erro ao limpar alertas de teste: {e}", level="error")
        raise

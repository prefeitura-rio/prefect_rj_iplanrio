# -*- coding: utf-8 -*-
"""
Tasks para pipeline de geracao de dados de teste COR
"""

import os
import uuid
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Any, Dict, List

import pandas as pd
from basedosdados import Base
from google.cloud import bigquery
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_iplanrio__cor_alerts_test_writer.constants import (
    CORTestWriterConstants,
    TEST_SCENARIOS,
)


def validate_environment(environment: str) -> str:
    """
    Valida environment contra whitelist para prevenir SQL injection.
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
    """Executa DML (DELETE) no BigQuery e retorna linhas afetadas"""
    log(f"Executando DML: {query[:200]}...")
    client = get_bigquery_client(billing_project_id)
    job = client.query(query)
    while not job.done():
        sleep(1)
    rows_affected = job.num_dml_affected_rows or 0
    log(f"DML afetou {rows_affected} linhas")
    return rows_affected


def generate_single_alert(
    alert_config: Dict[str, Any],
    scenario: str,
    environment: str,
    index: int,
) -> Dict[str, Any]:
    """
    Gera um unico alerta mockado.
    """
    prefix = CORTestWriterConstants.TEST_ALERT_PREFIX.value
    user_id = CORTestWriterConstants.TEST_USER_ID.value
    now = datetime.now()

    alert_id = f"{prefix}{scenario}_{uuid.uuid4().hex[:8]}"

    return {
        "alert_id": alert_id,
        "user_id": user_id,
        "alert_type": alert_config["alert_type"],
        "severity": alert_config.get("severity", "alta"),
        "description": alert_config.get("description", f"Alerta de teste {index+1}"),
        "address": alert_config.get("address", "Endereco de teste"),
        "latitude": alert_config["lat"],
        "longitude": alert_config["lng"],
        "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "environment": environment,
        "status": "pending",
        "aggregation_group_id": None,
        "sent_at": None,
    }


def save_alert_to_csv(alert: Dict[str, Any], data_path: str) -> str:
    """
    Salva um alerta em arquivo CSV no diretorio especificado.

    Returns:
        Caminho do arquivo CSV criado
    """
    df = pd.DataFrame([alert])

    # Criar diretorio se nao existir
    Path(data_path).mkdir(parents=True, exist_ok=True)

    # Nome do arquivo com timestamp unico
    filename = f"alert_{alert['alert_id']}.csv"
    filepath = os.path.join(data_path, filename)

    df.to_csv(filepath, index=False)
    return filepath


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
        Lista de configuracoes de alertas (ainda nao gerados)
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

    # Retorna as configuracoes de alerta do cenario
    return scenario_config["alerts"]


@task
def insert_alerts_to_bigquery(
    alert_configs: List[Dict[str, Any]],
    scenario: str,
    environment: str,
    delay_seconds: int = 10,
) -> int:
    """
    Insere alertas mockados no BigQuery um a um, usando create_table_and_upload_to_gcs_task.

    Args:
        alert_configs: Lista de configuracoes de alertas
        scenario: Nome do cenario
        environment: Ambiente (staging ou prod)
        delay_seconds: Delay em segundos entre cada alerta (default: 10s)

    Returns:
        Numero de alertas inseridos
    """
    if not alert_configs:
        log("Nenhum alerta para inserir")
        return 0

    env_validated = validate_environment(environment)
    dataset_id = CORTestWriterConstants.DATASET_ID.value
    table_id = CORTestWriterConstants.QUEUE_TABLE_ID.value
    root_folder = CORTestWriterConstants.ROOT_FOLDER.value

    inserted_count = 0
    total = len(alert_configs)

    for i, alert_config in enumerate(alert_configs):
        try:
            # Gerar alerta com timestamp atual
            alert = generate_single_alert(
                alert_config=alert_config,
                scenario=scenario,
                environment=env_validated,
                index=i,
            )

            # Salvar em CSV
            data_path = os.path.join(root_folder, f"test_alert_{i}")
            save_alert_to_csv(alert, data_path)

            # Upload para BigQuery
            create_table_and_upload_to_gcs_task(
                data_path=data_path,
                dataset_id=dataset_id,
                table_id=table_id,
                biglake_table=False,
                dump_mode="append",
            )

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
            log(f"Erro ao inserir alerta {i+1}: {e}", level="error")

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

    # Validar e sanitizar older_than_hours
    time_condition = ""
    if older_than_hours is not None:
        try:
            older_than_hours = int(older_than_hours)
        except (TypeError, ValueError):
            raise ValueError(
                f"older_than_hours deve ser um inteiro, recebeu: {older_than_hours!r}"
            )
        if older_than_hours < 1 or older_than_hours > 8760:
            raise ValueError(
                f"older_than_hours deve estar entre 1 e 8760, recebeu: {older_than_hours}"
            )
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

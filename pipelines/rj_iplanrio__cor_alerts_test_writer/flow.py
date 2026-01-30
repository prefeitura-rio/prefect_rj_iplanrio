# -*- coding: utf-8 -*-
"""
Pipeline para geracao de dados de teste do COR alerts aggregator.
Insere alertas mockados na tabela cor_alerts_queue para validar logica de agregacao.
"""

from datetime import datetime
from time import sleep

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__cor_alerts_test_writer.constants import (
    TEST_SCENARIOS,
)
from pipelines.rj_iplanrio__cor_alerts_test_writer.tasks import (
    cleanup_test_alerts,
    generate_mock_alerts,
    insert_alerts_to_bigquery,
)


@flow(log_prints=True)
def rj_iplanrio__cor_alerts_test_writer(
    environment: str = "staging",
    scenario: str = "mixed",
    cleanup_before: bool = True,
    cleanup_after: bool = False,
    delay_between_alerts: int = 10,
    cleanup_delay_seconds: int = 300,
):
    """
    Gera e insere alertas mockados na fila do COR para testes.

    Args:
        environment: Ambiente alvo (staging ou prod, recomendado staging)
        scenario: Cenario pre-definido ("single", "small_cluster", "large_cluster", "mixed", "edge_cases")
        cleanup_before: Remove dados de teste existentes antes de inserir
        cleanup_after: Remove dados apos insercao (util para testes temporarios)
        delay_between_alerts: Delay em segundos entre cada alerta (default: 10s)
        cleanup_delay_seconds: Tempo de espera antes do cleanup final (default: 300s = 5min)
    """
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rename_current_flow_run_task(
        new_name=f"cor_test_writer_{scenario}_{environment}_{run_timestamp}"
    )

    log(f"=== Iniciando geracao de dados de teste - Cenario: {scenario} ===")
    log(f"Ambiente: {environment}")

    # Validar cenario
    if scenario not in TEST_SCENARIOS:
        available = list(TEST_SCENARIOS.keys())
        raise ValueError(
            f"Cenario '{scenario}' nao encontrado. Disponiveis: {available}"
        )

    # Injetar credenciais BigQuery
    inject_bd_credentials_task(environment="prod")

    # 1. Cleanup antes (se solicitado)
    if cleanup_before:
        removed = cleanup_test_alerts(environment=environment)
        log(f"Cleanup inicial: {removed} alertas de teste removidos")

    # 2. Gerar alertas mockados baseado no cenario
    alerts = generate_mock_alerts(scenario=scenario, environment=environment)
    log(f"Gerados {len(alerts)} alertas mockados para cenario '{scenario}'")

    # 3. Inserir no BigQuery (tabela cor_alerts_queue) com delay entre alertas
    inserted = insert_alerts_to_bigquery(
        alerts=alerts,
        environment=environment,
        delay_seconds=delay_between_alerts,
    )
    log(f"Inseridos {inserted} alertas na fila do BigQuery")

    log(
        "Pipeline processadora (rj_iplanrio__cor_alerts_aggregator) "
        "vai processar estes alertas automaticamente a cada 2 minutos"
    )

    # 4. Cleanup apos (se solicitado)
    if cleanup_after:
        log(
            f"Aguardando {cleanup_delay_seconds} segundos para permitir "
            "processamento pela pipeline principal..."
        )
        sleep(cleanup_delay_seconds)
        removed = cleanup_test_alerts(environment=environment)
        log(f"Cleanup final: {removed} alertas de teste removidos")

    return {
        "scenario": scenario,
        "environment": environment,
        "alerts_generated": len(alerts),
        "alerts_inserted": inserted,
    }

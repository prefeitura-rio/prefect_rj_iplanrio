# -*- coding: utf-8 -*-
"""
Flow para agregacao e envio de alertas COR.

Executa a cada 2 minutos, busca alertas pendentes na tabela cor_alerts_queue,
agrupa por tipo e localizacao (500m radius) e envia para COR OnCall API
conforme regras de agregacao:

- 5+ alertas no cluster: dispara imediatamente
- 1-4 alertas + janela 7 min expirou: dispara
- 1-4 alertas + janela < 7 min: aguarda mais alertas
"""

from datetime import datetime

from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import (
    CORAlertAggregatorConstants,
)
from pipelines.rj_iplanrio__cor_alerts_aggregator.tasks import (
    fetch_pending_alerts,
    cluster_alerts_by_location,
    should_send_cluster,
    submit_cluster_to_cor_api,
    mark_alerts_as_sent,
)


@flow(log_prints=True)
def rj_iplanrio__cor_alerts_aggregator(
    environment: str = "staging",
    radius_meters: int = CORAlertAggregatorConstants.RADIUS_METERS.value,
    time_window_minutes: int = CORAlertAggregatorConstants.TIME_WINDOW_MINUTES.value,
    immediate_threshold: int = CORAlertAggregatorConstants.IMMEDIATE_THRESHOLD.value,
    dry_run: bool = False,
):
    """
    Agrega alertas COR pendentes e envia para API do COR.

    Regras de agregacao:
    - Agrupa alertas por tipo (enchente, alagamento, bolsao) em raio de 500m
    - 5+ alertas: envia imediatamente (limite atingido)
    - 1-4 alertas + janela 7 min expirou: envia
    - 1-4 alertas + janela ativa: aguarda mais alertas

    Args:
        environment: Ambiente (staging ou prod)
        radius_meters: Raio de agregacao em metros (default: 500)
        time_window_minutes: Janela de tempo em minutos (default: 7)
        immediate_threshold: Limite para disparo imediato (default: 5)
        dry_run: Se True, nao envia para COR API (apenas simula)
    """
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rename_current_flow_run_task(
        new_name=f"cor_alerts_aggregator_{environment}_{run_timestamp}"
    )

    log(f"Iniciando agregacao de alertas COR - {environment}")
    log(
        f"Parametros: raio={radius_meters}m, janela={time_window_minutes}min, limite={immediate_threshold}"
    )

    # Injetar credenciais
    inject_bd_credentials_task(environment="prod")

    # 1. Buscar alertas pendentes
    pending_alerts = fetch_pending_alerts(
        environment=environment,
        time_window_minutes=time_window_minutes,
    )

    if pending_alerts.empty:
        log("Nenhum alerta pendente encontrado")
        return {"alerts_processed": 0, "clusters_sent": 0}

    log(f"Encontrados {len(pending_alerts)} alertas pendentes")

    # 2. Agrupar por tipo e localizacao
    clusters = cluster_alerts_by_location(
        alerts_df=pending_alerts,
        radius_meters=radius_meters,
    )

    if not clusters:
        log("Nenhum cluster formado (alertas sem coordenadas?)")
        return {"alerts_processed": 0, "clusters_sent": 0}

    # 3. Processar cada cluster
    clusters_sent = 0
    alerts_sent = 0

    for cluster in clusters:
        should_send, reason = should_send_cluster(
            cluster=cluster,
            immediate_threshold=immediate_threshold,
            time_window_minutes=time_window_minutes,
        )

        log(
            f"Cluster {cluster.cluster_id} ({cluster.alert_type}): "
            f"{cluster.alert_count} alertas - {reason}"
        )

        if should_send:
            if dry_run:
                log(f"[DRY RUN] Simulando envio do cluster {cluster.cluster_id}")
                # In dry run, still mark as processed for testing
                clusters_sent += 1
                alerts_sent += cluster.alert_count
            else:
                # 4. Enviar para COR API
                result = submit_cluster_to_cor_api(
                    cluster=cluster,
                    environment=environment,
                )

                if result["success"]:
                    # 5. Marcar como enviado
                    updated = mark_alerts_as_sent(
                        alert_ids=result["alert_ids"],
                        aggregation_group_id=result["aggregation_group_id"],
                        environment=environment,
                    )

                    clusters_sent += 1
                    alerts_sent += updated
                    log(f"Cluster enviado: {updated} alertas marcados como 'sent'")
                else:
                    log(
                        f"Falha ao enviar cluster: {result.get('error')}",
                        level="error",
                    )

    log(
        f"Agregacao concluida: {clusters_sent} clusters enviados, "
        f"{alerts_sent} alertas processados"
    )

    return {
        "alerts_processed": alerts_sent,
        "clusters_sent": clusters_sent,
        "total_pending": len(pending_alerts),
        "total_clusters": len(clusters),
    }

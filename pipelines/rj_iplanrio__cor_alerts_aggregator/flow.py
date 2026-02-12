# -*- coding: utf-8 -*-
"""
Flow para agregacao e envio de alertas COR.
Executa a cada 2 minutos, busca alertas pendentes, agrupa por localizacao
e envia para COR OnCall API conforme regras de agregacao.

Regras: clustering 500m, janela 7min, threshold 5 alertas
"""

import json
import uuid
from datetime import datetime

import pandas as pd
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow
from prefect.concurrency.sync import concurrency

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import (
    CORAlertAggregatorConstants,
)

# Mapeamentos para construir payload de log
ALERT_TYPE_MAPPING = CORAlertAggregatorConstants.ALERT_TYPE_MAPPING.value
SEVERITY_PRIORITY_MAPPING = CORAlertAggregatorConstants.SEVERITY_PRIORITY_MAPPING.value
from pipelines.rj_iplanrio__cor_alerts_aggregator.tasks import (
    build_cluster_dataframe,
    cluster_alerts_by_location,
    create_date_partitions,
    fetch_pending_alerts,
    mark_alerts_as_sent,
    should_send_cluster,
    submit_cluster_to_cor_api,
)


@flow(log_prints=True)
def rj_iplanrio__cor_alerts_aggregator(
    environment: str = "staging",
    radius_meters: int = CORAlertAggregatorConstants.RADIUS_METERS.value,
    time_window_minutes: int = CORAlertAggregatorConstants.TIME_WINDOW_MINUTES.value,
    immediate_threshold: int = CORAlertAggregatorConstants.IMMEDIATE_THRESHOLD.value,
    dry_run: bool = False,
    destination: str = "google_sheets",
):
    """
    Agrega alertas COR pendentes e envia para API do COR ou BigQuery.

    Regras de agregacao:
    - Agrupa alertas por tipo (enchente, alagamento, bolsao) em raio de 500m
    - 5+ alertas em cluster: envia imediatamente (limite atingido)
    - 1-4 alertas + janela 7 min expirou: envia
    - 1-4 alertas + janela ativa: aguarda mais relatos

    Args:
        environment: Ambiente (staging ou prod)
        radius_meters: Raio de agregacao em metros (default: 500)
        time_window_minutes: Janela de tempo em minutos (default: 7)
        immediate_threshold: Limite para disparo imediato (default: 5)
        dry_run: Se True, nao envia para destino (apenas simula)
        destination: Destino dos alertas ("cor_api" ou "google_sheets")
    """
    valid_destinations = CORAlertAggregatorConstants.VALID_DESTINATIONS.value
    if destination not in valid_destinations:
        raise ValueError(
            f"Destination '{destination}' invalido. "
            f"Valores permitidos: {valid_destinations}"
        )
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rename_current_flow_run_task(
        new_name=f"cor_alerts_aggregator_{environment}_{run_timestamp}"
    )

    log(f"=== Iniciando agregacao de alertas COR - {environment} ===")
    log(f"Destino: {destination}")
    log(
        f"Parametros: raio={radius_meters}m, janela={time_window_minutes}min, limite={immediate_threshold}"
    )

    # Adquirir lock de concorrencia (limite 1)
    # Se outra execucao estiver em andamento, esta aguarda
    with concurrency("cor-alerts-aggregator", occupy=1):
        log("Lock de concorrencia adquirido")

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
        bq_dataframes = []
        bq_metadata_list = []

        for cluster in clusters:
            send_decision, reason = should_send_cluster(
                cluster=cluster,
                immediate_threshold=immediate_threshold,
                time_window_minutes=time_window_minutes,
            )

            log(
                f"Cluster {cluster.cluster_id} ({cluster.alert_type}): "
                f"{cluster.alert_count} alertas - {reason}"
            )

            if send_decision:
                if dry_run:
                    # Construir AgencyEventTypeCode com relatos + enderecos (igual cor_api.py)
                    type_code = ALERT_TYPE_MAPPING.get(
                        cluster.alert_type, cluster.alert_type.upper()
                    )
                    if len(cluster.descriptions) == 1:
                        addr = cluster.addresses[0][:80] if cluster.addresses else ""
                        agency_event_type = f"{type_code}: {cluster.descriptions[0][:150]} [{addr}]"
                    else:
                        relatos = " | ".join(
                            f"({i+1}) {d[:60]} [{cluster.addresses[i][:40] if i < len(cluster.addresses) else ''}]"
                            for i, d in enumerate(cluster.descriptions[:5])
                        )
                        if len(cluster.descriptions) > 5:
                            relatos += f" | (+{len(cluster.descriptions) - 5} relatos)"
                        agency_event_type = f"{type_code}: {relatos}"

                    # Construir payload para log
                    dry_run_payload = {
                        "EventId": str(uuid.uuid4()),
                        "Location": cluster.addresses[0] if cluster.addresses else "",
                        "Priority": SEVERITY_PRIORITY_MAPPING.get(cluster.severity, "02"),
                        "AgencyEventTypeCode": agency_event_type[:500],
                        "CreatedDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "Latitude": cluster.centroid_lat,
                        "Longitude": cluster.centroid_lng,
                    }
                    log(f"[DRY RUN] Simulando envio do cluster {cluster.cluster_id}")
                    log(f"[DRY RUN] Payload: {json.dumps(dry_run_payload, indent=2)}")
                    log(f"[DRY RUN] Alert IDs incluidos: {cluster.alert_ids}")
                elif destination == "cor_api":
                    # 4a. Enviar para COR API
                    result = submit_cluster_to_cor_api(
                        cluster=cluster,
                        environment=environment,
                    )

                    if result["success"]:
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
                else:  # google_sheets
                    # 4b. Preparar DataFrame para upload ao BigQuery
                    df, metadata = build_cluster_dataframe(
                        cluster=cluster,
                        environment=environment,
                    )
                    bq_dataframes.append(df)
                    bq_metadata_list.append(metadata)

        # 5. Upload para BigQuery (google_sheets) - uma unica operacao
        if destination == "google_sheets" and bq_dataframes and not dry_run:
            combined_df = pd.concat(bq_dataframes, ignore_index=True)

            dataset_id = CORAlertAggregatorConstants.DATASET_ID.value
            table_id = CORAlertAggregatorConstants.AGGREGATED_TABLE_ID.value
            root_folder = f"{CORAlertAggregatorConstants.ROOT_FOLDER.value}{table_id}"

            partitions_path = create_date_partitions(
                dataframe=combined_df,
                partition_column=CORAlertAggregatorConstants.PARTITION_COLUMN.value,
                file_format=CORAlertAggregatorConstants.FILE_FORMAT.value,
                root_folder=root_folder,
            )

            create_table_and_upload_to_gcs_task(
                data_path=partitions_path,
                dataset_id=dataset_id,
                table_id=table_id,
                dump_mode=CORAlertAggregatorConstants.DUMP_MODE.value,
                biglake_table=CORAlertAggregatorConstants.BIGLAKE_TABLE.value,
                source_format=CORAlertAggregatorConstants.FILE_FORMAT.value,
            )

            # Marcar alertas como enviados apos upload bem-sucedido
            for metadata in bq_metadata_list:
                updated = mark_alerts_as_sent(
                    alert_ids=metadata["alert_ids"],
                    aggregation_group_id=metadata["aggregation_group_id"],
                    environment=environment,
                )
                clusters_sent += 1
                alerts_sent += updated

            log(f"Upload BigQuery concluido: {len(bq_dataframes)} clusters, {len(combined_df)} linhas")

        log(
            f"Agregacao concluida: {clusters_sent} clusters enviados, {alerts_sent} alertas processados"
        )

    log("Lock de concorrencia liberado")

    return {
        "alerts_processed": alerts_sent,
        "clusters_sent": clusters_sent,
        "total_pending": len(pending_alerts),
        "total_clusters": len(clusters),
    }

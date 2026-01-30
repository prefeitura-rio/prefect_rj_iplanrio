# -*- coding: utf-8 -*-
"""
Tasks para pipeline de agregacao de alertas COR

Inclui: fetch, clustering espacial, decisao de envio, submissao API
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from time import sleep
from typing import Any, Dict, List, Tuple

import pandas as pd
from basedosdados import Base
from google.cloud import bigquery
from prefect import task

from iplanrio.pipelines_utils.logging import log

from pipelines.rj_iplanrio__cor_alerts_aggregator.constants import (
    CORAlertAggregatorConstants,
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
    valid_environments = CORAlertAggregatorConstants.VALID_ENVIRONMENTS.value
    env_lower = environment.strip().lower()

    if env_lower not in valid_environments:
        raise ValueError(
            f"Environment '{environment}' invalido. "
            f"Valores permitidos: {valid_environments}"
        )

    return env_lower


@dataclass
class AlertCluster:
    """Representa um cluster de alertas agregados"""

    cluster_id: int
    alert_type: str
    alert_ids: List[str]
    alert_count: int
    oldest_alert: datetime
    centroid_lat: float
    centroid_lng: float
    addresses: List[str]
    descriptions: List[str]
    severity: str  # Maior severidade do cluster


def get_bigquery_client(billing_project_id: str, bucket_name: str) -> bigquery.Client:
    """Obtem cliente BigQuery com credenciais"""
    credentials = Base(bucket_name=bucket_name)._load_credentials(mode="prod")
    return bigquery.Client(credentials=credentials, project=billing_project_id)


def query_bigquery(
    query: str, billing_project_id: str, bucket_name: str
) -> pd.DataFrame:
    """Executa query no BigQuery e retorna DataFrame"""
    log(f"Executando query: {query[:100]}...")
    client = get_bigquery_client(billing_project_id, bucket_name)
    job = client.query(query)
    while not job.done():
        sleep(1)
    results = job.result()
    df = results.to_dataframe(create_bqstorage_client=False)
    log(f"Query retornou {len(df)} linhas")
    return df


def execute_bigquery_dml(
    query: str, billing_project_id: str, bucket_name: str
) -> int:
    """Executa DML (UPDATE/DELETE) no BigQuery e retorna linhas afetadas"""
    log(f"Executando DML: {query[:100]}...")
    client = get_bigquery_client(billing_project_id, bucket_name)
    job = client.query(query)
    while not job.done():
        sleep(1)
    rows_affected = job.num_dml_affected_rows or 0
    log(f"DML afetou {rows_affected} linhas")
    return rows_affected


@task
def fetch_pending_alerts(
    environment: str,
    time_window_minutes: int = CORAlertAggregatorConstants.TIME_WINDOW_MINUTES.value,
) -> pd.DataFrame:
    """
    Busca alertas pendentes no BigQuery.

    Args:
        environment: staging ou prod
        time_window_minutes: Janela de tempo em minutos

    Returns:
        DataFrame com alertas pendentes
    """
    # Valida environment contra whitelist (previne SQL injection)
    env_validated = validate_environment(environment)

    billing_project = CORAlertAggregatorConstants.BILLING_PROJECT_ID.value
    dataset = "brutos_eai_logs_staging"  # Hardcoded por enquanto
    table = CORAlertAggregatorConstants.QUEUE_TABLE_ID.value

    log(f"Buscando alertas em {billing_project}.{dataset}.{table}")

    # Busca alertas pendentes com coordenadas validas
    # Expande janela um pouco para incluir alertas no limite
    query = f"""
    SELECT
        alert_id,
        user_id,
        alert_type,
        severity,
        description,
        address,
        latitude,
        longitude,
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', created_at) as created_at,
        environment
    FROM `{billing_project}.{dataset}.{table}`
    WHERE status = 'pending'
        AND environment = '{env_validated}'
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND PARSE_DATETIME('%Y-%m-%d %H:%M:%S', created_at) >= DATETIME_SUB(
            CURRENT_DATETIME('America/Sao_Paulo'),
            INTERVAL {time_window_minutes + 3} MINUTE
        )
    ORDER BY created_at ASC
    """

    return query_bigquery(query, billing_project, billing_project)


@task
def cluster_alerts_by_location(
    alerts_df: pd.DataFrame,
    radius_meters: int = CORAlertAggregatorConstants.RADIUS_METERS.value,
) -> List[AlertCluster]:
    """
    Agrupa alertas por tipo e localizacao usando DBSCAN espacial.

    Args:
        alerts_df: DataFrame com alertas pendentes
        radius_meters: Raio de agregacao em metros

    Returns:
        Lista de clusters de alertas
    """
    if alerts_df.empty:
        log("Nenhum alerta pendente para clusterizar")
        return []

    billing_project = CORAlertAggregatorConstants.BILLING_PROJECT_ID.value

    # Build the STRUCT array for the query
    # Só incluir campos necessários para clustering (alert_type, lat/lng, alert_id)
    struct_parts = []
    for _, row in alerts_df.iterrows():
        alert_id = str(row.alert_id).replace("'", "''")
        alert_type = str(row.alert_type).replace("'", "''")

        # Formatar created_at
        if isinstance(row.created_at, str):
            created_at_str = row.created_at
        else:
            created_at_str = row.created_at.strftime('%Y-%m-%d %H:%M:%S')

        lat = float(row.latitude)
        lng = float(row.longitude)

        # STRUCT simplificado - só o necessário para clustering
        struct = f"STRUCT('{alert_id}' AS alert_id, '{alert_type}' AS alert_type, {lat} AS latitude, {lng} AS longitude, DATETIME('{created_at_str}') AS created_at)"
        struct_parts.append(struct)

    structs_str = ", ".join(struct_parts)

    query = f"""
    WITH alerts AS (
        SELECT *
        FROM UNNEST([
            {structs_str}
        ])
    ),

    clustered AS (
        SELECT
            *,
            ST_CLUSTERDBSCAN(
                ST_GEOGPOINT(longitude, latitude),
                {radius_meters},
                1
            ) OVER (PARTITION BY alert_type) as cluster_id
        FROM alerts
    )

    SELECT
        alert_type,
        cluster_id,
        ARRAY_AGG(alert_id) as alert_ids,
        COUNT(*) as alert_count,
        MIN(created_at) as oldest_alert,
        AVG(latitude) as centroid_lat,
        AVG(longitude) as centroid_lng
    FROM clustered
    WHERE cluster_id IS NOT NULL
    GROUP BY alert_type, cluster_id
    ORDER BY alert_type, oldest_alert
    """

    result_df = query_bigquery(query, billing_project, billing_project)

    clusters = []
    for _, row in result_df.iterrows():
        alert_ids = row["alert_ids"]

        # Buscar metadados do DataFrame original usando alert_ids
        cluster_alerts = alerts_df[alerts_df["alert_id"].isin(alert_ids)]

        addresses = cluster_alerts["address"].tolist()
        descriptions = cluster_alerts["description"].tolist()
        severities = cluster_alerts["severity"].tolist()

        # Determina severidade maxima do cluster
        max_severity = "critica" if "critica" in severities else "alta"

        clusters.append(
            AlertCluster(
                cluster_id=row["cluster_id"],
                alert_type=row["alert_type"],
                alert_ids=alert_ids,
                alert_count=row["alert_count"],
                oldest_alert=row["oldest_alert"],
                centroid_lat=row["centroid_lat"],
                centroid_lng=row["centroid_lng"],
                addresses=addresses,
                descriptions=descriptions,
                severity=max_severity,
            )
        )

    log(f"Encontrados {len(clusters)} clusters de alertas")
    return clusters


@task
def should_send_cluster(
    cluster: AlertCluster,
    immediate_threshold: int = CORAlertAggregatorConstants.IMMEDIATE_THRESHOLD.value,
    time_window_minutes: int = CORAlertAggregatorConstants.TIME_WINDOW_MINUTES.value,
) -> Tuple[bool, str]:
    """
    Determina se um cluster deve ser enviado para COR API.

    Regras (SEMPRE espera 7 min OU 5 relatos, o que vier primeiro):
    - 5+ alertas: envia imediatamente (limite atingido)
    - 1-4 alertas + janela expirou: envia (agregado ou nao)
    - 1-4 alertas + janela ativa: aguarda

    Args:
        cluster: Cluster de alertas
        immediate_threshold: Limite para disparo imediato (default: 5)
        time_window_minutes: Janela de tempo em minutos (default: 7)

    Returns:
        (should_send, reason)
    """
    # Usa timezone de Sao Paulo para consistencia com BigQuery
    # (CURRENT_DATETIME('America/Sao_Paulo'))
    tz_saopaulo = ZoneInfo("America/Sao_Paulo")
    now = datetime.now(tz_saopaulo)

    # Handle timezone-aware comparison
    oldest_alert = cluster.oldest_alert
    if oldest_alert.tzinfo is None:
        # BigQuery retorna em America/Sao_Paulo quando nao especificado
        oldest_alert = oldest_alert.replace(tzinfo=tz_saopaulo)

    oldest_age_minutes = (now - oldest_alert).total_seconds() / 60

    # Regra 1: 5+ alertas - envia imediatamente (limite atingido)
    if cluster.alert_count >= immediate_threshold:
        return (
            True,
            f"Limite atingido ({cluster.alert_count}/{immediate_threshold}) - enviando imediatamente",
        )

    # Regra 2: 1-4 alertas - verifica se janela de 7 min expirou
    if oldest_age_minutes >= time_window_minutes:
        return (
            True,
            f"Janela expirada ({oldest_age_minutes:.1f} min) - enviando {cluster.alert_count} alerta(s)",
        )

    # Regra 3: 1-4 alertas + janela ativa - aguarda
    remaining = time_window_minutes - oldest_age_minutes
    return (
        False,
        f"Aguardando: {cluster.alert_count} alerta(s), {remaining:.1f} min restantes para janela expirar",
    )


@task
def submit_cluster_to_cor_api(
    cluster: AlertCluster,
    environment: str,
) -> Dict[str, Any]:
    """
    Envia cluster agregado para COR OnCall API.

    Args:
        cluster: Cluster de alertas a enviar
        environment: staging ou prod

    Returns:
        Resultado da submissao
    """
    from pipelines.rj_iplanrio__cor_alerts_aggregator.utils.cor_api import (
        COROnCallClient,
    )

    # Gera ID do grupo de agregacao
    aggregation_group_id = str(uuid.uuid4())

    # Monta endereco representativo (primeiro ou mais frequente)
    representative_address = cluster.addresses[0]

    try:
        client = COROnCallClient(environment=environment)
        result = client.submit_aggregated_alert(
            aggregation_group_id=aggregation_group_id,
            alert_type=cluster.alert_type,
            severity=cluster.severity,
            descriptions=cluster.descriptions,
            addresses=cluster.addresses,
            address=representative_address,
            latitude=cluster.centroid_lat,
            longitude=cluster.centroid_lng,
            alert_count=cluster.alert_count,
            alert_ids=cluster.alert_ids,
        )

        log(f"Cluster {cluster.cluster_id} enviado para COR: {result}")
        return {
            "success": True,
            "aggregation_group_id": aggregation_group_id,
            "alert_ids": cluster.alert_ids,
            "result": result,
        }

    except Exception as e:
        log(f"Erro ao enviar cluster {cluster.cluster_id}: {e}", level="error")
        return {
            "success": False,
            "aggregation_group_id": aggregation_group_id,
            "alert_ids": cluster.alert_ids,
            "error": str(e),
        }


@task
def mark_alerts_as_sent(
    alert_ids: List[str],
    aggregation_group_id: str,
    environment: str,
) -> int:
    """
    Marca alertas como enviados no BigQuery.

    Args:
        alert_ids: Lista de IDs de alertas
        aggregation_group_id: ID do grupo de agregacao
        environment: staging ou prod

    Returns:
        Numero de alertas atualizados
    """
    # Valida environment contra whitelist (previne SQL injection)
    env_validated = validate_environment(environment)

    billing_project = CORAlertAggregatorConstants.BILLING_PROJECT_ID.value
    dataset = "brutos_eai_logs_staging"  # Hardcoded por enquanto
    table = CORAlertAggregatorConstants.QUEUE_TABLE_ID.value

    log(f"Atualizando alertas em {billing_project}.{dataset}.{table}")

    alert_ids_str = "', '".join(alert_ids)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    query = f"""
    UPDATE `{billing_project}.{dataset}.{table}`
    SET
        status = 'sent',
        aggregation_group_id = '{aggregation_group_id}',
        sent_at = DATETIME('{now}')
    WHERE alert_id IN ('{alert_ids_str}')
        AND environment = '{env_validated}'
        AND status = 'pending'
    """

    return execute_bigquery_dml(query, billing_project, billing_project)

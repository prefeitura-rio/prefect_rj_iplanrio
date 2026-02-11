# -*- coding: utf-8 -*-
"""
Tasks para pipeline de agregacao de alertas COR

Inclui: fetch, clustering espacial, decisao de envio, submissao API
"""

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal, Tuple
from zoneinfo import ZoneInfo
from time import sleep

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
    dataset = "brutos_eai_logs_staging"  # Reads precisam do sufixo _staging explicito
    queue_table = CORAlertAggregatorConstants.QUEUE_TABLE_ID.value
    sent_table = CORAlertAggregatorConstants.SENT_TABLE_ID.value

    log(f"Buscando alertas em {billing_project}.{dataset}.{queue_table}")

    # Busca alertas pendentes com coordenadas validas
    # Filtra alertas ja enviados via tabela de controle (cor_alerts_sent)
    # Expande janela um pouco para incluir alertas no limite
    query = f"""
    SELECT
        q.alert_id,
        q.user_id,
        q.alert_type,
        q.severity,
        q.description,
        q.address,
        q.latitude,
        q.longitude,
        PARSE_DATETIME('%Y-%m-%d %H:%M:%S', q.created_at) as created_at,
        q.environment
    FROM `{billing_project}.{dataset}.{queue_table}` q
    LEFT JOIN `{billing_project}.{dataset}.{sent_table}` s
        ON q.alert_id = s.alert_id
        AND s.environment = '{env_validated}'
    WHERE s.alert_id IS NULL
        AND q.environment = '{env_validated}'
        AND q.latitude IS NOT NULL
        AND q.longitude IS NOT NULL
        AND PARSE_DATETIME('%Y-%m-%d %H:%M:%S', q.created_at) >= DATETIME_SUB(
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
    representative_address = cluster.addresses[0] if cluster.addresses else ""

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
def build_cluster_dataframe(
    cluster: AlertCluster,
    environment: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Constroi DataFrame com dados do cluster para upload ao BigQuery.

    Args:
        cluster: Cluster de alertas a enviar
        environment: staging ou prod

    Returns:
        Tupla (DataFrame com linha do cluster, metadata com aggregation_group_id e alert_ids)
    """
    env_validated = validate_environment(environment)

    aggregation_group_id = str(uuid.uuid4())

    alert_type_mapping = CORAlertAggregatorConstants.ALERT_TYPE_MAPPING.value
    severity_priority_mapping = CORAlertAggregatorConstants.SEVERITY_PRIORITY_MAPPING.value

    type_code = alert_type_mapping.get(cluster.alert_type, cluster.alert_type.upper())
    priority = severity_priority_mapping.get(cluster.severity, "02")

    representative_address = cluster.addresses[0] if cluster.addresses else ""

    # Construir AgencyEventTypeCode com relatos + enderecos
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

    now_sp = datetime.now(ZoneInfo("America/Sao_Paulo"))

    row = {
        "aggregation_group_id": aggregation_group_id,
        "event_id": str(uuid.uuid4()),
        "location": representative_address,
        "priority": priority,
        "agency_event_type_code": agency_event_type[:500],
        "created_date": now_sp.strftime("%Y-%m-%d %H:%M:%S"),
        "latitude": cluster.centroid_lat,
        "longitude": cluster.centroid_lng,
        "alert_ids": ",".join(cluster.alert_ids),
        "alert_count": cluster.alert_count,
        "alert_type": cluster.alert_type,
        "severity": cluster.severity,
        "environment": env_validated,
        "data_particao": now_sp.strftime("%Y-%m-%d"),
    }

    df = pd.DataFrame([row])

    metadata = {
        "aggregation_group_id": aggregation_group_id,
        "alert_ids": cluster.alert_ids,
    }

    log(
        f"Cluster {cluster.cluster_id} preparado para BigQuery: "
        f"{cluster.alert_count} alertas, aggregation_group_id={aggregation_group_id}"
    )

    return df, metadata


@task
def create_date_partitions(
    dataframe: pd.DataFrame,
    partition_column: str = "data_particao",
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder: str = "./data/",
) -> str:
    """
    Cria particoes por data e salva em disco.

    Args:
        dataframe: DataFrame com dados a particionar
        partition_column: Coluna de particao
        file_format: Formato do arquivo (csv ou parquet)
        root_folder: Diretorio raiz para salvar

    Returns:
        Caminho do diretorio raiz
    """
    if dataframe.empty:
        log("DataFrame vazio, nenhuma particao sera criada.")
        return root_folder

    df = dataframe.copy()

    if partition_column not in df.columns:
        df["data_particao"] = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime(
            "%Y-%m-%d"
        )
    else:
        df["data_particao"] = pd.to_datetime(
            df[partition_column], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["data_particao"])

    for date in df["data_particao"].unique():
        partition_df = df[df["data_particao"] == date].drop(columns=["data_particao"])

        partition_folder = os.path.join(
            root_folder,
            f"ano_particao={date[:4]}/mes_particao={date[5:7]}/data_particao={date}",
        )
        os.makedirs(partition_folder, exist_ok=True)

        file_path = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")

        if file_format == "csv":
            partition_df.to_csv(file_path, index=False)
        elif file_format == "parquet":
            partition_df.to_parquet(file_path, index=False)

    log(f"Arquivos salvos em {root_folder}")
    return root_folder


@task
def mark_alerts_as_sent(
    alert_ids: List[str],
    aggregation_group_id: str,
    environment: str,
) -> int:
    """
    Registra alertas como enviados na tabela de controle (cor_alerts_sent).

    Grava os alert_ids na tabela de controle via GCS upload.
    A tabela cor_alerts_queue (externa/GCS) nao suporta DML UPDATE,
    entao o controle de "ja enviado" e feito por tabela separada.

    Args:
        alert_ids: Lista de IDs de alertas
        aggregation_group_id: ID do grupo de agregacao
        environment: staging ou prod

    Returns:
        Numero de alertas registrados
    """
    from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs

    env_validated = validate_environment(environment)

    now_sp = datetime.now(ZoneInfo("America/Sao_Paulo"))
    sent_at = now_sp.strftime("%Y-%m-%d %H:%M:%S")

    rows = [
        {
            "alert_id": alert_id,
            "aggregation_group_id": aggregation_group_id,
            "sent_at": sent_at,
            "environment": env_validated,
            "data_particao": now_sp.strftime("%Y-%m-%d"),
        }
        for alert_id in alert_ids
    ]

    df = pd.DataFrame(rows)

    dataset_id = CORAlertAggregatorConstants.DATASET_ID.value
    table_id = CORAlertAggregatorConstants.SENT_TABLE_ID.value
    root_folder = f"{CORAlertAggregatorConstants.ROOT_FOLDER.value}{table_id}"

    # Salvar particoes em disco
    partition_column = CORAlertAggregatorConstants.PARTITION_COLUMN.value
    file_format = CORAlertAggregatorConstants.FILE_FORMAT.value

    df_copy = df.copy()
    date_str = now_sp.strftime("%Y-%m-%d")
    partition_folder = os.path.join(
        root_folder,
        f"ano_particao={date_str[:4]}/mes_particao={date_str[5:7]}/data_particao={date_str}",
    )
    os.makedirs(partition_folder, exist_ok=True)

    file_path = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")
    df_copy.drop(columns=[partition_column], errors="ignore").to_csv(
        file_path, index=False
    )

    # Upload para BigQuery
    create_table_and_upload_to_gcs(
        data_path=root_folder,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=CORAlertAggregatorConstants.DUMP_MODE.value,
        biglake_table=CORAlertAggregatorConstants.BIGLAKE_TABLE.value,
        source_format=file_format,
    )

    log(
        f"{len(alert_ids)} alertas registrados como enviados em {dataset_id}.{table_id} "
        f"(aggregation_group_id={aggregation_group_id})"
    )

    return len(alert_ids)

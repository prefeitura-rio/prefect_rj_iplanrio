# -*- coding: utf-8 -*-
"""
Tasks para o pipeline de relatorio mensal de sessoes Agentforce/WhatsApp
(Salesforce Data Cloud -> rj-crm-registry)
"""

import os
import urllib.parse
import uuid
from typing import Any, Dict, List, Literal, Tuple

import pandas as pd
import pendulum
import requests
from basedosdados import Base
from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__relatorio_salesforce.constants import PipelineConstants


@task
def get_first_and_last_day_of_previous_month() -> Tuple[str, str]:
    """Retorna (primeiro dia, ultimo dia) do mes anterior, como 'YYYY-MM-DD'."""
    today = pendulum.now("America/Sao_Paulo").date()
    first_day_of_current_month = today.replace(day=1)
    first_day_of_previous_month = first_day_of_current_month.subtract(months=1)
    start_date = first_day_of_previous_month
    end_date = first_day_of_current_month.subtract(days=1)
    print(f"Rodando relatorio para o periodo de {start_date} a {end_date}")
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


@task
def authenticate_salesforce() -> Tuple[str, str, str]:
    """Le SF_DC_CLIENT_ID / SF_DC_CLIENT_SECRET / SF_DC_INSTANCE_URL / SF_DC_DATASPACE
    via os.getenv (ja cadastradas no Infisical) e autentica via OAuth2 client-credentials.

    Mesmo padrao de authenticate_sfmc em rj_crm__get_history_data/tasks.py.
    Retorna (access_token, instance_url, dataspace).
    """
    client_id = os.getenv("SF_DC_CLIENT_ID", "")
    client_secret = os.getenv("SF_DC_CLIENT_SECRET", "")
    instance_url = os.getenv("SF_DC_INSTANCE_URL", "")
    dataspace = os.getenv("SF_DC_DATASPACE", "")

    missing = [
        name
        for name, val in {
            "SF_DC_CLIENT_ID": client_id,
            "SF_DC_CLIENT_SECRET": client_secret,
            "SF_DC_INSTANCE_URL": instance_url,
            "SF_DC_DATASPACE": dataspace,
        }.items()
        if not val
    ]
    if missing:
        raise ValueError(f"Variaveis de ambiente Salesforce ausentes: {missing}")

    print(f"Autenticando no Salesforce via {instance_url} (dataspace={dataspace})")
    response = requests.post(
        f"{instance_url}/services/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    response.raise_for_status()
    access_token = response.json()["access_token"]
    return access_token, instance_url, dataspace


def _build_session_sql(start_date: str, end_date_exclusive: str) -> str:
    """Monta a query SQL do Data Cloud para o intervalo [start_date, end_date_exclusive)."""
    return f"""SELECT
  sess.ssot__Id__c,
  sess.ssot__RelatedMessagingSessionId__c,
  CAST(COALESCE(SUM(CASE WHEN m.ssot__AiAgentInteractionMessageType__c = 'Input' THEN 1 ELSE 0 END), 0) AS INTEGER),
  CAST(COALESCE(SUM(CASE WHEN m.ssot__AiAgentInteractionMessageType__c = 'Output' THEN 1 ELSE 0 END), 0) AS INTEGER)
FROM ssot__AiAgentSession__dlm sess
LEFT JOIN ssot__AiAgentInteraction__dlm i
  ON i.ssot__AiAgentSessionId__c = sess.ssot__Id__c
LEFT JOIN ssot__AiAgentInteractionMessage__dlm m
  ON m.ssot__AiAgentInteractionId__c = i.ssot__Id__c
WHERE sess.ssot__RelatedMessagingSessionId__c IS NOT NULL
  AND sess.ssot__StartTimestamp__c >= '{start_date}'
  AND sess.ssot__StartTimestamp__c < '{end_date_exclusive}'
GROUP BY sess.ssot__Id__c, sess.ssot__RelatedMessagingSessionId__c
ORDER BY MAX(sess.ssot__StartTimestamp__c) DESC"""


def _query_data_cloud_sql(
    access_token: str, instance_url: str, sql: str, dataspace: str, page_size: int
) -> List[List[Any]]:
    """Executa uma query SQL no Data Cloud, paginando ate esgotar os resultados."""
    base_url = f"{instance_url}/services/data/{PipelineConstants.SF_API_VERSION.value}/ssot/query-sql"

    response = requests.post(
        f"{base_url}?dataspace={dataspace}",
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        json={"sql": sql, "rowLimit": page_size},
        timeout=120,
    )
    response.raise_for_status()
    data = response.json()

    all_rows = list(data["data"])
    total_rows = data["status"]["rowCount"]
    query_id = data["status"]["queryId"]

    offset = len(all_rows)
    while offset < total_rows:
        url = f"{base_url}/{query_id}/rows?dataspace={dataspace}&offset={offset}&rowLimit={page_size}"
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
            timeout=120,
        )
        response.raise_for_status()
        chunk = response.json()
        all_rows.extend(chunk.get("data", []))
        offset += len(chunk.get("data", []))

    return all_rows


@task
def fetch_raw_agentforce_sessions(
    access_token: str, instance_url: str, dataspace: str, start_date: str, end_date_exclusive: str
) -> pd.DataFrame:
    """Roda a query de sessoes agregadas no Data Cloud para o periodo informado."""
    effective_start_date = max(start_date, PipelineConstants.DATA_INICIO_HISTORICO.value)
    sql = _build_session_sql(effective_start_date, end_date_exclusive)

    print(f"Query Data Cloud (sessoes agregadas) de {effective_start_date} ate {end_date_exclusive}...")
    rows = _query_data_cloud_sql(
        access_token,
        instance_url,
        sql,
        dataspace=dataspace,
        page_size=PipelineConstants.DATACLOUD_PAGE_SIZE.value,
    )
    df = pd.DataFrame(rows, columns=["ai_agent_session_id", "messaging_session_id", "msg_usuario", "msg_bot"])
    print(f"Data Cloud total: {len(df)} sessoes")
    return df


def _fetch_messaging_sessions(
    access_token: str, instance_url: str, session_ids: List[str], batch_size: int
) -> Dict[str, Dict[str, str]]:
    """
    Enriquece uma lista de Id de MessagingSession via SOQL, retornando
    {SessionSF: {MS_Name, StartTime, EndTime, Telefone, NomeCidadao, Canal_WhatsApp}}.
    """
    unique_ids = list(
        dict.fromkeys(
            s for s in session_ids if s and s.strip() and len(s) in (15, 18) and s.isalnum() and s.startswith("0Mw")
        )
    )
    sessions: Dict[str, Dict[str, str]] = {}
    batches = [unique_ids[i : i + batch_size] for i in range(0, len(unique_ids), batch_size)]

    for batch in batches:
        expr = ",".join(f"'{s}'" for s in batch)
        soql = (
            "SELECT Id, Name, StartTime, EndTime, MessagingEndUser.Name, "
            "MessagingEndUser.MessagingPlatformKey, MessagingChannel.MasterLabel "
            f"FROM MessagingSession WHERE Id IN ({expr})"
        )
        url = (
            f"{instance_url}/services/data/{PipelineConstants.SF_API_VERSION.value}"
            f"/query?q={urllib.parse.quote(soql)}"
        )
        response = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=60)
        response.raise_for_status()

        for record in response.json().get("records", []):
            end_user = record.get("MessagingEndUser") or {}
            channel = record.get("MessagingChannel") or {}
            sessions[record["Id"]] = {
                "MS_Name": record.get("Name", ""),
                "StartTime": record.get("StartTime") or "",
                "EndTime": record.get("EndTime") or "",
                "Telefone": end_user.get("MessagingPlatformKey", ""),
                "NomeCidadao": end_user.get("Name", ""),
                "Canal_WhatsApp": channel.get("MasterLabel", ""),
            }

    return sessions


@task
def enrich_sessions_with_messaging_data(
    access_token: str, instance_url: str, df_sessions: pd.DataFrame
) -> pd.DataFrame:
    """Enriquece as sessoes com dados de MessagingSession (inicio/fim, contato, canal)."""
    session_ids = df_sessions["messaging_session_id"].dropna().tolist()
    print(f"Buscando dados de contato ({len(session_ids)} sessions)...")
    sessions = _fetch_messaging_sessions(
        access_token, instance_url, session_ids, batch_size=PipelineConstants.SOQL_BATCH_SIZE.value
    )
    print(f"Telefones encontrados: {len(sessions)}/{len(set(session_ids))} sessions")

    def _lookup(msid: str, field: str, default: str = "") -> str:
        return sessions.get(msid, {}).get(field, default)

    df = df_sessions.copy()
    df["ms_name"] = df["messaging_session_id"].map(lambda x: _lookup(x, "MS_Name"))
    df["inicio_sessao"] = df["messaging_session_id"].map(lambda x: _lookup(x, "StartTime"))
    df["fim_sessao"] = df["messaging_session_id"].map(lambda x: _lookup(x, "EndTime"))
    df["nome_cidadao"] = df["messaging_session_id"].map(lambda x: _lookup(x, "NomeCidadao"))
    df["telefone"] = df["messaging_session_id"].map(lambda x: _lookup(x, "Telefone"))
    df["canal_whatsapp"] = df["messaging_session_id"].map(lambda x: _lookup(x, "Canal_WhatsApp"))

    inicio_dt = pd.to_datetime(df["inicio_sessao"], errors="coerce", utc=True)
    fim_dt = pd.to_datetime(df["fim_sessao"], errors="coerce", utc=True)
    df["duracao_segundos"] = (fim_dt - inicio_dt).dt.total_seconds()

    antes = len(df)
    df = df[inicio_dt.notna()].copy()
    if len(df) < antes:
        print(
            f"Descartando {antes - len(df)} sessao(oes) sem InicioSessao "
            "(enriquecimento SOQL sem correspondencia)."
        )

    return df[
        [
            "ai_agent_session_id",
            "messaging_session_id",
            "ms_name",
            "inicio_sessao",
            "fim_sessao",
            "msg_usuario",
            "msg_bot",
            "duracao_segundos",
            "nome_cidadao",
            "telefone",
            "canal_whatsapp",
        ]
    ]


def _identify_new_session(group: pd.DataFrame, threshold_seconds: int, start_col: str) -> pd.Series:
    """Marca True no inicio de cada sessao 'movel': quando o gap para a sessao
    bruta anterior do mesmo telefone excede threshold_seconds.

    Adaptado de identificar_sessoes_cliente (rj_crm__relatorio_cvl/tasks.py),
    parametrizado pelo threshold em vez do 24*3600 fixo.
    """
    if group.empty:
        return pd.Series([], dtype=bool)

    nova_sessao = []
    ultimo_inicio = group.iloc[0][start_col]
    for _, row in group.iterrows():
        if (row[start_col] - ultimo_inicio).total_seconds() > threshold_seconds:
            nova_sessao.append(True)
            ultimo_inicio = row[start_col]
        else:
            nova_sessao.append(False)
    nova_sessao[0] = True
    return pd.Series(nova_sessao, index=group.index)


@task
def merge_agentforce_sessions(
    df_raw: pd.DataFrame,
    threshold_seconds: int,
    phone_col: str = "telefone",
    start_col: str = "inicio_sessao",
    end_col: str = "fim_sessao",
) -> pd.DataFrame:
    """Mescla sessoes brutas consecutivas do mesmo telefone cujo gap entre
    inicios seja menor que threshold_seconds, em uma unica sessao 'logica'.

    Reutilizavel para as tabelas de 24h (threshold_seconds=86400) e de 2h
    (threshold_seconds=7200) — e o unico ponto de logica que muda entre elas.
    """
    df = df_raw.copy()
    df[start_col] = pd.to_datetime(df[start_col], errors="coerce", utc=True)
    df[end_col] = pd.to_datetime(df[end_col], errors="coerce", utc=True)
    df = df.dropna(subset=[start_col])
    df = df.sort_values([phone_col, start_col])

    df["nova_sessao"] = df.groupby(phone_col, group_keys=False).apply(
        lambda g: _identify_new_session(g, threshold_seconds, start_col)
    )
    df["nova_sessao"] = df["nova_sessao"].fillna(False).astype(int)
    df["id_sessao_merged"] = df[phone_col].astype(str) + "_" + df.groupby(phone_col)["nova_sessao"].cumsum().astype(
        str
    )

    merged = (
        df.groupby("id_sessao_merged")
        .agg(
            telefone=(phone_col, "first"),
            inicio_sessao=(start_col, "min"),
            fim_sessao=(end_col, "max"),
            qtd_sessoes_brutas=(phone_col, "size"),
            msg_usuario=("msg_usuario", "sum"),
            msg_bot=("msg_bot", "sum"),
            nome_cidadao=("nome_cidadao", "first"),
            canal_whatsapp=("canal_whatsapp", "first"),
            ai_agent_session_ids=("ai_agent_session_id", lambda s: ",".join(s.astype(str))),
        )
        .reset_index()
    )
    merged["duracao_segundos"] = (merged["fim_sessao"] - merged["inicio_sessao"]).dt.total_seconds()

    print(
        f"Mesclagem (threshold={threshold_seconds}s): {len(df_raw)} sessoes brutas -> "
        f"{len(merged)} sessoes mescladas"
    )

    return merged[
        [
            "telefone",
            "id_sessao_merged",
            "inicio_sessao",
            "fim_sessao",
            "duracao_segundos",
            "qtd_sessoes_brutas",
            "msg_usuario",
            "msg_bot",
            "nome_cidadao",
            "canal_whatsapp",
            "ai_agent_session_ids",
        ]
    ]


@task
def create_date_partitions(
    dataframe: pd.DataFrame,
    partition_column: str,
    file_format: Literal["csv", "parquet"] = "csv",
    root_folder: str = "./data/",
) -> str:
    """Particiona um DataFrame em pastas Hive (ano/mes/dia) prontas para
    create_table_and_upload_to_gcs_task. Copiado do padrao ja usado em
    rj_crm__api_wetalkie/utils/tasks.py.
    """
    dataframe = dataframe.copy()
    dataframe[partition_column] = pd.to_datetime(dataframe[partition_column], errors="coerce")
    dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")
    if dataframe["data_particao"].isnull().any():
        raise ValueError("Algumas datas na coluna de particionamento nao puderam ser interpretadas.")

    dates = dataframe["data_particao"].unique()
    for _date in dates:
        partition_folder = os.path.join(
            root_folder,
            f"ano_particao={_date[:4]}/mes_particao={_date[5:7]}/data_particao={_date}",
        )
        os.makedirs(partition_folder, exist_ok=True)

        subset = dataframe[dataframe["data_particao"] == _date].drop(columns=["data_particao"])
        file_path = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")
        if file_format == "csv":
            subset.to_csv(file_path, index=False)
        elif file_format == "parquet":
            subset.to_parquet(file_path, index=False)

    log(f"Arquivos salvos em {root_folder}")
    return root_folder


@task
def check_month_not_loaded(
    dataset_id: str, table_id: str, report_month: str, fail_on_duplicate: bool = False
) -> None:
    """Checagem leve (nao bloqueante por padrao): avisa se ja existem linhas
    para report_month (YYYY-MM) na tabela de destino, ja que
    create_table_and_upload_to_gcs_task nao tem MERGE/dedup — reexecutar o
    flow com dump_mode=append duplicaria o mes.
    """
    project_id = PipelineConstants.BILLING_PROJECT_ID.value
    client = bigquery.Client(
        credentials=Base(bucket_name=project_id)._load_credentials(mode="prod"),
        project=project_id,
    )
    query = f"""
        SELECT COUNT(1) AS n
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE data_particao BETWEEN DATE('{report_month}-01') AND LAST_DAY(DATE('{report_month}-01'))
    """
    try:
        n = list(client.query(query).result())[0]["n"]
    except Exception as exc:  # tabela ainda nao existe na primeira carga
        print(f"Preflight: {dataset_id}.{table_id} ainda nao consultavel ({exc}). Assumindo primeira carga.")
        return

    if n > 0:
        message = (
            f"{n} linha(s) ja existem para {report_month} em {dataset_id}.{table_id}. "
            "Reexecutar com dump_mode=append vai DUPLICAR os dados deste mes "
            "(tabela BigLake, sem suporte a MERGE/DELETE)."
        )
        if fail_on_duplicate:
            raise ValueError(message)
        print(f"AVISO: {message}")

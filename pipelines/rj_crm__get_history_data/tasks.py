# -*- coding: utf-8 -*-
"""
Tasks para pipeline CRM Get History Data (SFMC)
Extrai dados de Data Extensions com sufixo 'historico' do Salesforce Marketing Cloud
"""

import json
import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from google.cloud import bigquery
from iplanrio.pipelines_utils.logging import log
from prefect import task

from pipelines.rj_crm__get_history_data.constants import GetHistoryDataConstants
from pipelines.rj_crm__get_history_data.utils.bigquery import (
    ensure_historico_tables,
    get_bq_client,
    historico_table_schema,
    truncate_and_load_tmp_table,
)
from pipelines.rj_crm__get_history_data.utils.sfmc import (
    build_field_types_soap_envelope,
    build_soap_envelope,
    normalize_field_name,
    parse_field_types_soap_response,
    parse_soap_response,
)


@task
def authenticate_sfmc() -> str:
    """
    Autentica com Salesforce Marketing Cloud via OAuth2 e retorna o access token.
    POST {SFMC_AUTH_URI}/v2/token
    """
    auth_uri = os.getenv("API_SFMC_AUTH_URL", "")
    client_id = os.getenv("API_SFMC_CLIENT_ID", "")
    client_secret = os.getenv("API_SFMC_CLIENT_SECRET", "")

    missing = [
        name for name, val in {
            "API_SFMC_AUTH_URL": auth_uri,
            "API_SFMC_CLIENT_ID": client_id,
            "API_SFMC_CLIENT_SECRET": client_secret,
        }.items()
        if not val
    ]
    if missing:
        raise ValueError(f"Variáveis de ambiente SFMC ausentes: {missing}")

    url = f"{auth_uri.rstrip('/')}/v2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    log(f"Autenticando no SFMC via {url}")
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()

    data = response.json()
    access_token = data.get("access_token")
    if not access_token:
        raise ValueError(f"access_token não encontrado na resposta SFMC: {list(data.keys())}")

    expires_in = data.get("expires_in", GetHistoryDataConstants.TOKEN_CACHE_SECONDS.value)
    log(f"Autenticação SFMC bem-sucedida. Token válido por {expires_in}s")
    return access_token


@task
def list_data_extensions_historico(access_token: str, soap_uri: str) -> List[Dict[str, str]]:
    """
    Lista todas as Data Extensions com nome terminando em 'historico' via SOAP API.
    Trata paginação SOAP via status MoreDataAvailable.

    Args:
        access_token: Token OAuth2 do SFMC
        soap_uri: URI base do endpoint SOAP (ex: https://mcXXX.soap.marketingcloudapis.com)

    Returns:
        Lista de dicts com {name, external_key, object_id}
    """
    endpoint = f"{soap_uri.rstrip('/')}/Service.asmx"
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "Retrieve",
    }

    all_des: List[Dict[str, str]] = []
    continue_request_id: Optional[str] = None
    page = 0

    while True:
        log(f"Buscando DataExtensions - página SOAP {page}")
        envelope = build_soap_envelope(access_token, continue_request_id)
        response = requests.post(endpoint, data=envelope, headers=headers, timeout=60)
        response.raise_for_status()

        overall_status, request_id, results = parse_soap_response(response.text)
        log(f"Página SOAP {page}: status={overall_status}, DEs retornadas={len(results)}")
        all_des.extend(results)

        if overall_status == "MoreDataAvailable":
            continue_request_id = request_id
            page += 1
        elif overall_status in ("OK", ""):
            break
        else:
            raise RuntimeError(f"SOAP API retornou status inesperado: {overall_status}")

    suffix = GetHistoryDataConstants.DE_SUFFIX.value
    historico_des = [de for de in all_des if de["name"].lower().endswith(suffix)]

    log(f"Total de DEs encontradas: {len(all_des)}, com sufixo '{suffix}': {len(historico_des)}")
    for de in historico_des:
        log(f"  -> {de['name']} (key={de['external_key']}, id={de['object_id']})")

    return historico_des


@task
def fetch_data_extension_data(
    access_token: str,
    external_key: str,
    de_name: str,
    rest_uri: str,
) -> List[Dict[str, Any]]:
    """
    Extrai todos os registros de uma Data Extension via REST API com paginação.
    GET {rest_uri}/data/v1/customobjectdata/key/{external_key}/rowset?$page={page}

    Args:
        access_token: Token OAuth2 do SFMC
        external_key: CustomerKey da DE
        de_name: Nome da DE (para logging)
        rest_uri: URI base do endpoint REST (ex: https://mcXXX.rest.marketingcloudapis.com)

    Returns:
        Lista de dicts com todos os registros da DE
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    base_url = f"{rest_uri.rstrip('/')}/data/v1/customobjectdata/key/{external_key}/rowset"

    all_rows: List[Dict[str, Any]] = []
    page = 1

    log(f"Iniciando extração de '{de_name}' (key={external_key})")

    while True:
        response = requests.get(base_url, headers=headers, params={"$page": page}, timeout=60)
        response.raise_for_status()

        data = response.json()
        page_count = data.get("pageCount", 1)
        items = data.get("items", [])

        log(f"  [{de_name}] Página {page}/{page_count}: {len(items)} registros")
        all_rows.extend(items)

        if page >= page_count:
            break
        page += 1

    log(f"  [{de_name}] Extração concluída: {len(all_rows)} registros no total")
    return all_rows


@task
def fetch_de_field_types(
    access_token: str,
    customer_key: str,
    de_name: str,
    soap_uri: str,
) -> List[Dict[str, str]]:
    """
    Retorna o schema (nome + tipo + pk) de uma Data Extension via SOAP API.
    Usa ObjectType=DataExtensionField filtrado por DataExtension.CustomerKey.
    """
    endpoint = f"{soap_uri.rstrip('/')}/Service.asmx"
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "Retrieve",
    }

    envelope = build_field_types_soap_envelope(access_token, customer_key)

    response = requests.post(endpoint, data=envelope, headers=headers, timeout=60)
    response.raise_for_status()

    fields = parse_field_types_soap_response(response.text)

    log(f"  [{de_name}] Schema: {len(fields)} campos | "
        f"phone={next((f['name'] for f in fields if f['type'].lower() == 'phone'), None)} | "
        f"pk={next((f['name'] for f in fields if f['is_pk'].lower() == 'true'), None)}")
    return fields


@task
def build_historico_dataframe(results: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Constrói, loga e retorna um DataFrame com uma linha por registro de cada DE.

    Colunas:
        de_nome      - nome da Data Extension
        telefone     - valor da coluna de tipo Phone
        id_de        - valor da coluna que é Primary Key
        entrada_data - valor do campo cujo nome contém "data de entrada"
        dados_json   - todos os campos do registro em JSON
    """
    rows = []
    for result in results:
        if result.get("status") != "success":
            continue

        de_name = result["de_name"]
        dados = result.get("dados", [])
        field_types = result.get("tipos", [])

        phone_col = next((f["name"] for f in field_types if f["type"].lower() == "phone"), None)
        pk_col = next((f["name"] for f in field_types if f["is_pk"].lower() == "true"), None)
        entrada_data_col = next(
            (f["name"] for f in field_types if "data de entrada" in normalize_field_name(f["name"])), None
        )
        if entrada_data_col is None:
            date_cols = [f["name"] for f in field_types if f["type"].lower() == "date"]
            if len(date_cols) == 1:
                entrada_data_col = date_cols[0]

        for item in dados:
            # O REST API do SFMC retorna cada registro como {"keys": {...}, "values": {...}};
            # campos que não são PK só existem em "values", então é preciso mesclar os dois.
            if isinstance(item, dict) and ("keys" in item or "values" in item):
                record = {**item.get("keys", {}), **item.get("values", {})}
            else:
                record = item
            rows.append({
                "de_nome": de_name,
                "telefone": record.get(phone_col) if phone_col else None,
                "id_de": record.get(pk_col) if pk_col else None,
                "entrada_data": record.get(entrada_data_col) if entrada_data_col else None,
                "dados_json": json.dumps(record, ensure_ascii=False),
            })

    df = pd.DataFrame(rows, columns=["de_nome", "telefone", "id_de", "entrada_data", "dados_json"])

    log("=" * 60)
    log("DATAFRAME HISTORICO SFMC")
    log(f"Shape: {df.shape[0]} linhas x {df.shape[1]} colunas")
    log("=" * 60)
    log(f"\n{df.to_string()}")

    return df


@task
def filter_recent_and_partition(df: pd.DataFrame, window_days: int) -> pd.DataFrame:
    """
    Filtra o DataFrame consolidado para manter apenas registros com entrada_data
    dentro dos últimos `window_days` dias. Linhas sem entrada_data parseável
    (DEs sem coluna de data) são sempre mantidas, pois não é possível avaliar
    sua idade.

    Adiciona data_particao (DATE): a entrada_data parseada, ou a data de
    execução para linhas sem entrada_data. Usada para o particionamento
    mensal das tabelas no BigQuery.
    """
    df = df.copy()
    entrada_parsed = pd.to_datetime(df["entrada_data"], errors="coerce", utc=True).dt.tz_localize(None)
    cutoff = datetime.now() - timedelta(days=window_days)

    keep_mask = entrada_parsed.isna() | (entrada_parsed >= cutoff)
    total_antes = len(df)
    df = df[keep_mask].copy()
    entrada_parsed = entrada_parsed[keep_mask]

    log(
        f"Janela de {window_days} dias: {total_antes} -> {len(df)} linha(s) "
        f"({total_antes - len(df)} descartada(s) por entrada_data antiga)."
    )

    df["data_particao"] = entrada_parsed.dt.date
    df["data_particao"] = df["data_particao"].fillna(date.today())

    key_columns = list(GetHistoryDataConstants.MERGE_KEY_COLUMNS.value)
    df = df.drop_duplicates(subset=key_columns).reset_index(drop=True)

    return df


@task
def load_recent_data_to_tmp_table(
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    tmp_table_id: str,
    final_table_id: str,
) -> bigquery.Client:
    """
    Garante que as tabelas nativas existam, trunca a tabela temporária e
    carrega nela os dados recentes (já filtrados pela janela de dias).
    """
    client = get_bq_client(project_id=project_id)
    ensure_historico_tables(
        client=client,
        project_id=project_id,
        dataset_id=dataset_id,
        tmp_table_id=tmp_table_id,
        final_table_id=final_table_id,
    )
    truncate_and_load_tmp_table(
        client=client,
        df=df,
        project_id=project_id,
        dataset_id=dataset_id,
        tmp_table_id=tmp_table_id,
    )
    return client


@task
def merge_tmp_into_historico(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    tmp_table_id: str,
    final_table_id: str,
) -> None:
    """
    Faz o MERGE da tabela temporária na tabela final histórica: atualiza
    registros já existentes (mesma chave de dedup) e insere os novos. Nunca
    apaga nada da tabela final.
    """
    key_columns = list(GetHistoryDataConstants.MERGE_KEY_COLUMNS.value)
    all_columns = [field.name for field in historico_table_schema()]
    update_columns = [col for col in all_columns if col not in key_columns]

    target = f"`{project_id}.{dataset_id}.{final_table_id}`"
    source = f"`{project_id}.{dataset_id}.{tmp_table_id}`"
    on_clause = " AND ".join(f"T.{col} IS NOT DISTINCT FROM S.{col}" for col in key_columns)
    update_clause = ", ".join(f"{col} = S.{col}" for col in update_columns)
    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join(f"S.{col}" for col in all_columns)

    query = f"""
        MERGE {target} AS T
        USING {source} AS S
        ON {on_clause}
        WHEN MATCHED THEN
          UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
    """

    log(f"Executando MERGE em {project_id}.{dataset_id}.{final_table_id}:\n{query}")
    job = client.query(query)
    job.result()
    log(f"MERGE concluído: {job.num_dml_affected_rows} linha(s) afetada(s).")


@task
def process_historico_extraction(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Agrega resultados da extração e loga resumo estruturado.

    Args:
        results: Lista de dicts com resultado por DE
                 (campos: de_name, external_key, status, total_rows, sample, error)

    Returns:
        Dict com metadados: des_sucesso, des_erro, total_registros, detalhes
    """
    des_sucesso = [r for r in results if r.get("status") == "success"]
    des_erro = [r for r in results if r.get("status") == "error"]
    total_registros = sum(r.get("total_rows", 0) for r in des_sucesso)

    log("=" * 60)
    log("RESUMO DA EXTRACAO DE HISTORICOS SFMC")
    log("=" * 60)
    log(f"DEs processadas com sucesso : {len(des_sucesso)}")
    log(f"DEs com erro                : {len(des_erro)}")
    log(f"Total de registros extraidos: {total_registros}")

    if des_sucesso:
        log("\nAmostras (1 linha por DE):")
        for r in des_sucesso:
            log(f"  [{r['de_name']}] total={r['total_rows']} | amostra={r.get('sample')}")

    if des_erro:
        log("\nErros detalhados:")
        for r in des_erro:
            log(f"  [{r['de_name']}] ERRO: {r.get('error')}", level="error")

    log("=" * 60)

    return {
        "des_sucesso": len(des_sucesso),
        "des_erro": len(des_erro),
        "total_registros": total_registros,
        "detalhes": results,
    }

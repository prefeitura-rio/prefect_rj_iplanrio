# -*- coding: utf-8 -*-
"""
Tasks for rj_crm__whitelist_whatsapp pipeline.
"""
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional

from prefect import task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.env import getenv_or_action

from pipelines.rj_crm__whitelist_whatsapp.constants import WhitelistConstants


@task
def get_whitelist_credentials() -> Dict[str, str]:
    """
    Retrieves credentials from environment variables (synced from Infisical).
    """
    log("Recuperando credenciais do ambiente (via Infisical)")
    
    return {
        "auth_url": getenv_or_action("RMI_AUTH_URL"),
        "client_id": getenv_or_action("RMI_CLIENT_ID"),
        "client_secret": getenv_or_action("RMI_CLIENT_SECRET"),
        "whitelist_url": getenv_or_action("RMI_WHITELIST_API_URL"), 
    }


@task(retries=3, retry_delay_seconds=60)
def fetch_whitelist_data(creds: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Authenticates and fetches all records from the Whitelist API using parallel requests.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # 1. Get Access Token
    log("Obtendo Access Token (API 1)")
    auth_payload = {
        "grant_type": "client_credentials",
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "scope": "profile email",
    }
    
    try:
        auth_resp = requests.post(
            creds["auth_url"], 
            data=auth_payload, 
            timeout=WhitelistConstants.TIMEOUT.value
        )
        auth_resp.raise_for_status()
        access_token = auth_resp.json().get("access_token")
    except Exception as e:
        log(f"Erro na autenticação: {e}")
        raise

    if not access_token:
        raise ValueError("Access token not found in response")

    # 2. Fetch Data with Parallelism
    log("Iniciando coleta de dados paralela (API 2 - Whitelist)")
    all_records = []
    headers = {"Authorization": f"Bearer {access_token}"}
    
    # Advanced Rate Limit Mitigation
    max_workers = 5  # Reduced from 10 to 5 to avoid 429s
    page_batch_size = 20
    current_page = 1
    finished = False

    def fetch_page(p):
        import time
        import random
        max_page_retries = 10 # Increased to 10
        base_wait = 20 # Base wait in seconds
        
        for attempt in range(max_page_retries):
            try:
                # Add a small initial jitter to avoid simultaneous burst
                if attempt == 0:
                    time.sleep(random.uniform(0, 2))
                    
                p_url = f"{creds['whitelist_url']}?page={p}&per_page={WhitelistConstants.PER_PAGE.value}"
                p_resp = requests.get(p_url, headers=headers, timeout=WhitelistConstants.TIMEOUT.value)
                
                if p_resp.status_code == 429:
                    # Exponential Backoff: 20s, 40s, 80s, etc. + Jitter
                    wait_time = (base_wait * (2 ** attempt)) + random.uniform(0, 5)
                    print(f"⚠️ [Página {p}] Rate limit (429) atingido. Aguardando {wait_time:.1f}s... (Tentativa {attempt+1}/{max_page_retries})")
                    time.sleep(wait_time)
                    continue
                
                p_resp.raise_for_status()
                return p_resp.json().get("whitelisted", [])
            except Exception as e:
                if attempt == max_page_retries - 1:
                    print(f"❌ [Página {p}] Falha definitiva após {max_page_retries} tentativas: {e}")
                    raise
                wait_time = 5 + random.uniform(0, 2)
                print(f"⚠️ [Página {p}] Erro na tentativa {attempt+1}: {e}. Tentando novamente em {wait_time:.1f}s...")
                time.sleep(wait_time)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while not finished:
            # Prepare a batch of future requests
            future_to_page = {
                executor.submit(fetch_page, current_page + i): current_page + i 
                for i in range(page_batch_size)
            }
            
            batch_has_data = False
            # Process results in order to check for empty pages
            sorted_futures = sorted(future_to_page.keys(), key=lambda f: future_to_page[f])
            
            for future in sorted_futures:
                page_num = future_to_page[future]
                try:
                    records = future.result()
                    if records:
                        all_records.extend(records)
                        batch_has_data = True
                    else:
                        # Found an empty page, this is the end
                        log(f"Fim da paginação detectado na página {page_num}")
                        finished = True
                        break
                except Exception as e:
                    log(f"Erro ao buscar página {page_num}: {e}")
                    raise

            if not batch_has_data:
                # If an entire batch returned no data (shouldn't happen before finding one empty page), stop
                finished = True
            
            current_page += page_batch_size
            log(f"Total acumulado: {len(all_records)} registros coletados.")

    return all_records


@task
def transform_whitelist_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Transforms the raw whitelist data into a DataFrame suitable for BigQuery.
    For standard runs (non-backfill).
    """
    if not raw_data:
        log("Nenhum dado para transformar")
        return pd.DataFrame()

    df = pd.DataFrame(raw_data)
    
    # Calculate week_start (Monday ISO)
    today = datetime.now()
    monday = today - pd.Timedelta(days=today.weekday())
    week_start_str = monday.strftime("%Y-%m-%d")
    
    df["week_start"] = week_start_str
    df["_prefect_extracted_at"] = datetime.now()
    df["data_particao"] = today.strftime("%Y-%m-%d")
    
    # Stringify non-partition columns
    for col in df.columns:
        if col != "data_particao":
            df[col] = df[col].astype(str)
            
    return df


@task
def generate_backfill_data(raw_data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Expands the current whitelist into historical daily snapshots.
    For each date D from min(added_at) to today, creates a partition 
    with records where added_at <= D.
    """
    if not raw_data:
        log("Nenhum dado para o backfill")
        return pd.DataFrame()

    df_base = pd.DataFrame(raw_data)
    
    # Pre-process added_at
    df_base["added_at_dt"] = pd.to_datetime(df_base["added_at"], errors="coerce")
    df_base = df_base.dropna(subset=["added_at_dt"])
    
    min_date = df_base["added_at_dt"].min().date()
    max_date = datetime.now().date()
    
    all_dates = pd.date_range(start=min_date, end=max_date, freq="D").date
    
    log(f"Iniciando backfill de {len(all_dates)} dias (de {min_date} até {max_date})")
    
    backfill_dfs = []
    
    for d in all_dates:
        # Filter records that existed on date 'd'
        df_snapshot = df_base[df_base["added_at_dt"].dt.date <= d].copy()
        
        if not df_snapshot.empty:
            # Add metadata
            df_snapshot["data_particao"] = d.strftime("%Y-%m-%d")
            # Calculate week_start for that specific date in history
            monday = d - pd.Timedelta(days=d.weekday())
            df_snapshot["week_start"] = monday.strftime("%Y-%m-%d")
            df_snapshot["_prefect_extracted_at"] = datetime.now()
            
            backfill_dfs.append(df_snapshot)

    if not backfill_dfs:
        return pd.DataFrame()

    df_final = pd.concat(backfill_dfs, ignore_index=True)
    
    # Drop temp column and stringify
    df_final = df_final.drop(columns=["added_at_dt"])
    for col in df_final.columns:
        if col != "data_particao":
            df_final[col] = df_final[col].astype(str)
    
    log(f"Backfill gerado: {len(df_final)} registros totais em {len(all_dates)} partições...")
    return df_final

# -*- coding: utf-8 -*-
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
import os

# Configurações fornecidas pelo usuário
TOKEN = "6bTVQZZQJ4YcWA5cEtgLLCnQ"
MONITOR_ID = "3839268"
START_DATE = "2025-11-17"
END_DATE = "2025-12-17"

BASE_URL_V2 = "https://uptime.betterstack.com/api/v2"
BASE_URL_V3 = "https://uptime.betterstack.com/api/v3"

def fetch_response_times(token, monitor_id, from_date, to_date):
    url = f"{BASE_URL_V2}/monitors/{monitor_id}/response-times"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"from": from_date, "to": to_date}
    
    print(f"Fetching response times from {from_date} to {to_date}...")
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    data = response.json()
    if "data" in data and "attributes" in data["data"]:
        return data["data"]["attributes"].get("regions", [])
    return []

def fetch_incidents(token, monitor_id, from_date, to_date):
    url = f"{BASE_URL_V3}/incidents"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "from": from_date,
        "to": to_date,
        "monitor_id": monitor_id,
        "per_page": 50
    }
    
    print(f"Fetching incidents from {from_date} to {to_date}...")
    all_incidents = []
    
    while url:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        incidents = data.get("data", [])
        all_incidents.extend(incidents)
        
        pagination = data.get("pagination", {})
        next_url = pagination.get("next")
        if next_url:
            url = next_url
            params = {}
        else:
            url = None
            
    return all_incidents

def transform_response_times(data, extraction_date):
    if not data:
        return pd.DataFrame()
    flattened_data = []
    for region_data in data:
        region_name = region_data.get("region")
        for measurement in region_data.get("response_times", []):
            record = measurement.copy()
            record["region"] = region_name
            record["data_particao"] = extraction_date
            flattened_data.append(record)
    return pd.DataFrame(flattened_data)

def transform_incidents(data, extraction_date):
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].astype(str)
    df["data_particao"] = extraction_date
    return df

def main():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    all_df_response = []
    all_df_incidents = []
    
    current_date = start
    while current_date <= end:
        date_str = current_date.strftime("%Y-%m-%d")
        
        # Response Times
        try:
            raw_response = fetch_response_times(TOKEN, MONITOR_ID, date_str, date_str)
            df_res = transform_response_times(raw_response, date_str)
            if not df_res.empty:
                all_df_response.append(df_res)
        except Exception as e:
            print(f"Erro ao buscar response times para {date_str}: {e}")
            
        # Incidents
        try:
            raw_incidents = fetch_incidents(TOKEN, MONITOR_ID, date_str, date_str)
            df_inc = transform_incidents(raw_incidents, date_str)
            if not df_inc.empty:
                all_df_incidents.append(df_inc)
        except Exception as e:
            print(f"Erro ao buscar incidentes para {date_str}: {e}")
            
        current_date += timedelta(days=1)
        
    # Agrega e salva CSVs
    if all_df_response:
        final_response = pd.concat(all_df_response, ignore_index=True)
        filename = "eai_gateway_response_times_historico.csv"
        final_response.to_csv(filename, index=False)
        print(f"Arquivo salvo: {filename} ({len(final_response)} linhas)")
    else:
        print("Nenhum dado de response times encontrado.")
        
    if all_df_incidents:
        final_incidents = pd.concat(all_df_incidents, ignore_index=True)
        filename = "eai_gateway_incidents_historico.csv"
        final_incidents.to_csv(filename, index=False)
        print(f"Arquivo salvo: {filename} ({len(final_incidents)} linhas)")
    else:
        print("Nenhum dado de incidentes encontrado.")

if __name__ == "__main__":
    main()
